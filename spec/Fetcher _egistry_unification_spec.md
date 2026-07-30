# [SPEC] Unify `cli.py` Import Orchestration Behind the Fetcher Registry

**Status:** Proposed
**Priority:** Medium (tech debt / maintainability)
**Created:** 2026-07-31
**Related:** `src/importer/fetchers/adapters.py`, `src/importer/cli.py`, `src/db/repository.py`

---

## 1. Problem

`FETCHER_REGISTRY` / `get_registry()` in `src/importer/fetchers/adapters.py` and the
`Fetcher` ABC in `src/importer/base_fetcher.py` document a generic pattern
("register a source once, the orchestrator runs it uniformly") that is **not
actually used anywhere in the codebase**. Every real caller —
`src/importer/cli.py`, `src/importer/parallel_importer.py`,
`src/tools/skills_tools.py` — imports concrete fetcher classes
(`ShoonyaFetcher`, `NSElibFetcher`, ...) directly and hand-rolls its own
control flow around them.

`cli.py::run_import()` in particular reimplements, per category, logic that
`MarketDataRepository.run_fetcher()` already provides in generic form:
watermark resolution, retry, dry-run, and insert — plus several things
`run_fetcher()` does **not** yet support:

| Capability | `cli.py` today | `Fetcher` / `run_fetcher()` today |
|---|---|---|
| Per-symbol progress bar | ✅ `rich.Progress` | ❌ none |
| Force a specific source (`shoonya`/`nse`/`yfinance`) | ✅ `data_source` param | ❌ fetcher is fixed to one source + its own internal fallback |
| Parallel fetch across symbols | ✅ `ThreadPoolExecutor`, 5 workers (`parallel_importer.py`) | ❌ `fetch()` is one synchronous call |
| Worst-case watermark across many symbols | ✅ `_resolve_from_date()`, per-symbol | ⚠️ `run_fetcher()` tracks one watermark per `(source_name, symbol_key)` group, not per symbol |

Because of this gap, the registry is effectively **dead code with misleading
docstrings**, and `cli.py` carries ~200 lines of per-category branching
(`if category in ("stocks", "us_stocks")`, `if category == "nse_indices"`,
`if category in ("stocks", "etfs")`, ...) that must be manually updated any
time fetcher behavior changes.

## 2. Goal

Make `get_registry()` the **single real entry point** for running any
category, so that:

- Adding a new data source is one line in `adapters.py` (`registry[...] = X`) —
  no `cli.py` changes required, ever.
- `cli.py::run_import()` collapses to one loop over `categories`, with no
  per-category special-casing.
- No existing capability regresses: progress bars, source override, and
  parallelism for stocks/etfs must keep working exactly as they do today.

## 3. Non-Goals

- Not changing the ClickHouse schema, DDL, or table shapes.
- Not changing watermark semantics for existing single-symbol-group fetchers
  (`fii_dii`, `cot`, `fx_rates`, `world_bank`, `imf_weo`) — they already work
  correctly through `run_fetcher()` today (per Option 2 groundwork).
- Not migrating `src/tools/skills_tools.py`'s direct fetcher usage — it's an
  interactive tool path, not the batch importer, and is out of scope here.

## 4. Design

### 4.1 `Fetcher` ABC — new optional capability flags and richer `fetch()` signature

`src/importer/base_fetcher.py`

```python
class Fetcher(ABC):
    source_name:  str
    symbol_key:   str
    description:  str = ""
    overlap_days: int = 3

    # NEW — opt-in capability flags, default False/None so every existing
    # Fetcher subclass keeps working unmodified.
    supports_parallel:        bool = False
    supports_source_override: bool = False

    @abstractmethod
    def fetch(
        self,
        from_date: date,
        to_date: date,
        *,
        source: str | None = None,          # NEW, optional
        progress_cb: Callable[[str], None] | None = None,  # NEW, optional
    ) -> list[dict[str, Any]]:
        """
        Pull rows from the external source.

        source      : override the fetcher's default source, if
                      supports_source_override is True. Ignored otherwise.
        progress_cb : called with a symbol string after each symbol
                      completes, if supports_parallel is True. Ignored
                      otherwise.
        """
```

Existing subclasses (`FIIDIIFetcher`, `COTGoldFetcher`, `WorldBankMacroFetcher`,
etc.) need **zero changes** — Python allows them to keep the narrower
`fetch(self, from_date, to_date)` signature since callers only pass the new
kwargs conditionally (see 4.3).

### 4.2 Per-symbol watermark support in the watermark store

Current schema (`market_data.import_watermarks`, unchanged):
```
ORDER BY (source, symbol, dataset)
```
This already supports a `symbol` value that is either a group key
(`"ETF_GROUP"`) or an individual symbol (`"GOLDBEES"`) — the table doesn't
care which. The gap is only in `MarketDataRepository.run_fetcher()`, which
currently always uses `fetcher.symbol_key` (one group value) for both read
and write.

**Add** a repository method for the per-symbol case, used only by
`supports_parallel` fetchers:

```python
def _resolve_group_from_date(
    self, ch, source: str, symbols: list[str], *,
    lookback_days: int, overlap_days: int, full: bool, today: date,
) -> date:
    """Worst-case (earliest) per-symbol watermark minus overlap.
    This is a straight port of cli.py's existing _resolve_from_date."""
```

```python
def _update_group_watermarks(self, ch, rows, source, dry_run) -> None:
    """Per-symbol watermark write. Port of cli.py's _update_watermarks."""
```

This preserves the exact existing watermark semantics for stocks/etfs (no
silent behavior change) while keeping the group-watermark path
(`get_watermark`/`set_watermark` on `symbol_key`) for every other fetcher.

### 4.3 `run_fetcher()` — parallel + progress + source-override support

`src/db/repository.py`

```python
def run_fetcher(
    self,
    fetcher: "Fetcher",
    *,
    dry_run: bool = False,
    full: bool = False,
    lookback_days: int = 3650,
    workers: int = 1,                                   # NEW
    source: str | None = None,                           # NEW
    progress_cb: Callable[[str], None] | None = None,     # NEW
) -> "FetchResult":
    ...
    use_group_watermark = fetcher.supports_parallel and hasattr(fetcher, "symbols")

    if use_group_watermark:
        from_date = self._resolve_group_from_date(
            ch, fetcher.source_name, [s for s, _ in fetcher.symbols],
            lookback_days=lookback_days, overlap_days=fetcher.overlap_days,
            full=full, today=today,
        )
    else:
        # existing single symbol_key watermark logic, unchanged

    fetch_kwargs = {}
    if fetcher.supports_source_override:
        fetch_kwargs["source"] = source
    if fetcher.supports_parallel and workers > 1:
        rows = self._fetch_parallel(fetcher, from_date, today, workers,
                                     progress_cb=progress_cb, **fetch_kwargs)
    else:
        rows = fetcher.fetch_with_retry(from_date, today, **fetch_kwargs)
    ...

    if use_group_watermark:
        self._update_group_watermarks(ch, rows, fetcher.source_name, dry_run)
    else:
        ch.set_watermark(fetcher.source_name, fetcher.symbol_key, fetcher.max_date(rows))
```

```python
def _fetch_parallel(self, fetcher, from_date, to_date, workers, *,
                     progress_cb=None, **fetch_kwargs) -> list[dict]:
    """
    Direct port of parallel_importer.run_parallel_stock_import's
    ThreadPoolExecutor logic, generalized to any supports_parallel Fetcher.
    Per-symbol exceptions are caught and logged — one bad symbol must not
    fail the batch. This preserves parallel_importer.py's existing
    fault-isolation behavior exactly.
    """
```

`fetch_with_retry()` in `base_fetcher.py` needs its signature widened to pass
through `**kwargs` to `self.fetch(...)` unchanged.

### 4.4 `adapters.py` — `ShoonyaFetcher` / `NSElibFetcher` / `YFinanceFetcher` gain the flags

```python
class ShoonyaFetcher(Fetcher):
    supports_parallel = True
    supports_source_override = True

    def fetch(self, from_date, to_date, *, source=None, progress_cb=None):
        if source == "nse":
            rows = NSElibFetcher(self.category, self.symbols).fetch(from_date, to_date)
        elif source == "yfinance":
            from src.importer.fetchers.yfinance_fetcher import fetch_ohlcv
            rows = fetch_ohlcv(self.symbols, self.category, from_date, to_date)
        else:
            rows = fetch_shoonya_ohlcv(self.symbols, self.category, from_date, to_date)
            # existing nselib -> yfinance fallback chain, unchanged
        if progress_cb:
            for sym, _ in self.symbols:
                progress_cb(sym)
        return rows
```

`NseIndexFetcher` is left as a plain (non-parallel, non-source-override)
`Fetcher` — its `cli.py` special case (line ~237, `if category ==
"nse_indices"`) collapses into the generic loop with no flags set.

### 4.5 `cli.py::run_import()` — collapse to one loop

```python
for category in categories:
    fetcher = get_registry().get(category)
    if fetcher is None:
        console.print(f"[yellow]⚠ Unknown category: {category}, skipping[/yellow]")
        continue

    console.print(f"\n[bold cyan]▶ {category.upper()}[/bold cyan]")

    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"),
                  BarColumn(), TaskProgressColumn(), console=console, transient=True) as progress:
        total = len(getattr(fetcher, "symbols", []) or [1])
        task = progress.add_task(f"Fetching {category}…", total=total)

        result = repo.run_fetcher(
            fetcher,
            dry_run=dry_run,
            full=full_reimport,
            lookback_days=lookback_days,
            workers=5 if fetcher.supports_parallel and category in ("stocks", "us_stocks") else 1,
            source=selected_source if fetcher.supports_source_override else None,
            progress_cb=lambda sym: progress.update(task, advance=1, description=f"[dim]{sym}[/dim]"),
        )

    console.print(f"  [green]✓[/green] {result.n} rows {'(dry-run)' if dry_run else 'inserted'}")
    summary_rows.append((category, result.source, result.n,
                          result.from_date.isoformat(), result.to_date.isoformat()))
```

All of: the `mf_holdings`, `nse_eod`, `amfi_category_flows`, and
`indian_macro` special sections further down in `run_import()` (lines
304+) are addressed in Phase 2 (see §6) — they are separate, lower-risk
migrations once the core loop above is proven.

## 5. Backward Compatibility

- `Fetcher` subclasses that don't set `supports_parallel`/
  `supports_source_override` are unaffected — default `fetch(from_date,
  to_date)` calls keep working exactly as today.
- Watermark table schema is unchanged; only which `(source, symbol)` pairs
  get written differs, and only for fetchers that opt into
  `supports_parallel` (i.e., only `ShoonyaFetcher`/`NSElibFetcher`, which
  already use per-symbol watermarks today via `cli.py`'s hand-rolled path —
  so this preserves existing behavior, not changes it).
- `FetchResult` (`src/db/repository.py`) needs no changes — `result.source`,
  `result.n`, `result.from_date`, `result.to_date` already cover what
  `summary_rows` needs.

## 6. Rollout Plan

1. **Phase 0** — Add `supports_parallel`/`supports_source_override` flags and
   widen `Fetcher.fetch()`/`fetch_with_retry()` signatures. No behavior change
   yet; existing callers unaffected.
2. **Phase 1** — Implement `_fetch_parallel`, `_resolve_group_from_date`,
   `_update_group_watermarks` in `repository.py`. Unit test against a fake
   `Fetcher` with `supports_parallel=True`.
3. **Phase 2** — Migrate `ShoonyaFetcher`/`NSElibFetcher`/`YFinanceFetcher` to
   the new signature. Update `tests/test_custom_import.py` mocks to match
   (`fetch(from_date, to_date, source=..., progress_cb=...)`).
4. **Phase 3** — Rewrite `cli.py::run_import()`'s stocks/etfs/nse_indices
   branches to the unified loop, gated by the full validation suite in §7
   (parity harness, watermark equivalence, fault-isolation test,
   source-override test, progress smoke test, staging dry-run diff) before
   any old branch is removed.
5. **Phase 4** — Migrate the remaining special sections (`mf_holdings`,
   `nse_eod`, `amfi_category_flows`, `indian_macro`) into registry entries.
6. **Phase 5** — Delete `parallel_importer.py`'s now-redundant
   `run_parallel_stock_import`/`import_single_stock`, and delete `cli.py`'s
   `_resolve_from_date`/`_update_watermarks` free functions once nothing
   references them.

## 7. End-to-End Validation Plan

The goal is to prove the unified path produces **identical outputs** to the
existing hand-rolled path before deleting any old code, then prove the new
capabilities (parallelism, source override, progress) actually work under
real conditions.

### 7.1 Parity harness — old path vs. new path, byte-for-byte

Before Phase 3 removes any `cli.py` branch, add a throwaway comparison script
(`scripts/validate_registry_parity.py`, deleted after Phase 5):

```python
old_rows = ShoonyaFetcher(category, symbols).fetch(from_date, today)          # today's code path
new_rows = get_registry()[category].fetch(from_date, today, source=None)      # new code path

assert len(old_rows) == len(new_rows)
old_keyed = {(r["symbol"], r["trade_date"]): r for r in old_rows}
new_keyed = {(r["symbol"], r["trade_date"]): r for r in new_rows}
assert old_keyed.keys() == new_keyed.keys()
for k in old_keyed:
    assert old_keyed[k] == new_keyed[k], f"Row mismatch at {k}"
```

Run this for every category in `ALL_CATEGORIES` against `--dry-run`, so no
writes happen. This is the gate for Phase 3 — it must pass before any old
branch is deleted.

### 7.2 Watermark equivalence check

Because `_resolve_group_from_date`/`_update_group_watermarks` are ports of
`cli.py`'s existing `_resolve_from_date`/`_update_watermarks`, run both
against a snapshot of `import_watermarks` (e.g. a ClickHouse test container
seeded with fixture watermark rows) and assert they compute the **same
`from_date`** and write the **same per-symbol watermark rows**:

```python
old_from = _resolve_from_date(ch, "shoonya", symbol_list, lookback_days=365, today=today)
new_from = repo._resolve_group_from_date(ch, "shoonya", symbol_list, lookback_days=365,
                                          overlap_days=3, full=False, today=today)
assert old_from == new_from
```

### 7.3 Parallel fault-isolation test

Since this is the riskiest part (§8, "Per-symbol fault isolation must be
preserved exactly"), write an explicit test with an injected failure:

```python
class FlakyFetcher(Fetcher):
    supports_parallel = True
    symbols = [("GOOD1", "GOOD1.NS"), ("BAD", "BAD.NS"), ("GOOD2", "GOOD2.NS")]

    def fetch(self, from_date, to_date, *, source=None, progress_cb=None):
        if source == "BAD":  # simulate one symbol raising
            raise RuntimeError("simulated failure")
        ...

def test_fetch_parallel_isolates_failures():
    result = repo._fetch_parallel(FlakyFetcher(), from_date, to_date, workers=3)
    assert {r["symbol"] for r in result} == {"GOOD1", "GOOD2"}  # BAD dropped, not fatal
```
This directly encodes the requirement from `parallel_importer.py`'s current
`try/except` behavior — a regression here would silently drop symbols instead
of failing loudly, which is exactly what must NOT happen.

### 7.4 Source-override integration test

Confirm `data_source=nse` / `data_source=yfinance` actually route through the
alternate path and not silently fall back to shoonya:

```python
@patch("src.importer.fetchers.adapters.NSElibFetcher.fetch")
def test_source_override_routes_to_nselib(mock_fetch):
    get_registry()["etfs"].fetch(from_date, today, source="nse")
    mock_fetch.assert_called_once()
```

### 7.5 Progress callback smoke test

Assert `progress_cb` fires exactly once per symbol, in any order (since
threads complete out of order):

```python
seen = []
repo.run_fetcher(fetcher, workers=5, progress_cb=lambda sym: seen.append(sym))
assert sorted(seen) == sorted(s for s, _ in fetcher.symbols)
```

### 7.6 Full dry-run against staging ClickHouse

Run `python -m src.importer.cli import --categories all --dry-run` against a
staging ClickHouse instance (not production) on both the pre-refactor branch
and the Phase-3 branch, and diff:

- Total row count per category (`FetchResult.n`)
- `from_date`/`to_date` per category
- CLI summary table output (`summary_rows`)

Any diff must be explained (e.g. a source's upstream API returned different
data between the two runs due to time passing) before merging — not assumed
benign.

### 7.7 Production canary

After Phases 1–4 land behind the unchanged CLI interface:
1. Run the new path in `--dry-run` mode in the real daily cron
   (`scripts/cron_daily_sync.sh`) alongside the old path for **one week**,
   logging both outputs but writing with the old path only.
2. Compare `import_failures` row counts between old/new — the new path must
   not produce *more* failures than the old one.
3. Only after a clean week, flip the cron job to the new path for real
   writes, with the old code kept (but unused) for one more release in case
   of rollback.

### 7.8 Rollback plan

Keep `parallel_importer.py` and `cli.py`'s `_resolve_from_date`/
`_update_watermarks` functions intact (unused, not deleted) through Phase 4.
Only delete them in Phase 5, after the production canary (§7.7) has run
cleanly for at least one full week of real daily imports.

## 8. Risks

- **Per-symbol fault isolation must be preserved exactly.** The current
  `ThreadPoolExecutor` + `try/except` in `parallel_importer.py` logs and skips
  a failing symbol without failing the batch. `_fetch_parallel` must replicate
  this precisely — a regression here would silently drop symbols instead of
  surfacing errors.
- **Mock drift in tests.** `tests/test_custom_import.py` patches
  `ShoonyaFetcher`/`NSElibFetcher` directly; changing their `fetch()`
  signature will break these mocks until updated (Phase 2 must include this).
- **Effort vs. payoff.** This is a real refactor across 4 files, not a
  cleanup. It only pays off if more parallel/source-overridable fetchers are
  expected later; if stocks/etfs remain the only ones that ever need this,
  Option 2 (registry only for the already-simple sources) is cheaper and
  lower-risk.

## 9. Open Questions

- Should `workers` be a fixed `5` (current default) or configurable per
  fetcher (e.g. a `default_workers` class attribute), so future
  high-volume sources can tune it independently?
- Do we want `progress_cb` to support failure/skip states (e.g.
  `progress_cb(sym, status="failed")`) so the progress bar can visually
  distinguish a fallback-to-yfinance from a clean primary-source fetch?
