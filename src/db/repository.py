"""
src/db/repository.py
────────────────────
MarketDataRepository — single access point for all ClickHouse reads.

Centralises:
  • Consistent FINAL usage on ReplacingMergeTree tables
  • Typed return shapes (tuples, dicts, DataFrames)
  • One place to add caching, retries, or schema changes

Usage
─────
    from src.db.repository import MarketDataRepository
    from src.db.pool import get_pool

    repo = MarketDataRepository(get_pool())
    fii, dii = repo.fii_dii_5d()
    pred      = repo.latest_ml_prediction()
    ohlcv_df  = repo.goldbees_ohlcv()
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

import pandas as pd

log = logging.getLogger(__name__)


# ── Result type for run_fetcher ───────────────────────────────────────────────

from dataclasses import dataclass, field


@dataclass
class FetchResult:
    """Outcome of a single MarketDataRepository.run_fetcher() call."""
    fetcher:   Any
    n:         int
    from_date: "date"
    to_date:   "date"
    dry_run:   bool = False
    skipped:   bool = False

    @property
    def source(self) -> str:
        return getattr(self.fetcher, "source_name", "?")

    @property
    def label(self) -> str:
        return getattr(self.fetcher, "description", self.source)

    def __str__(self) -> str:
        tag = " (dry-run)" if self.dry_run else (" (skipped)" if self.skipped else "")
        return f"{self.label}: {self.n} rows  {self.from_date}→{self.to_date}{tag}"


def _date_today() -> "date":
    from datetime import date as _d
    return _d.today()


class MarketDataRepository:
    """
    Typed read interface over the market_data ClickHouse database.

    Pass an CHPool instance (from src.db.pool.get_pool()). The repository
    borrows a client per call — it does not hold a long-lived connection.
    """

    def __init__(self, pool) -> None:
        self._pool = pool

    def _q(self, sql: str, parameters: dict | None = None):
        """Execute a query and return raw result rows."""
        with self._pool.get_client() as c:
            return c.query(sql, parameters=parameters or {}).result_rows

    def _qdf(self, sql: str) -> pd.DataFrame:
        """Execute a query and return a DataFrame."""
        return self._pool.query_df(sql)

    # ── FII / DII ─────────────────────────────────────────────────────────────

    def fii_dii_5d(self) -> tuple[float, float] | None:
        """5-day rolling FII and DII net flows in ₹ Crore.

        Returns None when no rows exist in the window (missing data),
        vs (0.0, 0.0) when rows exist but net flow is genuinely zero.
        """
        rows = self._q(
            "SELECT sum(fii_net_cr), sum(dii_net_cr), count(*) "
            "FROM market_data.fii_dii_flows FINAL "
            "WHERE trade_date >= today() - 5"
        )
        if rows and rows[0][2] and int(rows[0][2]) > 0:
            return float(rows[0][0] or 0), float(rows[0][1] or 0)
        return None

    # ── News sentiment ────────────────────────────────────────────────────────

    def news_sentiment_rows(self, days: int = 7) -> list[tuple[str, str]]:
        """
        Recent news rows: (etfs_impacted, sentiment).
        sentiment values: 'POSITIVE' | 'NEGATIVE' | 'NEUTRAL'
        """
        rows = self._q(
            f"SELECT etfs_impacted, sentiment "
            f"FROM market_data.news_articles FINAL "
            f"WHERE fetched_at >= now() - INTERVAL {days} DAY"
        )
        return [(str(r[0]), str(r[1])) for r in rows]

    # ── ML predictions ───────────────────────────────────────────────────────

    def latest_ml_prediction(self) -> dict[str, Any] | None:
        """Latest GOLDBEES ML prediction row, or None if table is empty."""
        rows = self._q(
            "SELECT expected_return_pct, prob_up, regime_signal, cv_auc_mean, as_of "
            "FROM market_data.ml_predictions FINAL "
            "ORDER BY as_of DESC LIMIT 1"
        )
        if not rows:
            return None
        r = rows[0]
        return {
            "expected_return_pct": float(r[0]),
            "prob_up":             float(r[1]),
            "regime_signal":       str(r[2]),
            "cv_auc_mean":         float(r[3]),
            "as_of":               r[4],
        }

    # ── Price data ───────────────────────────────────────────────────────────

    def ohlcv(self, symbol: str, category: str) -> pd.DataFrame:
        """
        Full OHLCV history for a symbol, deduplicated via argMax on imported_at.
        Returns columns: trade_date, open, high, low, close, volume.
        """
        return self._qdf(
            f"SELECT trade_date, "
            f"argMax(open, imported_at) AS open, "
            f"argMax(high, imported_at) AS high, "
            f"argMax(low, imported_at) AS low, "
            f"argMax(close, imported_at) AS close, "
            f"argMax(volume, imported_at) AS volume "
            f"FROM market_data.daily_prices FINAL "
            f"WHERE symbol = '{symbol}' AND category = '{category}' "
            f"GROUP BY trade_date ORDER BY trade_date ASC"
        )

    def latest_close(self, symbols: list[str], category: str) -> dict[str, float]:
        """Latest closing price for each symbol in the list."""
        sym_in = ", ".join(f"'{s}'" for s in symbols)
        rows = self._q(
            f"SELECT symbol, argMax(close, trade_date) "
            f"FROM market_data.daily_prices FINAL "
            f"WHERE symbol IN ({sym_in}) AND category = '{category}' "
            f"GROUP BY symbol"
        )
        return {str(r[0]): float(r[1]) for r in rows}

    # ── iNAV snapshots ────────────────────────────────────────────────────────

    def inav_latest_and_history(
        self,
        symbols: list[str],
        lookback_days: int = 30,
    ) -> tuple[dict[str, float], dict[str, list[float]]]:
        """
        Two-query batch for premium/discount analytics.

        Returns
        -------
        latest_map  : {symbol: latest_premium_pct}
        hist_map    : {symbol: [hourly_premium_pct, ...]}  within lookback window
        """
        from collections import defaultdict

        sym_in = ", ".join(f"'{s}'" for s in symbols)
        cutoff = (date.today() - timedelta(days=lookback_days)).isoformat()

        latest_rows = self._q(
            f"SELECT symbol, argMax(premium_discount_pct, snapshot_at) "
            f"FROM market_data.inav_snapshots FINAL "
            f"WHERE symbol IN ({sym_in}) GROUP BY symbol"
        )
        latest_map = {r[0]: float(r[1]) for r in latest_rows if r[1] is not None}

        hist_rows = self._q(
            f"SELECT symbol, toStartOfHour(snapshot_at), "
            f"argMax(premium_discount_pct, snapshot_at) "
            f"FROM market_data.inav_snapshots FINAL "
            f"WHERE symbol IN ({sym_in}) "
            f"  AND snapshot_at >= toDateTime('{cutoff} 00:00:00') "
            f"GROUP BY symbol, toStartOfHour(snapshot_at) "
            f"ORDER BY symbol, toStartOfHour(snapshot_at) ASC"
        )
        hist_map: dict[str, list[float]] = defaultdict(list)
        for r in hist_rows:
            hist_map[r[0]].append(float(r[2]))

        return latest_map, dict(hist_map)

    # ── Watermarks ───────────────────────────────────────────────────────────

    def get_watermark(self, source: str, symbol: str) -> "date | None":
        """Last successfully imported date for (source, symbol)."""
        from src.importer.clickhouse import ClickHouseImporter
        ch = ClickHouseImporter(**self._ch_kwargs())
        try:
            return ch.get_watermark(source, symbol)
        finally:
            ch.close()

    def set_watermark(self, source: str, symbol: str, last_date: "date") -> None:
        from src.importer.clickhouse import ClickHouseImporter
        ch = ClickHouseImporter(**self._ch_kwargs())
        try:
            ch.set_watermark(source, symbol, last_date)
        finally:
            ch.close()

    # ── Generic fetcher runner ────────────────────────────────────────────────

    def run_fetcher(
        self,
        fetcher: "Fetcher",
        *,
        dry_run: bool = False,
        full: bool = False,
        lookback_days: int = 3650,
        workers: int = 1,
        source: str | None = None,
        progress_cb=None,
        ch=None,
    ) -> "FetchResult":
        """
        Execute the full fetch → validate → insert → watermark cycle.

        Parameters
        ----------
        fetcher       : a Fetcher adapter instance
        dry_run       : fetch but do not write to ClickHouse
        full          : ignore watermarks; re-fetch full lookback window
        lookback_days : history depth on first import or full re-import
        workers       : if fetcher.supports_parallel and workers > 1, fetch
                        each symbol concurrently via a thread pool
        source        : override the fetcher's default source, if
                        fetcher.supports_source_override is True
        progress_cb   : called with a symbol string after each symbol
                        completes (parallel path only)
        ch            : an already-open ClickHouseImporter to reuse (e.g. so
                        a caller looping over many fetchers connects once).
                        If None, a connection is opened and closed here.

        Returns a FetchResult with row count and date range.
        """
        from datetime import timedelta
        from src.importer.clickhouse import ClickHouseImporter

        today = _date_today()
        owns_ch = ch is None
        if owns_ch:
            ch = ClickHouseImporter(**self._ch_kwargs())
        try:
            if owns_ch:
                ch.ensure_schema()

            # Watermarks must be keyed on the source that actually produces
            # the rows this run, not the fetcher's static default — a
            # --data-source override must not fragment watermark history
            # from the default-source run (or make tomorrow's default-source
            # run think it's already caught up when it isn't).
            effective_source = (source or fetcher.source_name) if fetcher.supports_source_override else fetcher.source_name

            use_group_watermark = fetcher.supports_parallel and hasattr(fetcher, "symbols")
            per_symbol_watermark = use_group_watermark and getattr(fetcher, "per_symbol_watermark", False)

            # Determine start date
            if per_symbol_watermark:
                # Each symbol resolves its own watermark inside _fetch_parallel
                # (see there) — this from_date is only a display fallback for
                # FetchResult, matching today's stock-summary behavior which
                # always shows the full lookback window regardless of actual
                # per-symbol watermarks used.
                from_date = today - timedelta(days=lookback_days)
            elif use_group_watermark:
                from_date = self._resolve_group_from_date(
                    ch, effective_source, [s for s, _ in fetcher.symbols],
                    lookback_days=lookback_days, overlap_days=fetcher.overlap_days,
                    full=full, today=today,
                )
            elif full:
                from_date = today - timedelta(days=lookback_days)
            else:
                wm = ch.get_watermark(effective_source, fetcher.symbol_key, dataset=fetcher.dataset)
                if wm is None:
                    from_date = today - timedelta(days=lookback_days)
                else:
                    from_date = wm - timedelta(days=fetcher.overlap_days)

            fetch_kwargs = {"source": source} if fetcher.supports_source_override else {}

            if use_group_watermark and workers > 1:
                rows = self._fetch_parallel(
                    fetcher, from_date, today, workers,
                    progress_cb=progress_cb,
                    ch=ch if per_symbol_watermark else None,
                    per_symbol_watermark=per_symbol_watermark,
                    lookback_days=lookback_days, full=full,
                    effective_source=effective_source,
                    **fetch_kwargs,
                )
            else:
                rows = fetcher.fetch_with_retry(from_date, today, **fetch_kwargs)

            if not rows:
                # fetch failed after retries — log to import_failures and skip
                self._record_failure(fetcher, from_date, today, "FetchError", "fetch returned empty after retries")
                return FetchResult(fetcher=fetcher, n=0, from_date=from_date,
                                   to_date=today, skipped=True)

            rows = fetcher.validate(rows)

            if dry_run:
                return FetchResult(fetcher=fetcher, n=fetcher.count_insertable(rows), from_date=from_date,
                                   to_date=fetcher.max_date(rows), dry_run=True)

            n = fetcher.insert(rows, ch)
            max_dt = fetcher.max_date(rows)

            if use_group_watermark:
                fetcher.write_group_watermarks(ch, rows, dry_run, source=source)
            else:
                ch.set_watermark(effective_source, fetcher.symbol_key, max_dt, dataset=fetcher.dataset)

            result = FetchResult(fetcher=fetcher, n=n, from_date=from_date, to_date=max_dt)

            # Publish post-import event — observers react asynchronously
            self._publish_imported(fetcher, result)

            return result

        finally:
            if owns_ch:
                ch.close()

    def _resolve_group_from_date(
        self, ch, source: str, symbols: list[str], *,
        lookback_days: int, overlap_days: int, full: bool, today: "date",
    ) -> "date":
        """
        Worst-case (earliest) per-symbol watermark minus overlap, across a
        group of symbols sharing one category (e.g. all ETF or stock
        tickers). Port of cli.py's _resolve_from_date, generalized for the
        registry-based orchestrator.
        """
        from datetime import timedelta

        if full:
            return today - timedelta(days=lookback_days)

        earliest: "date | None" = None
        for sym in symbols:
            wm = ch.get_watermark(source, sym)
            if wm is None:
                return today - timedelta(days=lookback_days)
            candidate = wm - timedelta(days=overlap_days)
            if earliest is None or candidate < earliest:
                earliest = candidate
        return earliest or (today - timedelta(days=lookback_days))

    def _fetch_parallel(self, fetcher, from_date, to_date, workers: int, *,
                         progress_cb=None, ch=None, per_symbol_watermark=False,
                         lookback_days: int = 3650, full: bool = False,
                         effective_source: str | None = None,
                         **fetch_kwargs) -> list[dict]:
        """
        Fetch fetcher.symbols concurrently, one thread per symbol, via
        fetcher.for_symbol(). Generalizes parallel_importer.py's
        ThreadPoolExecutor loop to any supports_parallel Fetcher.

        When per_symbol_watermark is set (e.g. StocksFetcher), each symbol
        resolves its OWN watermark here — a direct port of
        parallel_importer.import_single_stock's per-thread
        `ch.get_watermark(data_source, symbol, dataset="prices")` call — so
        one caught-up symbol's from_date is never dragged down to a
        full-lookback re-fetch just because another symbol in the same
        category has no watermark yet. `from_date` is otherwise used
        unchanged (the shared, pre-resolved group watermark case, e.g. etfs).

        Per-symbol failures are already swallowed by fetch_with_retry (which
        returns [] after exhausting retries) — one bad symbol contributes no
        rows but never fails the batch.
        """
        from datetime import timedelta
        from concurrent.futures import ThreadPoolExecutor, as_completed

        rows: list[dict] = []
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {}
            for sym, ticker in fetcher.symbols:
                sym_from = from_date
                if per_symbol_watermark and ch is not None:
                    if full:
                        sym_from = to_date - timedelta(days=lookback_days)
                    else:
                        wm = ch.get_watermark(effective_source, sym, dataset="prices")
                        sym_from = (
                            (to_date - timedelta(days=lookback_days)) if wm is None
                            else (wm - timedelta(days=fetcher.overlap_days))
                        )
                futures[executor.submit(
                    fetcher.for_symbol(sym, ticker).fetch_with_retry,
                    sym_from, to_date, **fetch_kwargs,
                )] = sym
            for future in as_completed(futures):
                sym = futures[future]
                try:
                    rows.extend(future.result())
                except Exception as exc:
                    log.error("%s: parallel fetch failed for %s: %s", fetcher.source_name, sym, exc)
                if progress_cb:
                    progress_cb(sym)
        return rows

    def _publish_imported(self, fetcher, result: "FetchResult") -> None:
        """Fire DataImportedEvent on the global EventBus after a successful insert."""
        try:
            from src.events.bus import get_event_bus, DataImportedEvent
            event = DataImportedEvent(
                source     = fetcher.source_name,
                category   = getattr(fetcher, "category", fetcher.symbol_key.lower()),
                symbol_key = fetcher.symbol_key,
                n_rows     = result.n,
                from_date  = result.from_date,
                to_date    = result.to_date,
            )
            get_event_bus().publish(event)
        except Exception as exc:
            log.warning("EventBus publish failed (non-fatal): %s", exc)

    def _record_failure(
        self,
        fetcher,
        from_date: "date",
        to_date: "date",
        error_class: str,
        error_msg: str,
    ) -> None:
        """Write a row to market_data.import_failures (best-effort; non-fatal)."""
        from datetime import datetime
        try:
            from src.importer.clickhouse import ClickHouseImporter
            ch = ClickHouseImporter(**self._ch_kwargs())
            try:
                ch._client.insert(
                    "market_data.import_failures",
                    [[
                        datetime.utcnow(),
                        fetcher.source_name,
                        getattr(fetcher, "dataset", "prices"),
                        fetcher.symbol_key,
                        from_date,
                        to_date,
                        error_class[:128],
                        error_msg[:512],
                        0,
                    ]],
                    column_names=[
                        "failed_at", "source", "dataset", "symbol",
                        "from_date", "to_date", "error_class", "error_msg", "retry_count",
                    ],
                )
            finally:
                ch.close()
        except Exception as exc:
            log.warning("import_failures write failed (non-fatal): %s", exc)

    def _ch_kwargs(self) -> dict:
        """Extract ClickHouse connection params from the pool config."""
        try:
            client = self._pool._pool[0] if hasattr(self._pool, "_pool") else None
            if client and hasattr(client, "_url"):
                import re
                m = re.match(r"http://([^:]+):(\d+)", str(client._url))
                if m:
                    return {"host": m.group(1), "port": int(m.group(2))}
        except Exception:
            pass
        return {}  # ClickHouseImporter defaults to localhost:8123

    # ── Signal composite ─────────────────────────────────────────────────────

    def latest_signal_composite(self, symbols: list[str]) -> dict[str, dict]:
        """Latest composite signal row per symbol."""
        sym_in = ", ".join(f"'{s}'" for s in symbols)
        rows = self._q(
            f"SELECT etf_symbol, composite_score, action, anomaly_flag "
            f"FROM market_data.signal_composite FINAL "
            f"WHERE etf_symbol IN ({sym_in}) "
            f"ORDER BY etf_symbol, as_of DESC LIMIT {len(symbols)} BY etf_symbol"
        )
        return {
            r[0]: {"composite_score": float(r[1]), "action": str(r[2]), "anomaly_flag": str(r[3])}
            for r in rows
        }

    def ml_prediction_asof(self, as_of) -> "dict | None":
        """Most recent ML prediction on or before `as_of` (str YYYY-MM-DD or date)."""
        rows = self._q(
            f"SELECT expected_return_pct, prob_up, regime_signal, cv_auc_mean, as_of "
            f"FROM market_data.ml_predictions FINAL "
            f"WHERE as_of <= '{as_of}' "
            f"ORDER BY as_of DESC LIMIT 1"
        )
        if not rows:
            return None
        r = rows[0]
        return {
            "expected_return_pct": float(r[0]),
            "prob_up":             float(r[1]),
            "regime_signal":       str(r[2]),
            "cv_auc_mean":         float(r[3]),
            "as_of":               str(r[4]),
        }

    def signal_composite_asof(self, symbol: str, as_of) -> "dict | None":
        """Most recent composite signal for `symbol` on or before `as_of`."""
        rows = self._q(
            f"SELECT etf_symbol, composite_score, action, anomaly_flag, as_of "
            f"FROM market_data.signal_composite FINAL "
            f"WHERE etf_symbol = '{symbol}' AND as_of <= '{as_of}' "
            f"ORDER BY as_of DESC LIMIT 1"
        )
        if not rows:
            return None
        r = rows[0]
        return {
            "composite_score": float(r[1]),
            "action":          str(r[2]),
            "anomaly_flag":    str(r[3]),
            "as_of":           str(r[4]),
        }

    # ── OU state (premium mean-reversion) ────────────────────────────────────

    def ou_state(self, symbol: str, as_of=None) -> "dict | None":
        """Most recent OU fit for `symbol` on or before `as_of` (default: today)."""
        if as_of is None:
            from datetime import date as _date
            as_of = _date.today().isoformat()
        rows = self._q(
            f"SELECT symbol, fit_date, theta, mu, sigma, half_life_days, n_obs, fit_r2 "
            f"FROM market_data.premium_ou_state FINAL "
            f"WHERE symbol = '{symbol}' AND fit_date <= '{as_of}' "
            f"ORDER BY fit_date DESC LIMIT 1"
        )
        if not rows:
            return None
        r = rows[0]
        return {
            "symbol":         str(r[0]),
            "fit_date":       str(r[1]),
            "theta":          float(r[2]),
            "mu":             float(r[3]),
            "sigma":          float(r[4]),
            "half_life_days": float(r[5]),
            "n_obs":          int(r[6]),
            "fit_r2":         float(r[7]),
        }

    def pair_state(self, symbol_a: str, symbol_b: str, as_of=None) -> "dict | None":
        """Most recent cointegration pair fit on or before `as_of`."""
        if as_of is None:
            from datetime import date as _date
            as_of = _date.today().isoformat()
        rows = self._q(
            f"SELECT symbol_a, symbol_b, fit_date, coint_pvalue, hedge_ratio, "
            f"       alpha, theta, mu, sigma, half_life_days "
            f"FROM market_data.premium_pair_state FINAL "
            f"WHERE symbol_a = '{symbol_a}' AND symbol_b = '{symbol_b}' "
            f"  AND fit_date <= '{as_of}' "
            f"ORDER BY fit_date DESC LIMIT 1"
        )
        if not rows:
            return None
        r = rows[0]
        return {
            "symbol_a":       str(r[0]),
            "symbol_b":       str(r[1]),
            "fit_date":       str(r[2]),
            "coint_pvalue":   float(r[3]),
            "hedge_ratio":    float(r[4]),
            "alpha":          float(r[5]),
            "theta":          float(r[6]),
            "mu":             float(r[7]),
            "sigma":          float(r[8]),
            "half_life_days": float(r[9]),
        }
