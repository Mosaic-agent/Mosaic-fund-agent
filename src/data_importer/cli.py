"""
src/importer/cli.py
────────────────────
Core import logic for the `mosaic import` CLI command.

Usage (called from src/main.py):
    from src.data_importer.cli import run_import
    run_import(categories=["stocks","etfs","commodities","indices","mf"])

Delta-sync strategy:
  1. For each (source, symbol), read the watermark from ClickHouse.
  2. If no watermark exists → first run: fetch `lookback_days` of history.
  3. Otherwise → delta run: fetch from (watermark_date − OVERLAP) to today.
  4. After successful insert, update the watermark.

The OVERLAP window (default 3 days) handles weekends and late-arriving
NAV corrections on MFAPI.in — re-inserting rows is safe because
ReplacingMergeTree deduplicates by (symbol, date).
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Optional

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.table import Table
from rich import box

from src.data_importer.source_preference import normalize_data_source

logger = logging.getLogger(__name__)


def run_import(
    categories: list[str],
    *,
    lookback_days: int = 3650,
    full_reimport: bool = False,
    dry_run: bool = False,
    console: Optional[Console] = None,
    clickhouse_host: str = "localhost",
    clickhouse_port: int = 8123,
    clickhouse_database: str = "market_data",
    clickhouse_user: str = "default",
    clickhouse_password: str = "",
    mf_holdings_month: Optional[date] = None,
    mf_holdings_months: int = 1,
    data_source: str = "",
    target_month: str = "",
    freshness_months: int = 0,
) -> None:
    """
    Run the historical data import for the specified categories.

    Parameters
    ----------
    categories         : list of category names to import (stocks, etfs, commodities,
                         indices, mf — or 'all' which maps to all categories)
    lookback_days      : how many calendar days of history to fetch on first run
    full_reimport      : ignore watermarks and re-fetch full lookback window
    dry_run            : fetch data but do NOT write to ClickHouse
    console            : Rich Console instance (created if None)
    clickhouse_*       : ClickHouse connection parameters
    mf_holdings_month  : import a specific month (overrides mf_holdings_months)
    mf_holdings_months : number of past months to import (default 1 = current month)
    data_source        : stock/ETF source; shoonya, nse, or yfinance
    """
    from src.data_importer.registry import (
        MF_SCHEME_CODES,
        MF_HOLDINGS_WATCHLIST,
        ALL_CATEGORIES,
    )
    from src.data_importer.clickhouse import ClickHouseImporter
    from config.settings import settings

    selected_source = normalize_data_source(data_source)
    if data_source and not selected_source:
        raise ValueError("data_source must be one of: shoonya, nse, yfinance")

    if console is None:
        console = Console()

    # Expand "all" shorthand
    if "all" in categories:
        categories = ALL_CATEGORIES

    # Normalize category list to map aliases (e.g. indian_macro_indicators -> indian_macro)
    normalized_categories = []
    for cat in categories:
        c_clean = cat.strip().lower()
        if c_clean == "indian_macro_indicators":
            if "indian_macro" not in normalized_categories:
                normalized_categories.append("indian_macro")
        elif c_clean:
            if c_clean not in normalized_categories:
                normalized_categories.append(c_clean)
    categories = normalized_categories

    today = date.today()

    # ── Connect + ensure schema ────────────────────────────────────────────
    try:
        ch = ClickHouseImporter(
            host=clickhouse_host,
            port=clickhouse_port,
            database=clickhouse_database,
            username=clickhouse_user,
            password=clickhouse_password,
        )
        if not dry_run:
            ch.ensure_schema()
    except Exception as exc:
        console.print(f"[bold red]✗ Cannot connect to ClickHouse:[/bold red] {exc}")
        console.print(
            "  Make sure ClickHouse is running. "
            "With Docker Compose: [bold]docker compose up clickhouse -d[/bold]"
        )
        raise SystemExit(1)

    # ── Summary table ──────────────────────────────────────────────────────
    summary_rows: list[tuple[str, str, int, str, str]] = []

    # ── Unified registry-driven import ── every category except mf_holdings
    # (idempotency guard lives here in cli.py) and skills_tools.py's
    # interactive single-symbol path is routed through get_registry().
    from src.data_importer.fetchers.adapters import get_registry
    from src.db.repository import MarketDataRepository

    repo = MarketDataRepository(pool=None)
    registry_categories = [
        c for c in categories
        if c in {"stocks", "us_stocks", "etfs", "commodities", "indices",
                 "nse_indices", "nse_eod", "indian_macro", "fx_rates",
                 "cot", "cb_reserves", "etf_aum", "mf", "fii_dii",
                 "world_bank", "imf_weo", "amfi_flows"}
    ]
    for category in registry_categories:
        fetcher = get_registry().get(category)
        if fetcher is None:
            console.print(f"[yellow]⚠ Unknown category: {category}, skipping[/yellow]")
            continue

        symbol_list = getattr(fetcher, "symbols", [])
        console.print(
            f"\n[bold cyan]▶ {category.upper()}[/bold cyan]"
            + (f" ({len(symbol_list)} symbols)" if symbol_list else "")
        )

        workers = 5 if fetcher.supports_parallel and category in ("stocks", "us_stocks") else 1
        effective_source = (
            selected_source if fetcher.supports_source_override and selected_source
            else fetcher.source_name
        )

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
            transient=True,
        ) as progress:
            task = progress.add_task(f"Fetching {category}…", total=len(symbol_list) or 1)

            if workers == 1:
                # Cosmetic pre-tick — matches the non-parallel categories' existing display.
                for nse_sym, _yahoo in symbol_list:
                    progress.update(task, advance=1, description=f"[dim]{nse_sym}[/dim]")

            def _advance(sym: str) -> None:
                progress.update(task, advance=1, description=f"[dim]{sym}[/dim]")

            result = repo.run_fetcher(
                fetcher,
                dry_run=dry_run,
                full=full_reimport,
                lookback_days=lookback_days,
                workers=workers,
                source=selected_source if fetcher.supports_source_override else None,
                progress_cb=_advance if workers > 1 else None,
                ch=ch,
            )

        if result.skipped:
            console.print("  [yellow]⚠ Fetch failed after retries — logged to import_failures, skipping.[/yellow]")
            continue

        console.print(f"  [green]✓[/green] {result.n} rows {'(dry-run)' if dry_run else 'inserted'}")
        summary_rows.append((
            category, effective_source, result.n,
            result.from_date.isoformat(), result.to_date.isoformat(),
        ))


    # ── NSE live iNAV snapshots ────────────────────────────────────────────────
    if "inav" in categories:
        from src.data_importer.registry import INAV_SYMBOLS
        from src.data_importer.fetchers.nse_inav_fetcher import fetch_inav_snapshots

        console.print(f"\n[bold cyan]▶ NSE iNAV SNAPSHOTS[/bold cyan] ({len(INAV_SYMBOLS)} ETFs)")
        console.print("  [dim]Live snapshot from NSE API (updated every ~15s during market hours)[/dim]")

        snapshot_rows = fetch_inav_snapshots(INAV_SYMBOLS)
        if not snapshot_rows:
            console.print("  [yellow]⚠ NSE returned no iNAV data — market may be closed or API blocked.[/yellow]")
        else:
            inserted = ch.insert_inav_snapshots(snapshot_rows, dry_run=dry_run)
            console.print(f"  [green]✓[/green] {inserted} snapshot(s) {'(dry-run)' if dry_run else 'stored'}")
            from datetime import datetime
            ts = snapshot_rows[0]["snapshot_at"]
            summary_rows.append(("inav", "nse", inserted, str(ts)[:10], str(ts)[:10]))
    if "mf_holdings" in categories:
        from src.data_importer.fetchers.adapters import MfHoldingsFetcher

        # NOTE: mstarpy.Funds.holdings() has NO date parameter — it always returns
        # the current Morningstar snapshot. We tag rows with the current month so
        # running this monthly builds a genuine time-series going forward.
        as_of_month = mf_holdings_month or date(today.year, today.month, 1)

        console.print(
            f"\n[bold cyan]▶ MF Holdings[/bold cyan] "
            f"({len(MF_HOLDINGS_WATCHLIST)} funds · snapshot as of {as_of_month})"
        )

        # Skip if this month's snapshot already exists (unless forced) — an
        # idempotency guard against real data, not delta-sync logic a Fetcher
        # watermark can express, so it stays here rather than in the ABC.
        existing_months: set = set()
        if not full_reimport and not dry_run:
            try:
                rows_ex = ch._client.query(
                    "SELECT DISTINCT as_of_month FROM market_data.mf_holdings"
                ).result_rows
                existing_months = {r[0] for r in rows_ex}
            except Exception:
                pass

        if as_of_month in existing_months:
            console.print(f"  [dim]{as_of_month} snapshot already imported — skipping. Use --full to overwrite.[/dim]")
        else:
            result = repo.run_fetcher(
                MfHoldingsFetcher(MF_HOLDINGS_WATCHLIST, as_of_month),
                dry_run=dry_run, full=True, ch=ch,
            )
            if result.skipped:
                console.print("  [yellow]⚠ No holdings returned — mstarpy/Morningstar may be unavailable.[/yellow]")
            else:
                console.print(
                    f"  [green]✓[/green] {result.n} rows "
                    f"{'(dry-run)' if dry_run else 'stored'} for {as_of_month}"
                )
                summary_rows.append(("mf_holdings", "morningstar", result.n,
                                     str(as_of_month), str(as_of_month)))

    # ── FII / DII Institutional Flows ─────────────────────────────────────────
    # ── Earnings ──────────────────────────────────────────────────────────────
    if "earnings" in categories:
        from src.data_importer.registry import US_STOCKS
        from src.data_importer.fetchers.earnings_fetcher import fetch_earnings

        console.print(f"\n[bold cyan]▶ EARNINGS[/bold cyan] ({len(US_STOCKS)} US stocks)")
        rows_e = fetch_earnings(US_STOCKS)
        if not rows_e:
            console.print("  [yellow]⚠ No earnings data returned.[/yellow]")
        else:
            inserted = ch.insert_stock_earnings(rows_e) if not dry_run else len(rows_e)
            console.print(f"  [green]✓[/green] {inserted} rows {'(dry-run)' if dry_run else 'inserted'}")
            summary_rows.append(("earnings", "yfinance", inserted, str(today), str(today)))

    # ── Insider Trades ────────────────────────────────────────────────────────
    if "insider" in categories:
        from src.data_importer.registry import US_STOCKS
        from src.data_importer.fetchers.insider_fetcher import fetch_insider_trades

        console.print(f"\n[bold cyan]▶ INSIDER TRADES[/bold cyan] ({len(US_STOCKS)} US stocks)")
        rows_i = fetch_insider_trades(US_STOCKS)
        if not rows_i:
            console.print("  [yellow]⚠ No insider trade data returned.[/yellow]")
        else:
            inserted = ch.insert_stock_insider(rows_i) if not dry_run else len(rows_i)
            console.print(f"  [green]✓[/green] {inserted} rows {'(dry-run)' if dry_run else 'inserted'}")
            summary_rows.append(("insider", "yfinance", inserted, str(today), str(today)))

    # ── Valuation Snapshot ────────────────────────────────────────────────────
    if "valuation" in categories:
        from src.data_importer.registry import US_STOCKS
        from src.data_importer.fetchers.valuation_fetcher import fetch_valuation

        console.print(f"\n[bold cyan]▶ VALUATION[/bold cyan] ({len(US_STOCKS)} US stocks)")
        rows_v = fetch_valuation(US_STOCKS)
        if not rows_v:
            console.print("  [yellow]⚠ No valuation data returned.[/yellow]")
        else:
            inserted = ch.insert_stock_valuation(rows_v) if not dry_run else len(rows_v)
            console.print(f"  [green]✓[/green] {inserted} rows {'(dry-run)' if dry_run else 'inserted'}")
            for r in rows_v:
                console.print(
                    f"  {r['symbol']:8s}  PE={r['trailing_pe']:.1f}  "
                    f"fwdPE={r['forward_pe']:.1f}  "
                    f"ROE={r['return_on_equity']:.1%}  "
                    f"Rec={r['recommendation']}"
                )
            summary_rows.append(("valuation", "yfinance", inserted, str(today), str(today)))


    # ── AMC Fund-Holdings Importers ───────────────────────────────────────────
    _amc_cats = [c for c in ("icici", "nippon", "icici-index", "dsp", "bajaj", "quant", "hdfc", "kotak", "abakkus", "abacus", "helios", "invesco", "canara") if c in categories]
    if _amc_cats:
        from src.data_importer.fetchers.amc_holdings_fetcher import fetch_amc_holdings

        for _amc_cat in _amc_cats:
            fetch_amc_holdings(
                _amc_cat,
                full_reimport=full_reimport,
                dry_run=dry_run,
                target_month=target_month,
                freshness_months=freshness_months,
            )

    ch.close()

    # ── Summary ────────────────────────────────────────────────────────────
    console.print()
    t = Table(title="Import Summary", box=box.ROUNDED, header_style="bold magenta")
    t.add_column("Category")
    t.add_column("Source")
    t.add_column("Rows", justify="right")
    t.add_column("From")
    t.add_column("To")
    for row in summary_rows:
        t.add_row(*[str(v) for v in row])
    console.print(t)
    if dry_run:
        console.print("\n[yellow]ℹ dry-run — no data was written to ClickHouse.[/yellow]")
    else:
        console.print("\n[bold green]✓ Import complete.[/bold green]")

    # ── Sanity Check ──────────────────────────────────────────────────────────
    if not dry_run:
        from src.utils.sanity_checker import detect_yoy_anomalies, detect_daily_anomalies
        
        console.print("\n[bold cyan]▶ Running Data Sanity Validator…[/bold cyan]")
        
        yoy_anomalies = detect_yoy_anomalies(ch._client)
        daily_anomalies = detect_daily_anomalies(ch._client)

        if yoy_anomalies or daily_anomalies:
            console.print("[bold yellow]⚠ Data Anomalies Detected![/bold yellow] Run [bold]python src/scripts/db/run_data_sanity_check.py[/bold] for full report.")
            
            if yoy_anomalies:
                console.print(f"  [red]• {len(yoy_anomalies)} YoY price anomalies found (e.g., >40% return in safe assets)[/red]")
            if daily_anomalies:
                console.print(f"  [red]• {len(daily_anomalies)} daily outliers found (e.g., >7% move)[/red]")
        else:
            console.print("  [green]✓ No immediate economic anomalies detected in safe assets.[/green]")
