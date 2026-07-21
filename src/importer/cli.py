"""
src/importer/cli.py
────────────────────
Core import logic for the `mosaic import` CLI command.

Usage (called from src/main.py):
    from src.importer.cli import run_import
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
from datetime import date, timedelta
from typing import Optional

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.table import Table
from rich import box

from src.importer.source_preference import normalize_data_source

logger = logging.getLogger(__name__)

# Days of overlap when doing a delta sync (to catch weekend / late corrections)
_OVERLAP_DAYS = 3


# ──────────────────────────────────────────────────────────────────────────────
# Helpers — DRY the repeated watermark/fetch/insert cycle
# ──────────────────────────────────────────────────────────────────────────────

def _resolve_from_date(
    ch,
    source: str,
    symbols: list[str] | str,
    *,
    lookback_days: int,
    overlap_days: int = _OVERLAP_DAYS,
    full_reimport: bool = False,
    dry_run: bool = False,
    today: date | None = None,
) -> date:
    """
    Compute the inclusive start date for a delta-sync fetch.

    For a single symbol pass it as a string; for a group pass a list.
    Returns the earliest (worst-case) watermark minus overlap, or falls back
    to (today − lookback_days) on first run.
    """
    today = today or date.today()
    if full_reimport:
        return today - timedelta(days=lookback_days)

    sym_list = [symbols] if isinstance(symbols, str) else symbols
    earliest: date | None = None
    for sym in sym_list:
        wm = ch.get_watermark(source, sym) if not dry_run else None
        if wm is None:
            return today - timedelta(days=lookback_days)
        candidate = wm - timedelta(days=overlap_days)
        if earliest is None or candidate < earliest:
            earliest = candidate
    return earliest or (today - timedelta(days=lookback_days))


def _update_watermarks(
    ch,
    rows: list[dict],
    source: str,
    *,
    date_field: str = "trade_date",
    dry_run: bool = False,
) -> None:
    """
    Set per-symbol watermarks from the fetched rows.
    Noop on dry_run or empty rows.
    """
    if dry_run or not rows:
        return
    symbols_seen = {r["symbol"] for r in rows}
    for sym in symbols_seen:
        sym_dates = [r[date_field] for r in rows if r["symbol"] == sym]
        if sym_dates:
            ch.set_watermark(source, sym, max(sym_dates))


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
    from src.importer.registry import (
        get_symbols_for_categories,
        MF_SCHEME_CODES,
        MF_HOLDINGS_WATCHLIST,
        ALL_CATEGORIES,
    )
    from src.importer.clickhouse import ClickHouseImporter
    from src.importer.fetchers.yfinance_fetcher import fetch_ohlcv
    from src.importer.fetchers.mfapi_fetcher import fetch_all_nav
    from config.settings import settings

    shoonya_active = bool(settings.shoonya_user_id and settings.shoonya_api_secret)
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

    # ── yfinance categories ────────────────────────────────────────────────
    cat_symbols = get_symbols_for_categories(categories)
    for category, symbol_list in cat_symbols.items():
        console.print(f"\n[bold cyan]▶ {category.upper()}[/bold cyan] ({len(symbol_list)} symbols)")

        if category in ("stocks", "us_stocks"):
            console.print(f"  [dim]Running parallel import (limit: 5 workers)...[/dim]")
            from src.importer.parallel_importer import run_parallel_stock_import
            
            clickhouse_config = {
                "host": clickhouse_host,
                "port": clickhouse_port,
                "database": clickhouse_database,
                "username": clickhouse_user,
                "password": clickhouse_password,
            }
            
            res = run_parallel_stock_import(
                symbols=symbol_list,
                category=category,
                lookback_days=lookback_days,
                full_reimport=full_reimport,
                workers=5,
                clickhouse_config=clickhouse_config,
                dry_run=dry_run,
                data_source=(selected_source or "shoonya") if category == "stocks" else "yfinance",
            )
            
            inserted = res["prices"]
            console.print(
                f"  [green]✓[/green] Completed parallel stock import. "
                f"Prices: {res['prices']} rows, Earnings: {res['earnings']} rows, "
                f"Insider: {res['insider']} rows, Valuations: {res['valuation']} rows."
            )
            
            summary_rows.append((
                category,
                f"{(selected_source or 'shoonya') if category == 'stocks' else 'yfinance'}_parallel",
                inserted,
                (today - timedelta(days=lookback_days)).isoformat(),
                today.isoformat(),
            ))
            continue

        # ── NSE-only indices via nselib ────────────────────────────────────
        if category == "nse_indices":
            from src.importer.fetchers.nse_index_fetcher import fetch_nse_indices

            from_date = _resolve_from_date(
                ch, "nselib_index", "NSE_INDICES",
                lookback_days=lookback_days, full_reimport=full_reimport,
                dry_run=dry_run, today=today,
            )

            console.print(f"  [dim]Fetching via nselib {from_date} → {today}…[/dim]")
            rows = fetch_nse_indices(symbol_list, from_date, today)
            inserted = ch.insert_prices(rows, dry_run=dry_run)
            console.print(f"  [green]✓[/green] {inserted} rows {'(dry-run)' if dry_run else 'inserted'}")

            _update_watermarks(ch, rows, "nselib_index", dry_run=dry_run)

            summary_rows.append((
                category, "nselib", inserted, from_date.isoformat(), today.isoformat(),
            ))
            continue

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
            transient=True,
        ) as progress:
            task = progress.add_task(f"Fetching {category}…", total=len(symbol_list))

            for nse_sym, _yahoo in symbol_list:
                progress.update(task, advance=1, description=f"[dim]{nse_sym}[/dim]")

            # Determine date range (worst-case watermark across all symbols)
            watermark_source = (selected_source or "shoonya") if category in ("stocks", "etfs") else "yfinance"
            from_date = _resolve_from_date(
                ch, watermark_source, [sym for sym, _ in symbol_list],
                lookback_days=lookback_days, full_reimport=full_reimport,
                dry_run=dry_run, today=today,
            )

            progress.update(task, description=f"Downloading {category} {from_date}→{today}…")
            if category in ("stocks", "etfs"):
                from src.importer.fetchers.adapters import NSElibFetcher, ShoonyaFetcher
                if selected_source == "nse":
                    rows = NSElibFetcher(category, symbol_list).fetch(from_date, today)
                elif selected_source == "yfinance":
                    rows = fetch_ohlcv(symbol_list, category, from_date, today)
                else:
                    rows = ShoonyaFetcher(category, symbol_list).fetch(from_date, today)
            else:
                rows = fetch_ohlcv(symbol_list, category, from_date, today)

        inserted = ch.insert_prices(rows, dry_run=dry_run)
        console.print(f"  [green]✓[/green] {inserted} rows {'(dry-run)' if dry_run else 'inserted'}")

        # Update watermarks
        _update_watermarks(ch, rows, watermark_source, dry_run=dry_run)

        summary_rows.append((
            category,
            selected_source or ("shoonya" if category == "etfs" and shoonya_active else "yfinance"),
            inserted,
            from_date.isoformat(),
            today.isoformat(),
        ))
    # ── NSE EOD OHLCV (available immediately after 3:30 PM IST) ─────────────
    if "nse_eod" in categories:
        from src.importer.registry import ETFS, STOCKS
        from src.importer.fetchers.nse_quote_fetcher import fetch_nse_eod

        nse_eod_symbols = ETFS + STOCKS
        console.print(
            f"\n[bold cyan]▶ NSE EOD OHLCV[/bold cyan] "
            f"({len(nse_eod_symbols)} symbols — ETFs + stocks)"
        )
        console.print(
            "  [dim]Direct NSE Quote API — available right after 3:30 PM IST, "
            "no Yahoo Finance delay[/dim]"
        )

        eod_rows: list[dict] = []
        for cat_name, sym_list in [("etfs", ETFS), ("stocks", STOCKS)]:
            fetched = fetch_nse_eod(sym_list, cat_name)
            eod_rows.extend(fetched)

        if not eod_rows:
            console.print(
                "  [yellow]⚠ NSE returned no EOD data — "
                "market may be open or API blocked.[/yellow]"
            )
        else:
            inserted = ch.insert_prices(eod_rows, dry_run=dry_run)
            console.print(
                f"  [green]✓[/green] {inserted} rows "
                f"{'(dry-run)' if dry_run else 'inserted'}"
            )
            _update_watermarks(ch, eod_rows, "nse_quote", dry_run=dry_run)
            summary_rows.append((
                "nse_eod", "nse_quote", inserted, str(today), str(today),
            ))

    # ── NSE live iNAV snapshots ────────────────────────────────────────────────
    if "inav" in categories:
        from src.importer.registry import INAV_SYMBOLS
        from src.importer.fetchers.nse_inav_fetcher import fetch_inav_snapshots

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
    # ── MF NAV ────────────────────────────────────────────────────────────
    if "mf" in categories:
        console.print(f"\n[bold cyan]▶ MF NAV[/bold cyan] ({len(MF_SCHEME_CODES)} schemes)")

        mf_from = _resolve_from_date(
            ch, "mfapi", list(MF_SCHEME_CODES),
            lookback_days=lookback_days, full_reimport=full_reimport,
            dry_run=dry_run, today=today,
        )

        console.print(f"  [dim]Fetching {mf_from} → {today} (MFAPI.in, polite delays)[/dim]")
        nav_rows = fetch_all_nav(MF_SCHEME_CODES, mf_from, today)
        inserted = ch.insert_nav(nav_rows, dry_run=dry_run)
        console.print(f"  [green]✓[/green] {inserted} rows {'(dry-run)' if dry_run else 'inserted'}")

        _update_watermarks(ch, nav_rows, "mfapi", date_field="nav_date", dry_run=dry_run)

        summary_rows.append(("mf", "mfapi", inserted, mf_from.isoformat(), today.isoformat()))

    # ── CFTC COT (hedge fund positioning) ────────────────────────────────────
    if "cot" in categories:
        from src.importer.fetchers.cot_fetcher import fetch_cot_gold

        console.print("\n[bold cyan]▶ CFTC COT — Gold (Managed Money)[/bold cyan]")
        console.print("  [dim]CFTC Disaggregated report, commodity code 088 (released Fridays)[/dim]")

        cot_wm = ch.get_watermark("cot", "GOLD") if (not dry_run and not full_reimport) else None
        cot_from = (cot_wm - timedelta(days=21)) if cot_wm else None   # 3-week overlap
        if full_reimport:
            console.print("  [dim]Full reimport — ignoring watermark, fetching full history[/dim]")
        cot_rows = fetch_cot_gold(from_date=cot_from, limit=2000 if not cot_wm else 500)
        if not cot_rows:
            console.print("  [yellow]⚠ No COT data returned — CFTC endpoint may be unavailable.[/yellow]")
        else:
            inserted = ch.insert_cot_gold(cot_rows, dry_run=dry_run)
            console.print(f"  [green]✓[/green] {inserted} weekly COT rows {'(dry-run)' if dry_run else 'stored'}")
            if not dry_run:
                ch.set_watermark("cot", "GOLD", max(r["report_date"] for r in cot_rows))
            latest = sorted(cot_rows, key=lambda r: r["report_date"])[-1]
            console.print(
                f"  Latest ({latest['report_date']}): "
                f"MM Net {latest['mm_net']:+,d}  |  "
                f"OI {latest['open_interest']:,d}  |  "
                f"MM pct OI {latest['mm_net'] / max(latest['open_interest'], 1) * 100:+.1f}%"
            )
            summary_rows.append(("cot", "cftc", inserted,
                                  str(min(r["report_date"] for r in cot_rows)),
                                  str(latest["report_date"])))

    # ── IMF Central Bank Gold Reserves ────────────────────────────────────────
    if "cb_reserves" in categories:
        from src.importer.fetchers.imf_reserves_fetcher import fetch_cb_reserves

        console.print("\n[bold cyan]▶ IMF IFS — Central Bank Gold Reserves[/bold cyan]")
        console.print("  [dim]9 countries · RAFAGOLD series · monthly · ~6-week publication lag[/dim]")

        cb_wm = ch.get_watermark("cb_reserves", "ALL") if not dry_run else None
        cb_from_year = cb_wm.year if cb_wm else 2010
        cb_rows = fetch_cb_reserves(from_year=cb_from_year)
        if not cb_rows:
            console.print("  [yellow]⚠ No CB reserves data returned — endpoint may be unavailable.[/yellow]")
        else:
            inserted = ch.insert_cb_reserves(cb_rows, dry_run=dry_run)
            console.print(f"  [green]✓[/green] {inserted} reserve rows {'(dry-run)' if dry_run else 'stored'}")
            if not dry_run:
                ch.set_watermark("cb_reserves", "ALL", max(r["ref_period"] for r in cb_rows))
            summary_rows.append(("cb_reserves", "world_bank", inserted,
                                  str(cb_from_year), str(max(r["ref_period"] for r in cb_rows))))

    # ── ETF AUM Snapshots (retail flow proxy) ─────────────────────────────────
    if "etf_aum" in categories:
        from src.importer.fetchers.etf_aum_fetcher import fetch_etf_aum

        console.print("\n[bold cyan]▶ Gold ETF AUM Snapshots[/bold cyan] (GLD · IAU · SGOL · PHYS)")
        console.print("  [dim]Daily AUM + implied gold tonnes via yfinance[/dim]")

        aum_rows = fetch_etf_aum()
        if not aum_rows:
            console.print("  [yellow]⚠ No ETF AUM data returned.[/yellow]")
        else:
            inserted = ch.insert_etf_aum(aum_rows, dry_run=dry_run)
            console.print(f"  [green]✓[/green] {inserted} ETF AUM snapshot(s) {'(dry-run)' if dry_run else 'stored'}")
            for r in aum_rows:
                console.print(
                    f"  {r['symbol']:6s}  AUM ${r['aum_usd']/1e9:.2f}B  "
                    f"price ${r['price']:.2f}  ~{r['implied_tonnes']:.0f}t"
                )
            summary_rows.append(("etf_aum", "yfinance", inserted,
                                  str(today), str(today)))

    # ── FX Rates (USD pairs) ───────────────────────────────────────────────────
    if "fx_rates" in categories:
        from src.importer.fetchers.fx_rates_fetcher import fetch_fx_rates, FX_PAIRS

        console.print("\n[bold cyan]▶ FX Rates — USD Pairs[/bold cyan] (USDINR · USDCNY · USDAED · USDSAR · USDKWD)")
        console.print("  [dim]Daily OHLC via Yahoo Finance — delta-synced per pair[/dim]")

        fx_from = _resolve_from_date(
            ch, "yfinance_fx", [sym for sym, _ in FX_PAIRS],
            lookback_days=lookback_days, full_reimport=full_reimport,
            dry_run=dry_run, today=today,
        )

        console.print(f"  [dim]Fetching {fx_from} → {today}[/dim]")
        fx_rows = fetch_fx_rates(from_date=fx_from, to_date=today)
        if not fx_rows:
            console.print("  [yellow]⚠ No FX data returned — Yahoo Finance may be unavailable.[/yellow]")
        else:
            inserted = ch.insert_fx_rates(fx_rows, dry_run=dry_run)
            console.print(f"  [green]✓[/green] {inserted} FX rate rows {'(dry-run)' if dry_run else 'stored'}")
            _update_watermarks(ch, fx_rows, "yfinance_fx", dry_run=dry_run)
            # Print latest close per pair
            latest_by_sym: dict[str, dict] = {}
            for r in fx_rows:
                if r["symbol"] not in latest_by_sym or r["trade_date"] > latest_by_sym[r["symbol"]]["trade_date"]:
                    latest_by_sym[r["symbol"]] = r
            for sym in [s for s, _ in FX_PAIRS if s in latest_by_sym]:
                r = latest_by_sym[sym]
                console.print(f"  {sym:8s}  {r['trade_date']}  close={r['close']:.4f}")
            summary_rows.append(("fx_rates", "yfinance", inserted,
                                  str(fx_from), str(today)))

    if "mf_holdings" in categories:
        from src.importer.fetchers.mf_holdings_fetcher import fetch_holdings

        # NOTE: mstarpy.Funds.holdings() has NO date parameter — it always returns
        # the current Morningstar snapshot. We tag rows with the current month so
        # running this monthly builds a genuine time-series going forward.
        as_of_month = mf_holdings_month or date(today.year, today.month, 1)

        console.print(
            f"\n[bold cyan]▶ MF Holdings[/bold cyan] "
            f"({len(MF_HOLDINGS_WATCHLIST)} funds · snapshot as of {as_of_month})"
        )

        # Skip if this month's snapshot already exists (unless forced)
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
            holdings_rows = fetch_holdings(MF_HOLDINGS_WATCHLIST, as_of_month)
            if not holdings_rows:
                console.print("  [yellow]⚠ No holdings returned — mstarpy may be unavailable.[/yellow]")
            else:
                inserted = ch.insert_mf_holdings(holdings_rows, dry_run=dry_run)
                console.print(
                    f"  [green]✓[/green] {inserted} rows "
                    f"{'(dry-run)' if dry_run else 'stored'} for {as_of_month}"
                )
                if not dry_run:
                    ch.set_watermark("mf_holdings", "ALL", as_of_month)
                summary_rows.append(("mf_holdings", "morningstar", inserted,
                                     str(as_of_month), str(as_of_month)))

    # ── FII / DII Institutional Flows ─────────────────────────────────────────
    # ── Earnings ──────────────────────────────────────────────────────────────
    if "earnings" in categories:
        from src.importer.registry import US_STOCKS
        from src.importer.fetchers.earnings_fetcher import fetch_earnings

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
        from src.importer.registry import US_STOCKS
        from src.importer.fetchers.insider_fetcher import fetch_insider_trades

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
        from src.importer.registry import US_STOCKS
        from src.importer.fetchers.valuation_fetcher import fetch_valuation

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

    # ── World Bank Macro Indicators ───────────────────────────────────────────
    if "world_bank" in categories:
        from src.importer.fetchers.worldbank_macro_fetcher import fetch_worldbank_macro

        console.print("\n[bold cyan]▶ World Bank WDI — Macro Indicators[/bold cyan]")
        console.print("  [dim]India + G4 peers · GDP growth, CPI, current account, "
                      "govt debt, savings, FDI, unemployment, exports · annual · no auth[/dim]")

        wb_wm = ch.get_watermark("world_bank", "MACRO_GROUP") if (not dry_run and not full_reimport) else None
        wb_from_year = (wb_wm.year - 1) if wb_wm else 2000   # overlap 1 year for revisions
        wb_rows = fetch_worldbank_macro(from_year=wb_from_year, to_year=today.year)

        if not wb_rows:
            console.print("  [yellow]⚠ No World Bank data returned — API may be unavailable.[/yellow]")
        else:
            inserted = ch.insert_macro_indicators(wb_rows) if not dry_run else len(wb_rows)
            console.print(f"  [green]✓[/green] {inserted} macro indicator rows {'(dry-run)' if dry_run else 'stored'}")
            if not dry_run:
                max_year = max(int(r["ref_year"]) for r in wb_rows)
                ch.set_watermark("world_bank", "MACRO_GROUP", date(max_year, 12, 31))
            # Show latest India GDP growth as a quick sanity check
            india_gdp = [r for r in wb_rows if r["country_code"] == "IN"
                         and r["indicator_code"] == "NY.GDP.MKTP.KD.ZG"]
            if india_gdp:
                latest_gdp = sorted(india_gdp, key=lambda r: r["ref_year"])[-1]
                console.print(
                    f"  India GDP growth ({latest_gdp['ref_year']}): "
                    f"{latest_gdp['value']:+.2f}%"
                )
            summary_rows.append(("world_bank", "world_bank", inserted,
                                  str(wb_from_year), str(today.year)))

    # ── IMF WEO Projections ───────────────────────────────────────────────────
    if "imf_weo" in categories:
        from src.importer.fetchers.imf_weo_fetcher import fetch_imf_weo

        console.print("\n[bold cyan]▶ IMF World Economic Outlook[/bold cyan]")
        console.print("  [dim]India + G4 peers · GDP, CPI, fiscal balance, "
                      "current account, unemployment · annual + 3-year forecasts · no auth[/dim]")

        imf_wm = ch.get_watermark("imf_weo", "MACRO_GROUP") if (not dry_run and not full_reimport) else None
        imf_from_year = (imf_wm.year - 1) if imf_wm else 2000   # overlap 1 year for WEO revisions
        imf_to_year   = today.year + 3                            # include forward projections
        imf_rows = fetch_imf_weo(from_year=imf_from_year, to_year=imf_to_year)

        if not imf_rows:
            console.print("  [yellow]⚠ No IMF WEO data returned — DataMapper API may be unavailable.[/yellow]")
        else:
            inserted = ch.insert_macro_indicators(imf_rows) if not dry_run else len(imf_rows)
            console.print(f"  [green]✓[/green] {inserted} WEO rows {'(dry-run)' if dry_run else 'stored'}")
            if not dry_run:
                actual_rows = [r for r in imf_rows if not r.get("is_forecast", 0)]
                if actual_rows:
                    max_actual_year = max(int(r["ref_year"]) for r in actual_rows)
                    ch.set_watermark("imf_weo", "MACRO_GROUP", date(max_actual_year, 12, 31))
            # Show India GDP growth forecast
            india_gdp = [r for r in imf_rows if r["country_code"] == "IN"
                         and r["indicator_code"] == "NGDP_RPCH"
                         and r["ref_year"] >= today.year]
            if india_gdp:
                forecasts = sorted(india_gdp, key=lambda r: r["ref_year"])[:3]
                fwd_str = "  ".join(f"{r['ref_year']}={r['value']:+.1f}%" for r in forecasts)
                console.print(f"  India GDP forecasts: {fwd_str}")
            summary_rows.append(("imf_weo", "imf_weo", inserted,
                                  str(imf_from_year), str(imf_to_year)))

    # ── Indian Macro Indicators ───────────────────────────────────────────────
    if "indian_macro" in categories:
        from src.importer.fetchers.indian_macro_fetcher import IndianMacroFetcher

        console.print("\n[bold cyan]▶ Indian Macro Indicators[/bold cyan]")
        console.print("  [dim]Scrapes industry and macro indicators (source: Tijori Finance)[/dim]")

        fetcher = IndianMacroFetcher()
        from_date = _resolve_from_date(
            ch, fetcher.source_name, fetcher.symbol_key,
            lookback_days=lookback_days, full_reimport=full_reimport,
            dry_run=dry_run, today=today,
        )

        console.print(f"  [dim]Fetching {from_date} → {today}…[/dim]")
        rows = fetcher.fetch_with_retry(from_date, today)

        if not rows:
            console.print("  [yellow]⚠ No Indian macro data returned.[/yellow]")
        else:
            inserted = fetcher.insert(rows, ch) if not dry_run else len(rows)
            console.print(f"  [green]✓[/green] {inserted} rows {'(dry-run)' if dry_run else 'stored'}")
            if not dry_run:
                try:
                    max_dt = fetcher.max_date(rows)
                    ch.set_watermark(fetcher.source_name, fetcher.symbol_key, max_dt)
                except Exception as e:
                    logger.warning("Failed to update watermark for indian_macro: %s", e)
            summary_rows.append(("indian_macro", "tijori", inserted,
                                  from_date.isoformat(), today.isoformat()))

    if "fii_dii" in categories:
        from src.importer.fetchers.fii_dii_fetcher import (
            fetch_fii_dii,
            fetch_fii_dii_fno,
            fetch_fii_dii_monthly
        )

        console.print("\n[bold cyan]▶ FII / DII Institutional Flows[/bold cyan]")
        console.print(
            "  [dim]NSE provisional cash-market data — "
            "FII & DII gross buy/sell/net in ₹ Crore[/dim]"
        )

        fii_from = _resolve_from_date(
            ch, "nse_fii_dii", "MARKET",
            lookback_days=lookback_days, full_reimport=full_reimport,
            dry_run=dry_run, today=today,
        )

        console.print(f"  [dim]Fetching {fii_from} → {today}[/dim]")
        
        # 1. Daily Cash Flows
        fii_rows = fetch_fii_dii(from_date=fii_from)
        if fii_rows:
            inserted = ch.insert_fii_dii_flows(fii_rows, dry_run=dry_run)
            console.print(f"  [green]✓[/green] {inserted} flow rows stored")
            if not dry_run:
                ch.set_watermark("nse_fii_dii", "MARKET", max(r["trade_date"] for r in fii_rows))
            latest_fii = sorted(fii_rows, key=lambda r: r["trade_date"])[-1]
            console.print(
                f"  Latest Cash ({latest_fii['trade_date']}): "
                f"FII Net ₹{latest_fii['fii_net_cr']:+,.0f} Cr  |  "
                f"DII Net ₹{latest_fii['dii_net_cr']:+,.0f} Cr"
            )
            summary_rows.append((
                "fii_dii", "nse", inserted,
                str(min(r["trade_date"] for r in fii_rows)),
                str(latest_fii["trade_date"]),
            ))
        else:
            console.print("  [yellow]⚠ No daily cash rows returned.[/yellow]")

        # 2. Daily F&O Participant OI
        fno_rows = fetch_fii_dii_fno(from_date=fii_from)
        if fno_rows:
            inserted = ch.insert_fii_dii_fno_daily(fno_rows, dry_run=dry_run)
            console.print(f"  [green]✓[/green] {inserted} F&O OI rows stored")
            latest_fno = sorted(fno_rows, key=lambda r: r["trade_date"])[-1]
            console.print(
                f"  Latest F&O ({latest_fno['trade_date']}): "
                f"FII Fut Net {latest_fno['fii_fut_net_oi']:+,.0f} | "
                f"FII Opt OI {latest_fno['fii_opt_overall_net_oi']:+,.0f}"
            )
        else:
            console.print("  [yellow]⚠ No F&O rows returned.[/yellow]")

        # 3. Monthly Aggregates (Always full sync for monthly as it's small)
        monthly_rows = fetch_fii_dii_monthly()
        if monthly_rows:
            inserted = ch.insert_fii_dii_monthly(monthly_rows, dry_run=dry_run)
            console.print(f"  [green]✓[/green] {inserted} monthly aggregate rows stored")
        else:
            console.print("  [yellow]⚠ No monthly rows returned.[/yellow]")

    if "amfi_flows" in categories:
        from src.importer.fetchers.amfi_flows_fetcher import fetch_amfi_category_flows

        console.print("\n[bold cyan]▶ AMFI Category-Wise Monthly Flows + AUM[/bold cyan]")
        console.print(
            "  [dim]AMFI industry data — monthly net flows & AUM by fund category[/dim]"
        )

        amfi_wm = ch.get_watermark("amfi_category_flows", "INDUSTRY", dataset="flows")
        months_back = 120 if full_reimport else 24
        if amfi_wm is not None and not full_reimport:
            today_m = date.today().replace(day=1)
            wm_m = amfi_wm.replace(day=1)
            diff_months = (today_m.year - wm_m.year) * 12 + (today_m.month - wm_m.month)
            months_back = max(2, diff_months + 1)

        console.print(f"  [dim]Fetching last {months_back} months[/dim]")

        amfi_rows = fetch_amfi_category_flows(months_back=months_back)
        if amfi_rows:
            inserted = ch.insert_amfi_category_flows(amfi_rows, dry_run=dry_run)
            console.print(f"  [green]✓[/green] {inserted} category flow rows stored")
            if not dry_run:
                latest_month = max(r["report_month"] for r in amfi_rows)
                ch.set_watermark("amfi_category_flows", "INDUSTRY", latest_month, dataset="flows")
            latest_m = max(r["report_month"] for r in amfi_rows)
            latest_rows = [r for r in amfi_rows if r["report_month"] == latest_m]
            latest_rows.sort(key=lambda r: r["net_flow_cr"], reverse=True)
            console.print(f"  Latest month: [bold]{latest_m.strftime('%b %Y')}[/bold]")
            for r in latest_rows[:3]:
                sign = "+" if r["net_flow_cr"] >= 0 else ""
                console.print(
                    f"    {r['category_name'][:30]:<30} "
                    f"Net: ₹{sign}{r['net_flow_cr']:,.0f} Cr  |  "
                    f"AUM: ₹{r['closing_aum_cr']:,.0f} Cr"
                )
            summary_rows.append((
                "amfi_flows", "amfi", inserted,
                str(min(r["report_month"] for r in amfi_rows)),
                str(max(r["report_month"] for r in amfi_rows)),
            ))
        else:
            console.print(
                "  [yellow]⚠ No AMFI category flow rows returned. "
                "Set AMFI_EXCEL_URL in .env as manual fallback.[/yellow]"
            )


    # ── AMC Fund-Holdings Importers ───────────────────────────────────────────
    _amc_cats = [c for c in ("icici", "nippon", "icici-index", "dsp", "bajaj", "quant") if c in categories]
    if _amc_cats:
        from src.importer.fetchers.amc_holdings_fetcher import fetch_amc_holdings

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
