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

logger = logging.getLogger(__name__)

# Days of overlap when doing a delta sync (to catch weekend / late corrections)
_OVERLAP_DAYS = 3


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

    if console is None:
        console = Console()

    # Expand "all" shorthand
    if "all" in categories:
        categories = ALL_CATEGORIES

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

            # Determine date range
            # Use a single from_date for the whole category (worst-case watermark)
            if full_reimport:
                from_date = today - timedelta(days=lookback_days)
            else:
                # Find earliest watermark across all symbols in this category
                earliest: date | None = None
                for nse_sym, _ in symbol_list:
                    wm = ch.get_watermark("yfinance", nse_sym) if not dry_run else None
                    if wm is None:
                        # Never imported — need full lookback
                        earliest = today - timedelta(days=lookback_days)
                        break
                    candidate = wm - timedelta(days=_OVERLAP_DAYS)
                    if earliest is None or candidate < earliest:
                        earliest = candidate
                from_date = earliest or (today - timedelta(days=lookback_days))

            progress.update(task, description=f"Downloading {category} {from_date}→{today}…")
            rows = fetch_ohlcv(symbol_list, category, from_date, today)

        inserted = ch.insert_prices(rows, dry_run=dry_run)
        console.print(f"  [green]✓[/green] {inserted} rows {'(dry-run)' if dry_run else 'inserted'}")

        # Update watermarks
        if not dry_run:
            symbols_seen = {r["symbol"] for r in rows}
            for sym in symbols_seen:
                sym_dates = [r["trade_date"] for r in rows if r["symbol"] == sym]
                if sym_dates:
                    ch.set_watermark("yfinance", sym, max(sym_dates))

        summary_rows.append((
            category,
            "yfinance",
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
            if not dry_run:
                symbols_seen = {r["symbol"] for r in eod_rows}
                for sym in symbols_seen:
                    sym_dates = [r["trade_date"] for r in eod_rows if r["symbol"] == sym]
                    if sym_dates:
                        ch.set_watermark("nse_quote", sym, max(sym_dates))
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

    # ── Yahoo Live Snapshot (DXY, etc.) ───────────────────────────────────────
    if "dxy_live" in categories:
        from src.importer.fetchers.yahoo_snapshot_fetcher import fetch_yahoo_snapshots
        dxy_symbols = [("DXY", "DX-Y.NYB")]

        console.print(f"\n[bold cyan]▶ YAHOO LIVE SNAPSHOTS[/bold cyan] (DXY)")
        console.print("  [dim]Live snapshot from Yahoo Finance (DXY every 5m)[/dim]")

        snapshot_rows = fetch_yahoo_snapshots(dxy_symbols)
        if not snapshot_rows:
            console.print("  [yellow]⚠ Yahoo returned no data for DXY.[/yellow]")
        else:
            inserted = ch.insert_inav_snapshots(snapshot_rows, dry_run=dry_run)
            console.print(f"  [green]✓[/green] {inserted} snapshot(s) {'(dry-run)' if dry_run else 'stored'}")
            from datetime import datetime
            ts = snapshot_rows[0]["snapshot_at"]
            summary_rows.append(("dxy_live", "yahoo", inserted, str(ts)[:10], str(ts)[:10]))
    # ── MF NAV ────────────────────────────────────────────────────────────
    if "mf" in categories:
        console.print(f"\n[bold cyan]▶ MF NAV[/bold cyan] ({len(MF_SCHEME_CODES)} schemes)")

        if full_reimport:
            mf_from = today - timedelta(days=lookback_days)
        else:
            earliest_mf: date | None = None
            for sym in MF_SCHEME_CODES:
                wm = ch.get_watermark("mfapi", sym) if not dry_run else None
                if wm is None:
                    earliest_mf = today - timedelta(days=lookback_days)
                    break
                candidate = wm - timedelta(days=_OVERLAP_DAYS)
                if earliest_mf is None or candidate < earliest_mf:
                    earliest_mf = candidate
            mf_from = earliest_mf or (today - timedelta(days=lookback_days))

        console.print(f"  [dim]Fetching {mf_from} → {today} (MFAPI.in, polite delays)[/dim]")
        nav_rows = fetch_all_nav(MF_SCHEME_CODES, mf_from, today)
        inserted = ch.insert_nav(nav_rows, dry_run=dry_run)
        console.print(f"  [green]✓[/green] {inserted} rows {'(dry-run)' if dry_run else 'inserted'}")

        if not dry_run:
            syms_seen = {r["symbol"] for r in nav_rows}
            for sym in syms_seen:
                sym_dates = [r["nav_date"] for r in nav_rows if r["symbol"] == sym]
                if sym_dates:
                    ch.set_watermark("mfapi", sym, max(sym_dates))

        summary_rows.append(("mf", "mfapi", inserted, mf_from.isoformat(), today.isoformat()))

    # ── CFTC COT (hedge fund positioning) ────────────────────────────────────
    if "cot" in categories:
        from src.importer.fetchers.cot_fetcher import fetch_cot_gold

        console.print("\n[bold cyan]▶ CFTC COT — Gold (Managed Money)[/bold cyan]")
        console.print("  [dim]CFTC Disaggregated report, commodity code 088 (released Fridays)[/dim]")

        cot_wm = ch.get_watermark("cot", "GOLD") if not dry_run else None
        cot_from = (cot_wm - timedelta(days=21)) if cot_wm else None   # 3-week overlap
        cot_rows = fetch_cot_gold(from_date=cot_from)
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

        # Use per-pair watermarks for delta sync
        fx_from = today - timedelta(days=lookback_days)
        if not full_reimport:
            earliest_fx: date | None = None
            for sym, _ in FX_PAIRS:
                wm = ch.get_watermark("yfinance_fx", sym) if not dry_run else None
                if wm is None:
                    earliest_fx = today - timedelta(days=lookback_days)
                    break
                candidate = wm - timedelta(days=_OVERLAP_DAYS)
                if earliest_fx is None or candidate < earliest_fx:
                    earliest_fx = candidate
            fx_from = earliest_fx or (today - timedelta(days=lookback_days))

        console.print(f"  [dim]Fetching {fx_from} → {today}[/dim]")
        fx_rows = fetch_fx_rates(from_date=fx_from, to_date=today)
        if not fx_rows:
            console.print("  [yellow]⚠ No FX data returned — Yahoo Finance may be unavailable.[/yellow]")
        else:
            inserted = ch.insert_fx_rates(fx_rows, dry_run=dry_run)
            console.print(f"  [green]✓[/green] {inserted} FX rate rows {'(dry-run)' if dry_run else 'stored'}")
            if not dry_run:
                for sym, _ in FX_PAIRS:
                    sym_dates = [r["trade_date"] for r in fx_rows if r["symbol"] == sym]
                    if sym_dates:
                        ch.set_watermark("yfinance_fx", sym, max(sym_dates))
            # Print latest close per pair
            from collections import defaultdict
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
    if "fii_dii" in categories:
        from src.importer.fetchers.fii_dii_fetcher import fetch_fii_dii

        console.print("\n[bold cyan]▶ FII / DII Institutional Flows[/bold cyan]")
        console.print(
            "  [dim]NSE provisional cash-market data — "
            "FII & DII gross buy/sell/net in ₹ Crore[/dim]"
        )

        fii_wm = ch.get_watermark("nse_fii_dii", "MARKET") if not dry_run else None
        if full_reimport or fii_wm is None:
            fii_from = today - timedelta(days=lookback_days)
        else:
            fii_from = fii_wm - timedelta(days=_OVERLAP_DAYS)

        console.print(f"  [dim]Fetching {fii_from} → {today}[/dim]")
        fii_rows = fetch_fii_dii(from_date=fii_from)

        if not fii_rows:
            console.print(
                "  [yellow]⚠ No FII/DII data returned — "
                "NSE API may be unavailable or market is closed.[/yellow]"
            )
        else:
            inserted = ch.insert_fii_dii_flows(fii_rows, dry_run=dry_run)
            console.print(
                f"  [green]✓[/green] {inserted} flow rows "
                f"{'(dry-run)' if dry_run else 'stored'}"
            )
            if not dry_run:
                ch.set_watermark(
                    "nse_fii_dii", "MARKET",
                    max(r["trade_date"] for r in fii_rows),
                )
            latest_fii = sorted(fii_rows, key=lambda r: r["trade_date"])[-1]
            console.print(
                f"  Latest ({latest_fii['trade_date']}): "
                f"FII Net ₹{latest_fii['fii_net_cr']:+,.0f} Cr  |  "
                f"DII Net ₹{latest_fii['dii_net_cr']:+,.0f} Cr"
            )
            summary_rows.append((
                "fii_dii", "nse",
                inserted,
                str(min(r["trade_date"] for r in fii_rows)),
                str(latest_fii["trade_date"]),
            ))

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
