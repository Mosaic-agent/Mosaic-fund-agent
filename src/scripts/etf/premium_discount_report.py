import sys
import argparse
from pathlib import Path
from collections import defaultdict
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent))

from src.db.pool import query_df
from src.utils.ist import fmt_ist, now_ist
from src.agents.comex_agent import ComexAgent
from src.tools.macro_event_scanner import scan_macro_events

INTL_SYMBOLS = {"MAFANG", "HNGSNGBEES", "MON100", "MASPTOP50", "MAHKTECH", "MONQ50"}

def classify_arbitrage_signal(symbol: str, premium_pct: float | None, inav: float | None, market_price: float | None) -> tuple[str, str, float | None, str]:
    """
    Classify ETF arbitrage signal.
    Returns: (signal_label, rich_style, parity_gap_pct, actionable_directive)
    """
    if premium_pct is None or inav is None or market_price is None or market_price <= 0:
        return ("⚪ NO DATA", "dim", None, "Hold")

    parity_gap = ((inav - market_price) / market_price) * 100
    is_intl = symbol.upper() in INTL_SYMBOLS

    if premium_pct > 40.0:
        return ("💥 BUBBLE (LIQUIDATE)", "bold white on red", parity_gap, "LIQUIDATE / DO NOT BUY")
    elif premium_pct > 25.0:
        return ("🚨 ARBITRAGE EXIT (SELL)", "bold red", parity_gap, "EXIT / SELL LONG")
    elif premium_pct > 12.0:
        return ("⚠️ CAUTION (OVERPRICED)", "bold yellow", parity_gap, "TRIM / AVOID ENTRY")
    elif is_intl and premium_pct > 5.0:
        return ("🟡 SCARCITY PREMIUM", "yellow", parity_gap, "HOLD WITH TRAILING STOP")
    elif not is_intl and premium_pct > 0.8:
        return ("⚠️ TRANSIENT PREMIUM", "yellow", parity_gap, "AVOID FRESH ENTRY")
    elif premium_pct < -1.5:
        return ("🟢 ARBITRAGE BUY (DEEP DISCOUNT)", "bold green", parity_gap, "STRONG BUY / ARBITRAGE ENTRY")
    elif premium_pct < -0.4:
        return ("🟢 ARBITRAGE ENTRY (DISCOUNT)", "green", parity_gap, "ACCUMULATE AT DISCOUNT")
    else:
        return ("⚪ FAIR VALUE (PARITY)", "cyan", parity_gap, "HOLD AT FAIR VALUE")

def get_amc(symbol):
    symbol = symbol.upper()
    if any(x in symbol for x in ["BEES", "CPSE"]):
        return "Nippon India Mutual Fund"
    elif "CASE" in symbol:
        return "Zerodha Mutual Fund"
    elif any(x in symbol for x in ["MAFANG", "MAHKTECH", "MASPTOP50", "SMALL250", "SMALLCAP"]):
        return "Mirae Asset Mutual Fund"
    elif "MON" in symbol:
        return "Motilal Oswal Mutual Fund"
    elif "HDFC" in symbol:
        return "HDFC Mutual Fund"
    elif "SETF" in symbol:
        return "SBI Mutual Fund"
    elif any(x in symbol for x in ["ICICI", "FMCGIETF"]):
        return "ICICI Prudential Mutual Fund"
    else:
        return "Other / Unclassified"

def refresh_inav_data(console):
    console.print("[yellow]Refreshing live iNAV snapshots from NSE...[/yellow]")
    import subprocess
    try:
        # Run import command via mosaic CLI inside container
        cmd = [sys.executable, "src/main.py", "import", "--category", "inav"]
        subprocess.run(cmd, check=True)
        console.print("[green]✓ Fresh iNAV data imported successfully.[/green]")
    except Exception as e:
        console.print(f"[red]Warning: failed to refresh iNAV data: {e}[/red]")

def generate_consolidated_report(console):
    report = []
    report.append("# Consolidated Market Report")
    report.append(f"Generated on: {fmt_ist(now_ist())}")
    report.append("")
    report.append("---")
    report.append("")
    
    # ── PART 1: ETF PREMIUM VS DISCOUNT & ARBITRAGE SIGNALS ──
    report.append("## 1. ETF Premium vs Discount Status & Arbitrage Signals")
    report.append("This section lists all 32 tracked ETFs, sorted from the highest premium to the deepest discount relative to their indicative Net Asset Value (iNAV), with actionable Arbitrage Entry / Exit directives.")
    report.append("")
    report.append("| Symbol | AMC | iNAV (₹) | Market Price (₹) | Premium (%) | Parity Gap (%) | Arbitrage Signal | Source | Last Seen (IST) |")
    report.append("| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |")
    
    sql = """
    SELECT 
        symbol,
        argMax(inav, snapshot_at) as latest_inav,
        argMax(market_price, snapshot_at) as latest_price,
        argMax(premium_discount_pct, snapshot_at) as latest_premium,
        argMax(source, snapshot_at) as data_source,
        argMax(snapshot_at, snapshot_at) as last_seen
    FROM market_data.inav_snapshots FINAL
    GROUP BY symbol
    ORDER BY latest_premium DESC
    """
    try:
        df_premium = query_df(sql)
        for _, row in df_premium.iterrows():
            symbol = row['symbol']
            amc = get_amc(symbol)
            inav = row['latest_inav']
            price = row['latest_price']
            premium = row['latest_premium']
            source = row['data_source']
            last_seen = fmt_ist(row['last_seen'])
            
            sig_label, _, parity_gap, directive = classify_arbitrage_signal(symbol, premium, inav, price)
            sign = "+" if premium > 0 else ""
            parity_str = f"{parity_gap:+.2f}%" if parity_gap is not None else "—"
            report.append(
                f"| **{symbol}** | {amc} | "
                f"₹{inav:,.4f} | "
                f"₹{price:,.4f} | "
                f"{sign}{premium:.3f}% | "
                f"{parity_str} | "
                f"{sig_label} | "
                f"`{source}` | "
                f"{last_seen} |"
            )
    except Exception as e:
        report.append(f"Failed to query iNAV premiums: {e}")
    report.append("")
    report.append("---")
    report.append("")
    
    # ── PART 2: COMEX COMMODITY TRENDS ──
    report.append("## 2. COMEX Commodity Pre-Market Signals")
    report.append("Pre-market commodity trend tracking based on live spot prices compared against the previous day's futures close.")
    report.append("")
    report.append("| Symbol | Name | Signal | Change (%) | Live Price |")
    report.append("| :---: | :--- | :---: | :---: | :---: |")
    
    try:
        comex_report = ComexAgent().run()
        commodities = comex_report.get("commodities", {})
        for sym, c in commodities.items():
            sig = c.get("signal", "UNKNOWN")
            chg = c.get("change_pct", 0.0)
            live = c.get("live_price", 0.0)
            sign = "+" if chg > 0 else ""
            report.append(
                f"| **{sym}** | {c.get('name', sym)} | "
                f"{sig} | "
                f"{sign}{chg:.3f}% | "
                f"${live:,.2f} |"
            )
    except Exception as e:
        report.append(f"Failed to fetch COMEX details: {e}")
    report.append("")
    report.append("---")
    report.append("")
    
    # ── PART 3: MACRO GEOPOLITICAL NEWS & THEMES ──
    report.append("## 3. Macro Geopolitical Scanner")
    report.append("Summary of active geopolitical and macroeconomic themes affecting ETF assets.")
    report.append("")
    
    try:
        macro_report = scan_macro_events(max_per_theme=4)
        
        # Display themes and headlines
        report.append("### Active Themes and Mapped ETF Impacts")
        report.append("")
        
        by_theme = defaultdict(list)
        for ev in macro_report.events:
            by_theme[ev.theme].append(ev)
            
        for theme_name, events in sorted(by_theme.items()):
            icon = events[0].icon
            conviction = events[0].conviction
            report.append(f"#### {icon} {theme_name} (Conviction: {conviction})")
            report.append(f"**Why it matters**: *{events[0].transmission}*")
            report.append("")
            report.append("**Recent Headlines**:")
            for ev in events:
                report.append(f"- **{ev.headline}** — *{ev.source}* ({ev.published_at[:16]})")
            report.append("")
            
            impact_map = events[0].impact
            bullish = [etf for etf, d in impact_map.items() if d == +1]
            bearish = [etf for etf, d in impact_map.items() if d == -1]
            if bullish:
                report.append(f"- 🟢 **Bullish Mapped ETFs**: {', '.join(bullish)}")
            if bearish:
                report.append(f"- 🔴 **Bearish Mapped ETFs**: {', '.join(bearish)}")
            report.append("")
            
        # Display Aggregated ETF signals
        report.append("### Aggregated ETF Macro Sentiment Signals")
        report.append("")
        report.append("| ETF Symbol | Net Article Flow | Signal |")
        report.append("| :--- | :---: | :---: |")
        
        # Sort signals
        net = macro_report.etf_net_signal
        sorted_etfs = sorted(net.items(), key=lambda x: x[1], reverse=True)
        for sym, score in sorted_etfs:
            if score >= 16:
                sig = "STRONG BULLISH"
            elif score >= 8:
                sig = "BULLISH"
            elif score <= -16:
                sig = "STRONG BEARISH"
            elif score <= -8:
                sig = "BEARISH"
            else:
                sig = "NEUTRAL"
            report.append(f"| **{sym}** | {score} | {sig} |")
            
    except Exception as e:
        report.append(f"Failed to fetch macro news details: {e}")
        
    markdown_content = "\n".join(report)
    console.print(markdown_content)
    
    # Save both locally and inside container
    output_path = Path("/app/output/consolidated_market_report.md")
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(markdown_content)
        console.print(f"\n[green]✓ Report written to {output_path}[/green]")
    except Exception as exc:
        console.print(f"\n[red]Failed to write report: {exc}[/red]")

def generate_report(sort_order="desc", group_by_amc=False, refresh=False, consolidated=False, arbitrage=False):
    console = Console()
    
    if refresh:
        refresh_inav_data(console)
        
    if consolidated:
        generate_consolidated_report(console)
        return
        
    sql = """
    SELECT 
        symbol,
        argMax(inav, snapshot_at) as latest_inav,
        argMax(market_price, snapshot_at) as latest_price,
        argMax(premium_discount_pct, snapshot_at) as latest_premium,
        argMax(source, snapshot_at) as data_source,
        argMax(snapshot_at, snapshot_at) as last_seen
    FROM market_data.inav_snapshots FINAL
    GROUP BY symbol
    ORDER BY latest_premium DESC
    """
    if sort_order == "asc":
        sql = sql.replace("DESC", "ASC")
        
    try:
        df = query_df(sql)
    except Exception as e:
        console.print(f"[red]Error querying ClickHouse: {e}[/red]")
        sys.exit(1)
        
    if df.empty:
        console.print("[yellow]No iNAV snapshots found in ClickHouse. Run 'python src/main.py import --category inav' first.[/yellow]")
        return

    # Load volume metrics map from daily_prices
    try:
        df_vol = query_df("""
            SELECT 
                symbol,
                argMax(volume, trade_date) as latest_vol,
                round(argMax(volume * close, trade_date) / 10000000, 2) as turnover_cr,
                round(avg(volume), 0) as avg_vol_20d,
                round(argMax(volume, trade_date) / nullif(avg(volume), 0), 2) as vol_multiple
            FROM (
                SELECT symbol, trade_date, close, volume
                FROM market_data.daily_prices FINAL
                WHERE trade_date >= today() - 35
                ORDER BY symbol, trade_date DESC
            )
            GROUP BY symbol
        """)
        vol_map = {r['symbol']: r for _, r in df_vol.iterrows()}
    except Exception:
        vol_map = {}

    # Add arbitrage classification to every row
    classified = []
    for _, row in df.iterrows():
        sig_label, sig_style, parity_gap, directive = classify_arbitrage_signal(
            row['symbol'], row['latest_premium'], row['latest_inav'], row['latest_price']
        )
        row_dict = dict(row)
        row_dict['sig_label'] = sig_label
        row_dict['sig_style'] = sig_style
        row_dict['parity_gap'] = parity_gap
        row_dict['directive'] = directive

        vinfo = vol_map.get(row['symbol'])
        if vinfo is not None:
            row_dict['turnover_cr'] = float(vinfo.get('turnover_cr') or 0.0)
            row_dict['vol_multiple'] = float(vinfo.get('vol_multiple') or 1.0)
        else:
            row_dict['turnover_cr'] = 0.0
            row_dict['vol_multiple'] = 1.0

        classified.append(row_dict)

    if arbitrage:
        # Filter to actionable signals only (exclude fair value hold)
        classified = [r for r in classified if "FAIR VALUE" not in r['sig_label']]

        # Print Executive Arbitrage Alert Dashboard
        console.print(
            Panel(
                "[bold cyan]⚡ Arbitrage Execution Engine — Live Entry / Exit Directives[/bold cyan]\n"
                "[dim]Exploits structural scarcity vs fair value iNAV parity with volume confirmation. "
                "Enter when underpriced or on deep mean-reversion dips; "
                "Exit/Liquidate when inflated into dangerous bubble premiums.[/dim]",
                border_style="magenta",
            )
        )
        
        # Summary lists
        entries = [r for r in classified if "ENTRY" in r['sig_label'] or "BUY" in r['sig_label']]
        exits   = [r for r in classified if "EXIT" in r['sig_label'] or "BUBBLE" in r['sig_label']]

        console.print("[bold green]🟢 Active Arbitrage Entry Opportunities (Discount / Dip Accumulation):[/bold green]")
        if entries:
            for e in entries:
                console.print(f"  • [bold]{e['symbol']}[/bold]: Prem {e['latest_premium']:+.2f}% | Parity Gap {e['parity_gap']:+.2f}% | Turnover ₹{e['turnover_cr']:.2f}Cr ({e['vol_multiple']:.2f}x Vol) ➔ [{e['sig_style']}]{e['sig_label']}[/{e['sig_style']}] ({e['directive']})")
        else:
            console.print("  [dim]• None currently trading at deep entry discounts.[/dim]")

        console.print("\n[bold red]🚨 Active Arbitrage Exit & Liquidation Alerts (High Downside Crash Risk):[/bold red]")
        if exits:
            for x in exits:
                console.print(f"  • [bold]{x['symbol']}[/bold]: Prem {x['latest_premium']:+.2f}% | Parity Gap {x['parity_gap']:+.2f}% | Turnover ₹{x['turnover_cr']:.2f}Cr ({x['vol_multiple']:.2f}x Vol) ➔ [{x['sig_style']}]{x['sig_label']}[/{x['sig_style']}] ({x['directive']})")
        else:
            console.print("  [dim]• None currently in extreme bubble territory.[/dim]")
            
        console.print()

    if group_by_amc:
        # Group by AMC
        amc_groups = defaultdict(list)
        for r in classified:
            amc = get_amc(r['symbol'])
            amc_groups[amc].append(r)
            
        for amc, rows in sorted(amc_groups.items()):
            table = Table(title=f"Latest iNAV & Arbitrage Directives — {amc}", show_header=True, header_style="bold cyan")
            table.add_column("Symbol", style="yellow", justify="left")
            table.add_column("iNAV (₹)", justify="right")
            table.add_column("Market Price (₹)", justify="right")
            table.add_column("Prem (%)", justify="right")
            table.add_column("Parity Gap", justify="right")
            table.add_column("Turnover", justify="right")
            table.add_column("Vol Mult", justify="right")
            table.add_column("Arbitrage Signal", justify="left")
            table.add_column("Source", justify="center")
            table.add_column("Last Seen (IST)", justify="center")
            
            for r in rows:
                premium = r['latest_premium']
                color = "green" if premium > 0 else "red"
                sign = "+" if premium > 0 else ""
                parity = f"{r['parity_gap']:+.2f}%" if r['parity_gap'] is not None else "—"
                par_col = "green" if (r['parity_gap'] or 0) >= 0 else ("red" if (r['parity_gap'] or 0) < -15 else "yellow")
                
                table.add_row(
                    r['symbol'],
                    f"₹{r['latest_inav']:,.4f}" if r['latest_inav'] is not None else "N/A",
                    f"₹{r['latest_price']:,.4f}" if r['latest_price'] is not None else "N/A",
                    f"[{color}]{sign}{premium:.3f}%[/{color}]" if premium is not None else "N/A",
                    f"[{par_col}]{parity}[/{par_col}]",
                    f"₹{r.get('turnover_cr', 0.0):.2f}Cr",
                    f"{r.get('vol_multiple', 1.0):.2f}x",
                    f"[{r['sig_style']}]{r['sig_label']}[/{r['sig_style']}]",
                    str(r['data_source']),
                    fmt_ist(r['last_seen'])
                )
            console.print(table)
            console.print()
    else:
        # Flat sorted table
        tbl_title = "Actionable ETF Arbitrage Signals" if arbitrage else f"ETF Premium/Discount & Arbitrage Standings (Sorted: {sort_order.upper()})"
        table = Table(title=tbl_title, show_header=True, header_style="bold magenta")
        table.add_column("Symbol", style="cyan", justify="left")
        table.add_column("iNAV (₹)", justify="right")
        table.add_column("Market Price (₹)", justify="right")
        table.add_column("Prem (%)", justify="right")
        table.add_column("Parity Gap", justify="right")
        table.add_column("Turnover", justify="right")
        table.add_column("Vol Mult", justify="right")
        table.add_column("Arbitrage Signal", justify="left")
        table.add_column("Source", justify="center")
        table.add_column("Last Seen (IST)", justify="center")
        
        for r in classified:
            premium = r['latest_premium']
            symbol = r['symbol']
            inav = r['latest_inav']
            price = r['latest_price']
            source = r['data_source']
            last_seen = fmt_ist(r['last_seen'])
            
            color = "green" if premium > 0 else "red"
            sign = "+" if premium > 0 else ""
            parity = f"{r['parity_gap']:+.2f}%" if r['parity_gap'] is not None else "—"
            par_col = "green" if (r['parity_gap'] or 0) >= 0 else ("red" if (r['parity_gap'] or 0) < -15 else "yellow")

            table.add_row(
                symbol,
                f"₹{inav:,.4f}" if inav is not None else "N/A",
                f"₹{price:,.4f}" if price is not None else "N/A",
                f"[{color}]{sign}{premium:.3f}%[/{color}]" if premium is not None else "N/A",
                f"[{par_col}]{parity}[/{par_col}]",
                f"₹{r.get('turnover_cr', 0.0):.2f}Cr",
                f"{r.get('vol_multiple', 1.0):.2f}x",
                f"[{r['sig_style']}]{r['sig_label']}[/{r['sig_style']}]",
                str(source),
                last_seen
            )
        console.print(table)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETF iNAV Premium/Discount Report & Arbitrage Engine")
    parser.add_argument("--sort", type=str, choices=["desc", "asc"], default="desc", help="Sort order for premium (default: desc)")
    parser.add_argument("--group-by-amc", action="store_true", help="Group ETFs by their AMC")
    parser.add_argument("--refresh", action="store_true", help="Refresh live iNAV snapshots before displaying report")
    parser.add_argument("--consolidated", action="store_true", help="Compile and display a consolidated report (iNAV premium/discount + COMEX + Macro)")
    parser.add_argument("--arbitrage", "-a", action="store_true", help="Filter and display actionable arbitrage entry and exit signals")
    
    args = parser.parse_args()
    generate_report(sort_order=args.sort, group_by_amc=args.group_by_amc, refresh=args.refresh, consolidated=args.consolidated, arbitrage=args.arbitrage)
