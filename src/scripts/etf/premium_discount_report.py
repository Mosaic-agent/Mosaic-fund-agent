import sys
import argparse
from pathlib import Path
from collections import defaultdict
from rich.console import Console
from rich.table import Table

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent))

from src.db.pool import query_df
from src.utils.ist import fmt_ist, now_ist
from src.agents.comex_agent import ComexAgent
from src.tools.macro_event_scanner import scan_macro_events

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
    
    # ── PART 1: ETF PREMIUM VS DISCOUNT ──
    report.append("## 1. ETF Premium vs Discount Status")
    report.append("This section lists all 32 tracked ETFs, sorted from the highest premium to the deepest discount relative to their indicative Net Asset Value (iNAV).")
    report.append("")
    report.append("| Symbol | AMC | iNAV (₹) | Market Price (₹) | Premium/Discount (%) | Source | Last Seen (IST) |")
    report.append("| :--- | :---: | :---: | :---: | :---: | :---: | :---: |")
    
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
            
            sign = "+" if premium > 0 else ""
            report.append(
                f"| **{symbol}** | {amc} | "
                f"₹{inav:,.4f} | "
                f"₹{price:,.4f} | "
                f"{sign}{premium:.3f}% | "
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

def generate_report(sort_order="desc", group_by_amc=False, refresh=False, consolidated=False):
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
        
    if group_by_amc:
        # Group by AMC
        amc_groups = {}
        for _, row in df.iterrows():
            symbol = row['symbol']
            amc = get_amc(symbol)
            if amc not in amc_groups:
                amc_groups[amc] = []
            amc_groups[amc].append(row)
            
        for amc, rows in sorted(amc_groups.items()):
            table = Table(title=f"Latest iNAV & Market Prices — {amc}", show_header=True, header_style="bold cyan")
            table.add_column("Symbol", style="yellow", justify="left")
            table.add_column("iNAV (₹)", justify="right")
            table.add_column("Market Close (₹)", justify="right")
            table.add_column("Prem/Disc (%)", justify="right")
            table.add_column("Source", justify="center")
            table.add_column("Last Seen (IST)", justify="center")
            
            for r in rows:
                premium = r['latest_premium']
                color = "green" if premium > 0 else "red"
                sign = "+" if premium > 0 else ""
                table.add_row(
                    r['symbol'],
                    f"₹{r['latest_inav']:,.4f}" if r['latest_inav'] is not None else "N/A",
                    f"₹{r['latest_price']:,.4f}" if r['latest_price'] is not None else "N/A",
                    f"[{color}]{sign}{premium:.3f}%[/{color}]" if premium is not None else "N/A",
                    str(r['data_source']),
                    fmt_ist(r['last_seen'])
                )
            console.print(table)
            console.print()
    else:
        # Flat sorted table
        table = Table(title=f"ETF Premium/Discount Standings (Sorted: {sort_order.upper()})", show_header=True, header_style="bold magenta")
        table.add_column("Symbol", style="cyan", justify="left")
        table.add_column("iNAV (₹)", justify="right")
        table.add_column("Market Price (₹)", justify="right")
        table.add_column("Prem/Disc (%)", justify="right")
        table.add_column("Source", justify="center")
        table.add_column("Last Seen (IST)", justify="center")
        
        for _, row in df.iterrows():
            premium = row['latest_premium']
            symbol = row['symbol']
            inav = row['latest_inav']
            price = row['latest_price']
            source = row['data_source']
            last_seen = fmt_ist(row['last_seen'])
            
            color = "green" if premium > 0 else "red"
            sign = "+" if premium > 0 else ""
            table.add_row(
                symbol,
                f"₹{inav:,.4f}" if inav is not None else "N/A",
                f"₹{price:,.4f}" if price is not None else "N/A",
                f"[{color}]{sign}{premium:.3f}%[/{color}]" if premium is not None else "N/A",
                str(source),
                last_seen
            )
        console.print(table)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETF iNAV Premium/Discount Report Generator")
    parser.add_argument("--sort", type=str, choices=["desc", "asc"], default="desc", help="Sort order for premium (default: desc)")
    parser.add_argument("--group-by-amc", action="store_true", help="Group ETFs by their AMC")
    parser.add_argument("--refresh", action="store_true", help="Refresh live iNAV snapshots before displaying report")
    parser.add_argument("--consolidated", action="store_true", help="Compile and display a consolidated report (iNAV premium/discount + COMEX + Macro)")
    
    args = parser.parse_args()
    generate_report(sort_order=args.sort, group_by_amc=args.group_by_amc, refresh=args.refresh, consolidated=args.consolidated)
