"""
src/formatters/output.py
─────────────────────────
Formats and saves the final portfolio intelligence report.

Supports:
  • JSON file output  (always generated)
  • Rich terminal     (pretty-printed table summary on screen)

Output files are saved to OUTPUT_DIR (from config, default: ./output/).
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import Any

from rich.columns import Columns
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich import box

from config.settings import settings

logger = logging.getLogger(__name__)


# ── File Output ───────────────────────────────────────────────────────────────

def save_json_report(report: dict[str, Any]) -> str:
    """
    Save the portfolio report as a formatted JSON file.

    Args:
        report: Portfolio report dict.

    Returns:
        Absolute path to the saved file.
    """
    os.makedirs(settings.output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"portfolio_report_{timestamp}.json"
    filepath = os.path.join(settings.output_dir, filename)

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    logger.info("Report saved to %s", filepath)
    return filepath


# ── Terminal Output ───────────────────────────────────────────────────────────

def _sentiment_color(score: float) -> str:
    if score > 0.3:
        return "green"
    if score < -0.3:
        return "red"
    return "yellow"


def _risk_color(score: float) -> str:
    if score >= 7:
        return "red"
    if score >= 5:
        return "yellow"
    return "green"


def _rec_color(rec: str) -> str:
    return {"BUY": "green", "SELL": "red", "HOLD": "yellow", "WATCH": "cyan"}.get(
        rec.upper(), "white"
    )


def _pnl_color(pct: str) -> str:
    try:
        val = float(pct.strip("%"))
        return "green" if val >= 0 else "red"
    except (ValueError, AttributeError):
        return "white"


def _render_inav_panel(inav: dict, symbol: str, console: Console) -> None:
    """
    Render a colour-coded iNAV premium/discount panel for an ETF holding.
    Only called when inav["is_etf"] is True.
    """
    label = inav.get("premium_discount_label") or "UNKNOWN"
    pct = inav.get("premium_discount_pct")
    inav_val = inav.get("inav")
    mkt = inav.get("market_price")
    source = inav.get("source") or "—"

    color_map = {
        "PREMIUM":    "yellow",
        "DISCOUNT":   "green",
        "FAIR VALUE": "cyan",
        "UNKNOWN":    "white",
    }
    color = color_map.get(label, "white")

    pct_str = f"{pct:+.2f}%" if pct is not None else "N/A"
    nav_str = f"\u20b9{inav_val:,.4f}" if inav_val else "N/A"
    mkt_str = f"\u20b9{mkt:,.2f}" if mkt else "N/A"

    tip_map = {
        "PREMIUM":    "\u26a0  ETF trading above NAV — consider waiting to buy more.",
        "DISCOUNT":   "\u2705 ETF trading below NAV — potential buying opportunity.",
        "FAIR VALUE": "\u2705 ETF trading close to fair value.",
    }
    tip = tip_map.get(label, "")

    content = (
        f"[bold]iNAV (per unit):[/bold]      {nav_str}\n"
        f"[bold]Market Price:[/bold]         {mkt_str}\n"
        f"[bold]Premium / Discount:[/bold]   [{color}]{pct_str}  ◀  {label}[/{color}]\n"
        f"[bold]Data Source:[/bold]          {source}\n"
    )
    if tip:
        content += f"\n[italic dim]{tip}[/italic dim]"

    console.print(
        Panel(
            content.strip(),
            title=f"[bold]iNAV Analysis — {symbol}[/bold]",
            border_style=color,
            padding=(0, 1),
        )
    )


def _render_historic_inav_panel(hist: dict, symbol: str, console: Console) -> None:
    """
    Render a 30-day historic iNAV sparkline + stats panel for an ETF.
    """
    avg_pct   = hist.get("avg_premium_discount_pct")
    avg_label = hist.get("avg_label", "UNKNOWN")
    trend     = hist.get("trend", "STABLE")
    spark     = hist.get("sparkline", "")
    n_points  = hist.get("data_points", 0)
    from_dt   = hist.get("from_date", "")
    to_dt     = hist.get("to_date", "")
    max_prem  = hist.get("max_premium", {})
    max_disc  = hist.get("max_discount", {})
    records   = hist.get("records", [])

    color_map = {"PREMIUM": "yellow", "DISCOUNT": "green", "FAIR VALUE": "cyan", "UNKNOWN": "white"}
    avg_color = color_map.get(avg_label, "white")

    trend_icon = {"WIDENING": "↑", "NARROWING": "↓", "STABLE": "→"}.get(trend, "")
    trend_color = {"WIDENING": "yellow", "NARROWING": "green", "STABLE": "cyan"}.get(trend, "white")

    avg_str = f"{avg_pct:+.2f}%" if avg_pct is not None else "N/A"

    # Build a mini table of the 5 most recent records
    recent = records[-5:] if len(records) >= 5 else records
    table_lines = []
    for r in reversed(recent):   # newest first
        lbl   = r.get("label", "")
        lcolor = color_map.get(lbl, "white")
        pct_s  = f"{r['premium_discount_pct']:+.2f}%"
        table_lines.append(
            f"  {r['date']}   iNAV ₹{r['nav']:>8.2f}   Close ₹{r['market_close']:>8.2f}   "
            f"[{lcolor}]{pct_s:>7}  {lbl}[/{lcolor}]"
        )

    content = (
        f"[bold]Period:[/bold]           {from_dt} → {to_dt}  ({n_points} trading days)\n"
        f"[bold]Avg Premium/Disc:[/bold]  [{avg_color}]{avg_str}  ◀  {avg_label}[/{avg_color}]\n"
        f"[bold]Trend:[/bold]             [{trend_color}]{trend_icon} {trend}[/{trend_color}]\n"
        f"[bold]Peak Premium:[/bold]      {max_prem.get('date', 'N/A')}  [{color_map['PREMIUM']}]{max_prem.get('pct', 'N/A'):+.2f}%[/{color_map['PREMIUM']}]\n"
        f"[bold]Peak Discount:[/bold]     {max_disc.get('date', 'N/A')}  [{color_map['DISCOUNT']}]{max_disc.get('pct', 'N/A'):+.2f}%[/{color_map['DISCOUNT']}]\n"
        f"\n[bold]30-Day P/D Sparkline:[/bold]  {spark}\n"
        f"\n[bold dim]Last 5 Trading Days:[/bold dim]\n"
    )
    content += "\n".join(table_lines)

    console.print(
        Panel(
            content.strip(),
            title=f"[bold]📈 Historic iNAV (30d) — {symbol}[/bold]",
            border_style="dim " + avg_color,
            padding=(0, 1),
        )
    )


def _render_comex_panel(comex: dict, console: Console) -> None:
    """
    Render a COMEX commodity pre-market signal panel.

    Shows live vs previous-close prices for Gold, Silver, Platinum,
    Palladium, Copper and a per-commodity BULLISH/BEARISH/NEUTRAL signal.
    """
    if comex.get("error"):
        console.print(
            Panel(
                f"[dim]{comex['error']}[/dim]",
                title="[bold]🌐 COMEX Pre-Market Signals[/bold]",
                border_style="dim",
            )
        )
        return

    overall  = comex.get("overall_signal", "UNKNOWN")
    summary  = comex.get("summary", "")
    run_time = comex.get("run_time_ist", "")
    pre_mkt  = comex.get("pre_market", False)
    commodities = comex.get("commodities", {})

    signal_color = {
        "STRONG BULLISH": "bright_green",
        "BULLISH":        "green",
        "NEUTRAL":        "yellow",
        "BEARISH":        "red",
        "STRONG BEARISH": "bright_red",
        "UNKNOWN":        "white",
    }
    signal_icon = {
        "STRONG BULLISH": "⬆⬆",
        "BULLISH":        "↑",
        "NEUTRAL":        "→",
        "BEARISH":        "↓",
        "STRONG BEARISH": "⬇⬇",
        "UNKNOWN":        "?",
    }

    pre_mkt_note = "  [italic dim](pre-market — NSE not yet open)[/italic dim]" if pre_mkt else ""
    overall_clr  = signal_color.get(overall, "white")
    overall_icon = signal_icon.get(overall, "?")

    # Build commodity rows
    rows: list[str] = []
    for sym, c in commodities.items():
        sig   = c.get("signal", "UNKNOWN")
        clr   = signal_color.get(sig, "white")
        icon  = signal_icon.get(sig, "?")
        name  = c.get("name", sym)
        emoji = c.get("emoji", "")
        live  = c.get("live_price")
        prev  = c.get("prev_close")
        chg   = c.get("change_pct")
        unit  = c.get("unit", "")
        etfs  = c.get("nse_etfs", [])

        live_str  = f"${live:,.4f}" if live is not None else "N/A"
        prev_str  = f"${prev:,.4f}" if prev is not None else "N/A"
        chg_str   = f"{chg:+.3f}%" if chg is not None else "N/A"
        etf_str   = f"  NSE: {', '.join(etfs)}" if etfs else ""

        rows.append(
            f"  {emoji} [bold]{name} ({sym})[/bold]  [{clr}]{icon} {sig}[/{clr}]\n"
            f"     Live: {live_str}   Prev Close: {prev_str}   "
            f"Change: [{clr}]{chg_str}[/{clr}]   Unit: {unit}{etf_str}"
        )

    body = (
        f"[bold]Overall Signal:[/bold]  [{overall_clr}]{overall_icon} {overall}[/{overall_clr}]"
        f"{pre_mkt_note}\n"
        f"[dim]{summary}[/dim]\n\n"
        + "\n\n".join(rows)
        + (f"\n\n[dim]Run time: {run_time}[/dim]" if run_time else "")
    )

    border = signal_color.get(overall, "dim")
    console.print(
        Panel(
            body.strip(),
            title="[bold]🌐 COMEX Pre-Market Signals[/bold]",
            border_style=border,
            padding=(0, 1),
        )
    )
    console.print()


def print_report_to_console(report: dict[str, Any], console: Console | None = None) -> None:
    """
    Pretty-print the portfolio intelligence report to the terminal using Rich.

    Args:
        report:  Portfolio report dict.
        console: Rich Console instance (creates a new one if None).
    """
    if console is None:
        console = Console()

    summary = report.get("portfolio_summary", {})
    holdings = report.get("holdings_analysis", [])
    risks = report.get("portfolio_risks", [])
    insights = report.get("actionable_insights", [])
    sector_alloc = report.get("sector_allocation", {})
    generated_at = report.get("generated_at", "")

    console.print()
    console.rule("[bold blue]📊 PORTFOLIO INTELLIGENCE REPORT[/bold blue]")
    if generated_at:
        console.print(f"[dim]Generated: {generated_at}[/dim]", justify="center")
    console.print()

    # ── COMEX Pre-Market Signals ──────────────────────────────────────────────
    comex = report.get("comex_signals")
    if comex:
        _render_comex_panel(comex, console)

    # ── Portfolio Summary Panel ───────────────────────────────────────────────
    pnl_pct = summary.get("total_pnl_percent", "0%")
    pnl_color = _pnl_color(pnl_pct)

    summary_text = (
        f"[bold]Total Value:[/bold]       {summary.get('total_value', 'N/A')}\n"
        f"[bold]Total Invested:[/bold]    {summary.get('total_invested', 'N/A')}\n"
        f"[bold]Total P&L:[/bold]         [{pnl_color}]{summary.get('total_pnl', 'N/A')} "
        f"({pnl_pct})[/{pnl_color}]\n"
        f"[bold]Holdings:[/bold]          {summary.get('num_holdings', 0)} "
        f"({summary.get('stock_count', 0)} stocks, {summary.get('etf_count', 0)} ETFs)\n"
        f"[bold]Direct Equity:[/bold]     {summary.get('direct_equity_allocation_pct', 0):.1f}%  "
        f"[bold]ETF:[/bold] {summary.get('etf_allocation_pct', 0):.1f}%\n"
        f"[bold]Health Score:[/bold]      {summary.get('health_score', 0):.1f}/100\n"
        f"[bold]Diversification:[/bold]   {summary.get('diversification_score', 0):.1f}/100"
    )
    console.print(Panel(summary_text, title="[bold]Portfolio Overview[/bold]", border_style="blue"))

    # ── Holdings Table ────────────────────────────────────────────────────────
    table = Table(
        title="Holdings Analysis",
        box=box.ROUNDED,
        show_header=True,
        header_style="bold magenta",
        border_style="dim",
    )
    table.add_column("Symbol", style="bold white", min_width=12)
    table.add_column("Type", min_width=6)
    table.add_column("Qty", justify="right", min_width=6)
    table.add_column("Avg Price ₹", justify="right", min_width=10)
    table.add_column("Curr Price ₹", justify="right", min_width=12)
    table.add_column("P&L %", justify="right", min_width=8)
    table.add_column("Sentiment", justify="center", min_width=10)
    table.add_column("Risk", justify="center", min_width=6)
    table.add_column("Signal", justify="center", min_width=8)
    table.add_column("Sector", min_width=18)

    for h in holdings:
        pnl = h.get("pnl_percent", 0)
        pnl_str = f"{pnl:+.1f}%"
        sent = h.get("sentiment_score", 0.0)
        risk = h.get("risk_score", 5.0)
        rec = h.get("recommendation", "")
        rec_str = f"[{_rec_color(rec)}]{rec}[/{_rec_color(rec)}]" if rec else "[dim]—[/dim]"

        table.add_row(
            h.get("symbol", ""),
            h.get("instrument_type", "STOCK")[:5],
            str(h.get("quantity", 0)),
            f"{h.get('average_buy_price', 0):,.2f}",
            f"{h.get('current_price', 0):,.2f}",
            f"[{'green' if pnl >= 0 else 'red'}]{pnl_str}[/{'green' if pnl >= 0 else 'red'}]",
            f"[{_sentiment_color(sent)}]{sent:+.2f}[/{_sentiment_color(sent)}]",
            f"[{_risk_color(risk)}]{risk:.0f}[/{_risk_color(risk)}]",
            rec_str,
            h.get("sector", "Unknown"),
        )

    console.print(table)
    console.print()

    # ── Per-Holding Insights ──────────────────────────────────────────────────
    for h in holdings:
        symbol = h.get("symbol", "")
        summary_text = h.get("summary", "")
        insights_list = h.get("key_insights", [])
        risk_signals = h.get("risk_signals", [])
        news = h.get("key_news", [])
        qr = h.get("latest_results", {})

        content = ""
        if summary_text:
            content += f"[italic]{summary_text}[/italic]\n\n"

        if insights_list:
            content += "[bold]Key Insights:[/bold]\n"
            content += "\n".join(f"  • {i}" for i in insights_list) + "\n"

        if risk_signals:
            content += "\n[bold red]Risk Signals:[/bold red]\n"
            content += "\n".join(f"  ⚠ {r}" for r in risk_signals) + "\n"

        if qr and qr.get("revenue_cr"):
            content += (
                f"\n[bold]Latest Results ({qr.get('period', 'N/A')}):[/bold] "
                f"Revenue ₹{qr.get('revenue_cr', 0):,.0f}Cr | "
                f"Profit ₹{qr.get('net_profit_cr', 0):,.0f}Cr | "
                f"Revenue YoY: {qr.get('revenue_yoy_pct', 0):+.1f}%\n"
            )

        if news:
            content += "\n[bold]Recent News:[/bold]\n"
            for n in news[:3]:
                sent_icon = "🟢" if n.get("sentiment") == "POSITIVE" else (
                    "🔴" if n.get("sentiment") == "NEGATIVE" else "🟡"
                )
                content += f"  {sent_icon} {n.get('title', '')[:80]}... [{n.get('source', '')}]\n"

        if content:
            console.print(
                Panel(
                    content.strip(),
                    title=f"[bold]{symbol}[/bold]",
                    border_style="dim cyan",
                )
            )

        # iNAV panel — only rendered for ETFs
        inav = h.get("inav_analysis")
        if inav and inav.get("is_etf"):
            _render_inav_panel(inav, symbol, console)

        # Historic iNAV panel — 30-day AMFI sparkline
        hist = h.get("historic_inav")
        if hist and not hist.get("error"):
            _render_historic_inav_panel(hist, symbol, console)

    # ── Sector Allocation ─────────────────────────────────────────────────────
    if sector_alloc:
        sector_table = Table(
            title="Sector Allocation",
            box=box.SIMPLE,
            show_header=True,
            header_style="bold",
        )
        sector_table.add_column("Sector", min_width=25)
        sector_table.add_column("Allocation %", justify="right", min_width=12)
        sector_table.add_column("Bar", min_width=30)

        for sector, pct in sorted(sector_alloc.items(), key=lambda x: x[1], reverse=True):
            bar_len = int(pct / 2)
            bar = "█" * bar_len
            color = "red" if pct > 40 else ("yellow" if pct > 25 else "green")
            sector_table.add_row(sector, f"{pct:.1f}%", f"[{color}]{bar}[/{color}]")

        console.print(sector_table)
        console.print()

    # ── Portfolio Risks ───────────────────────────────────────────────────────
    if risks:
        risk_text = "\n".join(f"  ⚠ {r}" for r in risks)
        console.print(Panel(risk_text, title="[bold red]Portfolio Risks[/bold red]", border_style="red"))
        console.print()

    # ── Actionable Insights ───────────────────────────────────────────────────
    if insights:
        insight_text = "\n".join(f"  → {i}" for i in insights)
        console.print(Panel(insight_text, title="[bold green]Actionable Insights[/bold green]", border_style="green"))
        console.print()

    # ── Rebalancing Signals ───────────────────────────────────────────────────
    rebalancing = report.get("rebalancing_signals", [])
    if rebalancing:
        rebal_text = "\n".join(f"  ⟳ {s}" for s in rebalancing)
        console.print(
            Panel(rebal_text, title="[bold yellow]Rebalancing Signals[/bold yellow]", border_style="yellow")
        )
        console.print()

    console.rule("[dim]End of Report[/dim]")
    console.print()
