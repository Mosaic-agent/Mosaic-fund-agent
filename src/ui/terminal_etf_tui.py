"""
src/ui/terminal_etf_tui.py
──────────────────────────
High-framerate, flicker-free terminal dashboard for the Real-Time ETF Opportunity
& iNAV Arbitrage Scanner powered by Rich.
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any

from rich.console import Console, Group
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

console = Console()


def create_etf_tui_layout() -> Layout:
    """Create the layout grid for the ETF opportunity scanner."""
    layout = Layout()
    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="main", ratio=1),
        Layout(name="footer", size=3),
    )
    return layout


def build_etf_tui_renderable(
    rows: list[dict[str, Any]],
    ws_connected: bool,
    active_tokens: int,
    amc_age_secs: float,
    last_update_time: str,
    top_alerts: list[str] | None = None,
) -> Panel:
    """Render the full composite ETF scanner dashboard as a rich Panel."""
    # 1. Header Bar
    status_color = "green" if ws_connected else "yellow"
    status_text = "LIVE WS CONNECTED" if ws_connected else "FALLBACK REST POLLING"

    header_text = Text()
    header_text.append("⚡ MOSAIC LIVE ETF OPPORTUNITY SCANNER ", style="bold cyan")
    header_text.append("│ ", style="dim")
    header_text.append(f"● {status_text} ", style=f"bold {status_color}")
    header_text.append("│ ", style="dim")
    header_text.append(f"Tokens: {active_tokens} ", style="bold white")
    header_text.append("│ ", style="dim")
    header_text.append(f"AMC Cache: {amc_age_secs:.0f}s ago ", style="cyan")
    header_text.append("│ ", style="dim")
    header_text.append(f"Updated: {last_update_time}", style="dim")

    # 2. Main Table
    table = Table(
        show_header=True,
        header_style="bold magenta",
        expand=True,
        box=None,
        padding=(0, 1),
    )

    table.add_column("Symbol", style="bold white", width=12)
    table.add_column("Category", style="dim", width=18)
    table.add_column("AMC", style="cyan", width=14)
    table.add_column("Live LTP", justify="right", style="bold", width=10)
    table.add_column("AMC iNAV", justify="right", width=10)
    table.add_column("Prem / Disc %", justify="right", width=16)
    table.add_column("Day Chg", justify="right", width=9)
    table.add_column("Order Flow Delta", justify="right", width=16)
    table.add_column("Signal Action", justify="center", width=16)

    # Sort rows by spread_pct ascending (deepest discount first)
    sorted_rows = sorted(
        rows,
        key=lambda r: r.get("spread_pct") if r.get("spread_pct") is not None else 999.0,
    )

    bargain_count = 0
    for r in sorted_rows:
        sym = r.get("symbol", "")
        cat = r.get("category", "")
        amc = r.get("amc", "")
        ltp = r.get("ltp")
        inav = r.get("inav")
        spread = r.get("spread_pct")
        day_chg = r.get("day_chg_pct", 0.0)
        cum_delta = r.get("cumulative_delta", 0.0)
        signal = r.get("signal", "⚪ FAIR")

        ltp_str = f"₹{ltp:.2f}" if ltp is not None else "-"
        inav_str = f"₹{inav:.2f}" if inav is not None else "-"

        # Explicit Premium vs Discount % formatting & color
        if spread is not None:
            if spread < -0.001:
                spread_str = f"[bold green]{abs(spread):.2f}% Discount[/bold green]"
            elif spread > 0.001:
                if spread > 1.0:
                    spread_str = f"[bold red]{spread:.2f}% Premium[/bold red]"
                else:
                    spread_str = f"[red]{spread:.2f}% Premium[/red]"
            else:
                spread_str = "[dim]0.00% Par[/dim]"
        else:
            spread_str = "-"

        # Day change formatting
        if day_chg > 0:
            chg_str = f"[green]+{day_chg:.2f}%[/green]"
        elif day_chg < 0:
            chg_str = f"[red]{day_chg:.2f}%[/red]"
        else:
            chg_str = f"[dim]0.00%[/dim]"

        # Cumulative Delta formatting
        if cum_delta > 0:
            delta_str = f"[bold green]+{cum_delta:,.0f} ▲[/bold green]"
        elif cum_delta < 0:
            delta_str = f"[red]{cum_delta:,.0f} ▼[/red]"
        else:
            delta_str = "[dim]0[/dim]"

        # Signal formatting
        if "ACCUMULATE" in signal:
            sig_str = f"[bold white on dark_green] {signal} [/bold white on dark_green]"
            bargain_count += 1
        elif "PASSIVE" in signal:
            sig_str = f"[black on yellow] {signal} [/black on yellow]"
        elif "OVERHEATED" in signal or "OVERPRICED" in signal:
            sig_str = f"[white on dark_red] {signal} [/white on dark_red]"
        else:
            sig_str = f"[dim]{signal}[/dim]"

        table.add_row(
            sym,
            cat,
            amc,
            ltp_str,
            inav_str,
            spread_str,
            chg_str,
            delta_str,
            sig_str,
        )

    # 3. Footer Summary
    footer_text = Text()
    if bargain_count > 0:
        footer_text.append(f"🔥 {bargain_count} ETF(s) in ACTIVE ACCUMULATION ZONE! ", style="bold green")
    else:
        footer_text.append("⚖️ No extreme discounts currently detected. Market spreads in fair zone. ", style="dim white")
    footer_text.append("│ Press Ctrl+C to stop scanner", style="dim cyan")

    return Panel(
        Group(
            header_text,
            Text(""),
            table,
            Text(""),
            footer_text,
        ),
        title="[bold cyan]Mosaic Real-Time Market Intelligence[/bold cyan]",
        border_style="cyan",
    )
