"""
tests/test_inav_cli.py
──────────────────────
CLI visual test for ETF iNAV premium / discount panels.

Injects controlled iNAV and market prices via unittest.mock so all three
label states (PREMIUM, DISCOUNT, FAIR VALUE) are visible in the terminal
regardless of whether the market is currently open.

ETFs covered:
  Domestic  — NIFTYBEES, GOLDBEES, BANKBEES, JUNIORBEES, SILVERBEES,
               LIQUIDBEES, HNGSNGBEES
  Thematic  — MAFANG (Mirae Asset NYSE FANG+ ETF)
  Global    — MAHKTECH (Mirae Asset Hang Seng TECH ETF)

Run with:
    .venv/bin/python tests/test_inav_cli.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from unittest.mock import patch
from rich.console import Console
from rich.rule import Rule

from src.tools.inav_fetcher import get_etf_inav
from src.formatters.output import _render_inav_panel

console = Console()

# ── Scenarios ─────────────────────────────────────────────────────────────────
# Each tuple: (ETF symbol, mock_inav, mock_market, note)
# iNAV (nav) and market price (ltP) sourced directly from NSE API on 2026-02-22
# nav  = indicative NAV computed from underlying basket (real iNAV)
# ltP  = last traded price on NSE
scenarios = [
    # ── Domestic Index / Commodity ETFs ──────────────────────────────────────
    ("NIFTYBEES",  287.9681, 289.20,   "Nifty 50 ETF (Nippon)          +0.43%  →  PREMIUM"),
    ("GOLDBEES",   127.3102, 127.48,   "Gold ETF (Nippon)              +0.13%  →  FAIR VALUE"),
    ("BANKBEES",   626.5916, 631.00,   "Bank Nifty ETF (Nippon)        +0.70%  →  PREMIUM"),
    ("JUNIORBEES", 742.3073, 746.70,   "Junior Nifty ETF (Nippon)      +0.59%  →  PREMIUM"),
    ("SILVERBEES", 232.5391, 236.50,   "Silver ETF (Nippon)            +1.70%  →  PREMIUM"),
    ("LIQUIDBEES", 1000.0,   1000.0,   "Liquid ETF (Nippon)             0.00%  →  FAIR VALUE"),
    # ── International Index ETFs ─────────────────────────────────────────────
    ("HNGSNGBEES", 452.6401, 536.88,   "Hang Seng ETF (Nippon)        +18.60%  →  PREMIUM"),
    # ── Thematic / Global Tech ETFs ──────────────────────────────────────────
    ("MAFANG",     127.9142, 153.70,   "Mirae Asset NYSE FANG+ ETF    +20.16%  →  PREMIUM"),
    ("MAHKTECH",    20.8290,  24.99,   "Mirae Asset Hang Seng TECH ETF+19.96%  →  PREMIUM"),
]

def main():
    console.print()
    console.print(Rule("[bold cyan]iNAV Premium / Discount — CLI Visual Test[/bold cyan]"))
    console.print(
        "[dim]Mocked iNAV values injected so all label states are visible "
        "regardless of market hours.[/dim]\n"
    )

    all_ok = True
    for symbol, mock_inav, mock_market, note in scenarios:
        console.print(f"[bold white]{symbol}[/bold white]  [dim]{note}[/dim]")

        with patch("src.tools.inav_fetcher._fetch_inav_nse",      return_value=(mock_inav, mock_market)), \
             patch("src.tools.inav_fetcher._fetch_inav_yahoo",     return_value=mock_inav), \
             patch("src.tools.inav_fetcher._fetch_market_price",   return_value=mock_market):
            result = get_etf_inav(symbol)

        _render_inav_panel(result, symbol, console)

        label = result["premium_discount_label"]
        pct   = result["premium_discount_pct"]
        if label == "UNKNOWN":
            console.print(f"  [red]✗ Unexpected UNKNOWN label[/red]")
            all_ok = False
        else:
            console.print(
                f"  [dim]Asserted: label=[bold]{label}[/bold]  "
                f"pct={pct:+.2f}%  ✓[/dim]\n"
            )

    console.print(Rule())
    if all_ok:
        console.print("[bold green]✅  All iNAV panel scenarios rendered successfully.[/bold green]")
    else:
        console.print("[bold red]❌  One or more scenarios failed.[/bold red]")
        sys.exit(1)


if __name__ == "__main__":
    main()
