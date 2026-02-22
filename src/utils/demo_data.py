"""
src/utils/demo_data.py
───────────────────────
Sample portfolio holdings used by the --demo flag.
Represents a realistic mix of NSE stocks and ETFs for testing the
full analysis pipeline without Zerodha MCP authentication.
"""

from __future__ import annotations

from src.models.portfolio import Holding, InstrumentType, Portfolio


def get_demo_holdings() -> list[Holding]:
    """
    Return a sample portfolio of 6 NSE holdings (4 stocks + 2 ETFs).
    Prices are approximate — Yahoo Finance will fetch live current prices.
    """
    return [
        Holding(
            tradingsymbol="RELIANCE",
            exchange="NSE",
            isin="INE002A01018",
            quantity=10,
            average_price=1350.0,
            last_price=1419.4,
            instrument_type=InstrumentType.STOCK,
        ),
        Holding(
            tradingsymbol="TCS",
            exchange="NSE",
            isin="INE467B01029",
            quantity=5,
            average_price=3200.0,
            last_price=2686.2,
            instrument_type=InstrumentType.STOCK,
        ),
        Holding(
            tradingsymbol="HDFCBANK",
            exchange="NSE",
            isin="INE040A01034",
            quantity=15,
            average_price=1550.0,
            last_price=1710.0,
            instrument_type=InstrumentType.STOCK,
        ),
        Holding(
            tradingsymbol="INFY",
            exchange="NSE",
            isin="INE009A01021",
            quantity=8,
            average_price=1700.0,
            last_price=1850.0,
            instrument_type=InstrumentType.STOCK,
        ),
        Holding(
            tradingsymbol="NIFTYBEES",
            exchange="NSE",
            isin="INF204KB16I2",
            quantity=50,
            average_price=240.0,
            last_price=255.0,
            instrument_type=InstrumentType.ETF,
        ),
        Holding(
            tradingsymbol="GOLDBEES",
            exchange="NSE",
            isin="INF204KB16F8",
            quantity=30,
            average_price=55.0,
            last_price=62.0,
            instrument_type=InstrumentType.ETF,
        ),
    ]


def get_demo_portfolio() -> Portfolio:
    return Portfolio(holdings=get_demo_holdings(), profile_name="Demo User")
