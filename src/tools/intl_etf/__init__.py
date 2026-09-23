"""
src/tools/intl_etf/__init__.py
──────────────────────────────
Consolidated International ETF Arbitrage, Scarcity Premium & Backtesting Module.
"""

from src.tools.intl_etf.models import (
    INTL_ETF_SYMBOLS,
    ROUND_TRIP_COST_PCT,
    STCG_EQUITY_RATE,
    LTCG_EQUITY_RATE,
    ArbitrageSignal,
    VolumeRegime,
    LiveSnapshot,
    BacktestSummary,
)
from src.tools.intl_etf.classifier import ArbitrageClassificationStrategy
from src.tools.intl_etf.engine import IntlETFEngine
from src.tools.intl_etf.presenter import IntlETFPresenter

__all__ = [
    "INTL_ETF_SYMBOLS",
    "ROUND_TRIP_COST_PCT",
    "STCG_EQUITY_RATE",
    "LTCG_EQUITY_RATE",
    "ArbitrageSignal",
    "VolumeRegime",
    "LiveSnapshot",
    "BacktestSummary",
    "ArbitrageClassificationStrategy",
    "IntlETFEngine",
    "IntlETFPresenter",
]
