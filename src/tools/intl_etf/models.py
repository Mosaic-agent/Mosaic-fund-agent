"""
src/tools/intl_etf/models.py
────────────────────────────
Domain models and constants for International ETF Scarcity Premium & Arbitrage.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Any, Optional
from pydantic import BaseModel, Field


# International ETFs affected by the RBI $7B overseas investment cap
INTL_ETF_SYMBOLS: list[str] = [
    "MAFANG",
    "HNGSNGBEES",
    "MON100",
    "MASPTOP50",
    "MAHKTECH",
    "MONQ50",
]

# Cost & Tax Constants (Post-Budget July 2024: Overseas equity ETFs taxed as equity)
ROUND_TRIP_COST_PCT: float = 0.10  # Brokerage + STT + Exchange turnover + Stamp duty
STCG_EQUITY_RATE: float = 0.208    # 20% + 4% cess
LTCG_EQUITY_RATE: float = 0.130    # 12.5% + 4% cess


class ArbitrageSignal(str, Enum):
    BUBBLE = "BUBBLE"          # Premium > 40% or Z >= 2.5
    EXIT = "EXIT"              # Premium >= 25% or Z >= 1.8
    TRIM = "TRIM"              # Premium > 12% or Z >= 1.0
    HOLD = "HOLD"              # Normal historical scarcity range
    ACCUMULATE = "ACCUMULATE"  # Z <= -1.0
    ENTRY = "ENTRY"            # Z <= -1.5 or Premium < -0.5%


class VolumeRegime(str, Enum):
    CLIMAX_DISTRIBUTION = "🔥 CLIMAX DISTRIBUTION"
    CIRCUIT_FROZEN = "🔒 CIRCUIT FROZEN"
    DEEP_LIQUIDITY = "🌊 DEEP LIQUIDITY"
    NORMAL_LIQUIDITY = "✅ NORMAL LIQUIDITY"
    THIN_LIQUIDITY = "⚠️ THIN LIQUIDITY"


class LiveSnapshot(BaseModel):
    symbol: str
    latest_premium: Optional[float] = None
    mean_premium: Optional[float] = None
    std_premium: Optional[float] = None
    z_score: Optional[float] = None
    market_price: Optional[float] = None
    inav: Optional[float] = None
    downside_risk_to_inav_pct: float = 0.0
    latest_volume: float = 0.0
    turnover_cr: float = 0.0
    avg_vol_20d: float = 0.0
    vol_multiple: float = 1.0
    volume_regime: str = VolumeRegime.NORMAL_LIQUIDITY.value
    action: str = "⚪ FAIR VALUE (HOLD)"
    action_style: str = "cyan"
    arbitrage_signal: str = ArbitrageSignal.HOLD.value
    tax_class: str = "equity"
    expected_reversion_pct: Optional[float] = None
    half_life_days: Optional[float] = None
    prob_revert_10d: Optional[float] = None
    inav_source: Optional[str] = None
    history_source: Optional[str] = None
    n_snapshots: int = 0
    error: Optional[str] = None


@dataclass
class BacktestSummary:
    symbol: str
    total_rows: int
    min_premium: float
    avg_premium: float
    max_premium: float
    entry_signals_count: int
    exit_signals_count: int
    entry_win_rate_20d: float
    entry_mean_ret_20d: float
    exit_avg_max_dd_20d: float
    exit_worst_drop: float
