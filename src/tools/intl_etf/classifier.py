"""
src/tools/intl_etf/classifier.py
────────────────────────────────
Strategy pattern implementation for ETF Scarcity Arbitrage Signals,
Volume Regime Identification, and Tax Hurdle / Downside Parity Sizing.
"""

from __future__ import annotations

from typing import Any
from src.tools.intl_etf.models import (
    ArbitrageSignal,
    VolumeRegime,
    ROUND_TRIP_COST_PCT,
    STCG_EQUITY_RATE,
    LTCG_EQUITY_RATE,
)


class ArbitrageClassificationStrategy:
    """Encapsulates rules for statistical arbitrage, scarcity premium, and volume regimes."""

    @staticmethod
    def classify_signal(
        premium_pct: float,
        z_score: float | None = None,
        z_entry_threshold: float = -1.5,
        z_accumulate_threshold: float = -1.0,
    ) -> tuple[str, str, str]:
        """
        Classify arbitrage regime.
        Returns: (action_label, rich_style, arbitrage_signal_enum_val)
        """
        z = z_score if z_score is not None else 0.0

        if premium_pct > 40.0 or z >= 2.5:
            return (
                "💥 BUBBLE (LIQUIDATE)",
                "bold white on red",
                ArbitrageSignal.BUBBLE.value,
            )
        elif premium_pct >= 25.0 or z >= 1.8:
            return (
                "🚨 ARBITRAGE EXIT (SELL)",
                "bold red",
                ArbitrageSignal.EXIT.value,
            )
        elif z <= z_entry_threshold or premium_pct < -0.5:
            return (
                "🟢 ARBITRAGE BUY (ENTRY)",
                "bold green",
                ArbitrageSignal.ENTRY.value,
            )
        elif z <= z_accumulate_threshold:
            return (
                "🟡 GOOD ENTRY (ACCUMULATE)",
                "green",
                ArbitrageSignal.ACCUMULATE.value,
            )
        elif premium_pct > 12.0 or z >= 1.0:
            return (
                "⚠️ CAUTION (OVERPRICED)",
                "bold yellow",
                ArbitrageSignal.TRIM.value,
            )
        else:
            return (
                "⚪ FAIR VALUE (HOLD)",
                "cyan",
                ArbitrageSignal.HOLD.value,
            )

    @staticmethod
    def classify_volume_regime(
        vol_multiple: float,
        turnover_cr: float,
        premium_pct: float,
    ) -> str:
        """Categorize secondary market order flow and liquidity conditions."""
        if vol_multiple >= 3.0 and premium_pct > 20.0:
            return VolumeRegime.CLIMAX_DISTRIBUTION.value
        elif vol_multiple <= 0.4 and premium_pct > 20.0:
            return VolumeRegime.CIRCUIT_FROZEN.value
        elif turnover_cr >= 10.0:
            return VolumeRegime.DEEP_LIQUIDITY.value
        elif turnover_cr >= 1.0:
            return VolumeRegime.NORMAL_LIQUIDITY.value
        else:
            return VolumeRegime.THIN_LIQUIDITY.value

    @staticmethod
    def calculate_parity_gap(market_price: float, inav: float) -> float:
        """
        Parity Gap (% downside if secondary price snaps immediately to iNAV).
        Returns: ((iNAV - Price) / Price) * 100.
        """
        if market_price <= 0 or inav <= 0:
            return 0.0
        return round(((inav - market_price) / market_price) * 100, 2)

    @staticmethod
    def apply_cost_tax_filter(expected_reversion_pct: float | None) -> dict[str, Any]:
        """Compute net P&L after STCG (20.8%) / LTCG (13.0%) and round-trip costs."""
        if expected_reversion_pct is None:
            return {}
        stcg = STCG_EQUITY_RATE
        ltcg = LTCG_EQUITY_RATE
        gross = abs(expected_reversion_pct)
        net_stcg = gross * (1 - stcg) - ROUND_TRIP_COST_PCT
        net_ltcg = gross * (1 - ltcg) - ROUND_TRIP_COST_PCT
        breakeven = ROUND_TRIP_COST_PCT / (1 - stcg) if stcg < 1 else float("inf")
        return {
            "expected_gross_pct": round(gross, 4),
            "net_pnl_stcg_pct": round(net_stcg, 4),
            "net_pnl_ltcg_pct": round(net_ltcg, 4),
            "breakeven_gross_pct": round(breakeven, 4),
            "is_profitable_after_costs": net_stcg > 0,
        }
