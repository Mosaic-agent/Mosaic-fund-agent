"""
src/ml/correlation/service.py
──────────────────────────────
Thin orchestrator: wires EventRegistry → Strategies → FindingsPipeline.
"""

from __future__ import annotations

import logging
from typing import List, Optional

import pandas as pd

from .event_registry import EventRegistry
from .filters import FindingsPipeline
from .models import CorrelationFinding
from .strategies import (
    CorrelationStrategy,
    CrossAssetCoMovementStrategy,
    PostMacroShockStrategy,
)

log = logging.getLogger(__name__)


class CorrelationService:
    """Orchestrates candidate event loading and pluggable correlation strategies.

    Default strategies:
      - PostMacroShockStrategy   — anomaly → macro event attribution
      - CrossAssetCoMovementStrategy — anomaly → FX/commodity shock attribution
    """

    def __init__(self) -> None:
        self._strategies: List[CorrelationStrategy] = []
        self._registry = EventRegistry()
        self._pipeline = FindingsPipeline()

        # Register default strategies (anomaly-first: detect anomalies, then
        # attribute them to external signals that impacted the price).
        self.register_strategy(PostMacroShockStrategy())
        self.register_strategy(CrossAssetCoMovementStrategy())

    def register_strategy(self, strategy: CorrelationStrategy) -> None:
        self._strategies.append(strategy)

    def find_correlations(
        self,
        symbol: str,
        df_ohlcv: pd.DataFrame,
        df_benchmark: Optional[pd.DataFrame] = None,
        lookback_days: int = 365,
    ) -> List[CorrelationFinding]:
        """Executes all registered strategies against price data and candidate events."""
        if df_ohlcv.empty or len(df_ohlcv) < 5:
            return []

        from src.ml.anomaly import run_composite_anomaly

        df_ohlcv = df_ohlcv.copy()
        df_ohlcv["trade_date"] = pd.to_datetime(df_ohlcv["trade_date"])

        # 1. Load corporate actions + run anomaly detection
        df_corp = self._registry.load_corp_actions(symbol)
        df_anomaly_res, _, _ = run_composite_anomaly(df_ohlcv, df_corp_actions=df_corp)

        # 2. Build candidate events from all sources
        events = self._registry.load_all(symbol, df_corp, lookback_days)

        # 3. Execute strategies
        findings: List[CorrelationFinding] = []
        for strat in self._strategies:
            try:
                strat_findings = strat.analyze(df_ohlcv, df_anomaly_res, df_benchmark, events)
                findings.extend(strat_findings)
            except Exception as exc:
                log.error("Correlation Strategy %s failed: %s", strat.name, exc, exc_info=True)

        # 4. Post-processing pipeline (quality → dedup → cluster)
        findings = self._pipeline.run(findings, symbol=symbol)

        return findings
