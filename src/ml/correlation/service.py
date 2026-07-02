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

    @staticmethod
    def _resolve_category(symbol: str) -> str:
        """Best-effort category lookup (etfs/stocks) for Qdrant payload tagging."""
        try:
            from src.db.pool import query_df
            df = query_df(
                "SELECT DISTINCT category FROM market_data.daily_prices FINAL "
                "WHERE symbol = {sym:String} LIMIT 1",
                parameters={"sym": symbol.upper()},
            )
            if not df.empty:
                return str(df.iloc[0]["category"])
        except Exception as exc:
            log.debug("Category lookup failed for %s: %s", symbol, exc)
        return ""

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

        # 1. Load corporate actions + run anomaly detection.
        # store=False: this call only enables the in-process result cache
        # (shared with chart plotting / risk-governor / search_anomaly_events
        # when they run on the identical symbol+window) — the actual Qdrant
        # write happens later in _store_attribution() with the correlation
        # outcome attached, so a plain un-attributed write here would race it.
        df_corp = self._registry.load_corp_actions(symbol)
        df_anomaly_res, _, _ = run_composite_anomaly(
            df_ohlcv, df_corp_actions=df_corp, symbol=symbol, store=False,
        )

        # 2. Build candidate events from all sources (df_ohlcv enables PELT
        #    regime-shift events — structural breaks in this symbol's returns).
        events = self._registry.load_all(symbol, df_corp, lookback_days, df_ohlcv=df_ohlcv)

        # 3. Execute strategies
        findings: List[CorrelationFinding] = []
        for strat in self._strategies:
            try:
                strat_findings = strat.analyze(df_ohlcv, df_anomaly_res, df_benchmark, events)
                findings.extend(strat_findings)
            except Exception as exc:
                log.error("Correlation Strategy %s failed: %s", strat.name, exc, exc_info=True)

        # 4. Post-processing pipeline (quality → dedup → cluster → precedent).
        # df_anomaly is passed through so the precedent stage can look up each
        # finding's regime label without re-running anomaly detection.
        findings = self._pipeline.run(findings, symbol=symbol, df_anomaly=df_anomaly_res)

        # 5. Persist attribution: for every flagged anomaly day, record either
        # the winning (highest-score) finding or None (UNEXPLAINED) so future
        # retrieve_similar_anomalies() calls carry real precedent, not just
        # statistical similarity.
        self._store_attribution(symbol, df_anomaly_res, findings)

        return findings

    @staticmethod
    def _store_attribution(
        symbol: str,
        df_anomaly: pd.DataFrame,
        findings: List[CorrelationFinding],
    ) -> None:
        if df_anomaly is None or df_anomaly.empty or "is_anomaly" not in df_anomaly.columns:
            return
        df_flagged = df_anomaly[df_anomaly["is_anomaly"]].copy()
        if df_flagged.empty:
            return

        best_by_date: dict[str, CorrelationFinding] = {}
        for f in findings:
            key = str(f.anomaly_date)
            if key not in best_by_date or f.correlation_score > best_by_date[key].correlation_score:
                best_by_date[key] = f

        attributions: dict[str, Optional[dict]] = {}
        for trade_date in pd.to_datetime(df_flagged["trade_date"]).dt.date:
            key = str(trade_date)
            f = best_by_date.get(key)
            attributions[key] = None if f is None else {
                "event_type": f.event.event_type.value,
                "label":      f.event.label,
                "score":      f.correlation_score,
                "confidence": f.confidence,
                "strategy":   f.strategy_name,
                "lag_days":   f.lead_lag_days,
            }

        try:
            from src.db.anomaly_vector import store_anomalies_with_attribution
            category = CorrelationService._resolve_category(symbol)
            store_anomalies_with_attribution(df_flagged, symbol, category, attributions)
        except Exception as exc:
            log.debug("Attribution store skipped for %s: %s", symbol, exc)
