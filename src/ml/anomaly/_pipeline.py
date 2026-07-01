"""Strategy classes, CompositeAnomalyPipeline, and run_composite_anomaly."""
from __future__ import annotations

import logging
import time
import warnings
from abc import ABC, abstractmethod

import pandas as pd

from ._features import build_features, robust_zscore
from ._garch import fit_garch_residuals
from ._isolation import fit_isolation_forest
from ._changepoint import fit_change_points
from ._regime import classify_regime
from ._cross_asset import _inject_cross_asset
from ._qdrant import _store_anomalies

log = logging.getLogger(__name__)


# ── Strategy ABC ──────────────────────────────────────────────────────────────

class AnomalyDetectorStrategy(ABC):
    """Abstract interface for all individual anomaly detection algorithms."""

    @abstractmethod
    def fit_predict(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Fit model on daily prices DataFrame and return it with computed scores."""
        pass


# ── Concrete strategies ───────────────────────────────────────────────────────

class RobustZScoreStrategy(AnomalyDetectorStrategy):
    """Calculates MAD-based robust Z-scores on daily return, trading range, and volume."""

    def __init__(self, window: int = 30):
        self.window = window

    def fit_predict(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        df = df.copy()
        df["z_return"] = robust_zscore(df["daily_return"].fillna(0), window=self.window)
        df["z_range"]  = robust_zscore(df["range_pct"],              window=self.window)
        df["z_robust"] = (df["z_return"].abs() + df["z_range"]) / 2.0
        df["z_volume"] = robust_zscore(df["volume"].fillna(0),       window=self.window)
        return df


class GarchResidualStrategy(AnomalyDetectorStrategy):
    """Fits a GARCH(1,1) model and standardizes residuals by conditional volatility."""

    def __init__(self):
        self.loglik: float = 0.0

    def fit_predict(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        df_res, loglik = fit_garch_residuals(df)
        self.loglik = loglik
        return df_res


class IsolationForestStrategy(AnomalyDetectorStrategy):
    """Runs Isolation Forest on price-based and cross-asset features to compute confidence."""

    def __init__(self, contamination: float = 0.03):
        self.contamination = contamination

    def fit_predict(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        return fit_isolation_forest(df, contamination=self.contamination)


class PeltChangePointStrategy(AnomalyDetectorStrategy):
    """Applies PELT Change-Point Detection to identify structural regime shifts."""

    def __init__(self, penalty: float | None = None, proximity_days: int = 3):
        self.penalty = penalty
        self.proximity_days = proximity_days

    def fit_predict(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        return fit_change_points(
            df, penalty=self.penalty, proximity_days=self.proximity_days
        )


# ── Pipeline orchestrator ─────────────────────────────────────────────────────

class CompositeAnomalyPipeline:
    """Orchestrates sequential anomaly strategies and computes consolidated regimes."""

    def __init__(
        self,
        z_threshold: float = 3.0,
        cp_boost: float = 1.15,
        df_cot: pd.DataFrame | None = None,
        df_fx: pd.DataFrame | None = None,
        df_corp_actions: pd.DataFrame | None = None,
        symbol: str = "",
        category: str = "",
    ):
        self.z_threshold  = z_threshold
        self.cp_boost     = cp_boost
        self.df_cot       = df_cot
        self.df_fx        = df_fx
        self.symbol       = symbol
        self.category     = category
        self.garch_loglik: float = 0.0

        # Pre-compute date sets so the full df_corp_actions DataFrame is not retained
        self._ca_all_dates: frozenset = frozenset()
        self._ca_suppress_dates: frozenset = frozenset()
        if df_corp_actions is not None and not df_corp_actions.empty:
            from src.importer.fetchers.nse_corporate_actions_fetcher import PRICE_IMPACTING_TYPES
            ca = df_corp_actions.copy()
            ca["ex_date"] = pd.to_datetime(ca["ex_date"]).dt.normalize()
            self._ca_all_dates = frozenset(ca["ex_date"])
            self._ca_suppress_dates = frozenset(
                ca.loc[ca["action_type"].isin(PRICE_IMPACTING_TYPES), "ex_date"]
            )

    def run(
        self,
        df: pd.DataFrame,
        rf_lags: int = 5,
        contamination: float = 0.03,
        z_window: int = 30,
        cp_penalty: float | None = None,
        cp_proximity_days: int = 3,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        df = build_features(df, rf_lags=rf_lags)

        # Inject cross-asset features when available
        df = _inject_cross_asset(df, df_cot=self.df_cot, df_fx=self.df_fx)

        strategies = [
            RobustZScoreStrategy(window=z_window),
            GarchResidualStrategy(),
            IsolationForestStrategy(contamination=contamination),
            PeltChangePointStrategy(penalty=cp_penalty, proximity_days=cp_proximity_days),
        ]

        for strategy in strategies:
            strat_name = strategy.__class__.__name__
            log.info(f"Running strategy: {strat_name}...")
            t0 = time.time()
            df = strategy.fit_predict(df)
            log.info(f"Finished strategy {strat_name} in {time.time() - t0:.4f}s")
            if isinstance(strategy, GarchResidualStrategy):
                self.garch_loglik = strategy.loglik

        df = classify_regime(df)

        # Change-point confirmation booster
        if self.cp_boost != 1.0 and bool(df["cp_confirmed"].any()):
            pre_flagged = df["final_z_abs"] > self.z_threshold
            mask = df["cp_confirmed"] & pre_flagged
            if mask.any():
                df.loc[mask, "final_z"] = df.loc[mask, "final_z"] * self.cp_boost
                df.loc[mask, "final_z_abs"] = df.loc[mask, "final_z"].abs()
                _keep = df["regime"].str.contains("Flash Crash|Volatile Breakout", na=False)
                relabel = mask & ~_keep
                df.loc[relabel, "regime"] = "🔀 Regime Shift (Change Point)"

        # ── Corporate action suppression ─────────────────────────────────────
        df["is_corporate_action"]  = False
        df["suppress_corp_action"] = False
        if self._ca_all_dates:
            df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.normalize()
            df["is_corporate_action"]  = df["trade_date"].isin(self._ca_all_dates)
            df["suppress_corp_action"] = df["trade_date"].isin(self._ca_suppress_dates)
            df.loc[df["suppress_corp_action"], "regime"] = "🏢 Price Driven by Company Event"

        # Suppress mechanical corporate-action price moves only for ETFs.
        # ETF corporate actions (NAV resets, bonus units) are pure admin events with no
        # market-signal content. Stock corporate actions (mergers, demergers, splits) are
        # real price events worth analysing even when mechanically triggered.
        is_etf = self.category.lower() in ("etfs", "etf")
        if is_etf:
            df["is_anomaly"] = (df["final_z_abs"] > self.z_threshold) & ~df["suppress_corp_action"]
        else:
            df["is_anomaly"] = df["final_z_abs"] > self.z_threshold

        df_flagged = df[df["is_anomaly"]].copy()

        if self.symbol and not df_flagged.empty and _store_anomalies is not None:
            try:
                _store_anomalies(df_flagged, self.symbol, self.category)
            except Exception as e:
                log.warning("AnomalyVector: store failed for %s: %s", self.symbol, e)

        return df, df_flagged


# ── Public wrapper ────────────────────────────────────────────────────────────

def run_composite_anomaly(
    df: pd.DataFrame,
    rf_lags: int = 5,           # kept for API compatibility, unused
    contamination: float = 0.03,
    z_threshold: float = 3.0,
    z_window: int = 30,
    df_cot: pd.DataFrame | None = None,
    df_fx: pd.DataFrame | None = None,
    cp_penalty: float | None = None,
    cp_proximity_days: int = 3,
    cp_boost: float = 1.15,
    df_corp_actions: pd.DataFrame | None = None,
    symbol: str = "",
    category: str = "",
) -> tuple[pd.DataFrame, pd.DataFrame, float]:
    """
    End-to-end composite anomaly detection.
    Defers execution to the OOP CompositeAnomalyPipeline.

    symbol / category: when provided, flagged anomalies are stored in Qdrant
    for future semantic retrieval (retrieve_similar_anomalies).
    """
    pipeline = CompositeAnomalyPipeline(
        z_threshold=z_threshold,
        cp_boost=cp_boost,
        df_cot=df_cot,
        df_fx=df_fx,
        df_corp_actions=df_corp_actions,
        symbol=symbol,
        category=category,
    )
    df_res, df_flagged = pipeline.run(
        df,
        rf_lags=rf_lags,
        contamination=contamination,
        z_window=z_window,
        cp_penalty=cp_penalty,
        cp_proximity_days=cp_proximity_days,
    )
    return df_res, df_flagged, pipeline.garch_loglik
