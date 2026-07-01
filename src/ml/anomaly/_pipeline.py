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
        garch_z_threshold: float | None = None,
    ):
        self.z_threshold  = z_threshold
        self.cp_boost     = cp_boost
        self.df_cot       = df_cot
        self.df_fx        = df_fx
        self.symbol       = symbol
        self.category     = category
        # Opt-in: when set, a large GARCH standardised residual (|z_resid| >
        # this) also flags a day, independent of z_robust. GARCH residual is
        # near-orthogonal to z_robust (validated corr ≈ 0.008), so this lets the
        # pipeline catch volatility-surprise shocks that a modest raw z-score
        # misses — and makes the Flash Crash regime reachable in df_flagged.
        # None = disabled (original z_robust×IF behaviour, no flag-rate change).
        self.garch_z_threshold = garch_z_threshold
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
        store: bool = True,
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
        # Base flag: amplified robust-z over threshold. Optionally OR in a large
        # GARCH residual (near-orthogonal signal) so vol-surprise shocks flag too.
        base_flag = df["final_z_abs"] > self.z_threshold
        if self.garch_z_threshold is not None and "z_resid_abs" in df.columns:
            base_flag = base_flag | (df["z_resid_abs"] > self.garch_z_threshold)

        is_etf = self.category.lower() in ("etfs", "etf")
        if is_etf:
            df["is_anomaly"] = base_flag & ~df["suppress_corp_action"]
        else:
            df["is_anomaly"] = base_flag

        df_flagged = df[df["is_anomaly"]].copy()

        if store and self.symbol and not df_flagged.empty and _store_anomalies is not None:
            try:
                _store_anomalies(df_flagged, self.symbol, self.category)
            except Exception as e:
                log.warning("AnomalyVector: store failed for %s: %s", self.symbol, e)

        return df, df_flagged


# ── Public wrapper ────────────────────────────────────────────────────────────

# In-process cache: several independent tools (chart plotting, risk governor,
# anomaly search, correlation engine) each call run_composite_anomaly() for
# the exact same symbol/window within one user request — re-fitting GARCH +
# Isolation Forest + PELT 3x over identical data. Keyed on every parameter
# that affects the result, so a hit is only ever served for a truly identical
# call. Gated on `symbol` being provided — without it there's no safe way to
# tell two different callers' anonymous DataFrames apart.
_RESULT_CACHE: dict[tuple, tuple] = {}

# Sentinel so callers that pass garch_z_threshold=None explicitly (e.g. unit
# tests wanting deterministic off) are distinguished from callers that omit it
# (production tools) and should inherit settings.anomaly_garch_z_threshold.
_GARCH_THRESH_UNSET = object()


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
    store: bool = True,
    publish_event: bool = False,
    garch_z_threshold: "float | None | object" = _GARCH_THRESH_UNSET,
) -> tuple[pd.DataFrame, pd.DataFrame, float]:
    """
    End-to-end composite anomaly detection.
    Defers execution to the OOP CompositeAnomalyPipeline.

    symbol / category: when provided, (a) enables the in-process result cache
      keyed on symbol/category/data-shape/params, and (b) — when store=True —
      flagged anomalies are stored in Qdrant for future semantic retrieval
      (retrieve_similar_anomalies).
    store: set False to use symbol/category for caching only, without
      triggering the Qdrant write (e.g. a caller that persists anomalies via
      its own separate write path and would otherwise double-write).
    publish_event: opt-in. When True and anomalies are flagged, fire an
      AnomalyDetectedEvent on the EventBus so post-anomaly observers (correlation
      attribution, news enrichment, alerting) react. Off by default so routine
      internal recomputes (charting, correlation's own recompute) stay quiet and
      cannot trigger a cascade. Only fires on a fresh compute — a cache hit
      returns before this, so identical repeat calls don't re-fire.
    """
    # Resolve GARCH voting: omitted (sentinel) → inherit the settings knob;
    # explicit None/float → honour it (unit tests pass None for determinism).
    if garch_z_threshold is _GARCH_THRESH_UNSET:
        try:
            from config.settings import settings
            garch_z_threshold = getattr(settings, "anomaly_garch_z_threshold", None)
        except Exception:
            garch_z_threshold = None

    cache_key = None
    if symbol:
        cache_key = (
            symbol.upper(), category, len(df),
            round(contamination, 6), round(z_threshold, 6), z_window,
            df_cot is not None, df_fx is not None, df_corp_actions is not None,
            cp_penalty, cp_proximity_days, round(cp_boost, 6),
            garch_z_threshold,
        )
        cached = _RESULT_CACHE.get(cache_key)
        if cached is not None:
            log.info("run_composite_anomaly: cache hit for %s — skipping recompute", symbol.upper())
            return cached
    pipeline = CompositeAnomalyPipeline(
        z_threshold=z_threshold,
        cp_boost=cp_boost,
        df_cot=df_cot,
        df_fx=df_fx,
        df_corp_actions=df_corp_actions,
        symbol=symbol,
        category=category,
        garch_z_threshold=garch_z_threshold,
    )
    df_res, df_flagged = pipeline.run(
        df,
        rf_lags=rf_lags,
        contamination=contamination,
        z_window=z_window,
        cp_penalty=cp_penalty,
        cp_proximity_days=cp_proximity_days,
        store=store,
    )
    result = (df_res, df_flagged, pipeline.garch_loglik)
    if cache_key is not None:
        _RESULT_CACHE[cache_key] = result

    if publish_event and symbol and not df_flagged.empty:
        try:
            from datetime import date
            from src.events.bus import get_event_bus, AnomalyDetectedEvent
            from src.events.observers import setup_observers
            setup_observers()  # idempotent — guarantees observers exist for any entrypoint
            latest = pd.to_datetime(df_flagged["trade_date"]).max()
            regimes = sorted({str(r) for r in df_flagged.get("regime", []) if str(r)})
            get_event_bus().publish(AnomalyDetectedEvent(
                symbol=symbol.upper(),
                category=category,
                n_anomalies=int(len(df_flagged)),
                latest_date=str(latest)[:10],
                regimes=regimes,
                to_date=latest.date() if hasattr(latest, "date") else date.today(),
            ))
        except Exception as exc:
            log.warning("run_composite_anomaly: event publish failed (non-fatal): %s", exc)

    return result
