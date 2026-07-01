"""
src/events/observers.py
────────────────────────
Concrete post-import observers.

Each observer reacts to DataImportedEvent fired by repo.run_fetcher().
Register all observers once at process startup by calling setup_observers().

Observer map
────────────
  ModelCacheInvalidator   → any GOLDBEES price data    → delete stale .joblib
  SignalAggregatorObserver→ etfs / fii_dii / cot / fx  → refresh composite scores
  MLPredictionObserver    → GOLDBEES prices             → re-run LightGBM pipeline
  SanityCheckObserver     → any import                  → run data anomaly checks
"""
from __future__ import annotations

import logging
import pathlib

from src.events.bus import AnomalyDetectedEvent, DataImportedEvent, Observer, get_event_bus

log = logging.getLogger(__name__)

# ── Categories that affect the signal composite ───────────────────────────────
_SIGNAL_CATEGORIES = {"etfs", "fii_dii", "cot", "fx_rates", "mf"}
_GOLDBEES_SOURCES  = {"yfinance", "nse_quote"}


# ── 1. Model cache invalidator (sync — instant, must happen before ML runs) ───

class ModelCacheInvalidator(Observer):
    """
    Deletes stale joblib model files when new GOLDBEES price data arrives.

    Runs sync (async_ok=False) to guarantee cache is cleared before any
    async MLPredictionObserver fires.
    """
    async_ok = False  # must complete before ML observer starts

    def handle(self, event: DataImportedEvent) -> None:
        if event.source not in _GOLDBEES_SOURCES:
            return
        if event.category not in ("etfs", "stocks"):
            return

        cache_dir = pathlib.Path(__file__).parents[2] / "output" / ".cache" / "ml_models"
        deleted = 0
        for f in cache_dir.glob("goldbees_lgbm_*.joblib"):
            try:
                f.unlink()
                deleted += 1
            except OSError:
                pass
        if deleted:
            log.info("ModelCacheInvalidator: cleared %d stale model file(s) "
                     "(new GOLDBEES data up to %s)", deleted, event.to_date)


# ── 2. Signal aggregator refresh (async — ~10s) ───────────────────────────────

class SignalAggregatorObserver(Observer):
    """
    Re-runs signal aggregation and persists results to ClickHouse when
    any of the signal-driving data sources are updated.
    """
    async_ok = True

    def handle(self, event: DataImportedEvent) -> None:
        if event.category not in _SIGNAL_CATEGORIES:
            return
        log.info("SignalAggregatorObserver: triggered by %s/%s import (%d rows)",
                 event.source, event.category, event.n_rows)
        try:
            from src.agents.signal_aggregator import run_signal_aggregation
            report = run_signal_aggregation(save=True, verbose=False)
            log.info("SignalAggregatorObserver: regime=%s, %d ETFs scored",
                     report.regime, len(report.signals))
        except Exception as exc:
            log.error("SignalAggregatorObserver failed: %s", exc)


# ── 3. ML prediction refresh (async — ~2s cached, ~30s cold) ─────────────────

class MLPredictionObserver(Observer):
    """
    Re-runs the LightGBM trend predictor when new GOLDBEES price data arrives.
    Model cache means this is ~2s when training data hasn't changed materially.
    """
    async_ok = True

    def handle(self, event: DataImportedEvent) -> None:
        if event.source not in _GOLDBEES_SOURCES:
            return
        if event.category not in ("etfs",):
            return
        log.info("MLPredictionObserver: triggered by %s/%s (%d rows up to %s)",
                 event.source, event.category, event.n_rows, event.to_date)
        try:
            from src.ml.trend_predictor import run_trend_prediction
            result = run_trend_prediction(verbose=False)
            log.info(
                "MLPredictionObserver: regime=%s prob_up=%.3f expected=%.3f%%",
                result["regime_signal"], result["prob_up"], result["expected_return_pct"],
            )
        except Exception as exc:
            log.error("MLPredictionObserver failed: %s", exc)


# ── 4. Data sanity check (async — runs after any import) ─────────────────────

class SanityCheckObserver(Observer):
    """
    Runs YoY and daily anomaly checks after any import.
    Logs a warning if anomalies are detected — does not block the import.
    """
    async_ok = True

    def handle(self, event: DataImportedEvent) -> None:
        try:
            from src.db.pool import get_pool
            from src.utils.sanity_checker import detect_yoy_anomalies, detect_daily_anomalies
            client = get_pool().get_client()
            yoy   = detect_yoy_anomalies(client)
            daily = detect_daily_anomalies(client)
            client.close()
            if yoy or daily:
                log.warning(
                    "SanityCheckObserver: %d YoY anomalies, %d daily outliers "
                    "after %s/%s import",
                    len(yoy), len(daily), event.source, event.category,
                )
        except Exception as exc:
            log.error("SanityCheckObserver failed: %s", exc)


# ── 5. Anomaly → correlation attribution + news enrichment (async) ────────────

class AnomalyCorrelationObserver(Observer):
    """
    On a flagged anomaly, run the correlation engine for that symbol — which
    attributes the anomaly to macro/FX/news events, persists the attribution to
    `market_anomalies` (feeding the precedent-weighting loop), and pulls
    stock-specific news into `news_articles` via the two-pass RAG retrieval.

    Reuses `find_anomaly_correlations` end to end. Runs async so detection never
    blocks on it. The internal `run_composite_anomaly` recompute uses store=False
    and publish_event=False (defaults), so this cannot re-fire the event —
    no cascade loop.
    """
    event_types = ["anomaly.detected"]
    async_ok = True

    def handle(self, event: AnomalyDetectedEvent) -> None:
        if not event.symbol:
            return
        log.info("AnomalyCorrelationObserver: correlating %s (%d anomalies up to %s)",
                 event.symbol, event.n_anomalies, event.latest_date)
        try:
            from src.tools.market.correlation_tools import find_anomaly_correlations
            find_anomaly_correlations.invoke({"symbol": event.symbol, "lookback_days": 365})
            log.info("AnomalyCorrelationObserver: attribution + news enrichment done for %s", event.symbol)
        except Exception as exc:
            log.error("AnomalyCorrelationObserver failed for %s: %s", event.symbol, exc)


# ── 6. Severe-anomaly alert (async) ───────────────────────────────────────────

class AnomalyAlertObserver(Observer):
    """
    Emits a high-visibility alert when a flagged anomaly is a severe regime
    (Flash Crash / Black Swan / Volatile Breakout). Prototype logs a structured
    warning; wire `src/tools/premium_alerts.py` here to push to Slack/webhook.
    """
    event_types = ["anomaly.detected"]
    async_ok = True

    def handle(self, event: AnomalyDetectedEvent) -> None:
        if not event.has_severe_regime():
            return
        log.warning(
            "🚨 ANOMALY ALERT: %s — %d flagged (latest %s); regimes: %s",
            event.symbol, event.n_anomalies, event.latest_date, ", ".join(event.regimes),
        )
        # To notify externally, call into src/tools/premium_alerts.py here.


# ── Setup ─────────────────────────────────────────────────────────────────────

_OBSERVERS_REGISTERED = False


def setup_observers() -> None:
    """
    Register all production observers with the global EventBus.
    Idempotent — safe to call from multiple entrypoints (CLI callback, scripts,
    or lazily before the first publish). Only the first call subscribes.

    Order matters for sync observers (ModelCacheInvalidator must come before
    MLPredictionObserver so cache is cleared before re-training starts).
    """
    global _OBSERVERS_REGISTERED
    if _OBSERVERS_REGISTERED:
        return
    bus = get_event_bus()
    # data.imported hooks
    bus.subscribe(ModelCacheInvalidator())      # sync — clears cache first
    bus.subscribe(MLPredictionObserver())       # async — re-trains after cache clear
    bus.subscribe(SignalAggregatorObserver())   # async — refreshes composite
    bus.subscribe(SanityCheckObserver())        # async — anomaly checks
    # anomaly.detected hooks
    bus.subscribe(AnomalyCorrelationObserver()) # async — attribute + news enrich
    bus.subscribe(AnomalyAlertObserver())       # async — severe-regime alert
    _OBSERVERS_REGISTERED = True
    log.info(
        "EventBus: observers registered (data.imported=%d, anomaly.detected=%d)",
        bus.observer_count("data.imported"), bus.observer_count("anomaly.detected"),
    )
