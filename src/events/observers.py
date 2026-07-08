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
from src.events.live_events import LiveAlertEvent

log = logging.getLogger(__name__)

# ── Categories that affect the signal composite ───────────────────────────────
_SIGNAL_CATEGORIES = {"etfs", "fii_dii", "cot", "fx_rates", "mf"}
_GOLDBEES_SOURCES  = {"yfinance", "nse_quote"}

# ── Index → Yahoo Finance ticker map, for live-alert news correlation ────────
_INDEX_YAHOO_TICKERS = {
    "NIFTY": "^NSEI",
    "NIFTY BANK": "^NSEBANK",
    "INDIA VIX": "^INDIAVIX",
}


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


# ── 7. Live monitor → news-race + single combined Slack alert (async) ────────

def _yahoo_ticker_for(symbol: str) -> str:
    if symbol in _INDEX_YAHOO_TICKERS:
        return _INDEX_YAHOO_TICKERS[symbol]
    from config.settings import settings
    return f"{symbol}{settings.nse_suffix}"


def _headline_from(article: dict, fallback_source: str) -> dict:
    """Normalise a raw gnews item (publisher: {title: ...}) or an already-
    normalised _yf_news_fetch item (source: ...) into {title, source, url}."""
    title = article.get("title", "")
    source = article.get("source", "")
    if not source:
        publisher = article.get("publisher")
        if isinstance(publisher, dict):
            source = publisher.get("title", "")
    return {"title": title, "source": source or fallback_source, "url": article.get("url", "")}


def _correlate_live_news(symbol: str, timeout_seconds: float) -> dict | None:
    """
    Race concurrent news lookups for `symbol`, bounded by a single overall
    `timeout_seconds` deadline (not per-source). Reuses the existing gnews/
    yfinance fetch helpers from macro_event_scanner — neither does a per-
    article requests.head() URL-resolution call, so this stays fast.

    For index symbols (NIFTY / NIFTY BANK / INDIA VIX) also races a broad
    market-wide query, since "correlated news for NIFTY" is closer to macro/
    geopolitical scanning than a single-stock news query.

    Returns the first usable {title, source, url}, or None if nothing came
    back within the deadline.
    """
    from concurrent.futures import ThreadPoolExecutor, wait

    from src.tools.macro_event_scanner import _gnews_fetch, _yf_news_fetch

    queries = [symbol]
    if symbol in _INDEX_YAHOO_TICKERS:
        queries.append("India stock market news today")

    executor = ThreadPoolExecutor(max_workers=len(queries) + 1)
    try:
        futures = [executor.submit(_gnews_fetch, q, 3) for q in queries]
        futures.append(executor.submit(_yf_news_fetch, _yahoo_ticker_for(symbol), 3))

        done, _not_done = wait(futures, timeout=timeout_seconds)

        articles: list[dict] = []
        for future in done:
            try:
                articles.extend(future.result() or [])
            except Exception as exc:
                log.debug("_correlate_live_news: a source failed for %s: %s", symbol, exc)
    finally:
        # Abandoned (not-yet-done) futures keep running on the pool's threads;
        # wait=False so a slow source never delays alert delivery.
        executor.shutdown(wait=False)

    if not articles:
        return None
    return _headline_from(articles[0], fallback_source="news")


def _persist_live_headline_to_qdrant(symbol: str, headline: dict) -> None:
    """Best-effort: persist whatever headline was found so it's available for
    later correlation, regardless of whether Slack delivery succeeds."""
    try:
        from src.ml.correlation.news_rag import embed_batch, upsert_to_qdrant

        text = f"{headline.get('title', '')}."
        vectors = embed_batch([text])
        if not vectors or all(v == 0.0 for v in vectors[0]):
            return
        upsert_to_qdrant([{
            "title": headline.get("title", ""),
            "source": headline.get("source", ""),
            "url": headline.get("url", ""),
            "published_at": "",
            "category": "live_alert",
            "sentiment": "NEUTRAL",
            "symbol": symbol,
        }], vectors)
    except Exception as exc:
        log.debug("LiveAlertObserver: Qdrant persist skipped for %s: %s", symbol, exc)


class LiveAlertObserver(Observer):
    """
    Fired by the live monitor (src/agents/live_monitor.py) when a 5-min bar
    trips a price/volume anomaly. Races concurrent news lookups (bounded by
    settings.live_monitor_news_timeout_seconds), then sends ONE combined
    Slack message — the anomaly plus any correlated headline found within
    that window (or an explicit "no news found" note if not) — and logs one
    row to market_data.live_alerts.

    This fills the stub in AnomalyAlertObserver ("wire to Slack/webhook
    here") but scoped to the new live.alert event type, leaving the EOD
    anomaly.detected path (different volume/urgency profile) untouched.
    """
    event_types = ["live.alert"]
    async_ok = True

    def handle(self, event: LiveAlertEvent) -> None:
        from config.settings import settings

        headline = None
        try:
            headline = _correlate_live_news(event.symbol, settings.live_monitor_news_timeout_seconds)
        except Exception as exc:
            log.warning("LiveAlertObserver: news correlation failed for %s: %s", event.symbol, exc)

        if headline:
            _persist_live_headline_to_qdrant(event.symbol, headline)

        delivered = self._send_slack(event, headline, settings.slack_webhook_url)
        self._log_to_clickhouse(event, headline, delivered_to_slack=delivered)

    @staticmethod
    def _send_slack(event: LiveAlertEvent, headline: dict | None, webhook_url: str) -> bool:
        if not webhook_url:
            log.debug("LiveAlertObserver: slack_webhook_url not set — logging only, no Slack delivery")
            return False

        lines = [
            f"🚨 *{event.symbol}* — {event.alert_type.replace('_', ' ')} "
            f"(z={event.zscore:.2f}) @ {event.timestamp.strftime('%H:%M IST')}",
            f"Price: {event.price:.2f}   Volume: {event.volume:,.0f} (baseline ~{event.baseline_avg_volume:,.0f})",
        ]
        if headline:
            lines.append(f"📰 {headline['title']} — _{headline['source']}_")
            if headline.get("url"):
                lines.append(headline["url"])
        else:
            lines.append("📰 No correlated news found within the alert window.")

        try:
            import requests
            resp = requests.post(webhook_url, json={"text": "\n".join(lines)}, timeout=5)
            resp.raise_for_status()
            return True
        except Exception as exc:
            log.warning("LiveAlertObserver: Slack delivery failed for %s: %s", event.symbol, exc)
            return False

    @staticmethod
    def _log_to_clickhouse(
        event: LiveAlertEvent,
        headline: dict | None,
        delivered_to_slack: bool,
        delivered_to_whatsapp: bool = False,
    ) -> None:
        try:
            from src.importer.clickhouse import ClickHouseImporter
            with ClickHouseImporter() as ch:
                ch.insert_live_alerts([{
                    "symbol": event.symbol,
                    "alert_timestamp": event.timestamp,
                    "alert_type": event.alert_type,
                    "zscore": event.zscore,
                    "price": event.price,
                    "volume": event.volume,
                    "baseline_avg_volume": event.baseline_avg_volume,
                    "correlated_headline": (headline or {}).get("title", ""),
                    "correlated_source": (headline or {}).get("source", ""),
                    "delivered_to_slack": delivered_to_slack,
                    "delivered_to_whatsapp": delivered_to_whatsapp,
                }])
        except Exception as exc:
            log.error("LiveAlertObserver: failed to log alert for %s to ClickHouse: %s", event.symbol, exc)


# ── 8. WhatsApp delivery via CallMeBot (async) ──────────────────────────────

class WhatsAppObserver(Observer):
    """
    Delivers live monitor alerts to WhatsApp via the CallMeBot free API.

    Setup (one-time, ~1 min):
      1. Add +34 644 597 079 to contacts as "CallMeBot".
      2. Send it: "I allow callmebot to send me messages"
      3. You receive your API key by WhatsApp within seconds.
      4. Set CALLMEBOT_WHATSAPP_PHONE and CALLMEBOT_WHATSAPP_APIKEY in .env.

    Rate limit: CallMeBot allows ~1 message/minute per phone number. This
    observer enforces a 60-second minimum gap between messages — if a second
    alert fires within that window it is logged at WARNING and skipped (not
    queued) so the live monitor never accumulates a delivery backlog.

    Message format is plain-text (no markdown) because WhatsApp does not render
    Slack-style ``*bold*`` or ``_italic_`` formatting in non-business accounts.
    """
    event_types = ["live.alert"]
    async_ok = True

    _CALLMEBOT_URL = "https://api.callmebot.com/whatsapp.php"
    _MIN_INTERVAL_SECONDS = 60  # CallMeBot rate limit

    def __init__(self):
        super().__init__()
        self._last_sent_at: float = 0.0  # epoch seconds

    def handle(self, event: LiveAlertEvent) -> None:
        from config.settings import settings
        phone = settings.callmebot_whatsapp_phone
        apikey = settings.callmebot_whatsapp_apikey
        if not phone or not apikey:
            log.debug("WhatsAppObserver: CALLMEBOT_WHATSAPP_PHONE/APIKEY not set — skipping")
            return

        import time
        now = time.time()
        gap = now - self._last_sent_at
        if gap < self._MIN_INTERVAL_SECONDS:
            log.warning(
                "WhatsAppObserver: rate-limit gap %.0fs < %ds — skipping WhatsApp for %s %s",
                gap, self._MIN_INTERVAL_SECONDS, event.symbol, event.alert_type,
            )
            return

        text = self._format_message(event)
        delivered = self._send(phone, apikey, text)
        if delivered:
            self._last_sent_at = now

    @staticmethod
    def _format_message(event: LiveAlertEvent) -> str:
        """Plain-text WhatsApp message — no markdown."""
        emoji = "\U0001f6a8" if event.alert_type == "price_break" else "\U0001f4ca"
        lines = [
            f"{emoji} MOSAIC ALERT",
            f"Symbol : {event.symbol}",
            f"Type   : {event.alert_type.replace('_', ' ').title()}",
            f"Z-score: {event.zscore:.2f}",
            f"Price  : {event.price:.2f}",
            f"Volume : {event.volume:,.0f}  (baseline ~{event.baseline_avg_volume:,.0f})",
            f"Time   : {event.timestamp.strftime('%H:%M IST')}",
        ]
        return "\n".join(lines)

    @staticmethod
    def _send(phone: str, apikey: str, text: str) -> bool:
        try:
            import urllib.parse
            import requests
            params = {
                "phone":  phone,
                "text":   text,
                "apikey": apikey,
            }
            resp = requests.get(
                WhatsAppObserver._CALLMEBOT_URL,
                params=params,
                timeout=10,
            )
            resp.raise_for_status()
            log.info("WhatsAppObserver: delivered to WhatsApp (%s)", phone[-4:])
            return True
        except Exception as exc:
            log.warning("WhatsAppObserver: delivery failed: %s", exc)
            return False


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
    # live.alert hooks (src/agents/live_monitor.py)
    bus.subscribe(LiveAlertObserver())          # async — news race + Slack + ClickHouse log
    bus.subscribe(WhatsAppObserver())           # async — CallMeBot WhatsApp (rate-limited)
    _OBSERVERS_REGISTERED = True
    log.info(
        "EventBus: observers registered (data.imported=%d, anomaly.detected=%d, live.alert=%d)",
        bus.observer_count("data.imported"), bus.observer_count("anomaly.detected"),
        bus.observer_count("live.alert"),
    )
