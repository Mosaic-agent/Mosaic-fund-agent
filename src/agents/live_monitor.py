"""
src/agents/live_monitor.py
─────────────────────────────
Standalone, long-running process that watches a symbol watchlist via Shoonya
live ticks during NSE market hours, aggregates into 5-minute bars, scores
each bar for price/volume anomalies, and fires alerts (see
src/events/observers.py:LiveAlertObserver) that get enriched with correlated
news and delivered to Slack.

This grew out of a manual investigation ("why did the market fall today?")
where we reconstructed — after the close — that Nifty broke sharply within
the same ~5-minute window as breaking news. This module surfaces that live.

Usage
─────
    ALLOW_LOCAL_RUN=1 python src/agents/live_monitor.py \\
        --watchlist-config config/live_watchlist.yaml

    # Pre-flight-only mode — cron this ~08:30 IST so a stale Shoonya session
    # is caught with time to fix it (via initiate_shoonya_login/
    # complete_shoonya_login) before market open:
    python src/agents/live_monitor.py --check-session-only

    # Exercise the full pipeline (bar builder → EventBus → LiveAlertObserver
    # → Slack/ClickHouse) with a synthetic tick sequence, no live Shoonya:
    python src/agents/live_monitor.py --dry-run

v1 limitations (explicitly out of scope, not oversights — see the approved
plan at .claude/plans/create-a-module-in-goofy-seal.md):
  - No NSE holiday calendar — will attempt to run on holidays, just see no ticks.
  - Watchlist is resolved once at startup; adding a symbol requires a restart.
  - z_threshold=3.0 is an EOD-derived starting point, not intraday-validated —
    expect a paper-testing tuning period (run with a lower threshold first).
"""
from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import threading
import time
from datetime import datetime, timedelta

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from config.settings import settings
from src.agents.live_bar_builder import LiveBarBuilder
from src.agents.live_watchlist import WatchlistEntry, resolve_watchlist
from src.events.bus import get_event_bus
from src.events.observers import setup_observers
from src.utils.ist import now_ist

log = logging.getLogger(__name__)

# Must match the symbol name used for the static INDIA VIX entry in
# src/agents/live_watchlist.py's _INDEX_ENTRIES.
VIX_SYMBOL = "INDIA VIX"


# ── Cross-symbol VIX confirmation gate ────────────────────────────────────────

def vix_confirms(
    *,
    event_symbol: str,
    event_alert_type: str,
    event_ts: datetime,
    vix_last_z: float | None,
    vix_last_bar_ts: datetime | None,
    bar_seconds: int,
    vix_confirmation_zscore: float,
) -> bool:
    """
    True if an alert should be forwarded, False if it should be suppressed
    for lack of VIX confirmation.

    Only gates price_break alerts on non-VIX symbols — volume_spike alerts
    and VIX's own alerts always pass through untouched. Fails OPEN (never
    suppresses) whenever VIX has no scored baseline yet (e.g. still in its
    own warm-up window) or its most recent scored bar is stale by more than
    one bar_seconds interval — a live system can't guarantee VIX's tick for
    bar T has already arrived by the moment another symbol's bar T closes,
    and dropping a real alert because of that race is worse than an
    occasional unconfirmed one getting through.
    """
    if event_symbol == VIX_SYMBOL or event_alert_type != "price_break":
        return True
    if vix_last_z is None or vix_last_bar_ts is None:
        return True
    if event_ts - vix_last_bar_ts > timedelta(seconds=bar_seconds):
        return True
    return abs(vix_last_z) >= vix_confirmation_zscore


# ── Session pre-flight ────────────────────────────────────────────────────────

def check_session() -> bool:
    """
    Validate the Shoonya session WITHOUT starting the websocket. Returns
    False (never hangs/prompts) if the cached session is expired and an
    interactive browser OAuth login would be required — get_shoonya_api()
    already returns None in that case when stdin isn't a TTY (or the
    terminal input is refused), which is exactly the unattended-process
    scenario this process runs in.
    """
    from src.importer.fetchers.shoonya_fetcher import get_shoonya_api
    api = get_shoonya_api()
    if api is None:
        log.critical(
            "Shoonya session check FAILED — cached session missing/expired and "
            "interactive OAuth login is required. Run "
            "initiate_shoonya_login/complete_shoonya_login (or "
            "src/scripts/portfolio/shoonya_login.py) before market open."
        )
        return False
    log.info("Shoonya session check OK.")
    return True


# ── Multi-symbol websocket manager ────────────────────────────────────────────

class LiveWebsocketManager:
    """
    One Shoonya websocket connection subscribed to every token in the
    watchlist at once (NorenApi.subscribe() natively joins a list with '#').
    Owns reconnect-with-backoff — the Shoonya SDK has no built-in reconnect —
    and an idle watchdog that force-reconnects if an actively-traded symbol
    goes silent for too long even though the socket looks "open".
    """

    IDLE_WATCHDOG_SECONDS = 30.0
    IDLE_THRESHOLD_SECONDS = 90.0

    def __init__(self, api, on_alert_events, reconnect_max_backoff: float = 60.0):
        self.api = api
        self._on_alert_events = on_alert_events  # callable(list[LiveAlertEvent]) -> None
        self._reconnect_max_backoff = reconnect_max_backoff
        self._backoff = 1.0
        self._running = False
        self._reconnect_lock = threading.Lock()
        self._watchdog_thread: threading.Thread | None = None

        self._builders: dict[str, LiveBarBuilder] = {}   # token -> builder
        self._vix_builder: LiveBarBuilder | None = None
        self._subscribe_keys: list[str] = []              # ["NSE|26000", ...]
        self._last_tick_at: dict[str, datetime] = {}

    def connect(self, watchlist: list[WatchlistEntry]) -> None:
        self._builders = {
            entry.token: LiveBarBuilder(
                symbol=entry.symbol,
                bar_seconds=settings.live_monitor_bar_seconds,
                buffer_size=settings.live_monitor_buffer_size,
                z_threshold=settings.live_monitor_zscore_threshold,
            )
            for entry in watchlist
        }
        self._vix_builder = next(
            (b for b in self._builders.values() if b.symbol == VIX_SYMBOL), None
        )
        if self._vix_builder is None:
            log.warning(
                "LiveWebsocketManager: %s not in watchlist — price_break alerts on "
                "other symbols will NOT be VIX-confirmation-gated.", VIX_SYMBOL,
            )
        self._subscribe_keys = [f"{entry.exchange}|{entry.token}" for entry in watchlist]
        log.info("LiveWebsocketManager: watching %d symbol(s): %s",
                 len(watchlist), ", ".join(e.symbol for e in watchlist))

        self._running = True
        self._start_websocket()
        self._watchdog_thread = threading.Thread(target=self._watchdog_loop, daemon=True)
        self._watchdog_thread.start()

    def stop(self) -> None:
        self._running = False
        closer = threading.Thread(target=self._close_websocket, daemon=True)
        closer.start()
        closer.join(timeout=2.0)
        if closer.is_alive():
            log.warning("LiveWebsocketManager: websocket close timed out after 2s; exiting anyway.")

    def flush_all(self):
        """Force-close every symbol's in-progress bar (e.g. at shutdown/market close)."""
        events = []
        for builder in self._builders.values():
            events.extend(builder.flush())
        return [e for e in events if self._vix_confirms(e)]

    # ── Websocket lifecycle ──────────────────────────────────────────────────

    def _start_websocket(self) -> None:
        self.api.start_websocket(
            order_update_callback=lambda _x: None,
            subscribe_callback=self._dispatch_tick,
            socket_open_callback=self._on_open,
            socket_close_callback=self._on_close,
            socket_error_callback=self._on_error,
        )

    def _close_websocket(self) -> None:
        try:
            self.api.close_websocket()
        except Exception:
            pass

    def _on_open(self) -> None:
        log.info("LiveWebsocketManager: websocket open — subscribing to %d token(s)",
                  len(self._subscribe_keys))
        self.api.subscribe(self._subscribe_keys)
        self._backoff = 1.0

    def _on_close(self, *_args) -> None:
        log.warning("LiveWebsocketManager: websocket closed")
        self._schedule_reconnect()

    def _on_error(self, err) -> None:
        log.warning("LiveWebsocketManager: websocket error: %s", err)
        self._schedule_reconnect()

    def _schedule_reconnect(self) -> None:
        if not self._running:
            return
        if not self._reconnect_lock.acquire(blocking=False):
            return  # a reconnect attempt is already in flight

        def _reconnect():
            try:
                delay = self._backoff
                log.info("LiveWebsocketManager: reconnecting in %.0fs...", delay)
                time.sleep(delay)
                self._backoff = min(self._backoff * 2, self._reconnect_max_backoff)
                if self._running:
                    self._start_websocket()
            finally:
                self._reconnect_lock.release()

        threading.Thread(target=_reconnect, daemon=True).start()

    def _dispatch_tick(self, tick: dict) -> None:
        if not tick:
            return
        token = tick.get("tk")
        builder = self._builders.get(token) if token else None
        if builder is None:
            return  # tick for a token we didn't subscribe to (or missing 'tk') — ignore

        self._last_tick_at[token] = now_ist()
        events = builder.on_tick(tick)
        events = [e for e in events if self._vix_confirms(e)]
        if events:
            self._on_alert_events(events)

    def _vix_confirms(self, event) -> bool:
        vix = self._vix_builder
        confirmed = vix_confirms(
            event_symbol=event.symbol,
            event_alert_type=event.alert_type,
            event_ts=event.timestamp,
            vix_last_z=vix.last_z_return if vix else None,
            vix_last_bar_ts=vix.last_scored_bar_ts if vix else None,
            bar_seconds=settings.live_monitor_bar_seconds,
            vix_confirmation_zscore=settings.live_monitor_vix_confirmation_zscore,
        )
        if not confirmed:
            log.info(
                "LiveWebsocketManager: suppressing %s price_break (z=%.2f) — "
                "no VIX confirmation in the same bar.", event.symbol, event.zscore,
            )
        return confirmed

    # ── Idle watchdog ─────────────────────────────────────────────────────────

    def _watchdog_loop(self) -> None:
        # Grace period before the watchdog starts judging silence, so startup
        # (no ticks yet) never triggers a spurious reconnect.
        time.sleep(self.IDLE_WATCHDOG_SECONDS)
        while self._running:
            now = now_ist()
            stale = [
                token for token, last in self._last_tick_at.items()
                if (now - last).total_seconds() > self.IDLE_THRESHOLD_SECONDS
            ]
            if stale and self._is_market_hours(now):
                log.warning(
                    "LiveWebsocketManager: %d symbol(s) silent >%.0fs during market "
                    "hours — forcing reconnect", len(stale), self.IDLE_THRESHOLD_SECONDS,
                )
                self._schedule_reconnect()
            time.sleep(self.IDLE_WATCHDOG_SECONDS)

    @staticmethod
    def _is_market_hours(now: datetime) -> bool:
        open_h, open_m = (int(x) for x in settings.market_open.split(":"))
        close_h, close_m = (int(x) for x in settings.market_close.split(":"))
        open_t = now.replace(hour=open_h, minute=open_m, second=0, microsecond=0)
        close_t = now.replace(hour=close_h, minute=close_m, second=0, microsecond=0)
        return open_t <= now <= close_t


# ── Dry-run: synthetic tick feed, no Shoonya at all ───────────────────────────

def _run_dry_run(on_alert_events) -> None:
    """
    Replay a synthetic tick sequence (stable baseline + one injected spike)
    through the exact same LiveBarBuilder → EventBus → LiveAlertObserver path
    used in production, without touching Shoonya. Exercises Slack delivery
    (if slack_webhook_url is configured) and ClickHouse logging end to end.
    """
    log.info("Dry-run: replaying a synthetic tick sequence for DRYRUN (no live Shoonya).")
    builder = LiveBarBuilder(
        "DRYRUN", bar_seconds=5, buffer_size=10,
        z_threshold=settings.live_monitor_zscore_threshold,
    )
    cum_volume = 0.0

    def feed(price: float, vol_delta: float) -> None:
        nonlocal cum_volume
        cum_volume += vol_delta
        tick = {"t": "tf", "lp": str(price), "v": str(cum_volume)}
        events = builder.on_tick(tick)
        if events:
            on_alert_events(events)
        time.sleep(builder.bar_seconds)

    prices = [100.00, 100.04, 99.97, 100.02, 99.99, 100.03, 99.98, 100.01, 100.02, 99.99, 100.00, 100.01]
    volumes = [1000, 980, 1050, 990, 1010, 970, 1030, 1000, 990, 1005, 995, 1000]
    for price, vol in zip(prices, volumes):
        feed(price, vol)

    feed(112.0, 60_000)   # opens the spike bar
    feed(112.05, 1_000)   # closes + scores the spike bar
    log.info("Dry-run complete.")


# ── Shutdown handling ──────────────────────────────────────────────────────────

def _install_sigint_hard_exit(stop_event: threading.Event) -> None:
    """First Ctrl+C sets the stop event for a graceful shutdown. A second
    Ctrl+C force-exits immediately, mirroring src/agents/intraday_agent.py."""
    state = {"count": 0}

    def _handler(_signum, _frame):
        state["count"] += 1
        if state["count"] >= 2:
            print("\nForce quitting...")
            os._exit(1)
        print("\nShutting down (Ctrl+C again to force)...")
        stop_event.set()

    signal.signal(signal.SIGINT, _handler)


# ── CLI entry point ────────────────────────────────────────────────────────────

def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    parser = argparse.ArgumentParser(description="Live multi-symbol anomaly + news-correlation monitor.")
    parser.add_argument("--watchlist-config", type=str, default=settings.live_monitor_watchlist_config,
                        help="Path to the ad-hoc watchlist YAML config file.")
    parser.add_argument("--check-session-only", action="store_true",
                        help="Validate the Shoonya session and exit (no websocket connect). "
                             "Intended to be cron'd ~08:30 IST before market open.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Replay a synthetic tick sequence through the full alert pipeline "
                             "instead of connecting to live Shoonya.")
    args = parser.parse_args()

    if args.check_session_only:
        return 0 if check_session() else 1

    setup_observers()

    def on_alert_events(events) -> None:
        bus = get_event_bus()
        for event in events:
            bus.publish(event)

    if args.dry_run:
        _run_dry_run(on_alert_events)
        return 0

    if not check_session():
        return 1

    from src.importer.fetchers.shoonya_fetcher import get_shoonya_api
    api = get_shoonya_api()
    if api is None:
        log.critical("Shoonya API unavailable at connect time — exiting.")
        return 1

    watchlist = resolve_watchlist(api, args.watchlist_config)
    if not watchlist:
        log.critical("Resolved watchlist is empty — nothing to watch. Exiting.")
        return 1

    manager = LiveWebsocketManager(api, on_alert_events)
    manager.connect(watchlist)

    stop_event = threading.Event()
    _install_sigint_hard_exit(stop_event)

    close_h, close_m = (int(x) for x in settings.market_close.split(":"))
    log.info("Live monitor running until %02d:%02d IST (or Ctrl+C)...", close_h, close_m)
    try:
        while not stop_event.is_set():
            now = now_ist()
            close_t = now.replace(hour=close_h, minute=close_m, second=0, microsecond=0)
            if now >= close_t:
                log.info("Market close reached — shutting down.")
                break
            stop_event.wait(timeout=1.0)
    finally:
        final_events = manager.flush_all()
        if final_events:
            on_alert_events(final_events)
        manager.stop()
        log.info("Live monitor stopped.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
