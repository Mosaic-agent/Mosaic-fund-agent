"""
src/agents/live_monitor.py
─────────────────────────────
Standalone, long-running process that watches a symbol watchlist via Shoonya
live ticks during NSE market hours, aggregates into 5-minute bars, scores
each bar for price/volume anomalies, and fires alerts (see
src/events/observers.py:LiveAlertObserver) that get enriched with correlated
news and delivered to Slack.

Data source cascade (tried in order):
  1. Shoonya websocket — real-time, zero delay (preferred).
  2. NSE quote scrape  — ~15-min delayed; polled every
     LIVE_MONITOR_POLL_INTERVAL_SECONDS (default 60 s).
  3. Yahoo snapshot    — ~15-min delayed; same cadence as NSE, used only when
     NSE returns no price for a symbol.

Shoonya is tried first. If the session is missing/expired, the monitor falls
back automatically to the polling path — no restart required. The polling
path synthesises a Shoonya-format tick dict and feeds it through the same
LiveBarBuilder → EventBus → LiveAlertObserver pipeline, so alert delivery,
VIX gating, and ClickHouse logging all behave identically.
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
from src.agents.live_bar_builder import LiveBarBuilder, TimeOfDayVolumeBaseline
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

    def connect(self, watchlist: list[WatchlistEntry], nse_baselines: dict[str, TimeOfDayVolumeBaseline] | None = None) -> None:
        nse_baselines = nse_baselines or {}
        self._builders = {
            entry.token: LiveBarBuilder(
                symbol=entry.symbol,
                bar_seconds=settings.live_monitor_bar_seconds,
                buffer_size=settings.live_monitor_buffer_size,
                z_threshold=settings.live_monitor_zscore_threshold,
                volume_baseline=nse_baselines.get(entry.symbol),
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


# ── Polling fallback: NSE quote → Yahoo snapshot ──────────────────────────────

class PollingFallbackManager:
    """
    Drop-in replacement for LiveWebsocketManager used when the Shoonya
    websocket session is unavailable.

    Polls each watchlist symbol on a fixed interval:
      1. NSE quote API  (``fetch_nse_eod``) — tries first for every symbol.
      2. Yahoo snapshot (``fetch_yahoo_snapshots``) — used for any symbol
         that NSE returned nothing for (e.g. indices with no NSE quote endpoint,
         or symbols traded only on Yahoo-mapped tickers).

    Both sources carry ~15-minute delay, so alert timing is approximate — but
    the bar-scoring, VIX gating, and Slack/ClickHouse delivery are identical
    to the websocket path because ticks are synthesised in Shoonya format
    (``{"t": "tf", "lp": "<price>", "v": "<cumulative_volume>"}``) and fed
    directly into the same ``LiveBarBuilder.on_tick()`` pipeline.
    """

    def __init__(self, on_alert_events, poll_interval_seconds: int = 60):
        self._on_alert_events = on_alert_events
        self._poll_interval = poll_interval_seconds
        self._running = False
        self._thread: threading.Thread | None = None

        self._builders: dict[str, LiveBarBuilder] = {}   # symbol -> builder
        self._vix_builder: LiveBarBuilder | None = None
        self._cum_volume: dict[str, float] = {}          # symbol -> running volume sum
        self._watchlist: list[WatchlistEntry] = []

    def connect(self, watchlist: list[WatchlistEntry], nse_baselines: dict[str, TimeOfDayVolumeBaseline] | None = None) -> None:
        self._watchlist = watchlist
        nse_baselines = nse_baselines or {}
        self._builders = {
            entry.symbol: LiveBarBuilder(
                symbol=entry.symbol,
                bar_seconds=settings.live_monitor_bar_seconds,
                buffer_size=settings.live_monitor_buffer_size,
                z_threshold=settings.live_monitor_zscore_threshold,
                volume_baseline=nse_baselines.get(entry.symbol),
            )
            for entry in watchlist
        }
        self._vix_builder = self._builders.get(VIX_SYMBOL)
        if self._vix_builder is None:
            log.warning(
                "PollingFallbackManager: %s not in watchlist — price_break alerts "
                "will NOT be VIX-confirmation-gated.", VIX_SYMBOL,
            )
        self._cum_volume = {entry.symbol: 0.0 for entry in watchlist}

        log.info(
            "PollingFallbackManager: watching %d symbol(s) via NSE→Yahoo poll "
            "every %ds (Shoonya unavailable): %s",
            len(watchlist), self._poll_interval,
            ", ".join(e.symbol for e in watchlist),
        )
        self._running = True
        self._thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=self._poll_interval + 5)

    def flush_all(self):
        events = []
        for builder in self._builders.values():
            events.extend(builder.flush())
        return [e for e in events if self._vix_confirms(e)]

    # ── Internal poll loop ────────────────────────────────────────────────────

    def _poll_loop(self) -> None:
        while self._running:
            try:
                self._poll_once()
            except Exception as exc:
                log.warning("PollingFallbackManager: poll cycle error: %s", exc)
            # Sleep in 1-second increments so stop() is responsive.
            for _ in range(self._poll_interval):
                if not self._running:
                    break
                time.sleep(1.0)

    def _poll_once(self) -> None:
        """
        Fetch prices for all watchlist symbols, cascade NSE → Yahoo, then
        synthesise a Shoonya-format tick for each symbol that returned a price.
        """
        from src.importer.fetchers.nse_quote_fetcher import fetch_nse_eod
        from src.importer.fetchers.yahoo_snapshot_fetcher import fetch_yahoo_snapshots

        # Build symbol lists for each source.
        # NSE quote: works for equities/ETFs — map (symbol, _) pairs.
        nse_pairs = [(e.symbol, "") for e in self._watchlist]
        nse_rows = fetch_nse_eod(nse_pairs, category="live_monitor")
        nse_prices: dict[str, tuple[float, float]] = {  # symbol -> (close, volume)
            r["symbol"]: (float(r["close"]), float(r.get("volume") or 0.0))
            for r in nse_rows
            if float(r.get("close") or 0) > 0
        }

        # Symbols NSE didn't return — try Yahoo (yf ticker = symbol + ".NS").
        missing = [e for e in self._watchlist if e.symbol not in nse_prices]
        yahoo_prices: dict[str, float] = {}
        if missing:
            yahoo_pairs = [(e.symbol, f"{e.symbol}.NS") for e in missing]
            yahoo_rows = fetch_yahoo_snapshots(yahoo_pairs)
            yahoo_prices = {
                r["symbol"]: float(r["market_price"])
                for r in yahoo_rows
                if float(r.get("market_price") or 0) > 0
            }

        # Dispatch a synthetic tick for every symbol that has a price.
        fetched: list[str] = []
        missing: list[str] = []
        for entry in self._watchlist:
            sym = entry.symbol
            if sym in nse_prices:
                price, vol = nse_prices[sym]
                self._cum_volume[sym] += vol
                source = "NSE"
            elif sym in yahoo_prices:
                price = yahoo_prices[sym]
                # Yahoo doesn't give intraday volume; keep cumulative unchanged
                # so volume z-scores degrade gracefully (flat volume → no spike).
                source = "Yahoo"
            else:
                missing.append(sym)
                continue

            fetched.append(f"{sym}={price:.2f}({source})")
            tick = {"t": "tf", "lp": str(price), "v": str(self._cum_volume[sym])}
            builder = self._builders[sym]
            events = builder.on_tick(tick)
            events = [e for e in events if self._vix_confirms(e)]
            if events:
                self._on_alert_events(events)

        if fetched:
            log.debug("PollingFallbackManager: imported — %s", ", ".join(fetched))
        if missing:
            log.warning("PollingFallbackManager: no price for %s — skipped this cycle", ", ".join(missing))

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
                "PollingFallbackManager: suppressing %s price_break (z=%.2f) — "
                "no VIX confirmation.", event.symbol, event.zscore,
            )
        return confirmed


# ── Volume baseline builders ─────────────────────────────────────────────────

def _fetch_nse_volume_baselines(watchlist: list[WatchlistEntry]) -> dict[str, TimeOfDayVolumeBaseline]:
    """
    Build TimeOfDayVolumeBaseline for NSE symbols (equities/ETFs only — indices
    like NIFTY/NIFTY BANK have no meaningful intraday volume on yfinance).
    Best-effort: symbols that fail or have too little data silently fall back
    to the flat rolling-window scoring in LiveBarBuilder.
    """
    # Skip indices — they have no tradable volume on yfinance.
    candidates = [e for e in watchlist if e.source not in ("index",)]
    if not candidates:
        return {}

    import yfinance as yf
    tickers = [f"{e.symbol}.NS" for e in candidates]
    sym_map = {f"{e.symbol}.NS": e.symbol for e in candidates}

    try:
        df = yf.download(tickers, period="60d", interval="5m", progress=False, auto_adjust=True)
    except Exception as exc:
        log.warning("NSE volume baseline backfill failed (%s) — using flat window.", exc)
        return {}
    if df.empty:
        log.warning("NSE volume baseline backfill returned no data — using flat window.")
        return {}

    df = df.tz_convert("Asia/Kolkata")
    baselines: dict[str, TimeOfDayVolumeBaseline] = {}
    for yahoo_ticker, nse_sym in sym_map.items():
        try:
            if len(tickers) == 1:
                vol = df["Volume"] if not hasattr(df["Volume"], "columns") else df["Volume"].iloc[:, 0]
            else:
                vol = df["Volume"][yahoo_ticker]
            vol = vol.dropna()
            if vol.empty or len(vol) < 100:
                continue
            baselines[nse_sym] = TimeOfDayVolumeBaseline.from_bars(
                list(vol.index.to_pydatetime()), list(vol.values),
            )
        except Exception as exc:
            log.warning("NSE volume baseline failed for %s (%s) — using flat window.", nse_sym, exc)

    if baselines:
        log.info("NSE volume baseline (Method C): built for %d symbol(s): %s",
                 len(baselines), ", ".join(baselines.keys()))
    return baselines


# ── COMEX commodity poller (always Yahoo — Shoonya has no COMEX feed) ────────

def _fetch_comex_volume_baselines(watchlist: list[WatchlistEntry]) -> dict[str, TimeOfDayVolumeBaseline]:
    """
    Backfills each COMEX symbol's TimeOfDayVolumeBaseline ("Method C" — see
    TimeOfDayVolumeBaseline docstring) from 60 days of historical 5-min bars,
    yfinance's max lookback at that interval. Best-effort: a symbol whose
    fetch fails or comes back too thin is simply left without a baseline, and
    LiveBarBuilder falls back to its flat rolling-window scoring for it —
    never a hard failure for the whole watchlist.
    """
    if not watchlist:
        return {}
    import yfinance as yf

    tickers = [e.yahoo_ticker for e in watchlist]
    try:
        df = yf.download(tickers, period="60d", interval="5m", progress=False, auto_adjust=True)
    except Exception as exc:
        log.warning("ComexYahooPoller: volume baseline backfill failed (%s) — using flat window for all symbols.", exc)
        return {}
    if df.empty:
        log.warning("ComexYahooPoller: volume baseline backfill returned no data — using flat window for all symbols.")
        return {}

    df = df.tz_convert("Asia/Kolkata")
    baselines: dict[str, TimeOfDayVolumeBaseline] = {}
    for entry in watchlist:
        try:
            vol = df["Volume"] if len(tickers) == 1 else df["Volume"][entry.yahoo_ticker]
            vol = vol.dropna()
            if vol.empty:
                continue
            baselines[entry.symbol] = TimeOfDayVolumeBaseline.from_bars(
                list(vol.index.to_pydatetime()), list(vol.values),
            )
        except Exception as exc:
            log.warning("ComexYahooPoller: volume baseline backfill failed for %s (%s) — using flat window.",
                        entry.symbol, exc)
    log.info("ComexYahooPoller: built time-of-day volume baseline for %d/%d symbol(s).",
              len(baselines), len(watchlist))
    return baselines


class ComexYahooPoller:
    """
    Polls COMEX commodity futures (gold, silver, platinum, palladium, copper)
    via Yahoo Finance on a fixed interval, independent of whichever manager
    (LiveWebsocketManager or PollingFallbackManager) is watching NSE symbols —
    Shoonya carries no COMEX feed, so this always runs on Yahoo regardless of
    Shoonya session state.

    volume_spike uses "Method C" (see TimeOfDayVolumeBaseline) — a time-of-day-
    relative volume z-score AND-confirmed by the return z-score — backed by a
    60-day historical baseline fetched once in connect(). VIX confirmation is
    skipped entirely: COMEX moves are US-market-driven and have no reason to
    correlate with NSE India VIX in the same 5-minute bar.
    """

    def __init__(self, on_alert_events, poll_interval_seconds: int = 60):
        self._on_alert_events = on_alert_events
        self._poll_interval = poll_interval_seconds
        self._running = False
        self._thread: threading.Thread | None = None

        self._builders: dict[str, LiveBarBuilder] = {}   # symbol -> builder
        self._watchlist: list[WatchlistEntry] = []
        self._cum_volume: dict[str, float] = {}          # symbol -> running volume sum

    def connect(self, watchlist: list[WatchlistEntry]) -> None:
        self._watchlist = watchlist
        baselines = _fetch_comex_volume_baselines(watchlist)
        self._builders = {
            entry.symbol: LiveBarBuilder(
                symbol=entry.symbol,
                bar_seconds=settings.live_monitor_bar_seconds,
                buffer_size=settings.live_monitor_buffer_size,
                z_threshold=settings.live_monitor_zscore_threshold,
                volume_baseline=baselines.get(entry.symbol),
            )
            for entry in watchlist
        }
        self._cum_volume = {entry.symbol: 0.0 for entry in watchlist}
        if not watchlist:
            return
        log.info(
            "ComexYahooPoller: watching %d COMEX symbol(s) via Yahoo every %ds: %s",
            len(watchlist), self._poll_interval,
            ", ".join(f"{e.symbol}({e.yahoo_ticker})" for e in watchlist),
        )
        self._running = True
        self._thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=self._poll_interval + 5)

    def flush_all(self):
        events = []
        for builder in self._builders.values():
            events.extend(builder.flush())
        return events

    def _poll_loop(self) -> None:
        while self._running:
            try:
                self._poll_once()
            except Exception as exc:
                log.warning("ComexYahooPoller: poll cycle error: %s", exc)
            for _ in range(self._poll_interval):
                if not self._running:
                    break
                time.sleep(1.0)

    def _poll_once(self) -> None:
        if not self._watchlist:
            return
        from src.importer.fetchers.yahoo_snapshot_fetcher import fetch_yahoo_snapshots

        pairs = [(e.symbol, e.yahoo_ticker) for e in self._watchlist]
        rows = fetch_yahoo_snapshots(pairs)
        prices = {
            r["symbol"]: (float(r["market_price"]), float(r.get("volume") or 0.0))
            for r in rows
            if float(r.get("market_price") or 0) > 0
        }

        fetched: list[str] = []
        missing: list[str] = []
        for entry in self._watchlist:
            sym = entry.symbol
            if sym not in prices:
                missing.append(sym)
                continue
            price, vol_delta = prices[sym]
            # yfinance's Volume is per-1min-bar (a real delta), not cumulative —
            # unlike Shoonya/NSE's field — so accumulate it ourselves into the
            # cumulative counter LiveBarBuilder.on_tick() expects.
            self._cum_volume[sym] += vol_delta
            fetched.append(f"{sym}={price:.2f}(vol={vol_delta:.0f})")
            tick = {"t": "tf", "lp": str(price), "v": str(self._cum_volume[sym])}
            events = self._builders[sym].on_tick(tick)
            if events:
                self._on_alert_events(events)

        if fetched:
            log.debug("ComexYahooPoller: imported — %s", ", ".join(fetched))
        if missing:
            log.warning("ComexYahooPoller: no price for %s — skipped this cycle", ", ".join(missing))




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

def _install_shutdown_signals(stop_event: threading.Event) -> None:
    """First SIGINT/SIGTERM sets the stop event for a graceful shutdown. A
    second SIGINT force-exits immediately, mirroring
    src/agents/intraday_agent.py. SIGTERM is handled too (not just SIGINT)
    because `docker compose stop` sends SIGTERM, not SIGINT — without this,
    stopping the containerized service would skip the graceful shutdown path
    (bar flush, manager/poller stop()) entirely."""
    state = {"count": 0}

    def _handler(_signum, _frame):
        state["count"] += 1
        if state["count"] >= 2:
            print("\nForce quitting...")
            os._exit(1)
        print("\nShutting down (Ctrl+C again to force)...")
        stop_event.set()

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)


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

    from src.importer.clickhouse import ClickHouseImporter
    ClickHouseImporter().ensure_schema()

    setup_observers()

    def on_alert_events(events) -> None:
        bus = get_event_bus()
        for event in events:
            bus.publish(event)

    if args.dry_run:
        _run_dry_run(on_alert_events)
        return 0

    # ── Source cascade: Shoonya → NSE poll → Yahoo poll ───────────────────────
    manager: LiveWebsocketManager | PollingFallbackManager

    shoonya_ok = check_session()
    if shoonya_ok:
        from src.importer.fetchers.shoonya_fetcher import get_shoonya_api
        api = get_shoonya_api()
        if api is None:
            log.warning("Shoonya session check passed but API object is None — "
                        "falling back to NSE/Yahoo polling.")
            shoonya_ok = False

    if shoonya_ok:
        watchlist = resolve_watchlist(api, args.watchlist_config)
        if not watchlist:
            log.critical("Resolved watchlist is empty — nothing to watch. Exiting.")
            return 1
        manager = LiveWebsocketManager(api, on_alert_events)
        log.info("Live monitor: using Shoonya websocket (real-time).")
    else:
        log.warning(
            "Shoonya unavailable — falling back to NSE→Yahoo polling "
            "(~15-min delayed, every %ds).",
            settings.live_monitor_poll_interval_seconds,
        )
        # resolve_watchlist needs an api object for token lookup; pass None so
        # it skips Shoonya token resolution and uses static index entries only.
        watchlist = resolve_watchlist(None, args.watchlist_config)
        if not watchlist:
            log.critical("Resolved watchlist is empty — nothing to watch. Exiting.")
            return 1
        manager = PollingFallbackManager(
            on_alert_events,
            poll_interval_seconds=settings.live_monitor_poll_interval_seconds,
        )

    # COMEX (exchange="COMEX") is always Yahoo-sourced — Shoonya has no COMEX
    # feed — so it's split out of the watchlist handed to the NSE manager and
    # run on its own poller regardless of which NSE data source is active.
    nse_watchlist = [e for e in watchlist if e.exchange != "COMEX"]
    comex_watchlist = [e for e in watchlist if e.exchange == "COMEX"]

    nse_baselines = _fetch_nse_volume_baselines(nse_watchlist)
    manager.connect(nse_watchlist, nse_baselines=nse_baselines)

    comex_poller = ComexYahooPoller(
        on_alert_events, poll_interval_seconds=settings.live_monitor_poll_interval_seconds,
    )
    comex_poller.connect(comex_watchlist)

    stop_event = threading.Event()
    _install_shutdown_signals(stop_event)

    # No NSE-close auto-exit: COMEX (near-24h session) runs independently of
    # NSE hours, and Shoonya/NSE-poll simply go idle (no ticks) outside NSE
    # hours rather than needing the whole process torn down. Runs until
    # Ctrl+C / SIGTERM (e.g. `docker compose stop`) either way.
    log.info("Live monitor running until Ctrl+C / SIGTERM (no auto-exit at NSE close)...")
    try:
        while not stop_event.is_set():
            stop_event.wait(timeout=1.0)
    finally:
        final_events = manager.flush_all() + comex_poller.flush_all()
        if final_events:
            on_alert_events(final_events)
        manager.stop()
        comex_poller.stop()
        log.info("Live monitor stopped.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
