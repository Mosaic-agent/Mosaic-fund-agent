"""
src/agents/live_bar_builder.py
────────────────────────────────
Pure, websocket-agnostic tick → bar → anomaly-score pipeline for the live
monitor (src/agents/live_monitor.py). Takes raw Shoonya tick dicts, rolls them
into fixed-interval OHLCV bars, and scores each closed bar with the same
MAD-based robust z-score used by the EOD anomaly pipeline
(src/ml/anomaly/_features.py:robust_zscore) — NOT the full GARCH/IsolationForest/
PELT composite pipeline, which is too expensive to refit per bar and needs a
long daily history to converge.

One LiveBarBuilder instance per symbol. Not thread-safe — call on_tick() from
a single thread per instance (the websocket manager's tick-dispatch thread).

Zero I/O, zero threading here by design: this module has no import-time
dependency on Shoonya/websockets/ClickHouse, so it can be unit-tested by
feeding it a synthetic or recorded tick sequence without touching any live
service (see tests/test_live_bar_builder.py).
"""
from __future__ import annotations

import logging
import math
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta

import pandas as pd

from src.events.live_events import LiveAlertEvent
from src.ml.anomaly._features import robust_zscore
from src.utils.ist import now_ist

log = logging.getLogger(__name__)

# Opening-auction bars are structurally different from an intraday anomaly
# (price discovery, not a live move) — exclude the first N minutes from
# scoring by default so the open doesn't false-positive every single day.
_OPENING_EXCLUSION_MINUTES = 5


@dataclass
class Bar:
    ts: datetime      # bar close time, IST (naive — caller's clock is already IST)
    open: float
    high: float
    low: float
    close: float
    volume: float     # per-bar traded volume (delta of Shoonya's cumulative `v`)


class LiveBarBuilder:
    """
    Aggregates raw ticks for ONE symbol into fixed-interval bars and scores
    each closed bar for a price-break or volume-spike anomaly.

    Args:
        symbol:            NSE trading symbol (or index name, e.g. "NIFTY")
        bar_seconds:       bar aggregation interval in seconds (default 300 = 5 min)
        buffer_size:       rolling bar buffer fed into robust_zscore (default 30 bars)
        z_threshold:       |z| above which a bar is flagged (default 3.0)
        market_open_hhmm:  (hour, minute) of the session open, for opening-bar exclusion
        clock:             zero-arg callable returning the current IST-aware datetime.
                            Defaults to now_ist() for live use; tests inject a fake
                            clock to deterministically control bar boundaries without
                            depending on wall-clock time (Shoonya ticks don't reliably
                            carry a usable per-tick timestamp field across feed types).
    """

    def __init__(
        self,
        symbol: str,
        bar_seconds: int = 300,
        buffer_size: int = 30,
        z_threshold: float = 3.0,
        market_open_hhmm: tuple[int, int] = (9, 15),
        clock=now_ist,
    ):
        self.symbol = symbol.strip().upper()
        self.bar_seconds = bar_seconds
        self.buffer_size = buffer_size
        self.z_threshold = z_threshold
        self.market_open_hhmm = market_open_hhmm
        self._clock = clock

        self._current_bar_start: datetime | None = None
        self._current_open: float | None = None
        self._current_high: float = -math.inf
        self._current_low: float = math.inf
        self._current_close: float | None = None
        self._current_bar_volume: float = 0.0

        self._prev_cum_volume: float | None = None  # Shoonya `v` is cumulative day volume
        self._closed_bars: deque[Bar] = deque(maxlen=buffer_size)

        # Most recent SCORED bar's price z-score/timestamp, regardless of
        # whether it crossed this builder's own alert threshold. Exposed so
        # the live monitor can use one symbol's move (e.g. INDIA VIX) as a
        # cross-symbol confirmation signal for another symbol's alert —
        # stays None until min_periods bars have accumulated.
        self.last_z_return: float | None = None
        self.last_scored_bar_ts: datetime | None = None

    # ── Tick ingestion ───────────────────────────────────────────────────────

    def on_tick(self, tick: dict) -> list[LiveAlertEvent]:
        """
        Ingest one raw Shoonya tick dict (keys: t, lp, v, ...). Rolls the
        current bar into the closed-bar buffer and scores it once
        bar_seconds has elapsed since the bar started. Returns a (possibly
        empty) list of LiveAlertEvent — a bar can trip both price_break AND
        volume_spike independently.
        """
        if not tick or tick.get("t") not in ("tk", "tf"):
            return []

        lp = tick.get("lp")
        if lp is None:
            return []
        price = float(lp)

        ts = self._tick_timestamp(tick)
        bar_start = self._bar_start_for(ts)

        events: list[LiveAlertEvent] = []
        if self._current_bar_start is None:
            self._open_bar(bar_start, price)
        elif bar_start != self._current_bar_start:
            closed = self._close_bar()
            if closed is not None:
                events.extend(self._score_bar(closed))
            self._open_bar(bar_start, price)

        # Per-tick traded volume = delta of Shoonya's cumulative day volume.
        v = tick.get("v")
        if v is not None:
            v_f = float(v)
            if self._prev_cum_volume is not None:
                delta = max(0.0, v_f - self._prev_cum_volume)
                self._current_bar_volume += delta
            self._prev_cum_volume = v_f

        self._current_high = max(self._current_high, price)
        self._current_low = min(self._current_low, price)
        self._current_close = price

        return events

    def flush(self) -> list[LiveAlertEvent]:
        """Force-close the in-progress bar (e.g. at market close/shutdown) and score it."""
        closed = self._close_bar()
        if closed is None:
            return []
        return self._score_bar(closed)

    # ── Bar mechanics ────────────────────────────────────────────────────────

    def _tick_timestamp(self, tick: dict) -> datetime:
        """Shoonya ticks don't reliably carry a per-tick IST timestamp field
        across all feed types — use the injected clock (wall-clock IST time
        in production; a fake incrementing clock in tests)."""
        return self._clock()

    def _bar_start_for(self, ts: datetime) -> datetime:
        """Bucket to the bar boundary. IST is a fixed UTC+5:30 offset with no
        DST, and a day divides evenly by any bar_seconds that itself divides
        86400, so epoch-based bucketing lines up with clean IST clock times
        (e.g. 09:15:00, 09:20:00, ... for bar_seconds=300)."""
        epoch_seconds = int(ts.timestamp())
        bucket_start = epoch_seconds - (epoch_seconds % self.bar_seconds)
        return datetime.fromtimestamp(bucket_start, tz=ts.tzinfo)

    def _open_bar(self, bar_start: datetime, price: float) -> None:
        self._current_bar_start = bar_start
        self._current_open = price
        self._current_high = price
        self._current_low = price
        self._current_close = price
        self._current_bar_volume = 0.0

    def _close_bar(self) -> Bar | None:
        if self._current_bar_start is None or self._current_close is None:
            return None
        bar = Bar(
            ts=self._current_bar_start + timedelta(seconds=self.bar_seconds),
            open=self._current_open,
            high=self._current_high,
            low=self._current_low,
            close=self._current_close,
            volume=self._current_bar_volume,
        )
        self._closed_bars.append(bar)
        return bar

    def _is_opening_bar(self, bar: Bar) -> bool:
        """True for the bar covering the opening auction — its START (not
        close) time is compared against the session open, since bar.ts is
        the bar's CLOSE time (bar_start + bar_seconds)."""
        open_h, open_m = self.market_open_hhmm
        bar_start = bar.ts - timedelta(seconds=self.bar_seconds)
        open_dt = bar_start.replace(hour=open_h, minute=open_m, second=0, microsecond=0)
        return open_dt <= bar_start < open_dt + timedelta(minutes=_OPENING_EXCLUSION_MINUTES)

    # ── Scoring ──────────────────────────────────────────────────────────────

    def _score_bar(self, bar: Bar) -> list[LiveAlertEvent]:
        if self._is_opening_bar(bar):
            return []
        min_periods = max(2, self.buffer_size // 2)
        if len(self._closed_bars) < min_periods:
            return []

        closes = pd.Series([b.close for b in self._closed_bars])
        volumes = pd.Series([b.volume for b in self._closed_bars])

        log_returns = closes.apply(math.log).diff().fillna(0.0)
        z_returns = robust_zscore(log_returns, window=self.buffer_size)
        z_volumes = robust_zscore(volumes, window=self.buffer_size)

        z_return = float(z_returns.iloc[-1])
        z_volume = float(z_volumes.iloc[-1])
        baseline_avg_volume = float(volumes.iloc[:-1].median()) if len(volumes) > 1 else 0.0

        # Record regardless of whether this bar crosses OUR OWN alert
        # threshold — a caller doing cross-symbol confirmation (e.g. "did VIX
        # also move") needs the raw z-score, not just threshold breaches.
        self.last_z_return = z_return
        self.last_scored_bar_ts = bar.ts

        events: list[LiveAlertEvent] = []
        if abs(z_return) >= self.z_threshold:
            events.append(LiveAlertEvent(
                symbol=self.symbol, timestamp=bar.ts, alert_type="price_break",
                zscore=z_return, price=bar.close, volume=bar.volume,
                baseline_avg_volume=baseline_avg_volume,
            ))
        # Volume is one-sided — a spike is anomalous, a lull is not.
        if z_volume >= self.z_threshold:
            events.append(LiveAlertEvent(
                symbol=self.symbol, timestamp=bar.ts, alert_type="volume_spike",
                zscore=z_volume, price=bar.close, volume=bar.volume,
                baseline_avg_volume=baseline_avg_volume,
            ))
        if events:
            log.info(
                "LiveBarBuilder[%s]: bar@%s close=%.2f vol=%.0f → z_return=%.2f z_volume=%.2f (%d alert(s))",
                self.symbol, bar.ts, bar.close, bar.volume, z_return, z_volume, len(events),
            )
        return events
