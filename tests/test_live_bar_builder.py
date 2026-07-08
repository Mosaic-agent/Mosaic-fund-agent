"""
tests/test_live_bar_builder.py
────────────────────────────────
Unit test for LiveBarBuilder — pure tick→bar→anomaly-score replay, no network,
no Shoonya, no ClickHouse. A fake incrementing clock drives deterministic bar
boundaries (Shoonya ticks don't reliably carry a usable per-tick timestamp).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.agents.live_bar_builder import Bar, LiveBarBuilder

_IST = timezone(timedelta(hours=5, minutes=30))


class _FakeClock:
    """Zero-arg callable clock with a manual .advance(seconds), injected into
    LiveBarBuilder so bar boundaries are deterministic in tests."""

    def __init__(self, start: datetime):
        self._t = start

    def __call__(self) -> datetime:
        return self._t

    def advance(self, seconds: float) -> None:
        self._t += timedelta(seconds=seconds)


def _make_builder(bar_seconds: int = 300, buffer_size: int = 10, z_threshold: float = 3.0):
    # 10:00 IST is well past the 09:15-09:20 opening-bar exclusion window.
    start = datetime(2024, 1, 2, 10, 0, 0, tzinfo=_IST)
    clock = _FakeClock(start)
    builder = LiveBarBuilder(
        "TESTSYM", bar_seconds=bar_seconds, buffer_size=buffer_size,
        z_threshold=z_threshold, clock=clock,
    )
    return builder, clock


def test_stable_bars_produce_no_alerts():
    builder, clock = _make_builder()
    cum_volume = 0.0
    all_events = []

    prices = [100.00, 100.04, 99.97, 100.02, 99.99, 100.03, 99.98, 100.01, 100.02, 99.99, 100.00, 100.01]
    volumes = [1000, 980, 1050, 990, 1010, 970, 1030, 1000, 990, 1005, 995, 1000]

    for price, vol_delta in zip(prices, volumes):
        cum_volume += vol_delta
        tick = {"t": "tf", "lp": str(price), "v": str(cum_volume)}
        all_events.extend(builder.on_tick(tick))
        clock.advance(builder.bar_seconds)

    assert all_events == []


def test_price_break_and_volume_spike_flagged_on_close():
    builder, clock = _make_builder()
    cum_volume = 0.0

    def feed(price: float, vol_delta: float) -> list:
        nonlocal cum_volume
        cum_volume += vol_delta
        tick = {"t": "tf", "lp": str(price), "v": str(cum_volume)}
        events = builder.on_tick(tick)
        clock.advance(builder.bar_seconds)
        return events

    # Warm up past min_periods (buffer_size // 2 = 5) with stable, lightly
    # jittered price/volume so the rolling MAD baseline is small but non-zero.
    prices = [100.00, 100.04, 99.97, 100.02, 99.99, 100.03, 99.98, 100.01, 100.02, 99.99, 100.00, 100.01]
    volumes = [1000, 980, 1050, 990, 1010, 970, 1030, 1000, 990, 1005, 995, 1000]
    for price, vol_delta in zip(prices, volumes):
        feed(price, vol_delta)

    # This tick closes the last stable bar (unremarkable) and opens the spike
    # bar — nothing should be flagged yet since the spike bar hasn't closed.
    pre_spike_events = feed(112.0, 60_000)  # ~12% jump, 60x normal volume
    assert pre_spike_events == []

    # The NEXT tick closes and scores the spike bar.
    spike_events = feed(112.05, 1_000)
    alert_types = {e.alert_type for e in spike_events}
    assert "price_break" in alert_types
    assert "volume_spike" in alert_types
    for event in spike_events:
        assert event.symbol == "TESTSYM"
        assert abs(event.zscore) >= builder.z_threshold


def test_opening_bar_is_excluded_from_scoring():
    # A full on_tick() integration test can't isolate this cleanly: the
    # opening bar is by definition the FIRST closed bar, so it never has
    # enough buffer (min_periods) to be scored regardless of the exclusion —
    # that's a separate, confounding reason for it to return []. Test the
    # exclusion predicate directly instead.
    builder, _ = _make_builder(bar_seconds=300)

    opening_bar = Bar(
        ts=datetime(2024, 1, 2, 9, 20, 0, tzinfo=_IST),  # closes the 09:15-09:20 bar
        open=100.0, high=150.0, low=100.0, close=150.0, volume=1_000_000,
    )
    assert builder._is_opening_bar(opening_bar) is True

    later_bar = Bar(
        ts=datetime(2024, 1, 2, 10, 5, 0, tzinfo=_IST),  # closes the 10:00-10:05 bar
        open=100.0, high=101.0, low=99.0, close=100.5, volume=1_000,
    )
    assert builder._is_opening_bar(later_bar) is False
