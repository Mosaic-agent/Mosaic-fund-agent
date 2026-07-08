"""
tests/test_live_monitor_vix_gate.py
──────────────────────────────────────
Unit tests for the cross-symbol VIX confirmation gate
(src/agents/live_monitor.py:vix_confirms) — a pure function, no websocket or
Shoonya dependency.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.agents.live_monitor import VIX_SYMBOL, vix_confirms

_IST = timezone(timedelta(hours=5, minutes=30))
_T = datetime(2024, 1, 2, 13, 45, 0, tzinfo=_IST)
_BAR_SECONDS = 300


def test_vix_own_alerts_always_pass():
    assert vix_confirms(
        event_symbol=VIX_SYMBOL, event_alert_type="price_break", event_ts=_T,
        vix_last_z=None, vix_last_bar_ts=None,
        bar_seconds=_BAR_SECONDS, vix_confirmation_zscore=2.0,
    ) is True


def test_volume_spike_never_gated():
    assert vix_confirms(
        event_symbol="RELIANCE", event_alert_type="volume_spike", event_ts=_T,
        vix_last_z=0.1, vix_last_bar_ts=_T,
        bar_seconds=_BAR_SECONDS, vix_confirmation_zscore=2.0,
    ) is True


def test_price_break_fails_open_when_vix_has_no_baseline():
    assert vix_confirms(
        event_symbol="RELIANCE", event_alert_type="price_break", event_ts=_T,
        vix_last_z=None, vix_last_bar_ts=None,
        bar_seconds=_BAR_SECONDS, vix_confirmation_zscore=2.0,
    ) is True


def test_price_break_fails_open_when_vix_data_is_stale():
    stale_ts = _T - timedelta(seconds=_BAR_SECONDS * 3)
    assert vix_confirms(
        event_symbol="RELIANCE", event_alert_type="price_break", event_ts=_T,
        vix_last_z=10.0, vix_last_bar_ts=stale_ts,
        bar_seconds=_BAR_SECONDS, vix_confirmation_zscore=2.0,
    ) is True


def test_price_break_suppressed_when_vix_move_too_small():
    assert vix_confirms(
        event_symbol="RELIANCE", event_alert_type="price_break", event_ts=_T,
        vix_last_z=0.4, vix_last_bar_ts=_T,
        bar_seconds=_BAR_SECONDS, vix_confirmation_zscore=2.0,
    ) is False


def test_price_break_confirmed_when_vix_moved_meaningfully():
    assert vix_confirms(
        event_symbol="RELIANCE", event_alert_type="price_break", event_ts=_T,
        vix_last_z=-3.5, vix_last_bar_ts=_T,
        bar_seconds=_BAR_SECONDS, vix_confirmation_zscore=2.0,
    ) is True
