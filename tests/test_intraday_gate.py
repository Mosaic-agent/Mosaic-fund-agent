"""Tests for the intraday regime filter (#1), time-of-day gate (#2),
and sample-reliability gate (#3)."""

from __future__ import annotations

import sys
import os
from datetime import datetime
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.agents.intraday_agent import (
    BaseIntradayAgent,
    ADX_TREND_THRESHOLD,
    CONTEXT_READINESS_CAP,
    MOMENTUM_BREAKOUT_LONGS,
    MIN_RELIABLE_TICKS,
)

IST = ZoneInfo("Asia/Kolkata")


def _make_agent(**overrides) -> BaseIntradayAgent:
    """Build a bare BaseIntradayAgent without __init__ (no DB/network)."""
    a = object.__new__(BaseIntradayAgent)
    a.adx_value = overrides.get("adx_value", 15.0)
    a.regime = overrides.get("regime", "Mean-Reverting")
    a.live_price = overrides.get("live_price", 100.0)
    a.prev_close = 100.0
    a.ema50 = 98.0
    a.ema200 = 95.0
    a.avg_vol_15d = 1_000_000
    a.best_bid = 99.9
    a.best_ask = 100.1
    return a


def _ist(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 7, 2, hour, minute, 0, tzinfo=IST)


# ── _session_phase ────────────────────────────────────────────────────

class TestSessionPhase:
    def test_opening(self):
        a = _make_agent()
        with patch("src.agents.intraday_agent.now_ist", return_value=_ist(9, 30)):
            assert a._session_phase() == "OPENING"

    def test_morning(self):
        a = _make_agent()
        with patch("src.agents.intraday_agent.now_ist", return_value=_ist(10, 30)):
            assert a._session_phase() == "MORNING"

    def test_midday(self):
        a = _make_agent()
        with patch("src.agents.intraday_agent.now_ist", return_value=_ist(12, 0)):
            assert a._session_phase() == "MIDDAY"

    def test_midday_boundary_start(self):
        a = _make_agent()
        with patch("src.agents.intraday_agent.now_ist", return_value=_ist(11, 30)):
            assert a._session_phase() == "MIDDAY"

    def test_midday_boundary_end(self):
        a = _make_agent()
        with patch("src.agents.intraday_agent.now_ist", return_value=_ist(13, 30)):
            assert a._session_phase() == "AFTERNOON"

    def test_afternoon(self):
        a = _make_agent()
        with patch("src.agents.intraday_agent.now_ist", return_value=_ist(14, 0)):
            assert a._session_phase() == "AFTERNOON"

    def test_closing(self):
        a = _make_agent()
        with patch("src.agents.intraday_agent.now_ist", return_value=_ist(14, 35)):
            assert a._session_phase() == "CLOSING"

    def test_off_before_market(self):
        a = _make_agent()
        with patch("src.agents.intraday_agent.now_ist", return_value=_ist(8, 0)):
            assert a._session_phase() == "OFF"

    def test_off_after_market(self):
        a = _make_agent()
        with patch("src.agents.intraday_agent.now_ist", return_value=_ist(20, 0)):
            assert a._session_phase() == "OFF"


# ── _context_unfavorable ──────────────────────────────────────────────

class TestContextUnfavorable:
    def test_mean_reverting_non_midday(self):
        a = _make_agent(regime="Mean-Reverting", adx_value=18.0)
        with patch("src.agents.intraday_agent.now_ist", return_value=_ist(10, 0)):
            gated, reason = a._context_unfavorable()
            assert gated is True
            assert "range-bound" in reason
            assert "midday" not in reason

    def test_trending_midday(self):
        a = _make_agent(regime="Trending", adx_value=30.0)
        with patch("src.agents.intraday_agent.now_ist", return_value=_ist(12, 0)):
            gated, reason = a._context_unfavorable()
            assert gated is True
            assert "midday" in reason
            assert "range-bound" not in reason

    def test_both_unfavorable(self):
        a = _make_agent(regime="Mean-Reverting", adx_value=18.0)
        with patch("src.agents.intraday_agent.now_ist", return_value=_ist(12, 0)):
            gated, reason = a._context_unfavorable()
            assert gated is True
            assert "range-bound" in reason
            assert "midday" in reason

    def test_favorable(self):
        a = _make_agent(regime="Trending", adx_value=30.0)
        with patch("src.agents.intraday_agent.now_ist", return_value=_ist(10, 0)):
            gated, reason = a._context_unfavorable()
            assert gated is False
            assert reason == ""


# ── State mapping ─────────────────────────────────────────────────────

class TestStateMapping:
    def test_standby(self):
        a = _make_agent()
        assert a._map_signal_to_state("STANDBY (CONTEXT)") == "🟠 STANDBY"

    def test_buy_still_long_setup(self):
        a = _make_agent()
        assert a._map_signal_to_state("BUY") == "🟡 LONG SETUP"

    def test_momentum_confirmed_still_active(self):
        a = _make_agent()
        assert a._map_signal_to_state("BUY (MOMENTUM CONFIRMED)") == "🟢 LONG ACTIVE"

    def test_avoid_still_avoid(self):
        a = _make_agent()
        assert a._map_signal_to_state("WATCH / AVOID (PREMIUM DRAG)") == "⛔ AVOID"


# ── Readiness cap ─────────────────────────────────────────────────────

class TestReadinessCap:
    def _score(self, agent, signal):
        momentum = {"vwap_z": 0.5, "cum_delta": 1000, "rsi": 60.0, "micro_mom": 0.02}
        score, breakdown = agent._calculate_confidence_score(
            price=100.0, volume=500_000, vwap=99.5,
            pct_from_ema=2.0, relative_vol=1.3, momentum=momentum,
            premium_pct=None, premium_threshold=None, signal=signal,
        )
        return score, breakdown

    def test_buy_capped_when_mean_reverting(self):
        a = _make_agent(regime="Mean-Reverting", adx_value=18.0)
        with patch("src.agents.intraday_agent.now_ist", return_value=_ist(10, 0)):
            score, bk = self._score(a, "BUY")
            assert score <= CONTEXT_READINESS_CAP
            assert all(k in bk for k in ("trend", "vwap", "delta", "vol", "rsi", "prem"))

    def test_buy_capped_when_midday(self):
        a = _make_agent(regime="Trending", adx_value=30.0)
        with patch("src.agents.intraday_agent.now_ist", return_value=_ist(12, 0)):
            score, _ = self._score(a, "BUY")
            assert score <= CONTEXT_READINESS_CAP

    def test_buy_momentum_capped(self):
        a = _make_agent(regime="Mean-Reverting", adx_value=18.0)
        with patch("src.agents.intraday_agent.now_ist", return_value=_ist(10, 0)):
            score, _ = self._score(a, "BUY (MOMENTUM CONFIRMED)")
            assert score <= CONTEXT_READINESS_CAP

    def test_buy_uncapped_when_favorable(self):
        a = _make_agent(regime="Trending", adx_value=30.0)
        with patch("src.agents.intraday_agent.now_ist", return_value=_ist(10, 0)):
            score, _ = self._score(a, "BUY")
            assert score > CONTEXT_READINESS_CAP

    def test_breakdown_keys_preserved(self):
        a = _make_agent(regime="Mean-Reverting", adx_value=18.0)
        with patch("src.agents.intraday_agent.now_ist", return_value=_ist(10, 0)):
            _, bk = self._score(a, "BUY")
            assert set(bk.keys()) == {"trend", "vwap", "delta", "vol", "rsi", "prem"}


# ── Exemptions ────────────────────────────────────────────────────────

class TestExemptions:
    """Signals exempt from the context gate should pass through unchanged."""

    def test_discount_accumulation_not_gated(self):
        a = _make_agent(regime="Mean-Reverting", adx_value=18.0)
        signal = "BUY (DISCOUNT + ACCUMULATION)"
        assert signal not in MOMENTUM_BREAKOUT_LONGS
        assert a._map_signal_to_state(signal) == "🟢 LONG ACTIVE"

    def test_premium_drag_not_gated(self):
        signal = "WATCH / AVOID (PREMIUM DRAG)"
        assert signal not in MOMENTUM_BREAKOUT_LONGS

    def test_accumulate_not_gated(self):
        signal = "ACCUMULATE (BUYING SUPPORT)"
        assert signal not in MOMENTUM_BREAKOUT_LONGS

    def test_bearish_divergence_not_gated(self):
        signal = "⚠ BEARISH DIVERGENCE"
        assert signal not in MOMENTUM_BREAKOUT_LONGS

    def test_hold_not_gated(self):
        assert "HOLD" not in MOMENTUM_BREAKOUT_LONGS

    def test_accumulate_readiness_uncapped(self):
        a = _make_agent(regime="Mean-Reverting", adx_value=18.0)
        momentum = {"vwap_z": 0.5, "cum_delta": 1000, "rsi": 60.0, "micro_mom": 0.02}
        with patch("src.agents.intraday_agent.now_ist", return_value=_ist(12, 0)):
            score, _ = a._calculate_confidence_score(
                price=100.0, volume=500_000, vwap=99.5,
                pct_from_ema=2.0, relative_vol=1.3, momentum=momentum,
                premium_pct=None, premium_threshold=None,
                signal="ACCUMULATE (BUYING SUPPORT)",
            )
            assert score > CONTEXT_READINESS_CAP


# ── Waiting-for list ──────────────────────────────────────────────────

class TestStandbyWaitingFor:
    def test_standby_lists_regime_reason(self):
        a = _make_agent(regime="Mean-Reverting", adx_value=18.0)
        with patch("src.agents.intraday_agent.now_ist", return_value=_ist(10, 0)):
            lines = a._generate_waiting_for_list(
                "🟠 STANDBY", 100.0, 99.5, {}, 1.0
            )
            text = "\n".join(lines)
            assert "Trending" in text
            assert "ADX" in text
            assert "VWAP" in text

    def test_standby_lists_midday_reason(self):
        a = _make_agent(regime="Trending", adx_value=30.0)
        with patch("src.agents.intraday_agent.now_ist", return_value=_ist(12, 0)):
            lines = a._generate_waiting_for_list(
                "🟠 STANDBY", 100.0, 99.5, {}, 1.0
            )
            text = "\n".join(lines)
            assert "midday" in text
            assert "13:30" in text

    def test_standby_lists_both_reasons(self):
        a = _make_agent(regime="Mean-Reverting", adx_value=18.0)
        with patch("src.agents.intraday_agent.now_ist", return_value=_ist(12, 0)):
            lines = a._generate_waiting_for_list(
                "🟠 STANDBY", 100.0, 99.5, {}, 1.0
            )
            text = "\n".join(lines)
            assert "Trending" in text
            assert "midday" in text


# ── Sample-reliability gate ──────────────────────────────────────────

class TestSampleReliability:
    """Momentum breakout longs should also gate on too-small a tick sample,
    independent of regime/session, and never affect exempt signals."""

    def test_low_sample_is_unfavorable(self):
        a = _make_agent(regime="Trending", adx_value=30.0)
        a._last_sample_size = 5
        with patch("src.agents.intraday_agent.now_ist", return_value=_ist(10, 0)):
            gated, reason = a._context_unfavorable()
            assert gated is True
            assert "sample size" in reason
            assert "5/" in reason
            assert str(MIN_RELIABLE_TICKS) in reason

    def test_sufficient_sample_is_favorable(self):
        a = _make_agent(regime="Trending", adx_value=30.0)
        a._last_sample_size = MIN_RELIABLE_TICKS
        with patch("src.agents.intraday_agent.now_ist", return_value=_ist(10, 0)):
            gated, reason = a._context_unfavorable()
            assert gated is False
            assert reason == ""

    def test_unset_sample_size_does_not_gate(self):
        """Bare instances (e.g. chat_cmd calling _calculate_confidence_score
        before evaluate_signal has run) must not crash or false-gate."""
        a = _make_agent(regime="Trending", adx_value=30.0)
        assert getattr(a, "_last_sample_size", None) is None
        with patch("src.agents.intraday_agent.now_ist", return_value=_ist(10, 0)):
            gated, reason = a._context_unfavorable()
            assert gated is False

    def test_readiness_capped_on_low_sample_alone(self):
        a = _make_agent(regime="Trending", adx_value=30.0)
        a._last_sample_size = 8
        momentum = {"vwap_z": 0.5, "cum_delta": 1000, "rsi": 60.0, "micro_mom": 0.02}
        with patch("src.agents.intraday_agent.now_ist", return_value=_ist(10, 0)):
            score, _ = a._calculate_confidence_score(
                price=100.0, volume=500_000, vwap=99.5,
                pct_from_ema=2.0, relative_vol=1.3, momentum=momentum,
                premium_pct=None, premium_threshold=None, signal="BUY",
            )
            assert score <= CONTEXT_READINESS_CAP

    def test_exempt_signal_not_capped_on_low_sample(self):
        a = _make_agent(regime="Trending", adx_value=30.0)
        a._last_sample_size = 3
        momentum = {"vwap_z": 0.5, "cum_delta": 1000, "rsi": 60.0, "micro_mom": 0.02}
        with patch("src.agents.intraday_agent.now_ist", return_value=_ist(10, 0)):
            score, _ = a._calculate_confidence_score(
                price=100.0, volume=500_000, vwap=99.5,
                pct_from_ema=2.0, relative_vol=1.3, momentum=momentum,
                premium_pct=None, premium_threshold=None,
                signal="ACCUMULATE (BUYING SUPPORT)",
            )
            assert score > CONTEXT_READINESS_CAP

    def test_standby_waiting_for_lists_sample_reason(self):
        a = _make_agent(regime="Trending", adx_value=30.0)
        a._last_sample_size = 12
        with patch("src.agents.intraday_agent.now_ist", return_value=_ist(10, 0)):
            lines = a._generate_waiting_for_list(
                "🟠 STANDBY", 100.0, 99.5, {}, 1.0
            )
            text = "\n".join(lines)
            assert "tick samples" in text
            assert "12/" in text

    def test_standby_waiting_for_omits_sample_reason_when_reliable(self):
        a = _make_agent(regime="Mean-Reverting", adx_value=18.0)
        a._last_sample_size = 50
        with patch("src.agents.intraday_agent.now_ist", return_value=_ist(10, 0)):
            lines = a._generate_waiting_for_list(
                "🟠 STANDBY", 100.0, 99.5, {}, 1.0
            )
            text = "\n".join(lines)
            assert "tick samples" not in text
