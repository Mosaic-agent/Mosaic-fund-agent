"""
tests/test_intent_router.py
────────────────────────────
Golden test suite for the intent router.

Tests both:
  1. The regex fallback (route_intent from sub_agents.py) — deterministic
  2. The LLM router contract (route_intent_llm) — falls back to regex when no API key

These 50+ test cases serve as a regression net: any change to routing logic
(regex patterns or LLM prompt) must still pass all golden pairs.
"""
from __future__ import annotations

import pytest

from src.agents.sub_agents import route_intent


# ── Golden pairs: (question, expected_intent) ─────────────────────────────────
# Grouped by expected intent for readability.

GOLDEN_PAIRS: list[tuple[str, str]] = [
    # ── main (default / import / general) ─────────────────────────────────────

    ("import nav of GOLDBEES", "main"),
    ("refresh stock prices", "main"),
    ("sync all data", "main"),
    ("backfill etf prices", "main"),
    ("import --category stocks", "main"),

    # ── signal ────────────────────────────────────────────────────────────────
    ("what is the composite signal for GOLDBEES?", "signal"),
    ("run goldbees pipeline", "signal"),
    ("today's gold signal", "signal"),
    ("show kelly weight for GOLDBEES", "signal"),
    ("what is the blended weight?", "signal"),
    ("GARCH volatility chart", "signal"),
    ("ml prediction for etfs", "signal"),
    ("regime signal", "signal"),
    ("buy signal for etfs", "signal"),
    ("plot price chart", "signal"),
    ("plot returns chart", "signal"),

    # ── macro ─────────────────────────────────────────────────────────────────
    ("what are the macro themes today?", "macro"),
    ("comex pre-market analysis", "macro"),
    ("FII flows this week", "macro"),
    ("DII flow trend", "macro"),
    ("gold price outlook", "macro"),
    ("crude oil impact on Indian markets", "macro"),
    ("what is usd-inr doing?", "macro"),
    ("is there a war risk from Iran?", "macro"),
    ("tariff impact on equities", "macro"),
    ("COT report for gold", "macro"),

    # ── deepdive ──────────────────────────────────────────────────────────────
    ("deep dive ADSK", "deepdive"),
    ("10-K filing for Apple", "deepdive"),
    ("SEC filing analysis for NVDA", "deepdive"),
    ("EDGAR annual report MSFT", "deepdive"),

    # ── news ──────────────────────────────────────────────────────────────────
    ("latest news on RELIANCE", "news"),
    ("market headlines today", "news"),
    ("etf news sentiment", "news"),
    ("what's happening with TCS?", "news"),
    ("breaking news for IT sector", "news"),

    # ── code ──────────────────────────────────────────────────────────────────
    ("write a python script to backtest momentum", "code"),
    ("create a new fetcher for NSE data", "code"),
    ("execute python code to analyze returns", "code"),

    # ── database ──────────────────────────────────────────────────────────────
    ("query the database for GOLDBEES prices", "database"),
    ("show me all tables in clickhouse", "database"),
    ("SELECT count() FROM market_data.daily_prices", "database"),
    ("describe table daily_prices", "database"),
    ("what are the watermarks?", "database"),

    # ── intl_etf ──────────────────────────────────────────────────────────────
    ("international etf performance", "intl_etf"),
    ("MAFANG ETF analysis", "intl_etf"),
    ("Hang Seng ETF regime", "intl_etf"),
    ("HNGSNGBEES premium", "intl_etf"),

    # ── research ──────────────────────────────────────────────────────────────
    ("autonomous research on gold ETFs", "research"),
    ("comprehensive analysis of HDFC Bank", "research"),
    ("deep research into pharma sector", "research"),
    ("full thesis on renewable energy stocks", "research"),
    ("why is GOLDBEES falling today?", "research"),
]


class TestRegexRouter:
    """Test the deterministic regex-based intent router."""

    @pytest.mark.parametrize("question,expected", GOLDEN_PAIRS)
    def test_golden_pair(self, question: str, expected: str) -> None:
        result = route_intent(question)
        assert result == expected, (
            f"route_intent({question!r}) = {result!r}, expected {expected!r}"
        )


class TestLLMRouter:
    """Test the LLM router contract (falls back to regex when no API key)."""

    def test_fallback_matches_regex(self) -> None:
        """When no LLM is available, route_intent_llm must match regex router."""
        from src.agents.intent_router import route_intent_llm, clear_intent_cache, _get_router_llm

        clear_intent_cache()

        # If LLM is available, skip this test — it validates fallback only
        if _get_router_llm() is not None:
            pytest.skip("LLM API key available — fallback test not applicable")

        for question, expected in GOLDEN_PAIRS[:10]:
            result = route_intent_llm(question)
            assert result == expected, (
                f"route_intent_llm({question!r}) = {result!r}, expected {expected!r} "
                f"(fallback should match regex router)"
            )

    def test_valid_intents_returned(self) -> None:
        """route_intent_llm must always return a valid intent string."""
        from src.agents.intent_router import route_intent_llm, VALID_INTENTS, clear_intent_cache

        clear_intent_cache()
        for question, _ in GOLDEN_PAIRS[:5]:
            result = route_intent_llm(question)
            assert result in VALID_INTENTS, (
                f"route_intent_llm({question!r}) returned invalid intent {result!r}"
            )

    def test_cache_works(self) -> None:
        """Repeated calls with the same question should hit cache."""
        from src.agents.intent_router import route_intent_llm, _intent_cache, clear_intent_cache

        clear_intent_cache()
        q = "what is the composite signal?"
        r1 = route_intent_llm(q)
        r2 = route_intent_llm(q)
        assert r1 == r2


class TestBudget:
    """Test the budget enforcement callback."""

    def test_budget_exceeded_on_tool_calls(self) -> None:
        from src.agents.budget import BudgetCallbackHandler, BudgetExceededError

        budget = BudgetCallbackHandler(max_tool_calls=2)
        budget.on_tool_start({"name": "tool_a"}, "arg1")
        budget.on_tool_start({"name": "tool_b"}, "arg2")
        with pytest.raises(BudgetExceededError, match="Total tool-call budget"):
            budget.on_tool_start({"name": "tool_c"}, "arg3")

    def test_per_tool_cap(self) -> None:
        from src.agents.budget import BudgetCallbackHandler, BudgetExceededError

        budget = BudgetCallbackHandler(
            max_tool_calls=100,
            tool_caps={"expensive_tool": 1},
        )
        budget.on_tool_start({"name": "expensive_tool"}, "arg1")
        with pytest.raises(BudgetExceededError, match="Per-tool budget"):
            budget.on_tool_start({"name": "expensive_tool"}, "arg2")

    def test_wall_clock_exceeded(self) -> None:
        from src.agents.budget import BudgetCallbackHandler, BudgetExceededError
        import time

        budget = BudgetCallbackHandler(max_wall_clock_s=0.01)
        time.sleep(0.02)
        with pytest.raises(BudgetExceededError, match="Wall-clock"):
            budget.on_tool_start({"name": "any"}, "arg")

    def test_summary(self) -> None:
        from src.agents.budget import BudgetCallbackHandler

        budget = BudgetCallbackHandler(max_tool_calls=10)
        budget.on_tool_start({"name": "a"}, "x")
        budget.on_tool_start({"name": "b"}, "y")
        s = budget.summary
        assert s["tool_calls"] == 2
        assert s["per_tool"] == {"a": 1, "b": 1}


class TestTracer:
    """Test the tracer module (unit tests — no ClickHouse required)."""

    def test_callback_handler_counts_steps(self) -> None:
        from src.agents.tracer import TracingCallbackHandler

        tracer = TracingCallbackHandler(agent="test")
        tracer.on_tool_start({"name": "t1"}, "arg")
        assert tracer._step == 1
        tracer.on_tool_start({"name": "t2"}, "arg")
        assert tracer._step == 2

    def test_run_id_generated(self) -> None:
        from src.agents.tracer import TracingCallbackHandler

        t1 = TracingCallbackHandler(agent="a")
        t2 = TracingCallbackHandler(agent="b")
        assert t1.run_id != t2.run_id
        assert len(t1.run_id) == 16
