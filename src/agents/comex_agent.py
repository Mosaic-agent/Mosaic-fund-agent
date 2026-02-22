"""
src/agents/comex_agent.py
──────────────────────────
Deep Agents-powered COMEX commodity pre-market signal agent.

Built with create_deep_agent (github.com/langchain-ai/deepagents) — the same
LangGraph agent harness used by NewsSentimentAgent.

Responsibilities:
  • Fetch live spot prices for all 5 COMEX commodities (XAU, XAG, XPT, XPD, HG)
    from gold-api.com and compare against Yahoo Finance previous-day closes
  • Classify each commodity as STRONG BULLISH / BULLISH / NEUTRAL / BEARISH / STRONG BEARISH
  • Identify which Indian NSE ETFs / stocks are directly affected
  • Return a structured pre-market context dict that PortfolioAgent embeds in
    the final report and the `comex` CLI command displays

Usage (CLI):
    python src/main.py comex

Usage (API):
    from src.agents.comex_agent import ComexAgent
    report = ComexAgent().run()
"""

from __future__ import annotations

import json
import logging
import threading
import time
from typing import Any

from langchain_core.tools import tool

from config.settings import settings
from src.tools.comex_fetcher import (
    _COMEX_SYMBOLS,
    _DEFAULT_FETCH,
    get_comex_signals,
    _is_pre_market_india,
)

logger = logging.getLogger(__name__)

# ── Per-invocation loop guard ──────────────────────────────────────────────────
# Each ComexAgent.run() resets _invoke_state before calling the deep agent.
# The tool checks the counter and refuses to run more than MAX_TOOL_CALLS times,
# which breaks the DeepSeek infinite-tool-call loop without killing the process.

_MAX_TOOL_CALLS = 2          # allow 1 real call + 1 safety margin
_invoke_state = threading.local()  # thread-safe; works with async too


def _reset_call_counter() -> None:
    _invoke_state.call_count = 0
    _invoke_state.start_time = time.monotonic()


def _increment_call_counter() -> int:
    if not hasattr(_invoke_state, "call_count"):
        _invoke_state.call_count = 0
        _invoke_state.start_time = time.monotonic()
    _invoke_state.call_count += 1
    elapsed = time.monotonic() - _invoke_state.start_time
    logger.debug(
        "ComexAgent tool call #%d  elapsed=%.1fs",
        _invoke_state.call_count,
        elapsed,
    )
    return _invoke_state.call_count

# ── System prompt ──────────────────────────────────────────────────────────────

COMEX_SYSTEM_PROMPT = """\
You are an Indian commodity pre-market analyst for NSE/BSE traders.

CRITICAL INSTRUCTION: Call fetch_all_comex_signals EXACTLY ONCE.
As soon as you receive the tool result, output the JSON immediately as your
final answer. Do NOT call any tool a second time. Do NOT loop.

WORKFLOW (one pass only):
  Step 1: Call fetch_all_comex_signals — get live signals for XAU, XAG, XPT, XPD, HG.
  Step 2: Immediately return the full JSON from that single call as your final answer.

Indian market context:
  XAU bullish → risk-off, bearish for equities → affects GOLDBEES, KOTAKGOLD
  XAG bullish → SILVERBEES
  HG  bullish → global growth signal → positive for VEDL, HINDALCO
  Flag STRONG signals (>±1%) as high priority.

STOP after Step 2. Do not call any tool again.
"""


# ── LangChain Tools ────────────────────────────────────────────────────────────

@tool
def fetch_all_comex_signals() -> dict[str, Any]:
    """
    Fetch live COMEX spot prices for all 5 commodities and compute pre-market
    signals for the Indian NSE market.

    Commodities covered:
      XAU — Gold      (GOLDBEES, KOTAKGOLD, HDFCGOLD, ICICIGOLD)
      XAG — Silver    (SILVERBEES)
      XPT — Platinum  (no direct NSE ETF)
      XPD — Palladium (no direct NSE ETF)
      HG  — Copper    (VEDL, HINDALCO — indirect)

    Returns a full structured dict:
      run_time_ist, pre_market, overall_signal, summary, commodities (per symbol).
    Each commodity entry includes: live_price, prev_close, change_usd, change_pct,
    signal (STRONG BULLISH → STRONG BEARISH), nse_etfs, source.

    Call this tool ONCE. After receiving the result, return it immediately.
    Do NOT call this tool again.

    Requires GOLD_API_KEY in .env.
    """
    call_n = _increment_call_counter()
    logger.info(
        "ComexAgent fetch_all_comex_signals invoked (call #%d / max %d)",
        call_n,
        _MAX_TOOL_CALLS,
    )
    if call_n > _MAX_TOOL_CALLS:
        logger.warning(
            "ComexAgent loop detected! Tool called %d times — returning "
            "loop_detected marker so agent stops.",
            call_n,
        )
        return {
            "loop_detected": True,
            "message": (
                "fetch_all_comex_signals has already been called. "
                "Use the result from call #1. Stop calling this tool."
            ),
        }
    return get_comex_signals()


@tool
def fetch_single_commodity(symbol: str) -> dict[str, Any]:
    """
    Fetch live COMEX signal for a single commodity symbol.

    Valid symbols: XAU (Gold), XAG (Silver), XPT (Platinum), XPD (Palladium), HG (Copper)

    Returns the same structure as fetch_all_comex_signals but for one commodity.
    """
    sym = symbol.strip().upper()
    if sym not in _COMEX_SYMBOLS:
        return {
            "error": f"Unknown symbol '{symbol}'. Valid: {', '.join(_COMEX_SYMBOLS.keys())}"
        }
    result = get_comex_signals(symbols=[sym])
    commodity = result.get("commodities", {}).get(sym)
    if not commodity:
        return {"error": f"No data returned for {sym}.", "raw": result}
    return {
        "symbol":       sym,
        "pre_market":   result.get("pre_market", False),
        "run_time_ist": result.get("run_time_ist", ""),
        **commodity,
    }


@tool
def get_comex_pre_market_context() -> dict[str, Any]:
    """
    Return current IST time context and whether the Indian market is open.

    Useful as a first step to decide whether a pre-market analysis is relevant.
    Returns: { is_pre_market, ist_time, market_opens_at, recommendation }
    """
    try:
        from datetime import datetime
        from zoneinfo import ZoneInfo
        ist_now = datetime.now(ZoneInfo("Asia/Kolkata"))
        ist_str = ist_now.strftime("%Y-%m-%d %H:%M:%S IST")
        pre = _is_pre_market_india()
        return {
            "ist_time":       ist_str,
            "is_pre_market":  pre,
            "market_opens_at": "09:15 IST",
            "recommendation": (
                "Run COMEX analysis now — market not yet open."
                if pre else
                "Market is open. COMEX signals are intraday context, not pre-market."
            ),
        }
    except Exception as exc:
        return {"error": str(exc)}


# All tools for this agent
COMEX_TOOLS = [fetch_all_comex_signals, fetch_single_commodity, get_comex_pre_market_context]


# ── Agent class ────────────────────────────────────────────────────────────────

class ComexAgent:
    """
    Deep Agents-powered COMEX commodity pre-market signal agent.

    Uses create_deep_agent from the deepagents SDK on top of LangGraph.
    Falls back to _run_direct() (direct tool call) when:
      - deepagents is not installed
      - The local LLM's context window is too small for the agent system prompt
      - The agent fails to return parseable JSON

    This guarantees a result is always returned regardless of LLM availability.
    """

    def __init__(self) -> None:
        self._llm = self._build_llm()
        self._agent = self._build_deep_agent()

    # ── LLM builder ───────────────────────────────────────────────────────────

    def _build_llm(self) -> Any:
        """
        Build LLM with same priority as PortfolioAgent and NewsSentimentAgent:
          1. Local OpenAI-compatible server (LLM_BASE_URL)
          2. Anthropic cloud
          3. OpenAI cloud
        """
        if settings.llm_base_url:
            from langchain_openai import ChatOpenAI
            logger.info(
                "ComexAgent — local LLM: %s @ %s",
                settings.llm_model,
                settings.llm_base_url,
            )
            return ChatOpenAI(
                model=settings.llm_model,
                base_url=settings.llm_base_url,
                api_key=settings.openai_api_key or "local",
                temperature=0,
                max_tokens=settings.llm_token_budget,
            )

        if settings.llm_provider.lower() == "anthropic":
            from langchain_anthropic import ChatAnthropic
            return ChatAnthropic(
                model=settings.llm_model,
                api_key=settings.anthropic_api_key,
                temperature=0,
                max_tokens=settings.llm_token_budget,
            )

        from langchain_openai import ChatOpenAI
        return ChatOpenAI(
            model=settings.llm_model,
            api_key=settings.openai_api_key,
            temperature=0,
            max_tokens=settings.llm_token_budget,
        )

    # ── Agent builder ─────────────────────────────────────────────────────────

    def _build_deep_agent(self) -> Any:
        """
        Build a Deep Agent using create_deep_agent from the deepagents package.
        Returns None when deepagents is not installed or creation fails.
        """
        try:
            from deepagents import create_deep_agent  # type: ignore[import]
        except ImportError:
            logger.debug(
                "deepagents not installed — ComexAgent will use direct mode. "
                "Install with: pip install deepagents"
            )
            return None

        try:
            return create_deep_agent(
                model=self._llm,
                tools=COMEX_TOOLS,
                system_prompt=COMEX_SYSTEM_PROMPT,
            )
        except Exception as exc:
            logger.warning("create_deep_agent (COMEX) failed (%s) — using direct mode.", exc)
            return None

    # ── Public API ─────────────────────────────────────────────────────────────

    def run(self) -> dict[str, Any]:
        """
        Run COMEX pre-market signal analysis.

        When the deep agent is available it will plan, call the tools, and
        return the structured JSON result.  Falls back to _run_direct()
        (immediate tool call, no LLM) on any failure.

        Loop protection:
          - recursion_limit=6 passed to LangGraph (hard cap on graph steps)
          - per-invocation tool call counter (_MAX_TOOL_CALLS=2)
          - If loop_detected marker returned, falls back to _run_direct()

        Returns:
            Full COMEX signals dict from get_comex_signals().
        """
        if self._agent is None:
            logger.info("ComexAgent deep agent unavailable — running direct.")
            return self._run_direct()

        # Reset the per-invocation loop guard before each run
        _reset_call_counter()
        logger.debug("ComexAgent.run() started — call counter reset.")

        user_message = (
            "Call fetch_all_comex_signals once and return the full JSON result. "
            "Do not call any tool more than once."
        )

        try:
            result = self._agent.invoke(
                {"messages": [{"role": "user", "content": user_message}]},
                config={"recursion_limit": 6},   # hard cap: 6 LangGraph steps max
            )
            msgs = result.get("messages", [])
            logger.debug(
                "ComexAgent.run() agent returned %d messages; "
                "tool was called %d time(s).",
                len(msgs),
                getattr(_invoke_state, "call_count", 0),
            )
            last_content = msgs[-1].content if msgs else ""

            # Extract JSON block from the agent's final message
            import re
            json_match = re.search(r"\{.*\}", last_content, re.DOTALL)
            if json_match:
                try:
                    parsed = json.loads(json_match.group())
                    if parsed.get("loop_detected"):
                        logger.warning(
                            "ComexAgent loop_detected in parsed result — "
                            "falling back to direct mode."
                        )
                        return self._run_direct()
                    if "commodities" in parsed or "overall_signal" in parsed:
                        logger.debug("ComexAgent returned valid COMEX JSON.")
                        return parsed
                except (json.JSONDecodeError, ValueError):
                    pass

            logger.info(
                "ComexAgent response did not contain parseable JSON "
                "(tool calls=%d) — falling back.",
                getattr(_invoke_state, "call_count", 0),
            )
        except Exception as exc:
            logger.warning(
                "ComexAgent invocation failed after %d tool call(s): %s — "
                "using direct mode.",
                getattr(_invoke_state, "call_count", 0),
                exc,
            )

        return self._run_direct()

    def _run_direct(self) -> dict[str, Any]:
        """
        Bypass the LLM agent and call get_comex_signals() directly.
        Always returns a result regardless of LLM availability.
        """
        return get_comex_signals()
