"""Sub-agent registry: get_subagent, run_subagent_for."""
from __future__ import annotations

import logging

from .base import _SubAgent
from .prompts import _fix_indicator_typos
from .routing import _needs_cloud
from src.agents.budget import BudgetExceededError
from .deepdive import DeepDiveSubAgent
from .india_equity import IndianEquityResearchSubAgent
from .signal import SignalSubAgent
from .macro import MacroSubAgent
from .mf import MFSubAgent
from .intl_etf import IntlETFSubAgent
from .news import NewsSubAgent
from .database import DatabaseSubAgent
from .code import CodeSubAgent
from .research import AutonomousResearchAgent

logger = logging.getLogger(__name__)

# ── Registry ──────────────────────────────────────────────────────────────────

_registry: dict[str, _SubAgent] = {}


def get_subagent(name: str) -> _SubAgent:
    """Return (lazily creating) a sub-agent by name."""
    if name not in _registry:
        cls_map: dict[str, type[_SubAgent]] = {
            "deepdive":     DeepDiveSubAgent,
            "research":     AutonomousResearchAgent,
            "india_equity": IndianEquityResearchSubAgent,
            "signal":       SignalSubAgent,
            "macro":        MacroSubAgent,
            "mf":           MFSubAgent,
            "intl_etf":     IntlETFSubAgent,
            "news":         NewsSubAgent,
            "code":         CodeSubAgent,
            "database":     DatabaseSubAgent,
        }
        cls = cls_map.get(name)
        if cls is None:
            raise ValueError(f"Unknown sub-agent: {name!r}  (valid: {list(cls_map)})")
        _registry[name] = cls()
    return _registry[name]


def run_subagent_for(intent: str, question: str, callbacks: list | None = None) -> str:
    """Run a named sub-agent, automatically routing to cloud LLM when needed.

    Parameters
    ----------
    callbacks:
        Pass [RichConsoleCallbackHandler()] to see live tool-call output.
        TracingCallbackHandler is always appended for observability.
    """
    # Fix common indicator typos before the sub-agent LLM sees the query
    question = _fix_indicator_typos(question)
    import os
    from src.agents.tracer import TracingCallbackHandler, log_trace
    from src.agents.budget import BudgetCallbackHandler
    import time

    # india_equity: narrow single-metric asks ("ITC dividend yield") don't need
    # the full 8-section research note — answer from one Yahoo Finance call.
    if intent == "india_equity":
        try:
            from .india_equity import try_quick_stat_answer
            quick = try_quick_stat_answer(question)
            if quick is not None:
                return quick
        except Exception as exc:
            logger.debug("run_subagent_for: quick-stat fast path failed (%s) — using full agent", exc)

    cloud_llm = None
    if _needs_cloud(question) or intent in ("deepdive", "research"):
        try:
            from src.agents.mosaic_fund_agent import MosaicFundAgent
            tmp = object.__new__(MosaicFundAgent)
            tmp._checkpointer = None
            cloud_llm = tmp._build_cloud_llm()
            if cloud_llm is not None:
                logger.info("run_subagent_for: using cloud LLM for %r", question[:60])
        except Exception as exc:
            logger.warning("run_subagent_for: could not build cloud LLM: %s", exc)

    # Build callback list with tracing and budget always enabled
    tracer = TracingCallbackHandler(agent=intent)
    budget = BudgetCallbackHandler()
    if callbacks is None:
        callbacks = []
        from config.settings import settings
        if os.getenv("VERBOSE") == "1" or settings.llm_think:
            from src.agents.mosaic_fund_agent import RichConsoleCallbackHandler
            callbacks.append(RichConsoleCallbackHandler(agent_name=intent))
    callbacks.extend([tracer, budget])

    # Log the routing decision itself
    log_trace(
        agent="router",
        run_id=tracer.run_id,
        tool_name="route_intent",
        args_json=f'{{"question": "{question[:200]}", "intent": "{intent}"}}',
        status="ok",
    )

    start = time.monotonic()
    try:
        result = get_subagent(intent).run(question, llm_override=cloud_llm, callbacks=callbacks)
    except BudgetExceededError as exc:
        logger.warning("run_subagent_for: budget exceeded for %r: %s", intent, exc)
        result = (
            f"⚠️ Response budget exceeded ({exc}) — try narrowing your question "
            "or asking for one section at a time."
        )
    elapsed_ms = int((time.monotonic() - start) * 1000)

    # Log completion
    log_trace(
        agent=intent,
        run_id=tracer.run_id,
        tool_name="_complete",
        latency_ms=elapsed_ms,
        result_json=result[:500] if result else "",
        status="ok",
    )

    return result
