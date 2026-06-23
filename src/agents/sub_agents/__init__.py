"""
src/agents/sub_agents — Façade package.

All public and private symbols re-exported verbatim so external callers
(chat_cmd, agent_tools, intent_router, mosaic_fund_agent, tests) require
zero changes.
"""
from .prompts import NO_LLM_CALC_RULE, _fix_indicator_typos
from .infra import _dedup_cache, _wrap_tool_for_dedup, _make_context_trimmer, _print_thinking_blocks
from .base import _NullAgent, _SubAgent, _get_message_text
from .routing import (
    _GENERAL_RESEARCH_RE,
    _IMPORT_RE,
    _needs_cloud,
    _fast_path_intent,
    _regex_route_intent,
    route_intent,
)
from .equity_gatherer import _gather_indian_equity_data
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
from .registry import _registry, get_subagent, run_subagent_for

__all__ = [
    "NO_LLM_CALC_RULE",
    "_fix_indicator_typos",
    "_dedup_cache",
    "_wrap_tool_for_dedup",
    "_make_context_trimmer",
    "_print_thinking_blocks",
    "_NullAgent",
    "_SubAgent",
    "_get_message_text",
    "_GENERAL_RESEARCH_RE",
    "_IMPORT_RE",
    "_needs_cloud",
    "_fast_path_intent",
    "_regex_route_intent",
    "route_intent",
    "_gather_indian_equity_data",
    "DeepDiveSubAgent",
    "IndianEquityResearchSubAgent",
    "SignalSubAgent",
    "MacroSubAgent",
    "MFSubAgent",
    "IntlETFSubAgent",
    "NewsSubAgent",
    "DatabaseSubAgent",
    "CodeSubAgent",
    "AutonomousResearchAgent",
    "_registry",
    "get_subagent",
    "run_subagent_for",
]
