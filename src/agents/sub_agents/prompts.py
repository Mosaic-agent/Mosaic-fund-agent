"""
src/agents/sub_agents/prompts.py
────────────────────────────────
Shared prompt constants and lightweight string helpers used by every
sub-agent. Kept in a dedicated module so they can be imported without
pulling in LangChain or the routing graph.
"""
from __future__ import annotations

import re

# ── Common indicator typo corrections ──────────────────────────────────────────

_INDICATOR_TYPOS: dict[str, str] = {
    "mcad": "MACD",
    "mcda": "MACD",
    "risi": "RSI",
    "bolinger": "Bollinger",
    "bolliger": "Bollinger",
    "boilinger": "Bollinger",
}

_INDICATOR_TYPO_RE = re.compile(
    r"\b(" + "|".join(re.escape(k) for k in _INDICATOR_TYPOS) + r")\b",
    re.IGNORECASE,
)


def _fix_indicator_typos(question: str) -> str:
    """Correct common misspellings of technical indicator names."""
    def _repl(m: re.Match) -> str:
        return _INDICATOR_TYPOS[m.group(0).lower()]
    return _INDICATOR_TYPO_RE.sub(_repl, question)


# ── Shared rule: injected into every agent system prompt ───────────────────────
NO_LLM_CALC_RULE = (
    "\n\nNUMERIC COMPUTATION RULE (mandatory — never violate): "
    "NEVER compute, estimate, or derive any number (returns, ratios, averages, "
    "percentages, scores, sums, differences, CAGR, PE, Kelly fractions, etc.) "
    "inside your response. ALL numeric work MUST be performed by a tool call "
    "(Python, SQL, or a dedicated function). You may ONLY narrate or format "
    "numbers that were returned verbatim by a tool. If no tool has produced a "
    "number, state that the data is unavailable — do NOT approximate."
)

CLICKHOUSE_FINAL_ALIAS_RULE = (
    "\n\nCLICKHOUSE SYNTAX RULE (mandatory — never violate): "
    "When writing ad-hoc SQL queries using `query_clickhouse_db`, every ReplacingMergeTree table MUST use the `FINAL` modifier. "
    "If you declare an alias for the table, the alias MUST be declared BEFORE the `FINAL` modifier. "
    "Example: `FROM market_data.mf_holdings AS h FINAL` or `FROM market_data.mf_holdings h FINAL`. "
    "NEVER write `FROM market_data.mf_holdings FINAL AS h` or `FROM market_data.mf_holdings FINAL h`, as ClickHouse will raise a syntax error."
)

