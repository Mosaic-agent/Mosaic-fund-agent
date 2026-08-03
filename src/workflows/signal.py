"""
src/workflows/signal.py
───────────────────────
LangGraph StateGraph for the ETF signal dashboard.

Replaces SignalSubAgent (ReAct, RECURSION_LIMIT=20).

Phases
------
1. resolve    — extract ETF symbol from question (0 LLM tokens)
2. build_plan — enumerate the fetch steps (0 LLM tokens)
3. [approval] — _show_and_approve_plan() outside graph: displays plan, waits for Y/n/edit
4. fetch      — all 6 data sources in parallel via ThreadPoolExecutor (0 LLM tokens)
5. synthesise — 1 LLM call assembles the signal dashboard

Token savings: ~4 000 vs ~18 000 for the ReAct equivalent (~78%).
"""
from __future__ import annotations

import logging
import os
import re

from langchain_core.runnables import RunnableConfig
from langgraph.graph import StateGraph, END

from .base import _get_llm, _par_datasets, SYNTH_SUFFIX, _get_checkpointer, _thread_id, _show_and_approve_plan, generate_plan_llm
from .plan_store import save_plan
from .state import MosaicState

logger = logging.getLogger(__name__)

# ETF tickers tracked by the signal pipeline
_KNOWN_TICKERS = frozenset({
    "GOLDBEES", "LIQUIDBEES", "NIFTYBEES", "BANKBEES", "ITBEES",
    "PSUBEES", "MAFANG", "SILVERBEES", "CPSEETF", "ICICIB22",
    "JUNIORBEES", "SETFNIF50", "NETFIT", "MOM100", "AUTOBEES",
    "PHARMABEES", "INFRABEES", "HNGSNGBEES",
})

# Words that look like tickers but aren't
_NOT_TICKERS = frozenset({
    "OVER", "LAST", "DAYS", "SHOW", "FIND", "EXPLAIN", "ANALYSE", "ANALYZE",
    "WHAT", "WHEN", "WHERE", "WHICH", "WITH", "THIS", "THAT", "THEN", "FROM",
    "GIVE", "LIST", "TELL", "ETFS", "GOLD", "SIGNAL", "TODAY",
    # Session-context and English words that match the ticker regex
    "SESSION", "CONTEXT", "PRIOR", "TURNS", "USER", "MASTER", "ROTA",
    "COMPOSITE", "SCORE", "SCORES", "REPORT", "CHART", "MARKET", "AGENT",
    "SHOW", "DATA", "ALL", "FOR", "EACH", "RANK", "RANKED", "ACROSS",
})


class SignalState(MosaicState):
    symbol:    str      # resolved ETF ticker, e.g. "GOLDBEES"
    # ── fetched data sections ──────────────────────────────────────────────
    goldbees:  str      # run_goldbees_pipeline
    composite: str      # run_daily_signal_composite
    risk_gov:  str      # run_risk_governor_analysis
    inav:      str      # run_premium_alerts
    etf_news:  str      # run_etf_news_sentiment
    chart:     str      # plot_price_chart for resolved symbol
    scores:    str      # plot_signal_scores bar chart
    # ── output ────────────────────────────────────────────────────────────
    report:    str


# ── Node 1: resolve symbol ────────────────────────────────────────────────────

def _resolve_node(state: SignalState) -> dict:
    # Strip injected session-context prefix before symbol extraction
    raw_q = re.sub(r"^\s*\[Session context.*?\]\s*", "", state["question"], flags=re.DOTALL)
    q = raw_q.upper()
    # Check known tickers first
    for tk in _KNOWN_TICKERS:
        if tk in q:
            return {"symbol": tk}
    # Fallback: only accept matches that end in a real ETF suffix to avoid
    # plain English words (SESSION, MARKET, …) masquerading as tickers
    m = re.search(r"\b([A-Z]{4,12}(BEES|ETF|100|50))\b", q)
    if m and m.group(1) not in _NOT_TICKERS:
        return {"symbol": m.group(1)}
    return {"symbol": "GOLDBEES"}   # default for gold/signal queries


# ── Node 2: fetch all data in parallel ───────────────────────────────────────

def _fetch_node(state: SignalState, config: RunnableConfig) -> dict:
    sym = state["symbol"]

    def _goldbees():
        from src.tools.skills_tools import run_goldbees_pipeline
        return str(run_goldbees_pipeline.invoke({}, config=config))

    def _composite():
        from src.tools.skills_tools import run_daily_signal_composite
        return str(run_daily_signal_composite.invoke({"save": False}, config=config))

    def _risk_gov():
        from src.tools.skills_tools import run_risk_governor_analysis
        return str(run_risk_governor_analysis.invoke({"symbol": sym}, config=config))

    def _inav():
        from src.tools.skills_tools import run_premium_alerts
        return str(run_premium_alerts.invoke({}, config=config))

    def _etf_news():
        from src.tools.skills_tools import run_etf_news_sentiment
        return str(run_etf_news_sentiment.invoke({"save": False}, config=config))

    def _chart():
        from src.tools.chart_tools import plot_price_chart
        return str(plot_price_chart.invoke({"symbol": sym, "days": 90}, config=config))

    def _scores():
        from src.tools.chart_tools import plot_signal_scores
        return str(plot_signal_scores.invoke({}, config=config))

    datasets = _par_datasets({
        "goldbees":  _goldbees,
        "composite": _composite,
        "risk_gov":  _risk_gov,
        "inav":      _inav,
        "etf_news":  _etf_news,
        "chart":     _chart,
        "scores":    _scores,
    })
    return {**{k: v.content for k, v in datasets.items()}, "datasets": datasets}


# ── Node 3: synthesise ────────────────────────────────────────────────────────

_SYNTH_PROMPT = (
    "You are a quantitative ETF signal analyst. All data has been pre-fetched for you. "
    "Synthesise the data into a structured signal dashboard with these sections:\n\n"
    "1. **GOLDBEES Signal** — report prob_up, expected_return_pct, regime_signal, "
    "and weights.blended_50 VERBATIM from the pipeline output.\n"
    "2. **Composite ETF Scores** — present in a clean Markdown table (symbol | score | regime).\n"
    "3. **Risk Governor** — GARCH vol, position sizing, blended weight.\n"
    "4. **iNAV Premium/Discount** — highlight any significant premium or discount.\n"
    "5. **News Sentiment** — 1-sentence summary per ETF category.\n\n"
    "TABLE FORMATTING MANDATE: Use ONLY standard Markdown tables with pipe characters `|` and "
    "hyphens `-`. Never use Unicode box-drawing characters (╭, ─, ┬, ┐, ├, ┼, ┤, ╰, ┴, ╯, │, etc.).\n\n"
    "CHART PLACEMENT: \n"
    "  - After Section 2 (Composite ETF Scores table), include the literal token `[CHART:plot_signal_scores]` "
    "on its own line so the composite-score bar chart appears below the table.\n"
    "  - In Section 1 (GOLDBEES Signal), include `[CHART:price]` on its own line where the 90-day "
    "price chart belongs.\n\n"
    "CRITICAL: Never invent composite scores, labels (ACCUMULATE/STRONG BUY), or ML metrics. "
    "Use only the data provided. If a section is unavailable, write 'Data unavailable'."
)


def _synthesise_node(state: SignalState, config: RunnableConfig) -> dict:
    from langchain_core.messages import SystemMessage, HumanMessage
    try:
        from src.utils.caveman import get_caveman_prompt
        caveman = get_caveman_prompt()
    except Exception:
        caveman = ""

    llm = _get_llm()
    if llm is None:
        # No LLM — return raw sections concatenated
        sections = [state.get(k, "") for k in ("goldbees", "composite", "risk_gov", "inav", "etf_news")]
        return {"report": "\n\n---\n\n".join(s for s in sections if s)}

    data = "\n\n---\n\n".join(
        f"### {k}\n{state.get(k, '')}" for k in
        ("goldbees", "composite", "risk_gov", "inav", "etf_news", "chart", "scores")
        if state.get(k)
    )
    result = llm.invoke([
        SystemMessage(content=_SYNTH_PROMPT + caveman + SYNTH_SUFFIX),
        HumanMessage(content=f"Question: {state['question']}\n\nPre-fetched signal data:\n{data}"),
    ], config=config)
    from .base import _render_report
    report = _render_report(result).strip() or data
    return {"report": report}


# ── Graph construction ────────────────────────────────────────────────────────

_GRAPH = None


def _build_graph():
    global _GRAPH
    if _GRAPH is not None:
        return _GRAPH
    g = StateGraph(SignalState)
    g.add_node("resolve",    _resolve_node)
    g.add_node("fetch",      _fetch_node)
    g.add_node("synthesise", _synthesise_node)
    g.set_entry_point("resolve")
    g.add_edge("resolve",    "fetch")
    g.add_edge("fetch",      "synthesise")
    g.add_edge("synthesise", END)
    _GRAPH = g.compile(checkpointer=_get_checkpointer())
    return _GRAPH


# ── Public entry point ────────────────────────────────────────────────────────

def _build_plan(symbol: str, question: str) -> list[str]:
    """Describe the fetch steps as a human-readable plan."""
    default_steps = [
        f"Fetch GOLDBEES ML pipeline (prob_up, regime_signal, blended_50)",
        f"Fetch daily signal composite scores for all tracked ETFs",
        f"Fetch GARCH risk governor position sizing for {symbol}",
        f"Fetch live iNAV premium/discount alerts for all ETFs",
        f"Fetch ETF category news sentiment scan",
        f"Render price chart for {symbol} (90 days)",
    ]
    return generate_plan_llm(question, intent="signal", default_plan=default_steps)


def run(question: str, callbacks: list | None = None) -> str:
    """
    Run the ETF signal workflow.

    Parameters
    ----------
    question : User question about ETF signals, GOLDBEES, or the signal dashboard.
    callbacks : LangChain callback handlers (e.g. BudgetCallbackHandler, tracer)
                forwarded into every node's LLM/tool invocations.

    Returns
    -------
    str
        Formatted Markdown signal dashboard.
    """
    # ── Step 1: resolve symbol ─────────────────────────────────────────────
    clean_q = re.sub(r"^\s*\[Session context.*?\]\s*", "", question, flags=re.DOTALL)
    q_upper = clean_q.upper()
    symbol = "GOLDBEES"
    for tk in _KNOWN_TICKERS:
        if tk in q_upper:
            symbol = tk
            break
    else:
        m = re.search(r"\b([A-Z]{4,12}(BEES|ETF|100|50))\b", q_upper)
        if m and m.group(1) not in _NOT_TICKERS:
            symbol = m.group(1)

    # ── Step 2: build plan ─────────────────────────────────────────────────
    plan = _build_plan(symbol, question)

    # ── Step 3: save plan ──────────────────────────────────────────────────
    plan_id = save_plan("signal", question, plan, metadata={"symbol": symbol})

    # ── Step 4: show plan and get approval ────────────────────────────────
    approved = _show_and_approve_plan(question, plan, intent="signal")
    if approved is None:
        return "Plan cancelled by user."
    plan = approved

    # ── Step 5: run the fetch + synthesise graph ──────────────────────────
    graph = _build_graph()
    config = {
        "configurable": {"thread_id": _thread_id("signal", question)},
        "callbacks": callbacks or [],
    }
    initial_state: SignalState = {
        "question": question, "symbol": symbol,
        "plan": plan, "plan_id": plan_id,
        "goldbees": "", "composite": "", "risk_gov": "",
        "inav": "", "etf_news": "", "chart": "", "scores": "",
        "datasets": {},
        "report": "",
    }
    result = graph.invoke(initial_state, config=config)
    return result.get("report", "*Signal workflow returned no report*")
