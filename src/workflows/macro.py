"""
src/workflows/macro.py
──────────────────────
LangGraph StateGraph for the macro theme scanner.

Replaces MacroSubAgent (ReAct, RECURSION_LIMIT=20).

Phases
------
1. build_plan — enumerate fetch steps; conditionally include geo-news (0 LLM tokens)
2. [approval] — _show_and_approve_plan() outside graph: displays plan, waits for Y/n/edit
3. fetch      — all macro data sources in parallel via ThreadPoolExecutor (0 LLM tokens)
4. synthesise — 1 LLM call assembles the macro outlook

Token savings: ~3 500 vs ~12 000 for the ReAct equivalent (~71%).
"""
from __future__ import annotations

import logging
import os

from langchain_core.runnables import RunnableConfig
from langgraph.graph import StateGraph, END

from .base import _get_llm, _par_datasets, SYNTH_SUFFIX, _get_checkpointer, _thread_id, _show_and_approve_plan, generate_plan_llm
from .plan_store import save_plan
from .state import MosaicState

logger = logging.getLogger(__name__)

# Geopolitical / commodity keywords that trigger `search_financial_news`
_GEO_KEYWORDS = frozenset({
    "iran", "russia", "china", "war", "sanction", "crude", "opec", "ukraine",
    "taiwan", "oil", "geopolit", "conflict", "trade war", "tariff", "embargo",
})


class MacroState(MosaicState):
    geo_query:  bool    # True → include search_financial_news in fetch
    # ── fetched data sections ──────────────────────────────────────────────
    macro:      str     # run_macro_scanner
    comex:      str     # run_comex_analysis
    fii_dii:    str     # plot_fii_dii_chart
    dxy:        str     # get_dxy_context
    indicators: str     # run_market_indicators
    news:       str     # search_financial_news (geo queries only)
    # ── output ────────────────────────────────────────────────────────────
    report:     str


# ── Node: fetch all macro data in parallel ────────────────────────────────────

def _fetch_node(state: MacroState, config: RunnableConfig) -> dict:
    q = state["question"]
    q_lower = q.lower()

    def _comex():
        from src.tools.skills_tools import run_comex_analysis
        return str(run_comex_analysis.invoke({}, config=config))

    def _dxy():
        from src.tools.market_context import get_dxy_context
        return str(get_dxy_context.invoke({"days": 30}, config=config))

    # Targeted query check: if user asked specifically for COMEX/metals/gold/silver/copper
    is_comex_targeted = any(kw in q_lower for kw in ("comex", "gold futures", "silver futures", "copper futures", "metals", "bullion")) and not any(kw in q_lower for kw in ("full macro", "all themes", "fii", "breadth", "overview", "nifty"))

    if is_comex_targeted:
        fetchers = {
            "comex": _comex,
            "dxy":   _dxy,
        }
    else:
        def _macro():
            from src.tools.skills_tools import run_macro_scanner
            return str(run_macro_scanner.invoke({"max_themes": 5}, config=config))

        def _fii_dii():
            from src.tools.chart_tools import plot_fii_dii_chart
            return str(plot_fii_dii_chart.invoke({"days": 30}, config=config))

        def _indicators():
            from src.tools.skills_tools import run_market_indicators
            return str(run_market_indicators.invoke({}, config=config))

        fetchers = {
            "macro":      _macro,
            "comex":      _comex,
            "fii_dii":    _fii_dii,
            "dxy":        _dxy,
            "indicators": _indicators,
        }

        if state.get("geo_query"):
            def _news():
                from src.tools.news_search import search_financial_news
                return str(search_financial_news.invoke({"query": q}, config=config))
            fetchers["news"] = _news

    datasets = _par_datasets(fetchers)
    return {**{k: v.content for k, v in datasets.items()}, "datasets": datasets}


# ── Node: synthesise ──────────────────────────────────────────────────────────

_SYNTH_PROMPT = (
    "You are a macro analyst covering Indian and global markets. "
    "All data has been pre-fetched. Synthesise into a structured macro outlook with:\n\n"
    "1. **Active Macro Themes** — list each theme with ETF net score and direction. "
    "Net score interpretation: ≥+16 strong bullish | +8 to +15 moderate | ≤−16 strong bearish.\n"
    "2. **COMEX Pre-Market** — gold/silver/copper signals.\n"
    "3. **FII/DII Flows** — 30-day trend and directional read.\n"
    "4. **DXY Context** — dollar trend and gold/INR implication.\n"
    "5. **Market Breadth** — Nifty breadth, valuation, stress indicators.\n"
    "6. **Geopolitical News** — if present, table of key headlines + macro implication.\n"
    "7. **ETF Positioning** — 2-3 sentence actionable summary.\n\n"
    "CRITICAL: Only cite prices and flows from the tool data above — never from training knowledge. "
    "Net scores are article counts, NOT % return forecasts. "
    "Never add FII flow amounts unless they appear in the Quant Overlay data."
)

_COMEX_TARGETED_PROMPT = (
    "You are a metals and commodities quant analyst. "
    "Synthesise pre-fetched COMEX and DXY data into a clean, ultra-concise pre-market metals report. "
    "Present key futures/spot prices (Gold, Silver, Copper) and COT positioning in standard Markdown tables. "
    "Do NOT add unasked sections like market breadth, FII flows, or general macro themes. "
    "Keep the output clean, direct, and focused solely on metals & currency context."
)


def _synthesise_node(state: MacroState, config: RunnableConfig) -> dict:
    from langchain_core.messages import SystemMessage, HumanMessage
    try:
        from src.utils.caveman import get_caveman_prompt
        caveman = get_caveman_prompt()
    except Exception:
        caveman = ""

    llm = _get_llm()
    if llm is None:
        sections = [state.get(k, "") for k in ("macro", "comex", "fii_dii", "dxy", "indicators", "news")]
        return {"report": "\n\n---\n\n".join(s for s in sections if s)}

    q_lower = state["question"].lower()
    is_comex_targeted = any(kw in q_lower for kw in ("comex", "gold futures", "silver futures", "copper futures", "metals", "bullion")) and not any(kw in q_lower for kw in ("full macro", "all themes", "fii", "breadth", "overview", "nifty"))

    synth_prompt = _COMEX_TARGETED_PROMPT if is_comex_targeted else _SYNTH_PROMPT

    data = "\n\n---\n\n".join(
        f"### {k}\n{state.get(k, '')}" for k in
        ("macro", "comex", "fii_dii", "dxy", "indicators", "news")
        if state.get(k)
    )
    result = llm.invoke([
        SystemMessage(content=synth_prompt + caveman + SYNTH_SUFFIX),
        HumanMessage(content=f"Question: {state['question']}\n\nPre-fetched macro data:\n{data}"),
    ], config=config)
    report = str(result.content).strip() or data
    return {"report": report}


# ── Graph construction ────────────────────────────────────────────────────────

_GRAPH = None


def _build_graph():
    global _GRAPH
    if _GRAPH is not None:
        return _GRAPH
    g = StateGraph(MacroState)
    g.add_node("fetch",      _fetch_node)
    g.add_node("synthesise", _synthesise_node)
    g.set_entry_point("fetch")
    g.add_edge("fetch",      "synthesise")
    g.add_edge("synthesise", END)
    _GRAPH = g.compile(checkpointer=_get_checkpointer())
    return _GRAPH


# ── Public entry point ────────────────────────────────────────────────────────

def _build_plan(question: str, geo_query: bool) -> list[str]:
    steps = [
        "Run macro scanner — active themes, ETF net article flows",
        "Run COMEX pre-market analysis — gold/silver/copper signals",
        "Fetch FII/DII flow chart (30 days)",
        "Fetch DXY context — dollar trend, gold/INR correlation",
        "Run market indicators — Nifty breadth, valuation, stress",
    ]
    if geo_query:
        steps.append(f"Search financial news: \"{question[:60]}\"")
    return generate_plan_llm(question, intent="macro", default_plan=steps)


def run(question: str, callbacks: list | None = None) -> str:
    """
    Run the macro theme scanner workflow.

    Parameters
    ----------
    question : User question about macro trends, gold, FII flows, geopolitics, etc.
    callbacks : LangChain callback handlers (e.g. BudgetCallbackHandler, tracer)
                forwarded into every node's LLM/tool invocations.

    Returns
    -------
    str
        Formatted Markdown macro outlook.
    """
    q_lower = question.lower()
    geo_query = any(kw in q_lower for kw in _GEO_KEYWORDS)

    # ── Check if query is actually a single-name equity stock ──────────────
    try:
        from src.tools.company_resolver import _local_indian_lookup
        from src.agents.signal_sources import SIGNAL_ETFS
        sym = _local_indian_lookup(question)
        if sym and sym not in SIGNAL_ETFS:
            from .india_equity import run as run_india_equity
            logger.info("macro workflow: question %r is an equity stock query (%s) — delegating to india_equity", question, sym)
            return run_india_equity(question, callbacks=callbacks)
    except Exception:
        pass

    # ── Build + save plan ─────────────────────────────────────────────────
    plan = _build_plan(question, geo_query)
    plan_id = save_plan("macro", question, plan, metadata={"geo_query": geo_query})

    # ── Show plan and get approval ─────────────────────────────────────────
    approved = _show_and_approve_plan(question, plan, intent="macro")
    if approved is None:
        return "Plan cancelled by user."
    plan = approved

    # ── Run fetch + synthesise graph ───────────────────────────────────────
    graph = _build_graph()
    config = {
        "configurable": {"thread_id": _thread_id("macro", question)},
        "callbacks": callbacks or [],
    }
    initial_state: MacroState = {
        "question":   question,
        "plan":       plan,
        "plan_id":    plan_id,
        "geo_query":  geo_query,
        "macro":      "", "comex":  "", "fii_dii":    "",
        "dxy":        "", "indicators": "", "news": "",
        "datasets":   {},
        "report":     "",
    }
    result = graph.invoke(initial_state, config=config)
    return result.get("report", "*Macro workflow returned no report*")
