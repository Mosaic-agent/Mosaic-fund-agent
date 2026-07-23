"""
src/workflows/news.py
─────────────────────
LangGraph StateGraph for financial news aggregation.

Replaces NewsSubAgent (ReAct, RECURSION_LIMIT=12).

Phases
------
1. resolve    — resolve company/ETF to NSE symbol (0 LLM tokens)
2. build_plan — enumerate fetch steps (0 LLM tokens)
3. [approval] — _show_and_approve_plan() outside graph: displays plan, waits for Y/n/edit
4. fetch      — GNews + NewsAPI + DB news in parallel (0 LLM tokens)
5. aggregate  — pure-Python merge + dedup + Markdown table (0 LLM tokens for basic news)
               One optional LLM call if question asks for analysis/sentiment/why

Token savings: ~1 500 vs ~8 000 for the ReAct equivalent (~81%).
"""
from __future__ import annotations

import logging
import re

from langchain_core.runnables import RunnableConfig
from langgraph.graph import StateGraph, END

from .base import _get_llm, _par_datasets, SYNTH_SUFFIX, _get_checkpointer, _thread_id, _show_and_approve_plan, generate_plan_llm
from .plan_store import save_plan
from .state import MosaicState

logger = logging.getLogger(__name__)

# Keywords that trigger an LLM synthesis pass after aggregation
_ANALYSIS_KEYWORDS = frozenset({
    "analysis", "analyse", "analyze", "sentiment", "why", "explain",
    "reason", "impact", "what happened", "what drove", "summary",
})


class NewsState(MosaicState):
    symbol:       str   # resolved NSE ticker (empty → broad query)
    company_name: str
    needs_llm:    bool  # True → run LLM synthesis after aggregation
    # ── fetched data ──────────────────────────────────────────────────────
    gnews:    str       # get_stock_news
    newsapi:  str       # get_newsapi_stock_news
    db_news:  str       # get_db_news
    etf_scan: str       # run_etf_news_sentiment (broad queries)
    # ── output ────────────────────────────────────────────────────────────
    report:   str


# ── Node 1: resolve company → symbol ─────────────────────────────────────────

def _resolve_node(state: NewsState) -> dict:
    """
    Attempt to resolve a company name to an NSE symbol.
    Falls back to empty string for broad/ETF queries.
    """
    question = state["question"]
    try:
        from src.tools.company_resolver import resolve_company_info
        info = resolve_company_info(question)
        return {
            "symbol":       info.get("symbol") or "",
            "company_name": info.get("company_name") or question,
        }
    except Exception as exc:
        logger.debug("news_workflow: resolve failed: %s", exc)
        return {"symbol": "", "company_name": question}


# ── Node 2: fetch all news sources in parallel ────────────────────────────────

def _fetch_node(state: NewsState, config: RunnableConfig) -> dict:
    sym = state["symbol"]
    cn  = state["company_name"]
    q   = state["question"]
    inp = f"{sym}|{cn}" if sym else cn

    def _gnews():
        from src.tools.news_search import get_stock_news
        return str(get_stock_news.invoke({"input_str": inp}, config=config))

    def _newsapi():
        from src.tools.newsapi_search import get_newsapi_stock_news
        return str(get_newsapi_stock_news.invoke({"input_str": inp}, config=config))

    def _db_news():
        from src.tools.news_search import get_db_news
        cat = _infer_category(q)
        return str(get_db_news.invoke({"category": cat, "sentiment": ""}, config=config))

    fetchers = {
        "gnews":   _gnews,
        "newsapi": _newsapi,
        "db_news": _db_news,
    }

    # For broad ETF/market queries, also run the category sentiment scan
    if not sym or any(kw in q.lower() for kw in ("etf", "market", "category", "latest")):
        def _etf_scan():
            from src.tools.skills_tools import run_etf_news_sentiment
            return str(run_etf_news_sentiment.invoke({"save": False}, config=config))
        fetchers["etf_scan"] = _etf_scan

    datasets = _par_datasets(fetchers)
    return {**{k: v.content for k, v in datasets.items()}, "datasets": datasets}


def _infer_category(question: str) -> str:
    """Map question keywords to ETF news DB category names."""
    q = question.lower()
    if any(k in q for k in ("gold", "silver", "precious", "metal", "comex")):
        return "gold"
    if any(k in q for k in ("nifty", "index", "broad")):
        return "nifty"
    if any(k in q for k in ("bank", "finance", "nifty bank")):
        return "bank"
    if any(k in q for k in ("it", "tech", "software", "infosys", "tcs")):
        return "it"
    if any(k in q for k in ("psu", "public sector")):
        return "psu"
    if any(k in q for k in ("pharma", "health")):
        return "pharma"
    if any(k in q for k in ("auto", "automobile")):
        return "auto"
    return ""


# ── Node 3: aggregate and format ─────────────────────────────────────────────

def _aggregate_node(state: NewsState, config: RunnableConfig) -> dict:
    """
    Pure-Python merge + deduplicate + Markdown table.
    Optional 1 LLM call when question asks for analysis/sentiment/why.
    """
    # Collect raw text from all sources
    raw_blocks = [
        state.get("gnews", ""),
        state.get("newsapi", ""),
        state.get("db_news", ""),
        state.get("etf_scan", ""),
    ]
    combined = "\n\n---\n\n".join(b for b in raw_blocks if b)

    if not state.get("needs_llm"):
        # No LLM — return the combined raw data with a header
        header = f"## News: {state['question'][:80]}\n\n"
        return {"report": header + combined}

    # LLM synthesis for analysis/sentiment/why questions
    llm = _get_llm()
    if llm is None:
        return {"report": combined}

    from langchain_core.messages import SystemMessage, HumanMessage
    try:
        from src.utils.caveman import get_caveman_prompt
        caveman = get_caveman_prompt()
    except Exception:
        caveman = ""

    synth_prompt = (
        "You are the Mosaic News Agent — Indian financial news aggregator. "
        "You have been given pre-fetched news from multiple sources. "
        "Present results as a Markdown table:\n"
        "| Title | Source | Date | Sentiment |\n\n"
        "Then write 2-3 sentences summarising: dominant sentiment (bullish/bearish/mixed), "
        "key themes, and any actionable observation. "
        "Never invent headlines — only report what is in the data."
    )
    result = llm.invoke([
        SystemMessage(content=synth_prompt + caveman + SYNTH_SUFFIX),
        HumanMessage(content=f"Question: {state['question']}\n\nNews data:\n{combined[:6000]}"),
    ], config=config)
    report = str(result.content).strip() or combined
    return {"report": report}


# ── Graph construction ────────────────────────────────────────────────────────

_GRAPH = None


def _build_graph():
    global _GRAPH
    if _GRAPH is not None:
        return _GRAPH
    g = StateGraph(NewsState)
    g.add_node("resolve",   _resolve_node)
    g.add_node("fetch",     _fetch_node)
    g.add_node("aggregate", _aggregate_node)
    g.set_entry_point("resolve")
    g.add_edge("resolve",   "fetch")
    g.add_edge("fetch",     "aggregate")
    g.add_edge("aggregate", END)
    _GRAPH = g.compile(checkpointer=_get_checkpointer())
    return _GRAPH


# ── Public entry point ────────────────────────────────────────────────────────

def _build_plan(question: str, needs_llm: bool) -> list[str]:
    q_lower = question.lower()
    steps = [
        "Resolve company/ETF name to NSE symbol",
        "Fetch news from Google News (GNews)",
        "Fetch news from NewsAPI.org",
        "Fetch saved news from ClickHouse news_articles table",
    ]
    if any(kw in q_lower for kw in ("etf", "market", "category", "latest")):
        steps.append("Run ETF category news sentiment scan (all 10 categories)")
    if needs_llm:
        steps.append("Synthesise headlines into sentiment analysis (1 LLM call)")
    return generate_plan_llm(question, intent="news", default_plan=steps)


def run(question: str, callbacks: list | None = None) -> str:
    """
    Run the news aggregation workflow.

    Parameters
    ----------
    question : User question about news — company, ETF, broad market, or sentiment.
    callbacks : LangChain callback handlers (e.g. BudgetCallbackHandler, tracer)
                forwarded into every node's LLM/tool invocations.

    Returns
    -------
    str
        Markdown news table + optional sentiment summary.
    """
    q_lower = question.lower()
    needs_llm = any(kw in q_lower for kw in _ANALYSIS_KEYWORDS)

    # ── Build + save plan ─────────────────────────────────────────────────
    plan = _build_plan(question, needs_llm)
    plan_id = save_plan("news", question, plan, metadata={"needs_llm": needs_llm})

    # ── Show plan and get approval ─────────────────────────────────────────
    approved = _show_and_approve_plan(question, plan, intent="news")
    if approved is None:
        return "Plan cancelled by user."
    plan = approved

    # ── Run graph ──────────────────────────────────────────────────────────
    graph = _build_graph()
    config = {
        "configurable": {"thread_id": _thread_id("news", question)},
        "callbacks": callbacks or [],
    }
    initial_state: NewsState = {
        "question":     question,
        "symbol":       "",
        "company_name": question,
        "plan":         plan,
        "plan_id":      plan_id,
        "needs_llm":    needs_llm,
        "gnews":        "",
        "newsapi":      "",
        "db_news":      "",
        "etf_scan":     "",
        "datasets":     {},
        "report":       "",
    }
    result = graph.invoke(initial_state, config=config)
    return result.get("report", "*News workflow returned no report*")
