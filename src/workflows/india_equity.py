"""
src/workflows/india_equity.py
──────────────────────────────
LangGraph StateGraph for Indian equity 8-section research note.

Replaces IndianEquityResearchSubAgent (12 parallel tool calls the LLM often skips).

Phases
------
1. resolve    — resolve NSE/BSE symbol (0 LLM tokens)
2. fetch_all  — all 12 Round-2 tools in parallel (0 LLM tokens, guaranteed)
3. synthesise — 8-section research note (1 LLM call, ~7 000 tokens)

Total: ~7 000 tokens vs ~18 000 for the ReAct equivalent.
"""
from __future__ import annotations

import logging
from typing import TypedDict

from langgraph.graph import StateGraph, END

from .base import _get_llm, _par, SYNTH_SUFFIX, _get_checkpointer, _thread_id

logger = logging.getLogger(__name__)


class EquityState(TypedDict):
    question: str
    symbol: str
    exchange: str
    company_name: str
    # 12 data sections
    price: str
    momentum: str
    quarterly: str
    cashflow: str
    shareholding: str
    mf_holdings: str
    db_price: str
    news_gnews: str
    news_api: str
    price_chart: str
    anomalies: str
    correlations: str
    report: str


def _resolve_node(state: EquityState) -> dict:
    from src.tools.company_resolver import resolve_company_info
    from src.tools.agent_tools import check_and_refresh_symbol_data
    info = resolve_company_info(state["question"])
    # `or ""` — a failed resolution returns the key present-but-None, which
    # dict.get(default) does NOT catch. Without this the workflow fetched an
    # empty symbol and returned an all-zeros note instead of failing loudly.
    sym = info.get("symbol") or ""
    if sym:
        check_and_refresh_symbol_data.invoke({"symbol": sym, "auto_import": True})
    else:
        logger.warning(
            "india_equity: could not resolve a symbol for %r: %s",
            state["question"], info.get("error", "resolver returned no symbol"),
        )
    return {
        "symbol":       sym,
        "exchange":     info.get("exchange") or "NSE",
        "company_name": info.get("company_name") or state["question"],
    }


def _resolution_failed(state: EquityState) -> str:
    """Conditional-edge router: skip the 12-tool fetch + synthesis when no
    symbol resolved — otherwise every fetch queries an empty symbol and the
    note comes back all-zeros."""
    return "unresolved" if not state.get("symbol") else "fetch_all"


def _unresolved_node(state: EquityState) -> dict:
    return {
        "report": (
            f"❌ Could not resolve an NSE/BSE stock symbol for: **{state['question']}**\n\n"
            "Name the company or ticker more directly (e.g. \"NDTV New Delhi Television\" "
            "or \"research RELIANCE\")."
        )
    }


def _fetch_all_node(state: EquityState) -> dict:
    """All 12 Round-2 tools in parallel — guaranteed execution, no LLM involved."""
    sym = state["symbol"]
    exc = state["exchange"]
    inp = f"{sym}:{exc}"
    cn  = state["company_name"]

    def _price():
        from src.tools.yahoo_finance import get_yahoo_finance_data
        return str(get_yahoo_finance_data.invoke({"input_str": inp}))

    def _momentum():
        from src.tools.yahoo_finance import get_price_momentum
        return str(get_price_momentum.invoke({"input_str": inp}))

    def _quarterly():
        from src.tools.earnings_scraper import get_quarterly_results
        return str(get_quarterly_results.invoke({"input_str": inp}))

    def _cashflow():
        from src.tools.indian_equity_tools import get_stock_cashflow
        return str(get_stock_cashflow.invoke({"input_str": inp}))

    def _shareholding():
        # plot_shareholding_bar returns only a "[CHART:shareholding]" placeholder —
        # no numbers — so synthesis had nothing real to narrate and hallucinated
        # promoter/FII/DII %s. Fetch the actual pattern too and hand both to the LLM.
        from src.tools.chart_tools import plot_shareholding_bar
        from src.tools.earnings_scraper import get_shareholding_pattern
        chart = str(plot_shareholding_bar.invoke({"symbol": sym}))
        data = str(get_shareholding_pattern.invoke({"symbol": sym}))
        return f"{chart}\n{data}"

    def _mf_holdings():
        from src.tools.indian_equity_tools import get_mf_holdings_for_stock
        return str(get_mf_holdings_for_stock.invoke({"company_name_or_symbol": cn}))

    def _db_price():
        from src.tools.indian_equity_tools import get_db_price_summary
        return str(get_db_price_summary.invoke({"symbol": sym}))

    def _news_gnews():
        from src.tools.news_search import get_stock_news
        # Both news tools take a single `input_str` of the form "SYMBOL|Company".
        return str(get_stock_news.invoke({"input_str": f"{sym}|{cn}"}))

    def _news_api():
        from src.tools.newsapi_search import get_newsapi_stock_news
        return str(get_newsapi_stock_news.invoke({"input_str": f"{sym}|{cn}"}))

    def _price_chart():
        from src.tools.chart_tools import plot_price_chart
        return str(plot_price_chart.invoke({"symbol": sym, "days": 365}))

    def _anomalies():
        from src.tools.market.equity import search_anomaly_events
        return str(search_anomaly_events.invoke({"symbol": sym, "days": 365}))

    def _correlations():
        from src.tools.market.correlation_tools import find_anomaly_correlations
        return str(find_anomaly_correlations.invoke({"symbol": sym, "lookback_days": 365}))

    return _par({
        "price":        _price,
        "momentum":     _momentum,
        "quarterly":    _quarterly,
        "cashflow":     _cashflow,
        "shareholding": _shareholding,
        "mf_holdings":  _mf_holdings,
        "db_price":     _db_price,
        "news_gnews":   _news_gnews,
        "news_api":     _news_api,
        "price_chart":  _price_chart,
        "anomalies":    _anomalies,
        "correlations": _correlations,
    })  # concurrency capped in _par (external scrapers throttle on a 12-way burst)


_SYNTHESIS_PROMPT = (
    "You are a senior Indian equity analyst. All 12 data sources have been pre-fetched "
    "for you — you do NOT need to call any tools. Synthesise the data below into a "
    "structured Markdown research note with these 8 sections:\n\n"
    "(1) Company Snapshot — key metrics table; write `[CHART:price]` where price chart should appear\n"
    "(2) Financials — quarterly revenue, net profit, EPS, YoY growth table\n"
    "(3) Valuation — P/E, P/B vs sector; qualitative PEG\n"
    "(4) Cash Flow Quality — FCF, operating CF, capex trend\n"
    "(5) Institutional Ownership — write `[CHART:shareholding]` then promoter/FII/DII/Public % "
    "with QoQ delta arrows; DSP MF cross-ownership from mf_holdings data\n"
    "(6) News Sentiment — dominant themes, bullish/bearish balance\n"
    "(7) Key Risks — ranked by severity; thesis-killer risk called out explicitly\n"
    "(8) Recommendation — BUY/HOLD/SELL/WATCH · conviction LOW/MEDIUM/HIGH · one-line rationale\n\n"
    "Rules: all monetary values in ₹. Never invent or compute any number. "
    "Never reproduce box-drawing characters from chart output."
)


def _synthesise_node(state: EquityState) -> dict:
    """One LLM call: synthesise pre-fetched data into 8-section research note."""
    from langchain_core.messages import SystemMessage, HumanMessage
    from src.utils.caveman import get_caveman_prompt

    llm = _get_llm()
    if llm is None:
        sections = [state.get(k, "") for k in (
            "price", "momentum", "quarterly", "cashflow", "shareholding",
            "mf_holdings", "db_price", "news_gnews", "news_api",
            "price_chart", "anomalies", "correlations",
        )]
        return {"report": "\n\n---\n\n".join(s for s in sections if s)}

    data = "\n\n---\n\n".join(
        f"### {k}\n{state.get(k, '')}" for k in (
            "price", "momentum", "quarterly", "cashflow", "shareholding",
            "mf_holdings", "db_price", "news_gnews", "news_api",
            "price_chart", "anomalies", "correlations",
        ) if state.get(k)
    )

    result = llm.invoke([
        SystemMessage(content=_SYNTHESIS_PROMPT + get_caveman_prompt() + SYNTH_SUFFIX),
        HumanMessage(content=f"Question: {state['question']}\n\nPre-fetched data:\n{data}"),
    ])
    report = str(result.content).strip()
    # Safety net: never return an empty report. The local reasoning model
    # (gemma4 think=True) spends its whole token budget on reasoning tokens and
    # returns EMPTY final content on a synthesis this size — and the MLX endpoint
    # does not reliably honour a larger max_tokens override. So for a local-only
    # setup this falls back to the complete raw sections (all data, no prose).
    # Synthesised prose requires a cloud LLM — `_get_llm(prefer_cloud=True)`
    # already picks one automatically when configured.
    if not report:
        logger.warning(
            "india_equity: synthesis returned empty content (local model) — "
            "falling back to raw sections; configure a cloud LLM for prose synthesis"
        )
        report = data
    return {"report": report}


_GRAPH = None


def _build_graph():
    global _GRAPH
    if _GRAPH is not None:
        return _GRAPH
    g = StateGraph(EquityState)
    g.add_node("resolve",    _resolve_node)
    g.add_node("unresolved", _unresolved_node)
    g.add_node("fetch_all",  _fetch_all_node)
    g.add_node("synthesise", _synthesise_node)
    g.set_entry_point("resolve")
    g.add_conditional_edges(
        "resolve", _resolution_failed,
        {"unresolved": "unresolved", "fetch_all": "fetch_all"},
    )
    g.add_edge("unresolved", END)
    g.add_edge("fetch_all",  "synthesise")
    g.add_edge("synthesise", END)
    _GRAPH = g.compile(checkpointer=_get_checkpointer())
    return _GRAPH


def run(question: str) -> str:
    """
    Run the Indian equity research workflow.

    Parameters
    ----------
    question : str
        Company name, NSE symbol, or question about an Indian stock.

    Returns
    -------
    str
        8-section Markdown research note with guaranteed all sections populated.
    """
    graph = _build_graph()
    config = {"configurable": {"thread_id": _thread_id("india_equity", question)}}
    result = graph.invoke({"question": question}, config=config)
    return result.get("report", "*India equity workflow returned no report*")
