"""
src/workflows/autonomous_research.py
─────────────────────────────────────
LangGraph StateGraph for multi-domain equity research.

Replaces AutonomousResearchAgent (RECURSION_LIMIT=50 ReAct loop).

Phases
------
1. resolve    — resolve symbol + refresh data (0 LLM tokens)
2. fetch_all  — parallel data fetch: price, fundamentals, institutional,
                macro, news, volatility (0 LLM tokens, ThreadPoolExecutor)
3. correlate  — anomaly events + correlation analysis (0 LLM tokens)
4. verify     — adversarial bear-case generation (1 LLM call, ~800 tokens)
5. synthesise — 8-section research note (1 LLM call, ~8 000 tokens)

Total: ~8 800 tokens vs ~42 000 for the ReAct equivalent.
"""
from __future__ import annotations

import logging

from langchain_core.runnables import RunnableConfig
from langgraph.graph import StateGraph, END

from .base import _get_llm, _par_datasets, SYNTH_SUFFIX, _get_checkpointer, _thread_id
from .state import MosaicState

logger = logging.getLogger(__name__)

# ── State ─────────────────────────────────────────────────────────────────────

class ResearchState(MosaicState):
    symbol: str
    exchange: str
    company_name: str
    # Data sections (populated by fetch_all / correlate)
    price: str
    fundamentals: str
    institutional: str
    macro: str
    news: str
    volatility: str
    correlation: str
    # Synthesis
    bear_cases: str
    report: str


# ── Nodes ─────────────────────────────────────────────────────────────────────

def _resolve_node(state: ResearchState, config: RunnableConfig) -> dict:
    """Resolve company name/symbol and ensure price data is fresh."""
    from src.tools.company_resolver import resolve_company_info
    from src.tools.agent_tools import check_and_refresh_symbol_data

    info = resolve_company_info(state["question"])
    # info.get("symbol", "") is NOT enough — a failed resolution returns the
    # key WITH an explicit None value, and dict.get()'s default only applies
    # when the key is absent. `or ""` catches both "absent" and "present but None".
    sym = info.get("symbol") or ""
    if info.get("error") or not sym:
        logger.warning(
            "autonomous_research: could not resolve a symbol for %r: %s",
            state["question"], info.get("error", "resolver returned no symbol"),
        )
    else:
        status = check_and_refresh_symbol_data.invoke({"symbol": sym, "auto_import": True}, config=config)
        logger.info("autonomous_research: resolved %s — data status: %s", sym, status[:40])
    return {
        "symbol":       sym,
        "exchange":     info.get("exchange") or "NSE",
        "company_name": info.get("company_name") or state["question"],
    }


def _resolution_failed(state: ResearchState) -> str:
    """Conditional-edge router: skip the expensive fetch/correlate/verify/synthesise
    chain entirely when no symbol resolved — no point running 6 parallel fetches
    and 2 LLM calls against a symbol that doesn't exist."""
    return "unresolved" if not state.get("symbol") else "fetch_all"


def _unresolved_node(state: ResearchState) -> dict:
    """Early-exit report when the company/symbol resolver found nothing."""
    return {
        "report": (
            f"❌ Could not resolve a stock symbol for: **{state['question']}**\n\n"
            "Try naming the company or ticker explicitly, e.g. "
            "\"research RELIANCE\" or \"research Asian Paints\"."
        )
    }


def _fetch_all_node(state: ResearchState, config: RunnableConfig) -> dict:
    """Fan-out all data sources in parallel — zero LLM calls."""
    sym = state["symbol"]
    exc = state["exchange"]
    inp = f"{sym}:{exc}"

    def _price():
        from src.tools.yahoo_finance import get_yahoo_finance_data, get_price_momentum
        from src.tools.indian_equity_tools import get_db_price_summary
        yf  = get_yahoo_finance_data.invoke({"input_str": inp}, config=config)
        mom = get_price_momentum.invoke({"input_str": inp}, config=config)
        db  = get_db_price_summary.invoke({"symbol": sym}, config=config)
        return f"## Valuation\n{yf}\n\n## Momentum\n{mom}\n\n## DB Price Summary\n{db}"

    def _fundamentals():
        from src.tools.earnings_scraper import get_quarterly_results
        from src.tools.indian_equity_tools import get_stock_cashflow
        qr = get_quarterly_results.invoke({"input_str": inp}, config=config)
        cf = get_stock_cashflow.invoke({"input_str": inp}, config=config)
        return f"## Quarterly Results\n{qr}\n\n## Cash Flow\n{cf}"

    def _institutional():
        from src.tools.indian_equity_tools import get_mf_holdings_for_stock, get_fii_dii_summary
        from src.tools.chart_tools import plot_shareholding_bar
        mf  = get_mf_holdings_for_stock.invoke({"company_name_or_symbol": state["company_name"]}, config=config)
        fii = get_fii_dii_summary.invoke({"days": 7}, config=config)
        sh  = plot_shareholding_bar.invoke({"symbol": sym}, config=config)
        return f"## MF Holdings\n{mf}\n\n## FII/DII Summary\n{fii}\n\n## Shareholding\n{sh}"

    def _macro():
        from src.tools.runners import run_macro_scanner
        from src.tools.market_context import get_dxy_context
        macro = run_macro_scanner.invoke({"max_themes": 3}, config=config)
        dxy   = get_dxy_context.invoke({"days": 30}, config=config)
        return f"## Macro Themes\n{macro}\n\n## DXY Context\n{dxy}"

    def _news():
        from src.tools.news_search import get_stock_news, search_financial_news
        from src.tools.newsapi_search import get_newsapi_stock_news
        gn  = get_stock_news.invoke({"company_name": state["company_name"], "days": 14}, config=config)
        na  = get_newsapi_stock_news.invoke({"symbol": sym}, config=config)
        sf  = search_financial_news.invoke({"query": f"{sym} {state['company_name']} India"}, config=config)
        return f"## GNews\n{gn}\n\n## NewsAPI\n{na}\n\n## Financial News\n{sf}"

    def _volatility():
        from src.tools.market.gold import run_risk_governor_analysis
        from src.tools.chart_tools import plot_price_chart, plot_macd_chart
        rg   = run_risk_governor_analysis.invoke({"symbol": sym}, config=config)
        pc   = plot_price_chart.invoke({"symbol": sym, "days": 365}, config=config)
        macd = plot_macd_chart.invoke({"symbol": sym, "days": 180}, config=config)
        return f"## Risk Governor\n{rg}\n\n## Price Chart\n{pc}\n\n## MACD\n{macd}"

    datasets = _par_datasets({
        "price":         _price,
        "fundamentals":  _fundamentals,
        "institutional": _institutional,
        "macro":         _macro,
        "news":          _news,
        "volatility":    _volatility,
    })
    return {**{k: v.content for k, v in datasets.items()}, "datasets": datasets}


def _correlate_node(state: ResearchState, config: RunnableConfig) -> dict:
    """Anomaly detection + FX correlation — sequential after price data is in."""
    from src.tools.market.equity import search_anomaly_events
    from src.tools.market.correlation_tools import find_anomaly_correlations
    sym = state["symbol"]
    anomalies = search_anomaly_events.invoke({"symbol": sym, "days": 365}, config=config)
    corr      = find_anomaly_correlations.invoke({"symbol": sym, "lookback_days": 365}, config=config)
    return {"correlation": f"## Anomaly Events\n{anomalies}\n\n## Correlation Analysis\n{corr}"}


def _verify_node(state: ResearchState, config: RunnableConfig) -> dict:
    """One LLM call: generate 3 bear cases that could invalidate a bullish thesis."""
    from langchain_core.messages import SystemMessage, HumanMessage
    llm = _get_llm()
    if llm is None:
        return {"bear_cases": "*Bear-case verification unavailable — no LLM configured*"}

    data_snapshot = "\n\n".join([
        state.get("price", "")[:1000],
        state.get("fundamentals", "")[:800],
        state.get("institutional", "")[:600],
    ])
    result = llm.invoke([
        SystemMessage(content=(
            "You are an adversarial equity analyst. Given live market data, generate "
            "exactly 3 specific, data-grounded bear cases that could invalidate a "
            "bullish thesis on this stock. Be concrete — cite the numbers from the "
            "data, not generic sector risks. Never invent numbers." + SYNTH_SUFFIX
        )),
        HumanMessage(content=(
            f"Stock: {state['symbol']} ({state['company_name']})\n\n"
            f"Data:\n{data_snapshot}"
        )),
    ], config=config)
    from src.agents.sub_agents.base import _get_message_text
    return {"bear_cases": _get_message_text(result.content)}


_SYNTHESIS_PROMPT = (
    "You are a senior Indian equity research analyst. All market data has been "
    "pre-collected for you across 6 parallel data sources — you do NOT need to call "
    "any tools. Your task is to synthesise this into a structured Markdown research note.\n\n"
    "Write exactly these 8 sections:\n"
    "(1) Company Snapshot — key metrics table (sector, market cap, P/E, P/B, 52w range, price); "
    "then write `[CHART:price]` on its own line where the 1-year price chart belongs\n"
    "(2) Financials — latest quarterly revenue, net profit, EPS, YoY growth\n"
    "(3) Valuation — P/E and P/B vs sector; qualitative PEG; "
    "write `[CHART:macd]` on its own line where the MACD momentum chart belongs\n"
    "(4) Cash Flow Quality — FCF trend, operating CF vs capex\n"
    "(5) Institutional Ownership — write `[CHART:shareholding]` on its own line first, "
    "then promoter %, FII/DII QoQ delta, DSP MF cross-ownership\n"
    "(6) News Sentiment — dominant themes, bullish/bearish balance\n"
    "(7) Key Risks — ranked by severity; integrate bear cases from the adversarial analysis\n"
    "(8) Recommendation — BUY/HOLD/SELL/WATCH · conviction LOW/MEDIUM/HIGH · one-line rationale\n\n"
    "TABLE FORMATTING: Use ONLY standard Markdown tables with pipe `|` and hyphens `-`. "
    "Never use Unicode box-drawing characters.\n\n"
    "Rules: all monetary values in ₹. Never invent numbers — only narrate data provided below."
)


def _synthesise_node(state: ResearchState, config: RunnableConfig) -> dict:
    """One LLM call: synthesise pre-collected data into an 8-section research note."""
    from langchain_core.messages import SystemMessage, HumanMessage
    from src.utils.caveman import get_caveman_prompt

    llm = _get_llm()
    if llm is None:
        sections = [state.get(k, "") for k in
                    ("price", "fundamentals", "institutional", "macro", "news", "volatility", "correlation")]
        return {"report": "\n\n---\n\n".join(s for s in sections if s)}

    all_data = "\n\n---\n\n".join(
        f"### {k.title()}\n{state.get(k, '')}"
        for k in ("price", "fundamentals", "institutional", "macro", "news", "volatility", "correlation")
        if state.get(k)
    )

    result = llm.invoke([
        SystemMessage(content=_SYNTHESIS_PROMPT + get_caveman_prompt() + SYNTH_SUFFIX),
        HumanMessage(content=(
            f"Question: {state['question']}\n\n"
            f"Bear cases from adversarial analysis:\n{state.get('bear_cases', '')}\n\n"
            f"Pre-collected data:\n{all_data}"
        )),
    ], config=config)
    from .base import _render_report
    report = _render_report(result).strip()
    if not report:
        logger.warning(
            "autonomous_research: synthesis returned empty content — falling back to raw sections"
        )
        report = all_data
    return {"report": report}


# ── Graph ─────────────────────────────────────────────────────────────────────

_GRAPH = None


def _build_graph():
    global _GRAPH
    if _GRAPH is not None:
        return _GRAPH

    g = StateGraph(ResearchState)
    g.add_node("resolve",    _resolve_node)
    g.add_node("unresolved", _unresolved_node)
    g.add_node("fetch_all",  _fetch_all_node)
    g.add_node("correlate",  _correlate_node)
    g.add_node("verify",     _verify_node)
    g.add_node("synthesise", _synthesise_node)

    g.set_entry_point("resolve")
    g.add_conditional_edges(
        "resolve", _resolution_failed,
        {"unresolved": "unresolved", "fetch_all": "fetch_all"},
    )
    g.add_edge("unresolved", END)
    g.add_edge("fetch_all",  "correlate")
    g.add_edge("correlate",  "verify")
    g.add_edge("verify",     "synthesise")
    g.add_edge("synthesise", END)

    _GRAPH = g.compile(checkpointer=_get_checkpointer())
    return _GRAPH


# ── Public API ────────────────────────────────────────────────────────────────

def run(question: str, callbacks: list | None = None) -> str:
    """
    Run the autonomous research workflow for a given question.

    Parameters
    ----------
    question : str
        Free-text question, e.g. "comprehensive research on ADANIENT" or
        "why is Reliance Industries rising?"
    callbacks : LangChain callback handlers (e.g. BudgetCallbackHandler, tracer)
                forwarded into every node's LLM/tool invocations.

    Returns
    -------
    str
        8-section Markdown research note.
    """
    graph = _build_graph()
    config = {
        "configurable": {"thread_id": _thread_id("autonomous_research", question)},
        "callbacks": callbacks or [],
    }
    result = graph.invoke({"question": question}, config=config)
    return result.get("report", "*Research workflow returned no report*")
