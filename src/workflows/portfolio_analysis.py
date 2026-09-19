"""
src/workflows/portfolio_analysis.py
────────────────────────────────────
LangGraph StateGraph for portfolio analysis with adversarial verification.

The Portfolio Agent — the canonical `/analyze` implementation for both the
CLI and chat REPL. Replaces MosaicFundAgent.run_full_analysis().

Phases
------
1. sync_holdings — fetch live holdings from Kite MCP, delta-sync into
                   ClickHouse (src/data_importer/portfolio_sync.py) (0 LLM)
2. discover    — read current open holdings + holding period from ClickHouse (0 LLM)
3. enrich_all  — parallel: price + news + earnings per holding (0 LLM)
4. score_all   — LLM: BUY/HOLD/SELL/EXIT + conviction per holding
5. verify_high — adversarial: refute HIGH-conviction scores
6. fetch_macro — parallel: COMEX + macro scanner + FII/DII (0 LLM)
7. synthesise  — portfolio summary with verified scores (1 LLM call)

Token savings vs current: ~60% for 10 holdings, ~75% for 20+.
"""
from __future__ import annotations

import json
import logging

from langgraph.graph import StateGraph, END

from .base import _get_llm, _par, _par_datasets, SYNTH_SUFFIX, _get_checkpointer, _thread_id
from .state import MosaicState

logger = logging.getLogger(__name__)


class PortfolioState(MosaicState):
    holdings: list          # rows from MarketDataRepository.current_holdings_with_period()
    sync_summary: dict      # what changed since the last Kite sync
    enriched: list          # [{symbol, price, news, earnings}]
    scored: list            # [{symbol, action, conviction, rationale}]
    verified: list          # scored list with HIGH ones adversarially checked
    macro_context: str
    report: str


def _sync_holdings_node(state: PortfolioState) -> dict:
    """Fetch live Kite holdings, diff against ClickHouse, persist the delta."""
    import asyncio
    from src.data_importer.portfolio_sync import sync_portfolio_holdings

    try:
        summary = asyncio.run(sync_portfolio_holdings())
    except Exception as exc:
        logger.warning("portfolio_analysis sync_holdings_node: %s", exc)
        summary = {
            "opened": [], "increased": [], "decreased": [],
            "avg_price_changed": [], "closed": [], "unchanged_count": 0,
            "error": str(exc),
        }
    return {"sync_summary": summary}


def _discover_node(state: PortfolioState) -> dict:
    """Read current open holdings + holding period from ClickHouse."""
    try:
        from src.db.pool import get_pool
        from src.db.repository import MarketDataRepository
        repo = MarketDataRepository(get_pool())
        df = repo.current_holdings_with_period()
        holdings = df.to_dict("records") if not df.empty else []
    except Exception as exc:
        logger.warning("portfolio_analysis discover_node: %s", exc)
        holdings = []

    from config.settings import settings
    max_h = settings.max_holdings_per_run
    if max_h and max_h > 0:
        holdings = holdings[:max_h]

    logger.info("portfolio_analysis: discovered %d holdings", len(holdings))
    return {"holdings": holdings}


def _enrich_all_node(state: PortfolioState) -> dict:
    """Parallel enrich: price + news + earnings for every holding.

    Refreshes ClickHouse daily_prices for just this portfolio's symbols
    (not a blanket "stocks" category backfill) — see `skip_global_freshness`
    on `_par()`.
    """
    holdings = state.get("holdings", [])
    if not holdings:
        return {"enriched": []}

    def _make_enricher(h: dict):
        sym = h.get("tradingsymbol", "")
        def _enrich():
            from src.tools.agent_tools import check_and_refresh_symbol_data
            from src.tools.yahoo_finance import get_yahoo_finance_data
            from src.tools.news_search import get_stock_news
            from src.tools.earnings_scraper import get_quarterly_results
            check_and_refresh_symbol_data.invoke({"symbol": sym, "auto_import": True})
            price    = str(get_yahoo_finance_data.invoke({"input_str": f"{sym}:NSE"}))
            news     = str(get_stock_news.invoke({"company_name": sym, "days": 7}))
            earnings = str(get_quarterly_results.invoke({"input_str": f"{sym}:NSE"}))
            return {**h, "price": price, "news": news, "earnings": earnings}
        return _enrich

    fetchers = {h.get("tradingsymbol", str(i)): _make_enricher(h)
                for i, h in enumerate(holdings)}
    results = _par(fetchers, max_workers=min(len(holdings), 10), skip_global_freshness=True)
    enriched = [v for v in results.values() if isinstance(v, dict)]
    return {"enriched": enriched}


def _score_all_node(state: PortfolioState) -> dict:
    """One LLM call per holding: assign BUY/HOLD/SELL/EXIT + conviction."""
    from langchain_core.messages import SystemMessage, HumanMessage

    llm = _get_llm()
    enriched = state.get("enriched", [])
    if not enriched or llm is None:
        scored = [
            {**h, "action": "HOLD", "conviction": "LOW", "rationale": "LLM unavailable"}
            for h in enriched
        ]
        return {"scored": scored}

    scored = []
    system = SystemMessage(content=(
        "You are a portfolio analyst. For each holding given, return a JSON object with "
        "keys: action (BUY|HOLD|SELL|EXIT), conviction (LOW|MEDIUM|HIGH), rationale (one sentence). "
        "Base your assessment only on the data provided. Never invent numbers." + SYNTH_SUFFIX
    ))

    for h in enriched:
        sym = h.get("tradingsymbol", "?")
        data = (
            f"Symbol: {sym}\nQty: {h.get('quantity','?')}\n"
            f"Avg price: {h.get('average_price','?')}\nPnL: {h.get('pnl','?')}\n\n"
            f"Price data:\n{h.get('price','')[:600]}\n\n"
            f"News:\n{h.get('news','')[:400]}\n\n"
            f"Earnings:\n{h.get('earnings','')[:400]}"
        )
        try:
            result = llm.invoke([system, HumanMessage(content=data)])
            from src.agents.sub_agents.base import _get_message_text
            content = _get_message_text(result.content).strip()
            try:
                start  = content.find("{")
                end    = content.rfind("}") + 1
                parsed = json.loads(content[start:end]) if start >= 0 else {}
            except Exception:
                parsed = {}
            scored.append({
                **h,
                "action":     parsed.get("action", "HOLD"),
                "conviction": parsed.get("conviction", "LOW"),
                "rationale":  parsed.get("rationale", content[:200]),
            })
        except Exception as exc:
            logger.warning("score_all: %s failed: %s", sym, exc)
            scored.append({**h, "action": "HOLD", "conviction": "LOW", "rationale": str(exc)[:100]})

    return {"scored": scored}


def _verify_high_node(state: PortfolioState) -> dict:
    """Adversarial: try to refute HIGH-conviction scores. Downgrade if refuted."""
    from langchain_core.messages import SystemMessage, HumanMessage

    llm = _get_llm()
    scored = state.get("scored", [])
    if llm is None:
        return {"verified": scored}

    verified = []
    system = SystemMessage(content=(
        "You are an adversarial analyst. Given a BUY or SELL recommendation with HIGH "
        "conviction, try to refute it. Return JSON: {\"refuted\": true|false, \"reason\": str}. "
        "Only return refuted=true if you find a genuine data-grounded counter-argument. "
        "Never invent numbers." + SYNTH_SUFFIX
    ))

    for h in scored:
        if h.get("conviction") != "HIGH" or h.get("action") not in ("BUY", "SELL", "EXIT"):
            verified.append(h)
            continue
        data = (
            f"Symbol: {h.get('tradingsymbol')}\nAction: {h.get('action')}\n"
            f"Rationale: {h.get('rationale')}\n\n"
            f"Price:\n{h.get('price','')[:500]}\n\n"
            f"Earnings:\n{h.get('earnings','')[:400]}"
        )
        try:
            result = llm.invoke([system, HumanMessage(content=data)])
            from src.agents.sub_agents.base import _get_message_text
            content = _get_message_text(result.content).strip()
            try:
                start  = content.find("{")
                end    = content.rfind("}") + 1
                parsed = json.loads(content[start:end]) if start >= 0 else {}
            except Exception:
                parsed = {}
            if parsed.get("refuted"):
                logger.info(
                    "verify_high: %s HIGH-conviction REFUTED: %s",
                    h.get("tradingsymbol"), parsed.get("reason", "")[:80],
                )
                verified.append({
                    **h,
                    "conviction": "MEDIUM",
                    "rationale": h["rationale"] + f" [DOWNGRADED: {parsed.get('reason', '')}]",
                })
            else:
                verified.append(h)
        except Exception as exc:
            logger.warning("verify_high: %s failed: %s", h.get("tradingsymbol"), exc)
            verified.append(h)

    return {"verified": verified}


def _fetch_macro_node(state: PortfolioState) -> dict:
    """Parallel macro context fetch — zero LLM calls."""
    def _comex():
        from src.tools.runners import run_comex_analysis
        return run_comex_analysis.invoke({})

    def _macro():
        from src.tools.runners import run_macro_scanner
        return run_macro_scanner.invoke({"max_themes": 3})

    def _fii():
        from src.tools.indian_equity_tools import get_fii_dii_summary
        return get_fii_dii_summary.invoke({"days": 7})

    # comex/macro/fii tools each do their own narrowly-scoped freshness check
    # internally — skip the blanket all-category check here too.
    datasets = _par_datasets({"comex": _comex, "macro": _macro, "fii": _fii}, skip_global_freshness=True)
    macro_context = "\n\n---\n\n".join(
        f"## {k.title()}\n{v.content}" for k, v in datasets.items()
    )
    return {"macro_context": macro_context, "datasets": datasets}


def _sync_summary_md(sync_summary: dict) -> str:
    """Render the 'since last sync' delta section from the sync_holdings node."""
    if not sync_summary:
        return ""
    lines = []
    for label, key in (("🆕 Opened", "opened"), ("📈 Increased", "increased"),
                        ("📉 Decreased", "decreased"), ("⚙️ Avg price changed", "avg_price_changed"),
                        ("❌ Closed", "closed")):
        symbols = sync_summary.get(key) or []
        if symbols:
            lines.append(f"- {label}: {', '.join(symbols)}")
    unchanged = sync_summary.get("unchanged_count", 0)
    if not lines:
        return f"## Since Last Sync\nNo changes detected ({unchanged} holdings unchanged).\n"
    lines.append(f"- Unchanged: {unchanged} holdings")
    return "## Since Last Sync\n" + "\n".join(lines) + "\n"


def _synthesise_node(state: PortfolioState) -> dict:
    from langchain_core.messages import SystemMessage, HumanMessage

    llm = _get_llm()
    verified = state.get("verified", [])
    sync_md = _sync_summary_md(state.get("sync_summary", {}))

    if llm is None or not verified:
        rows = "\n".join(
            f"- {h.get('tradingsymbol','?')}: {h.get('action','?')} "
            f"({h.get('conviction','?')}) — {h.get('rationale','')}"
            for h in verified
        )
        return {"report": f"{sync_md}\n## Portfolio Scores\n{rows}\n\n## Macro\n{state.get('macro_context', '')}"}

    holdings_table = "\n".join(
        f"| {h.get('tradingsymbol','?')} | {h.get('quantity','?')} | {h.get('pnl','?')} "
        f"| {h.get('holding_period_days','?')} | {h.get('action','?')} | {h.get('conviction','?')} "
        f"| {h.get('rationale','')} |"
        for h in verified
    )

    result = llm.invoke([
        SystemMessage(content=(
            "You are a senior portfolio analyst. Synthesise the holdings analysis into:\n"
            "1. Portfolio health summary (2-3 sentences)\n"
            "2. Since-last-sync changes (given verbatim — do not recompute)\n"
            "3. Holdings table with holding period/action/conviction\n"
            "4. Top 3 risks ranked by severity\n"
            "5. Recommended actions (concrete, prioritised)\n"
            "6. Macro context and how it affects the portfolio\n"
            "Never compute numbers — only narrate tool output." + SYNTH_SUFFIX
        )),
        HumanMessage(content=(
            f"Since last sync:\n{sync_md}\n\n"
            "Holdings:\n"
            "| Symbol | Qty | PnL | Holding Period (days) | Action | Conviction | Rationale |\n"
            "|--------|-----|-----|------------------------|--------|------------|-----------|\n"
            f"{holdings_table}\n\n"
            f"Macro context:\n{state.get('macro_context', '')}"
        )),
    ])
    from .base import _render_report
    return {"report": _render_report(result)}


_GRAPH = None


def _build_graph():
    global _GRAPH
    if _GRAPH is not None:
        return _GRAPH
    g = StateGraph(PortfolioState)
    g.add_node("sync_holdings", _sync_holdings_node)
    g.add_node("discover",    _discover_node)
    g.add_node("enrich_all",  _enrich_all_node)
    g.add_node("score_all",   _score_all_node)
    g.add_node("verify_high", _verify_high_node)
    g.add_node("fetch_macro", _fetch_macro_node)
    g.add_node("synthesise",  _synthesise_node)
    g.set_entry_point("sync_holdings")
    g.add_edge("sync_holdings", "discover")
    g.add_edge("discover",    "enrich_all")
    g.add_edge("enrich_all",  "score_all")
    g.add_edge("score_all",   "verify_high")
    g.add_edge("verify_high", "fetch_macro")
    g.add_edge("fetch_macro", "synthesise")
    g.add_edge("synthesise",  END)
    _GRAPH = g.compile(checkpointer=_get_checkpointer())
    return _GRAPH


def run() -> str:
    """
    Run the portfolio analysis workflow.

    Syncs live holdings from Kite MCP into ClickHouse (with delta detection),
    then reads the current open holdings + holding period back out.

    Returns
    -------
    str
        Portfolio report: holdings table, scores, macro context, recommended actions.
    """
    from datetime import date
    graph = _build_graph()
    config = {"configurable": {"thread_id": _thread_id("portfolio_analysis", str(date.today()))}}
    result = graph.invoke({
        "holdings": [], "sync_summary": {}, "enriched": [], "scored": [],
        "verified": [], "macro_context": "", "datasets": {}, "report": "",
    }, config=config)
    return result.get("report", "*Portfolio workflow returned no report*")
