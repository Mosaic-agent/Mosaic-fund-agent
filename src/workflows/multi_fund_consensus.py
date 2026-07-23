"""
src/workflows/multi_fund_consensus.py
──────────────────────────────────────
LangGraph StateGraph for cross-fund MF consensus analysis.

Replaces run_multi_asset_consensus (single tool call, no per-fund context).

Phases
------
1. fetch_all_funds — 7 funds in parallel via run_multi_asset_holdings_mom_yoy (0 LLM)
2. fetch_consensus — aggregate cross-fund view via run_multi_asset_consensus (0 LLM)
3. synthesise      — cross-fund signal synthesis (1 LLM call, ~4 000 tokens)

Total: ~4 000 tokens. Richer than current: per-fund context + synthesis.
"""
from __future__ import annotations

import logging

from langgraph.graph import StateGraph, END

from .base import _get_llm, _par_datasets, SYNTH_SUFFIX, _get_checkpointer, _thread_id
from .state import MosaicState

logger = logging.getLogger(__name__)

_FUNDS = [
    "NIPPON_INDIA_MULTI_ASSET_ALLOCATION_FUND",
    "NIPPON_INDIA_MULTI_ASSET_OMNI_FOF",
    "DSP_MULTI_ASSET",
    "DSP_MULTI_ASSET_OMNI_FOF",
    "BAJAJ_FINSERV_MULTI_ASSET_ALLOCATION_FUND",
    "QUANT_MULTI_ASSET",
    "ICICI_MULTI_ASSET",
]


class ConsensusState(MosaicState):
    period: str           # 'mom' | 'yoy'
    fund_reports: dict    # {fund_name: str}
    consensus_report: str
    synthesis: str


def _fetch_all_funds_node(state: ConsensusState) -> dict:
    """Fetch per-fund MoM/YoY report for all 7 funds in parallel."""
    period = state.get("period", "mom")

    def _make_fetcher(fund: str):
        def _fetch():
            from src.tools.runners import run_multi_asset_holdings_mom_yoy
            return run_multi_asset_holdings_mom_yoy.invoke({
                "fund": fund,
                "top": 15,
                "no_yoy": (period == "mom"),
            })
        return _fetch

    datasets = _par_datasets({fund: _make_fetcher(fund) for fund in _FUNDS})
    reports = {k: v.content for k, v in datasets.items()}
    return {"fund_reports": reports, "datasets": datasets}


def _fetch_consensus_node(state: ConsensusState) -> dict:
    """Fetch aggregate cross-fund consensus."""
    from src.tools.runners import run_multi_asset_consensus
    period = state.get("period", "mom")
    report = run_multi_asset_consensus.invoke({
        "period": period,
        "min_funds": 2,
        "min_delta": 0.10,
        "top": 20,
    })
    return {"consensus_report": str(report)}


def _synthesise_node(state: ConsensusState) -> dict:
    from langchain_core.messages import SystemMessage, HumanMessage

    llm = _get_llm()
    if llm is None:
        parts = [f"## {fn}\n{rep}" for fn, rep in state.get("fund_reports", {}).items()]
        parts.append(f"## Cross-Fund Consensus\n{state.get('consensus_report', '')}")
        return {"synthesis": "\n\n---\n\n".join(parts)}

    fund_section = "\n\n---\n\n".join(
        f"### {fn}\n{rep}"
        for fn, rep in state.get("fund_reports", {}).items()
        if rep and "unavailable" not in rep
    )

    result = llm.invoke([
        SystemMessage(content=(
            "You are an Indian mutual-fund analyst covering multi-asset schemes. "
            "You have per-fund MoM position changes AND the cross-fund consensus. "
            "Synthesise into:\n"
            "1. Securities with 3+ fund consensus (strongest conviction signals)\n"
            "2. Asset-class rotation direction (risk-on/risk-off signal)\n"
            "3. Notable fund-level divergences from the consensus\n"
            "4. 'What this signals' paragraph with a directional view\n"
            "Present all structured data and lists of securities in standard Markdown tables (using pipes `|` and hyphens `-`). Never use any Unicode box-drawing or frame characters (such as ╭, ─, ┬, ┐, ├, ┼, ┤, ╰, ┴, ╯, ┌, ┐, │, etc.) to draw tables or borders. Every table must begin and end with standard pipes (e.g. | Col 1 | Col 2 |).\n"
            "Never invent numbers — only narrate tool output." + SYNTH_SUFFIX
        )),
        HumanMessage(content=(
            f"Period: {state.get('period', 'mom').upper()}\n\n"
            f"Per-fund reports:\n{fund_section}\n\n"
            f"Cross-fund consensus:\n{state.get('consensus_report', '')}"
        )),
    ])
    return {"synthesis": str(result.content)}


_GRAPH = None


def _build_graph():
    global _GRAPH
    if _GRAPH is not None:
        return _GRAPH
    g = StateGraph(ConsensusState)
    g.add_node("fetch_all_funds", _fetch_all_funds_node)
    g.add_node("fetch_consensus", _fetch_consensus_node)
    g.add_node("synthesise",      _synthesise_node)
    g.set_entry_point("fetch_all_funds")
    g.add_edge("fetch_all_funds", "fetch_consensus")
    g.add_edge("fetch_consensus", "synthesise")
    g.add_edge("synthesise",      END)
    _GRAPH = g.compile(checkpointer=_get_checkpointer())
    return _GRAPH


def run(period: str = "mom") -> str:
    """
    Run the multi-fund consensus workflow.

    Parameters
    ----------
    period : str
        'mom' for month-over-month or 'yoy' for year-over-year analysis.

    Returns
    -------
    str
        Cross-fund synthesis: per-fund deltas + consensus signal.
    """
    graph = _build_graph()
    config = {"configurable": {"thread_id": _thread_id("multi_fund_consensus", period)}}
    result = graph.invoke(
        {"period": period, "fund_reports": {}, "consensus_report": "", "synthesis": "", "datasets": {}},
        config=config,
    )
    return result.get("synthesis", "*Multi-fund consensus workflow returned no report*")
