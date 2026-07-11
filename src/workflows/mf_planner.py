"""
src/workflows/mf_planner.py
────────────────────────────
LangGraph Plan-Execute-Replan StateGraph for mutual fund research.

Replaces MFSubAgent (ReAct, RECURSION_LIMIT=30).

The MF intent is open-ended: what data to fetch depends on what previous steps return.
A static parallel-fetch graph is insufficient. Instead:

Phases
------
1. [outside graph] plan   — LLM decomposes question into ordered steps (1 LLM call)
2. [outside graph] show   — _show_and_approve_plan() for Y/n/edit before execution
3. executor               — runs next step with MF tool suite (mini ReAct, 1 tool call)
4. replanner              — assesses progress; rewrites remaining steps or terminates (1 LLM call)
   ↺ loop back to executor until replanner says "done" or step_count >= max_steps

Self-improvement example
------------------------
Q: "Why is DSP trimming gold?"
  Initial plan: [run_multi_asset_consensus, run MoM changes for DSP_MULTI_ASSET]
  After step 1 → consensus shows Nippon ALSO trimming gold
  Replanner adds: [run MoM for NIPPON_INDIA_..., run_whale_tracker]  ← self-improved plan

Token savings: ~6 000-12 000 vs ~25 000 for the ReAct equivalent (~55-76%).
"""
from __future__ import annotations

import json
import logging
from typing import Annotated, Literal, TypedDict

from langchain_core.messages import SystemMessage, HumanMessage
from langgraph.graph import StateGraph, END
from pydantic import BaseModel

from .base import _get_llm, SYNTH_SUFFIX, _get_checkpointer, _thread_id, _show_and_approve_plan
from .plan_store import save_plan

logger = logging.getLogger(__name__)

_MAX_STEPS_DEFAULT = 8

# ── Pydantic schemas for structured LLM output ───────────────────────────────

class Plan(BaseModel):
    steps: list[str]   # ordered list of 2-6 step descriptions


class ReplanDecision(BaseModel):
    action:       Literal["continue", "revise", "done"]
    revised_plan: list[str] = []   # populated when action="revise"
    response:     str = ""         # populated when action="done"


# ── State ─────────────────────────────────────────────────────────────────────

class MFPlanExecute(TypedDict):
    input:      str              # original question (preserved throughout)
    question:   str              # alias for input (used by _show_and_approve_plan)
    plan:       list[str]        # remaining steps; replanner rewrites this each cycle
    past_steps: list             # list of [step_description, result_str] pairs
    step_count: int              # monotonic counter; terminates at max_steps
    max_steps:  int              # cap (default 8)
    plan_id:    str              # PlanStore reference ID
    response:   str              # set by replanner when done; drives END edge


# ── Planner prompt ────────────────────────────────────────────────────────────

_PLANNER_PROMPT = """\
You are a mutual fund research planner for the Mosaic platform (Indian markets).
Decompose the user's question into an ordered list of 2-6 concrete tool-call steps.
Each step is one sentence describing WHAT to look up and WHICH tool to use.

Available tools
───────────────
run_multi_asset_consensus()
    Cross-fund consensus: which securities are all multi-asset funds collectively buying/trimming.
    Use for: "smart money", "pattern across funds", "collectively buying".

run_multi_asset_holdings_mom_yoy(fund=NAME)
    MoM/YoY position changes for a single fund. fund= values:
      DSP_MULTI_ASSET | DSP_MULTI_ASSET_OMNI_FOF
      NIPPON_INDIA_MULTI_ASSET_ALLOCATION_FUND | NIPPON_INDIA_MULTI_ASSET_OMNI_FOF
      BAJAJ_FINSERV_MULTI_ASSET_ALLOCATION_FUND | QUANT_MULTI_ASSET
    Use for: "MoM/YoY changes in <fund>", "top adds/exits in <fund>".

run_whale_tracker()
    Theme-level exposure (Gold/Silver/Nuclear/Infra) across all 7 multi-asset funds.
    Use for: "theme rotation", "gold exposure", "nuclear theme".

get_mf_holdings_for_stock(company_name_or_symbol=NAME)
    Reverse lookup: which funds hold a specific stock.
    Use for: "which funds hold <stock>", "cross-ownership of <stock>".

find_funds_holding(query=TEXT)
    Qdrant semantic search: find funds holding a security or ISIN.

run_fund_mom_returns(scheme_code=CODE or search=TEXT)
    NAV MoM return history for any Indian MF.
    Use for: "NAV returns", "how has <fund> performed".

run_dsp_multi_asset_comparison()
    DSP cross-fund weighted comparison across all DSP active equity funds.

query_clickhouse_db(sql=QUERY)
    Ad-hoc SQL against market_data.mf_holdings FINAL or mf_nav FINAL.

Rules
─────
- Return ONLY a JSON array of step strings. No extra text.
- 2-6 steps maximum. Don't pad with unnecessary steps.
- Always start with the broadest signal (consensus) before drilling into a single fund.

Example output:
["Call run_multi_asset_consensus() to see cross-fund regime",
 "Call run_multi_asset_holdings_mom_yoy(fund='DSP_MULTI_ASSET') for DSP position deltas"]
"""

_EXECUTOR_PROMPT = """\
You are a mutual fund tool executor for the Mosaic platform.

Execute ONLY the next step described below using EXACTLY ONE tool call.
Return the raw tool output. Do NOT call more than one tool.

Past results are provided for context. Do not repeat a tool already called.
"""

_REPLANNER_PROMPT_TEMPLATE = """\
You are a mutual fund research replanner.

Original question: {input}
Completed steps and results:
{past_steps}

Remaining plan steps:
{plan}

Step count: {step_count} / {max_steps}

Based on what you have learned so far, decide:
  "continue" — the remaining plan steps are still correct; proceed to next step
  "revise"   — rewrite the remaining steps because new information changed the approach
  "done"     — you have enough data to answer the question; write the final response

Rules:
- If step_count >= {max_steps}, ALWAYS choose "done".
- If plan is empty, choose "done".
- When "done", write a complete Markdown response in the `response` field.
- When "revise", provide the revised remaining steps in `revised_plan`.
- When synthesising, present all structured data in standard Markdown tables (using pipes `|` and hyphens `-`). Never use any Unicode box-drawing or frame characters (such as ╭, ─, ┬, ┐, ├, ┼, ┤, ╰, ┴, ╯, ┌, ┐, │, etc.) to draw tables or borders. Every table must begin and end with standard pipes (e.g. | Col 1 | Col 2 |).
- End with a "What this signals" paragraph connecting position deltas to a directional view.
- NEVER compute numbers yourself — narrate only from the tool results above.
"""


# ── MF tool suite (shared across executor invocations) ────────────────────────

def _get_mf_tools() -> list:
    from src.tools.skills_tools import (
        run_multi_asset_holdings_mom_yoy,
        run_multi_asset_consensus,
        run_whale_tracker,
        run_dsp_multi_asset_comparison,
        run_fund_mom_returns,
        query_clickhouse_db,
    )
    from src.tools.indian_equity_tools import get_mf_holdings_for_stock
    from src.tools.market.mf_tools import find_funds_holding, find_similar_funds, search_mf_exposure
    from src.tools.chart_tools import plot_fund_holdings_chart
    from src.tools.db_tools import describe_db_table
    from src.tools.news_search import get_stock_news
    return [
        run_multi_asset_holdings_mom_yoy,
        run_multi_asset_consensus,
        run_whale_tracker,
        run_dsp_multi_asset_comparison,
        run_fund_mom_returns,
        get_mf_holdings_for_stock,
        find_funds_holding,
        find_similar_funds,
        search_mf_exposure,
        plot_fund_holdings_chart,
        query_clickhouse_db,
        describe_db_table,
        get_stock_news,
    ]


# ── Node: executor ────────────────────────────────────────────────────────────

def _executor_node(state: MFPlanExecute) -> dict:
    """Execute the next step from the plan using a mini ReAct agent."""
    if not state["plan"]:
        # No steps left — force done
        return {"step_count": state["step_count"] + 1}

    next_step = state["plan"][0]
    logger.info("mf_planner: executor step %d/%d: %s", state["step_count"] + 1, state["max_steps"], next_step[:80])

    # Format past results for context (truncated to avoid token bloat)
    past_context = ""
    for step_desc, step_result in state["past_steps"][-3:]:   # last 3 steps only
        past_context += f"\nStep: {step_desc}\nResult: {str(step_result)[:500]}\n"

    llm = _get_llm()
    if llm is None:
        # No LLM — keyword-route the step
        result_str = _keyword_execute(next_step)
    else:
        from langgraph.prebuilt import create_react_agent
        tools = _get_mf_tools()
        executor_agent = create_react_agent(
            model=llm,
            tools=tools,
            prompt=_EXECUTOR_PROMPT,
        )
        try:
            exec_result = executor_agent.invoke(
                {"messages": [HumanMessage(
                    content=f"Execute this step: {next_step}\n\nPrevious results:{past_context}"
                )]},
                {"recursion_limit": 6},
            )
            result_str = _extract_tool_result(exec_result)
        except Exception as exc:
            logger.warning("mf_planner: executor failed (%s) — keyword fallback", exc)
            result_str = _keyword_execute(next_step)

    new_past = list(state["past_steps"]) + [[next_step, result_str]]
    return {
        "past_steps": new_past,
        "plan":       state["plan"][1:],   # pop the completed step
        "step_count": state["step_count"] + 1,
    }


def _extract_tool_result(agent_result: dict) -> str:
    """Pull the last ToolMessage or AIMessage text from a ReAct agent result."""
    from langchain_core.messages import ToolMessage, AIMessage
    msgs = agent_result.get("messages", [])
    for m in reversed(msgs):
        if isinstance(m, ToolMessage):
            return str(m.content)[:3000]
    for m in reversed(msgs):
        if isinstance(m, AIMessage) and m.content:
            content = m.content
            if isinstance(content, list):
                return " ".join(str(c.get("text", c)) for c in content if isinstance(c, dict))[:3000]
            return str(content)[:3000]
    return "*Step produced no output*"


def _keyword_execute(step: str) -> str:
    """Keyword-based fallback executor for local models without tool-calling."""
    s = step.lower()
    try:
        if "consensus" in s:
            from src.tools.skills_tools import run_multi_asset_consensus
            period = "yoy" if "yoy" in s else "mom"
            return str(run_multi_asset_consensus.invoke({"period": period, "top": 15}))
        if "whale" in s or "theme" in s:
            from src.tools.skills_tools import run_whale_tracker
            return str(run_whale_tracker.invoke({}))
        if "nav" in s or "return" in s:
            from src.tools.skills_tools import run_fund_mom_returns
            return str(run_fund_mom_returns.invoke({}))
        # Default: MoM changes
        from src.tools.skills_tools import run_multi_asset_holdings_mom_yoy
        for canonical, hint in [
            ("DSP_MULTI_ASSET",                          "dsp"),
            ("NIPPON_INDIA_MULTI_ASSET_ALLOCATION_FUND", "nippon"),
            ("BAJAJ_FINSERV_MULTI_ASSET_ALLOCATION_FUND","bajaj"),
            ("QUANT_MULTI_ASSET",                        "quant"),
        ]:
            if hint in s:
                return str(run_multi_asset_holdings_mom_yoy.invoke({"fund": canonical}))
        return str(run_multi_asset_holdings_mom_yoy.invoke({"list_funds": True}))
    except Exception as exc:
        return f"*Keyword execution failed: {exc}*"


# ── Node: replanner ───────────────────────────────────────────────────────────

def _replanner_node(state: MFPlanExecute) -> dict:
    """Assess progress and decide: continue / revise plan / produce final answer."""
    llm = _get_llm()
    if llm is None:
        # No LLM → synthesise from past steps directly
        return {"response": _synthesise_past_steps(state)}

    past_fmt = "\n".join(
        f"[{i+1}] {s}: {r}" for i, (s, r) in enumerate(state["past_steps"])
    )
    prompt = _REPLANNER_PROMPT_TEMPLATE.format(
        input=state["input"],
        past_steps=past_fmt,
        plan="\n".join(f"- {s}" for s in state["plan"]),
        step_count=state["step_count"],
        max_steps=state["max_steps"],
    )

    try:
        structured_llm = llm.with_structured_output(ReplanDecision)
        decision: ReplanDecision = structured_llm.invoke([
            SystemMessage(content="You are a mutual fund research replanner."),
            HumanMessage(content=prompt + SYNTH_SUFFIX),
        ])
    except Exception as exc:
        logger.warning("mf_planner: replanner structured output failed (%s) — synthesising", exc)
        return {"response": _synthesise_past_steps(state)}

    if decision is None:
        logger.warning("mf_planner: replanner structured output returned None — synthesising")
        return {"response": _synthesise_past_steps(state)}

    if decision.action == "done":
        logger.info("mf_planner: replanner done after %d steps", state["step_count"])
        return {"response": decision.response or _synthesise_past_steps(state)}

    if decision.action == "revise" and decision.revised_plan:
        logger.info(
            "mf_planner: replanner revised plan at step %d (%d → %d steps)",
            state["step_count"], len(state["plan"]), len(decision.revised_plan),
        )
        # Save revised plan for audit trail
        try:
            save_plan(
                intent="mf",
                question=state["input"],
                steps=decision.revised_plan,
                metadata={"revision": state["step_count"], "parent_plan_id": state.get("plan_id", "")},
            )
        except Exception:
            pass
        return {"plan": decision.revised_plan}

    # "continue" — pop the next step (executor already did it; plan was updated there)
    return {}


def _synthesise_past_steps(state: MFPlanExecute) -> str:
    """Fallback synthesiser: concatenate all past step results."""
    parts = [f"## MF Research: {state['input']}\n"]
    for i, (step, result) in enumerate(state["past_steps"]):
        parts.append(f"### Step {i+1}: {step}\n{result}")
    return "\n\n".join(parts)


# ── Conditional edge ──────────────────────────────────────────────────────────

def _should_continue(state: MFPlanExecute) -> str:
    if state.get("response"):
        return "done"
    if state["step_count"] >= state["max_steps"]:
        return "done"
    return "executor"


# ── Graph construction ────────────────────────────────────────────────────────

_GRAPH = None


def _build_graph():
    global _GRAPH
    if _GRAPH is not None:
        return _GRAPH
    g = StateGraph(MFPlanExecute)
    g.add_node("executor",  _executor_node)
    g.add_node("replanner", _replanner_node)
    g.set_entry_point("executor")
    g.add_edge("executor", "replanner")
    g.add_conditional_edges(
        "replanner",
        _should_continue,
        {"executor": "executor", "done": END},
    )
    _GRAPH = g.compile(checkpointer=_get_checkpointer())
    return _GRAPH


# ── Outside-graph: generate plan with LLM ────────────────────────────────────

def _generate_plan(question: str) -> list[str]:
    """Use LLM to decompose the question into a concrete plan. Fallback to keyword heuristics."""
    llm = _get_llm()
    if llm is None:
        return _keyword_plan(question)
    try:
        structured_llm = llm.with_structured_output(Plan)
        result: Plan = structured_llm.invoke([
            SystemMessage(content=_PLANNER_PROMPT),
            HumanMessage(content=question),
        ])
        if result is not None:
            steps = [s.strip() for s in result.steps if s.strip()]
            if steps:
                return steps[:6]   # cap at 6 steps
    except Exception as exc:
        logger.warning("mf_planner: LLM plan generation failed (%s) — using keyword plan", exc)
    return _keyword_plan(question)


def _keyword_plan(question: str) -> list[str]:
    """Fast keyword-based plan for local models without structured output."""
    q = question.lower()
    if any(k in q for k in ("consensus", "collectively", "smart money", "all fund", "pattern")):
        return [
            "Call run_multi_asset_consensus() to see cross-fund holdings regime",
            "Call run_whale_tracker() for theme-level gold/silver/nuclear exposure",
        ]
    if any(k in q for k in ("which fund", "who holds", "funds holding", "reverse")):
        import re
        m = re.search(r"hold(?:s|ing)?\s+([A-Za-z0-9 &\-\.]+?)(?:\?|$|\.)", question, re.I)
        target = m.group(1).strip() if m else question
        return [f"Call get_mf_holdings_for_stock(company_name_or_symbol='{target}')"]
    if any(k in q for k in ("nav return", "mom return", "performance")):
        return ["Call run_fund_mom_returns() to fetch NAV MoM returns"]
    # Default: consensus → specific fund
    for canonical, hint in [
        ("DSP_MULTI_ASSET",                          "dsp multi"),
        ("NIPPON_INDIA_MULTI_ASSET_ALLOCATION_FUND", "nippon"),
        ("BAJAJ_FINSERV_MULTI_ASSET_ALLOCATION_FUND","bajaj"),
        ("QUANT_MULTI_ASSET",                        "quant"),
    ]:
        if hint in q:
            return [
                "Call run_multi_asset_consensus() to see cross-fund context",
                f"Call run_multi_asset_holdings_mom_yoy(fund='{canonical}') for MoM changes",
            ]
    return [
        "Call run_multi_asset_consensus() to see cross-fund holdings pattern",
        "Call run_multi_asset_holdings_mom_yoy() to show position changes",
    ]


# ── Public entry point ────────────────────────────────────────────────────────

def run(question: str) -> str:
    """
    Run the MF Plan-Execute-Replan workflow.

    Parameters
    ----------
    question : Open-ended user question about mutual fund holdings,
               NAV returns, cross-fund consensus, or theme exposure.

    Returns
    -------
    str
        Formatted Markdown MF research note with Markdown tables.
    """
    # ── Step 1: generate plan (1 LLM call, outside graph) ────────────────
    plan = _generate_plan(question)

    # ── Step 2: save plan ─────────────────────────────────────────────────
    plan_id = save_plan("mf", question, plan)

    # ── Step 3: show plan and get approval ────────────────────────────────
    approved = _show_and_approve_plan(question, plan, intent="mf")
    if approved is None:
        return "Plan cancelled by user."
    plan = approved

    # ── Step 4: run execute-replan graph ──────────────────────────────────
    graph = _build_graph()
    config = {"configurable": {"thread_id": _thread_id("mf_planner", question)}}
    initial_state: MFPlanExecute = {
        "input":      question,
        "question":   question,
        "plan":       plan,
        "past_steps": [],
        "step_count": 0,
        "max_steps":  _MAX_STEPS_DEFAULT,
        "plan_id":    plan_id,
        "response":   "",
    }
    result = graph.invoke(initial_state, config=config)
    return result.get("response", "*MF planner returned no response*")
