"""
src/workflows
─────────────
LangGraph StateGraph workflows — token-efficient alternatives to ReAct sub-agents.

Each workflow uses pure-Python nodes for all data fetch (0 LLM tokens) and
reserves LLM calls only for synthesis/adversarial verification (1–2 total).

Token savings: 55–81% vs equivalent ReAct loops.

Public API
----------
    # Existing
    from src.workflows.autonomous_research import run as run_research
    from src.workflows.india_equity import run as run_equity
    from src.workflows.multi_fund_consensus import run as run_consensus
    from src.workflows.portfolio_analysis import run as run_portfolio

    # New: parallel-fetch (signal, macro, news)
    from src.workflows.signal import run as run_signal
    from src.workflows.macro import run as run_macro
    from src.workflows.news import run as run_news

    # New: Plan-Execute-Replan (mf)
    from src.workflows.mf_planner import run as run_mf_planner

    # Plan persistence
    from src.workflows.plan_store import save_plan, find_similar_plans, load_plan
"""
from .autonomous_research import run as run_autonomous_research
from .india_equity import run as run_india_equity_research
from .multi_fund_consensus import run as run_multi_fund_consensus
from .portfolio_analysis import run as run_portfolio_analysis
from .signal import run as run_signal
from .macro import run as run_macro
from .news import run as run_news
from .mf_planner import run as run_mf_planner
from .plan_store import save_plan, find_similar_plans, load_plan

__all__ = [
    # existing
    "run_autonomous_research",
    "run_india_equity_research",
    "run_multi_fund_consensus",
    "run_portfolio_analysis",
    # new workflows
    "run_signal",
    "run_macro",
    "run_news",
    "run_mf_planner",
    # plan store
    "save_plan",
    "find_similar_plans",
    "load_plan",
]
