"""
src/workflows
─────────────
LangGraph StateGraph workflows — token-efficient alternatives to ReAct sub-agents.

Each workflow uses pure-Python nodes for all data fetch (0 LLM tokens) and
reserves LLM calls only for synthesis/adversarial verification (1–2 total).

Token savings: 80–90% vs equivalent ReAct loops.

Public API
----------
    from src.workflows.autonomous_research import run as run_research
    from src.workflows.india_equity import run as run_equity
    from src.workflows.multi_fund_consensus import run as run_consensus
    from src.workflows.portfolio_analysis import run as run_portfolio
"""
from .autonomous_research import run as run_autonomous_research
from .india_equity import run as run_india_equity_research
from .multi_fund_consensus import run as run_multi_fund_consensus
from .portfolio_analysis import run as run_portfolio_analysis

__all__ = [
    "run_autonomous_research",
    "run_india_equity_research",
    "run_multi_fund_consensus",
    "run_portfolio_analysis",
]
