"""
src/utils/report_loader.py
──────────────────────────
Utility to load the most recent portfolio report from the output directory.

Used by PortfolioAgent.ask() to inject prior portfolio context into the
LangGraph ReAct loop — so the agent can answer questions about the user's
portfolio without re-fetching all data from scratch on every ask().
"""

from __future__ import annotations

import glob
import json
import logging
import os
from typing import Any

from config.settings import settings

logger = logging.getLogger(__name__)


def load_latest_report(output_dir: str = "./output") -> dict[str, Any] | None:
    """
    Load the most recent portfolio JSON report from output_dir.

    Report filenames are timestamped (portfolio_report_YYYYMMDD_HHMMSS.json),
    so a lexicographic sort is sufficient to find the most recent one.

    Returns:
        Parsed report dict, or None if no reports exist or the file is unreadable.
    """
    pattern = os.path.join(output_dir, "portfolio_report_*.json")
    files = sorted(glob.glob(pattern))
    if not files:
        logger.debug("No portfolio reports found in %s", output_dir)
        return None

    latest = files[-1]
    try:
        with open(latest, encoding="utf-8") as f:
            data = json.load(f)
        logger.debug("Loaded latest report from %s", latest)
        return data
    except Exception as exc:
        logger.warning("Could not load report %s: %s", latest, exc)
        return None


def _compact_context(report: dict[str, Any], context_window: int | None = None) -> str:
    """
    Build a token-efficient context string from a portfolio report.

    Includes only the information the agent is likely to need for answering
    natural-language questions:
      - Portfolio-level summary (value, P&L, health score)
      - Top sector allocation
      - Per-holding key facts (symbol, P&L%, risk, sentiment, recommendation)
      - Portfolio risks and rebalancing signals

    The full report is intentionally NOT injected to avoid blowing the context
    window — the agent can call tools for deeper data if needed.

    The structural caps (sectors, risks, rebalancing signals, insights) scale
    automatically with *context_window* so local small models get a lean
    context while large cloud models receive more detail.
    """
    cw = context_window if context_window is not None else settings.llm_context_window
    if cw <= 8192:
        # Small local model — keep context minimal
        max_sectors, max_risks, max_rebal, max_insights = 3, 3, 3, 2
    elif cw <= 32768:
        # Mid-range (GPT-4o-mini, GPT-3.5, etc.) — balanced detail
        max_sectors, max_risks, max_rebal, max_insights = 5, 4, 4, 3
    else:
        # Large cloud model (GPT-4o, Claude 3.5, etc.) — full detail
        max_sectors, max_risks, max_rebal, max_insights = 8, 6, 6, 5

    lines: list[str] = []

    # Portfolio summary
    summary = report.get("portfolio_summary", {})
    lines.append(
        f"Portfolio: {summary.get('num_holdings', '?')} holdings  |  "
        f"Value: {summary.get('total_value', 'N/A')}  |  "
        f"P&L: {summary.get('total_pnl', 'N/A')} ({summary.get('total_pnl_percent', 'N/A')})  |  "
        f"Health: {summary.get('health_score', 'N/A')}/100  |  "
        f"Diversification: {summary.get('diversification_score', 'N/A')}/100"
    )
    lines.append(
        f"Stocks: {summary.get('stock_count', 0)}  |  "
        f"ETFs: {summary.get('etf_count', 0)}  |  "
        f"Direct equity: {summary.get('direct_equity_allocation_pct', 0):.1f}%  |  "
        f"ETF: {summary.get('etf_allocation_pct', 0):.1f}%"
    )

    # Top sectors
    sector_alloc = report.get("sector_allocation", {})
    if sector_alloc:
        top = sorted(sector_alloc.items(), key=lambda x: x[1], reverse=True)[:max_sectors]
        lines.append("Top sectors: " + ", ".join(f"{s}={v:.1f}%" for s, v in top))

    # Per-holding compact summary
    holdings = report.get("holdings_analysis", [])
    if holdings:
        lines.append("Holdings:")
        for h in holdings:
            rec = h.get("recommendation", "")
            rec_str = f" [{rec}]" if rec else ""
            lines.append(
                f"  {h.get('symbol')}: "
                f"P&L {h.get('pnl_percent', 0):+.1f}%  |  "
                f"Risk {h.get('risk_score', 5):.0f}/10  |  "
                f"Sentiment {h.get('sentiment_score', 0):+.2f}{rec_str}  |  "
                f"Sector: {h.get('sector', 'Unknown')}"
            )

    # Portfolio risks
    risks = report.get("portfolio_risks", [])
    if risks:
        lines.append("Portfolio risks:")
        lines.extend(f"  - {r}" for r in risks[:max_risks])

    # Rebalancing signals
    rebalancing = report.get("rebalancing_signals", [])
    if rebalancing:
        lines.append("Rebalancing signals:")
        lines.extend(f"  - {s}" for s in rebalancing[:max_rebal])

    # Actionable insights (top 3)
    insights = report.get("actionable_insights", [])
    if insights:
        lines.append("Actionable insights:")
        lines.extend(f"  - {i}" for i in insights[:max_insights])

    # Timestamp
    generated_at = report.get("generated_at", "")
    if generated_at:
        lines.append(f"Report generated at: {generated_at}")

    return "\n".join(lines)
