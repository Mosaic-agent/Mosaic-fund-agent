"""
src/utils/error_utils.py
────────────────────────
Standardized error formatting for tool calling and agent exceptions across Mosaic.

Guarantees that every tool error or agent exception includes an explicit 'Impact: ...'
statement that is returned directly to the LLM observation context or user transcript.
"""

from __future__ import annotations

from typing import Any


def format_tool_error(
    tool_name: str,
    error: Any,
    impact: str | None = None,
) -> str:
    """
    Format a tool execution exception or error string into a standardized,
    LLM-readable message containing an explicit impact statement.

    Args:
        tool_name: Name of the tool that raised the error (e.g. 'query_clickhouse_db').
        error: Exception object or raw error message string.
        impact: Optional custom impact statement describing how downstream reasoning,
                data availability, or calculations are affected. If omitted, a
                sensible default is generated based on tool_name.

    Returns:
        Formatted string: "Tool Error [<tool_name>]: <error_detail>. Impact: <impact_detail>"
    """
    err_str = str(error).strip() if error else "Unknown error"
    if not impact:
        impact = (
            f"Tool '{tool_name}' failed to complete. Downstream analysis using this "
            f"tool's output will be incomplete or rely on cached/fallback data."
        )
    return f"Tool Error [{tool_name}]: {err_str}. Impact: {impact}"


def format_agent_error(
    agent_name: str,
    error: Any,
    impact: str | None = None,
) -> str:
    """
    Format an agent execution exception or delegation failure into a standardized,
    LLM-readable message containing an explicit impact statement.

    Args:
        agent_name: Name of the agent or sub-agent (e.g. 'signal', 'india_equity', 'ComexAgent').
        error: Exception object or raw error message string.
        impact: Optional custom impact statement describing how the agent's workflow or
                report output is affected. If omitted, a sensible default is generated.

    Returns:
        Formatted string: "Agent Error [<agent_name>]: <error_detail>. Impact: <impact_detail>"
    """
    err_str = str(error).strip() if error else "Unknown error"
    if not impact:
        impact = (
            f"Agent '{agent_name}' encountered an error and could not complete its task. "
            f"Findings for this domain will be omitted or rely on direct fallbacks."
        )
    return f"Agent Error [{agent_name}]: {err_str}. Impact: {impact}"
