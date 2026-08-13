"""Agent tool for searching historical market context via the Qdrant market_data collection."""
from __future__ import annotations

from langchain_core.tools import tool


@tool
def search_historical_market_context(
    query: str,
    symbol: str = "",
    category: str = "",
) -> str:
    """Search for historical market periods similar to a described context.

    Use this to find precedent market conditions — e.g., periods when DXY
    fell while gold rose, or when USDINR was above 85 with FII selling.
    Only use when the user asks about historical market regime similarity,
    not for general price lookups.

    Args:
        query: Natural language description of the market context to search for.
        symbol: Optional symbol filter (e.g., 'GOLDBEES', 'USDINR').
        category: Optional category filter (e.g., 'etfs', 'fx_rates').
    """
    from src.db.market_vector import search_market_context

    results = search_market_context(
        query=query, k=8, symbol=symbol, category=category,
    )
    if not results:
        return (
            "No matching historical market context found in the market_data "
            "Qdrant collection. The collection may be empty or Qdrant unavailable."
        )

    lines = ["### Historical Market Context Matches\n"]
    for i, r in enumerate(results, 1):
        dt = r.get("data_type", "?")
        lines.append(
            f"{i}. **{r.get('symbol', '?')}** ({r.get('category', '?')}) "
            f"{r.get('trade_date', '?')} — similarity={r.get('similarity', 0):.3f}"
        )
        if dt == "price":
            lines.append(
                f"   O={r.get('open', 0):.2f} H={r.get('high', 0):.2f} "
                f"L={r.get('low', 0):.2f} C={r.get('close', 0):.2f} "
                f"V={r.get('volume', 0):.0f}"
            )
        elif dt == "cot":
            lines.append(
                f"   MM_net={r.get('mm_net', 0):.0f} "
                f"Comm_net={r.get('comm_net', 0):.0f} "
                f"OI={r.get('open_interest', 0):.0f}"
            )
        elif dt == "macro":
            lines.append(
                f"   {r.get('indicator_name', '?')} = {r.get('value', 0):.4f}"
            )
        elif dt == "nav":
            lines.append(f"   NAV={r.get('nav', 0):.4f}")
        elif dt == "fx_rate":
            lines.append(
                f"   O={r.get('open', 0):.4f} H={r.get('high', 0):.4f} "
                f"L={r.get('low', 0):.4f} C={r.get('close', 0):.4f}"
            )
    return "\n".join(lines)


MARKET_CONTEXT_TOOLS = [search_historical_market_context]
