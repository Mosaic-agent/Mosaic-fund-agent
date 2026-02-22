"""
src/tools/zerodha_mcp_tools.py
──────────────────────────────
LangChain Tool wrappers around the Zerodha Kite MCP client.

Each tool is registered with LangChain's @tool decorator so the agent
can invoke them via the tool-calling interface.

Tools exposed:
  • fetch_portfolio_holdings  – get all CNC holdings from Zerodha
  • fetch_open_positions      – get intraday / short-term positions
  • fetch_account_profile     – get user name and account details
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from langchain_core.tools import tool

from src.clients.mcp_client import KiteMCPClient
from src.models.portfolio import Holding, InstrumentType, Portfolio, Position

logger = logging.getLogger(__name__)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _detect_instrument_type(symbol: str, instrument_type_raw: str) -> InstrumentType:
    """
    Infer whether a holding is a stock or ETF based on symbol / type field.
    Common Indian ETFs end with BEES, ETF, or have known names.
    """
    etf_keywords = ["BEES", "ETF", "NIFTYBEES", "JUNIORBEES", "GOLDBEES", "LIQUIDBEES"]
    s = symbol.upper()
    if any(kw in s for kw in etf_keywords):
        return InstrumentType.ETF
    raw = (instrument_type_raw or "").upper()
    if "ETF" in raw:
        return InstrumentType.ETF
    if "MF" in raw or "MUTUAL" in raw:
        return InstrumentType.MUTUAL_FUND
    return InstrumentType.STOCK


def _parse_holdings(raw: list[dict[str, Any]]) -> list[Holding]:
    """Convert raw Kite API holding dicts into Holding Pydantic models."""
    holdings: list[Holding] = []
    for item in raw:
        try:
            symbol = item.get("tradingsymbol", "")
            instrument_type = _detect_instrument_type(
                symbol, item.get("instrument_type", "")
            )
            holding = Holding(
                tradingsymbol=symbol,
                exchange=item.get("exchange", "NSE"),
                isin=item.get("isin", ""),
                quantity=int(item.get("quantity", 0)),
                t1_quantity=int(item.get("t1_quantity", 0)),
                average_price=float(item.get("average_price", 0)),
                last_price=float(item.get("last_price", 0)),
                close_price=float(item.get("close_price", 0)),
                pnl=float(item.get("pnl", 0)),
                day_change=float(item.get("day_change", 0)),
                day_change_percentage=float(item.get("day_change_percentage", 0)),
                instrument_type=instrument_type,
            )
            holdings.append(holding)
        except Exception as exc:
            logger.warning("Could not parse holding %s: %s", item, exc)
    return holdings


def _parse_positions(raw: dict[str, Any]) -> list[Position]:
    """Convert raw Kite API position dicts into Position models."""
    positions: list[Position] = []
    net = raw.get("net", []) if isinstance(raw, dict) else []
    for item in net:
        try:
            positions.append(
                Position(
                    tradingsymbol=item.get("tradingsymbol", ""),
                    exchange=item.get("exchange", "NSE"),
                    product=item.get("product", ""),
                    quantity=int(item.get("quantity", 0)),
                    average_price=float(item.get("average_price", 0)),
                    last_price=float(item.get("last_price", 0)),
                    pnl=float(item.get("pnl", 0)),
                    day_change_percentage=float(item.get("day_change_percentage", 0)),
                )
            )
        except Exception as exc:
            logger.warning("Could not parse position %s: %s", item, exc)
    return positions


# ── LangChain Tools ───────────────────────────────────────────────────────────

@tool
def fetch_portfolio_holdings(_: str = "") -> dict[str, Any]:
    """
    Fetch all long-term portfolio holdings from Zerodha Kite via MCP.

    Returns a dict containing:
      - holdings: list of holding objects (symbol, qty, avg_price, current_price, P&L)
      - total_invested_inr: total capital deployed
      - total_current_value_inr: current market value
      - total_pnl_inr: unrealised P&L
      - total_pnl_pct: unrealised P&L %
    """
    async def _fetch() -> dict[str, Any]:
        async with KiteMCPClient() as client:
            raw = await client.get_holdings()

        holdings = _parse_holdings(raw)

        portfolio = Portfolio(holdings=holdings)

        return {
            "holdings": [h.model_dump() for h in holdings],
            "total_invested_inr": round(portfolio.total_invested, 2),
            "total_current_value_inr": round(portfolio.total_current_value, 2),
            "total_pnl_inr": round(portfolio.total_pnl, 2),
            "total_pnl_pct": round(portfolio.total_pnl_percent, 2),
            "num_holdings": len(holdings),
        }

    return asyncio.run(_fetch())


@tool
def fetch_open_positions(_: str = "") -> dict[str, Any]:
    """
    Fetch current open positions (intraday / short-term) from Zerodha Kite via MCP.

    Returns a dict with a 'positions' list and count.
    """
    async def _fetch() -> dict[str, Any]:
        async with KiteMCPClient() as client:
            raw = await client.get_positions()

        positions = _parse_positions(raw)
        return {
            "positions": [p.model_dump() for p in positions],
            "num_positions": len(positions),
        }

    return asyncio.run(_fetch())


@tool
def fetch_account_profile(_: str = "") -> dict[str, Any]:
    """
    Fetch the Zerodha account profile (user name, email, broker, etc.) via MCP.
    """
    async def _fetch() -> dict[str, Any]:
        async with KiteMCPClient() as client:
            return await client.get_profile()

    return asyncio.run(_fetch())


@tool
def initiate_kite_login(_: str = "") -> str:
    """
    Initiate Zerodha Kite authentication via MCP.

    Returns the browser authorization URL that the user must visit to log in.
    After successful login, portfolio tools will have access to live data.
    """
    async def _login() -> str:
        async with KiteMCPClient() as client:
            url = await client.login()
        return url

    url = asyncio.run(_login())
    return f"Please open this URL in your browser to authenticate with Kite:\n{url}"


# Convenience list of all Zerodha tools for the agent
ZERODHA_TOOLS = [
    initiate_kite_login,
    fetch_account_profile,
    fetch_portfolio_holdings,
    fetch_open_positions,
]
