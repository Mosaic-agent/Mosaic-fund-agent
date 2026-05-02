"""
scripts/save_portfolio_holdings.py
──────────────────────────────────
Fetches current Zerodha holdings via Kite MCP and stores them in ClickHouse.
"""

import asyncio
import logging
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.clients.mcp_client import KiteMCPClient
from src.importer.clickhouse import ClickHouseImporter
from src.tools.zerodha_mcp_tools import _parse_holdings
from config.settings import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def main():
    logger.info("Starting portfolio holdings backup to ClickHouse...")
    
    # 1. Fetch holdings from Kite
    try:
        async with KiteMCPClient() as client:
            raw = await client.get_holdings()
            holdings = _parse_holdings(raw)
    except Exception as exc:
        logger.error("Failed to fetch holdings from Kite: %s", exc)
        return

    if not holdings:
        logger.warning("No holdings found to store.")
        return

    logger.info("Found %d holdings.", len(holdings))

    # 2. Convert to dicts for ClickHouse
    rows = [h.model_dump() for h in holdings]
    # Remove any extra keys not in schema if necessary, 
    # but model_dump() should match mostly.
    
    # 3. Store in ClickHouse
    try:
        importer = ClickHouseImporter(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            username=settings.clickhouse_user,
            password=settings.clickhouse_password,
        )
        importer.ensure_schema()
        count = importer.insert_user_holdings(rows)
        logger.info("Successfully stored %d holdings in market_data.user_holdings.", count)
    except Exception as exc:
        logger.error("Failed to store holdings in ClickHouse: %s", exc)

if __name__ == "__main__":
    asyncio.run(main())
