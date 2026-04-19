"""
scripts/backup_zerodha_account.py
──────────────────────────────────
Fetches Zerodha profile, margins, positions, and orders via Kite MCP tools
and stores them in ClickHouse for historical tracking.
"""

import asyncio
import logging
import sys
import os
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.importer.clickhouse import ClickHouseImporter
from config.settings import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# To call MCP tools without the client context issues seen in direct script runs,
# we would normally use the KiteMCPClient. However, the MCP server on this 
# environment seems to prefer direct tool calls through the system's MCP mechanism.
# Since this script runs as a standalone process, we'll try the KiteMCPClient again
# but if it fails, we'll explain the manual steps.

async def run_backup():
    from src.clients.mcp_client import KiteMCPClient
    
    logger.info("Connecting to Zerodha Kite MCP...")
    async with KiteMCPClient() as client:
        try:
            # 1. Profile
            logger.info("Fetching profile...")
            profile = await client.get_profile()
            
            # 2. Margins
            logger.info("Fetching margins...")
            margins_raw = await client.get_margins()
            
            # 3. Positions
            logger.info("Fetching positions...")
            positions_raw = await client.get_positions()
            
            # 4. Orders
            logger.info("Fetching orders...")
            orders_raw = await client.get_orders()
            
        except Exception as exc:
            logger.error("Failed to fetch data from Kite: %s", exc)
            return

    # 5. Process Data for ClickHouse
    
    # Margins processing
    margins_rows = []
    for segment in ['equity', 'commodity']:
        seg_data = margins_raw.get(segment, {})
        if seg_data.get('enabled'):
            margins_rows.append({
                "segment": segment,
                "cash": seg_data.get('available', {}).get('cash', 0.0),
                "available_balance": seg_data.get('net', 0.0),
                "utilised_debits": seg_data.get('utilised', {}).get('debits', 0.0),
                "utilised_m2m": seg_data.get('utilised', {}).get('m2m_unrealised', 0.0),
                "utilised_holding_sales": seg_data.get('utilised', {}).get('holding_sales', 0.0),
            })

    # Positions processing
    positions_rows = []
    # Kite positions is usually a list under 'net' or a direct list
    net_positions = positions_raw.get('net', []) if isinstance(positions_raw, dict) else (positions_raw if isinstance(positions_raw, list) else [])
    for p in net_positions:
        positions_rows.append({
            "tradingsymbol": p["tradingsymbol"],
            "exchange": p["exchange"],
            "instrument_token": p["instrument_token"],
            "product": p["product"],
            "quantity": p["quantity"],
            "average_price": p["average_price"],
            "last_price": p["last_price"],
            "pnl": p["pnl"]
        })

    # Orders processing
    orders_rows = []
    # Kite orders is usually a list
    all_orders = orders_raw if isinstance(orders_raw, list) else []
    for o in all_orders:
        orders_rows.append({
            "order_id": o["order_id"],
            "parent_order_id": o.get("parent_order_id", ""),
            "status": o["status"],
            "tradingsymbol": o["tradingsymbol"],
            "exchange": o["exchange"],
            "transaction_type": o["transaction_type"],
            "order_type": o["order_type"],
            "quantity": o["quantity"],
            "filled_quantity": o["filled_quantity"],
            "pending_quantity": o["pending_quantity"],
            "price": o["price"],
            "average_price": o["average_price"],
            "order_timestamp": o["order_timestamp"]
        })

    # 6. Save to ClickHouse
    try:
        importer = ClickHouseImporter(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            username=settings.clickhouse_user,
            password=settings.clickhouse_password,
        )
        importer.ensure_schema()
        
        importer.insert_user_profile(profile)
        logger.info("Saved user profile.")
        
        importer.insert_user_margins(margins_rows)
        logger.info("Saved %d margin segments.", len(margins_rows))
        
        importer.insert_user_positions(positions_rows)
        logger.info("Saved %d active positions.", len(positions_rows))
        
        importer.insert_user_orders(orders_rows)
        logger.info("Saved %d orders.", len(orders_rows))
        
    except Exception as exc:
        logger.error("Failed to store data in ClickHouse: %s", exc)

if __name__ == "__main__":
    asyncio.run(run_backup())
