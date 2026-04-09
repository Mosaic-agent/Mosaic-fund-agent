"""
scripts/backfill_inav_snapshots.py
───────────────────────────────────
Backfills the `inav_snapshots` table using historical `daily_prices` (market price)
and `mf_nav` (NAV) data from ClickHouse.

This is useful for newly added symbols to provide an immediate Z-score
for the Scarcity Premium Alert scan.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
import clickhouse_connect
from config.settings import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# International ETFs to backfill
SYMBOLS = ["MAFANG", "HNGSNGBEES", "MAHKTECH", "MON100", "MASPTOP50"]

def backfill():
    client = clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
    )

    for sym in SYMBOLS:
        logger.info(f"Backfilling {sym}...")
        
        # Join daily_prices and mf_nav on trade_date = nav_date
        query = f"""
            SELECT
                p.trade_date,
                p.close AS market_price,
                n.nav AS inav
            FROM (
                SELECT trade_date, argMax(close, imported_at) AS close
                FROM market_data.daily_prices
                WHERE symbol = '{sym}'
                GROUP BY trade_date
            ) AS p
            INNER JOIN (
                SELECT nav_date, argMax(nav, imported_at) AS nav
                FROM market_data.mf_nav
                WHERE symbol = '{sym}'
                GROUP BY nav_date
            ) AS n
            ON p.trade_date = n.nav_date
            ORDER BY p.trade_date ASC
        """
        
        rows = client.query(query).result_rows
        if not rows:
            logger.warning(f"No matching price and NAV data found for {sym}")
            continue

        insert_data = []
        for trade_date, market_price, inav in rows:
            # Create a UTC datetime for 10:00 AM (roughly market hours)
            snapshot_at = datetime.combine(trade_date, datetime.min.time()).replace(hour=10)
            prem_disc = ((market_price - inav) / inav * 100) if inav else 0.0
            
            insert_data.append([
                sym,
                snapshot_at,
                inav,
                market_price,
                round(prem_disc, 4),
                "Historical_Backfill"
            ])

        if insert_data:
            client.insert(
                "market_data.inav_snapshots",
                insert_data,
                column_names=["symbol", "snapshot_at", "inav", "market_price",
                              "premium_discount_pct", "source"],
                settings={"max_partitions_per_insert_block": 1000}
            )
            logger.info(f"✓ Inserted {len(insert_data)} historical snapshots for {sym}")

    client.close()

if __name__ == "__main__":
    backfill()
