from __future__ import annotations

import pandas as pd

from ..base import DossierSection
from ..context import DossierContext


class BulkBlockDealsSection(DossierSection):
    key = "deals"
    title = "Bulk & Block Deals"

    def fetch(self, ctx: DossierContext) -> str:
        from src.db.pool import get_pool

        client = get_pool().get_client()
        rows = client.query(f"""
            SELECT deal_date, client_name, buy_sell, quantity, trade_price, value_cr
            FROM market_data.bulk_block_deals FINAL
            WHERE symbol = '{ctx.symbol}'
            ORDER BY value_cr DESC
            LIMIT 10
        """).result_rows

        if not rows:
            return "*No recent exchange bulk/block deal filings found for this ticker.*"

        deals_df = pd.DataFrame(rows, columns=["Date", "Client Name", "Action", "Shares", "Price (₹)", "Value (₹ Cr)"])
        return "```text\n" + deals_df.to_string(index=False) + "\n```"
