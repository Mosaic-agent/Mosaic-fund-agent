from __future__ import annotations

from ..base import DossierSection
from ..context import DossierContext


class InsiderTradesSection(DossierSection):
    """StockScans-sourced insider transactions (market_data.stock_insider_trades —
    promoters, immediate relatives, and other designated insiders). Imported via
    the stockscans_insider fetcher but never surfaced anywhere in the codebase
    until now — the import side existed, the read side didn't."""

    key = "insider"
    title = "Insider Trading Activity (StockScans)"

    def fetch(self, ctx: DossierContext) -> str:
        from src.db.pool import get_pool

        df = get_pool().query_df(f"""
            SELECT transaction_date, insider_name, relation, transaction_type, shares, value
            FROM market_data.stock_insider_trades FINAL
            WHERE symbol = '{ctx.symbol}'
            ORDER BY transaction_date DESC
            LIMIT 20
        """)

        if df.empty:
            return "*No insider transactions found for this symbol in StockScans data.*"

        df["transaction_type"] = df["transaction_type"].replace("", "Unspecified").replace("-", "Unspecified")
        df["value_cr"] = (df["value"] / 1e7).round(2)

        buys = df[df["transaction_type"] == "Buy"]["value_cr"].sum()
        sells = df[df["transaction_type"] == "Sell"]["value_cr"].sum()
        lines = [f"**Net (last {len(df)} disclosed transactions):** Buys ₹{buys:,.2f} Cr | Sells ₹{sells:,.2f} Cr\n"]

        lines.append("```text")
        table = df[["transaction_date", "insider_name", "relation", "transaction_type", "shares", "value_cr"]]
        table = table.rename(columns={"value_cr": "value_cr_₹"})
        lines.append(table.to_string(index=False))
        lines.append("```")
        return "\n".join(lines)
