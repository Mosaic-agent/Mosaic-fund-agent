from __future__ import annotations

from ..base import DossierSection
from ..context import DossierContext
from ..utils import make_markdown_table


class ValuationUniverseSection(DossierSection):
    """Market-cap-ranked universe snapshot from stock_valuation (StockScans-
    sourced — see stockscans_fetcher.py / StockScansFetcher). Renamed from
    the deleted stock_quant_deepdive_cli.py's "Peer Valuation Matrix" — it
    queries every symbol in the table sorted by market cap, not a
    sector/peer-filtered comparison set, so the old name overclaimed what the
    query does."""

    key = "valuation"
    title = "Valuation Universe Snapshot (Market-Cap Ranked, Not Sector-Filtered Peers)"

    def fetch(self, ctx: DossierContext) -> str:
        from src.db.pool import query_df

        df = query_df("""
            SELECT symbol, snapshot_date,
                   market_cap / 10000000 AS market_cap_cr,
                   trailing_pe, forward_pe, price_to_book, price_to_sales,
                   return_on_equity * 100 AS roe_pct,
                   profit_margin * 100 AS net_margin_pct,
                   revenue_growth * 100 AS rev_growth_pct,
                   earnings_growth * 100 AS eps_growth_pct,
                   recommendation
            FROM market_data.stock_valuation FINAL
            ORDER BY market_cap_cr DESC
        """)
        if df.empty:
            return "*No rows in stock_valuation.*"

        df_latest = df.sort_values("snapshot_date", ascending=False).groupby("symbol").first().reset_index()
        df_latest = df_latest.sort_values("market_cap_cr", ascending=False).head(15)
        return make_markdown_table(df_latest)
