from __future__ import annotations

from ..base import DossierSection
from ..context import DossierContext


class LiveOrderBookSection(DossierSection):
    """Live Shoonya order-book microstructure — market-hours-only, so it
    must not run by default (unlike every other section, which reads
    historical/disclosed data)."""

    key = "obi"
    title = "Live Order Book Microstructure (Shoonya OBI)"

    def enabled(self, ctx: DossierContext) -> bool:
        return ctx.live

    def fetch(self, ctx: DossierContext) -> str:
        from src.data_importer.tool_fetchers.shoonya_tools import fetch_and_calculate_obi, format_order_book_ascii

        obi_data = fetch_and_calculate_obi(ctx.symbol)
        if not obi_data:
            return "*No live order book data (market closed or symbol unavailable).*"
        return "```text\n" + format_order_book_ascii(obi_data) + "\n```"
