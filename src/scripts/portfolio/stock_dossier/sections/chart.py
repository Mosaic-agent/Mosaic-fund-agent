from __future__ import annotations

from ..base import DossierSection
from ..context import DossierContext


class PriceChartSection(DossierSection):
    """Delegates to the shared plot_price_chart tool, which already renders
    🔴/🏦 anomaly and corporate-action markers centrally — not a bespoke
    plotext implementation."""

    key = "chart"
    title = "Price & Volume Trend (Terminal ASCII Visual)"

    def fetch(self, ctx: DossierContext) -> str:
        from src.tools.chart_tools import plot_price_chart

        chart = plot_price_chart.invoke({"symbol": ctx.symbol, "days": ctx.days})
        return "```text\n" + str(chart) + "\n```"
