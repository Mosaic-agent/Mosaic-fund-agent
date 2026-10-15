from __future__ import annotations

from ..base import DossierSection
from ..context import DossierContext


class AnomalySection(DossierSection):
    """Delegates to the shared search_anomaly_events tool (GARCH + Isolation
    Forest + PELT composite — the same pipeline that places red dots on the
    chart), not a bespoke z-score reimplementation. The deleted
    run_stock_quant_workflow.py's inline `z_volume>2 OR z_robust>2` check gave
    a different, simplified answer for the same stock than this tool does."""

    key = "anomalies"
    title = "Price Anomalies & News Correlation"

    def fetch(self, ctx: DossierContext) -> str:
        from src.tools.market.equity import search_anomaly_events

        return str(search_anomaly_events.invoke({"symbol": ctx.symbol, "days": ctx.days}))
