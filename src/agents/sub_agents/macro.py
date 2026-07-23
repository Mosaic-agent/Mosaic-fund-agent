"""Macro sub-agent: COMEX commodities, FII/DII flows, macro theme scanner."""
from __future__ import annotations

import logging

from .base import _SubAgent

logger = logging.getLogger(__name__)

class MacroSubAgent(_SubAgent):
    """
    Macro analysis: COMEX commodity signals, FII/DII flows, macro theme scanner.

    Uses the StateGraph workflow (src/workflows/macro.py) as the primary path
    (~71% token savings vs ReAct). Falls back to the ReAct agent on failure.
    """

    def run(self, question: str, llm_override: Any = None, callbacks: list | None = None) -> str:
        """Try the StateGraph workflow first, fall back to ReAct loop.

        Set MOSAIC_USE_WORKFLOWS=0 to force the ReAct agent for debugging.
        """
        import os
        if os.getenv("MOSAIC_USE_WORKFLOWS", "1") != "0":
            try:
                from src.workflows.macro import run as _wf_run
                logger.info("MacroSubAgent: routing → StateGraph workflow")
                return _wf_run(question, callbacks=callbacks)
            except Exception as exc:
                logger.warning(
                    "MacroSubAgent: workflow failed (%s), falling back to ReAct — track this", exc
                )
        return super().run(question, llm_override=llm_override, callbacks=callbacks)

    SYSTEM_PROMPT = (
        "You are a macro analyst covering Indian and global commodity markets. "
        "You handle both quantitative macro signals AND news on geopolitical topics.\n\n"
        "## Macro signals\n"
        "Use `run_macro_scanner` to scan live macro/geopolitical events and map "
        "their directional impact to ETFs. "
        "Use `run_comex_analysis` for COMEX gold/silver/copper pre-market price signals. "
        "Use `run_whale_tracker` to track weight shifts and institutional moves in core macro themes (Gold, Silver, Nuclear, Energy, Infra) across multi-asset funds. "
        "Use `get_dxy_context` to get the current US Dollar Index (DXY) level, 5-day and "
        "20-day change, trend direction, and macro interpretation for gold and INR. "
        "Call `get_dxy_context` whenever the user asks about the dollar, DXY, USD strength, "
        "or its impact on gold / USDINR. "
        "Use `query_clickhouse_db` to read `market_data.fii_dii_flows FINAL`, "
        "`market_data.cot_gold FINAL`, and `market_data.indian_macro_indicators FINAL` for institutional and macroeconomic trend data. "
        "Net article flow index interpretation: ≥+16 = strong bullish | +8 to +15 = moderate bullish "
        "| ≤−16 = strong bearish.\n\n"
        "## Index stats (valuation & breadth)\n"
        "Use `run_market_indicators` to fetch the index valuation (weighted P/E, P/B), market breadth (% of stocks above 50/200 DMA, Advances/Declines), and macro stress indicators (rupee stress DXY deviation, gold ETF SPDR GLD tonnes flow, sector rotation rank). "
        "When asked about general market health, daily overview, or index valuations, ALWAYS run `run_market_indicators` and integrate this quantitative context with `run_macro_scanner` output.\n\n"
        "## Geopolitical / country news\n"
        "When the query is about a country or geopolitical event (Iran, Russia, crude oil, "
        "sanctions, war, etc.), call `search_financial_news` with a focused query such as "
        "'Iran oil sanctions Indian market impact' to fetch live news articles. "
        "Then call `run_macro_scanner` to get the ETF net article flows. "
        "Present both: news table first, then ETF net article flows.\n\n"
        "## Charts\n"
        "If the user asks for a chart, visualisation, or trend:\n"
        "- FII/DII flow trend → `plot_fii_dii_chart(days)`\n"
        "- DXY trend chart → `plot_dxy_chart(days)` (default 365 for 1 year)\n"
        "- Gold/silver/commodity price trend → `plot_price_chart(symbol, days)`\n"
        "- Multi-asset fund holdings, allocations, or institutional shifts → `run_whale_tracker` (automatically appends ASCII trend charts)\n"
        "Always call the appropriate chart tool to render the visual when requested.\n\n"
        "CRITICAL: Only cite prices and flows from live tool output — never from "
        "training-time knowledge. Gold, FII, USDINR change daily."
    )

    def _get_tools(self) -> list:
        from src.tools.skills_tools import (
            run_macro_scanner,
            run_comex_analysis,
            query_clickhouse_db,
            run_whale_tracker,
            run_market_indicators,
        )
        from src.tools.market_context import get_dxy_context
        from src.tools.news_search import search_financial_news, get_db_news
        from src.tools.market.correlation_tools import find_anomaly_correlations
        from src.tools.chart_tools import plot_fii_dii_chart, plot_price_chart, plot_dxy_chart
        from src.tools.report_publisher import publish_research_pdf, publish_consolidated_pdf
        return [
            run_macro_scanner,
            run_comex_analysis,
            query_clickhouse_db,
            run_whale_tracker,
            run_market_indicators,
            get_dxy_context,
            search_financial_news,
            get_db_news,
            find_anomaly_correlations,
            plot_fii_dii_chart,
            plot_price_chart,
            plot_dxy_chart,
            publish_research_pdf,
            publish_consolidated_pdf,
        ]
