"""News sub-agent: financial news aggregation and sentiment."""
from __future__ import annotations

import logging

from .base import _SubAgent

logger = logging.getLogger(__name__)

class NewsSubAgent(_SubAgent):
    """
    Financial news aggregation and sentiment agent.

    Sources
    -------
    • Google News (GNews)   — free, no quota, good for Indian market news
    • NewsAPI.org           — richer metadata, 100 req/day free tier
    • ClickHouse            — saved ETF news from previous `etf-news` runs
    • ETF news sentinel     — run_etf_news_sentiment for category-level news

    Workflow
    --------
    1. Company query → resolve symbol → get_stock_news + get_newsapi_stock_news (parallel)
    2. General query → search_financial_news (free-text GNews)
    3. Historical/saved → get_db_news (ClickHouse news_articles table)
    4. ETF category → run_etf_news_sentiment
    Synthesise: Markdown table + 2-3 sentence sentiment summary.

    Uses the StateGraph workflow (src/workflows/news.py) as the primary path
    (~81% token savings vs ReAct). Falls back to the ReAct agent on failure.
    """

    def run(self, question: str, llm_override: Any = None, callbacks: list | None = None) -> str:
        """Try the StateGraph workflow first, fall back to ReAct loop.

        Set MOSAIC_USE_WORKFLOWS=0 to force the ReAct agent for debugging.
        """
        import os
        if os.getenv("MOSAIC_USE_WORKFLOWS", "1") != "0":
            try:
                from src.workflows.news import run as _wf_run
                logger.info("NewsSubAgent: routing → StateGraph workflow")
                return _wf_run(question, callbacks=callbacks)
            except Exception as exc:
                logger.warning(
                    "NewsSubAgent: workflow failed (%s), falling back to ReAct — track this", exc
                )
        return super().run(question, llm_override=llm_override, callbacks=callbacks)

    SYSTEM_PROMPT = (
        "You are the Mosaic News Agent — an Indian financial news aggregator.\n\n"
        "## Workflow\n"
        "**Company/ETF news** (e.g. 'news on HDFC', 'news for gold bees'):\n"
        "  1. Call `resolve_company` to get the NSE symbol.\n"
        "  2. Call `get_stock_news` AND `get_newsapi_stock_news` in parallel using \"SYMBOL|Company Name\".\n"
        "  3. Merge results, deduplicate by title, sort by date.\n\n"
        "**Broad queries** ('market news today', 'etf news', 'earnings news'):\n"
        "  1. Call `search_financial_news(query)` with a focused search string.\n\n"
        "**Saved ETF news** ('saved news', 'news sentiment for gold'):\n"
        "  1. Call `get_db_news(category='gold', sentiment='')` to query ClickHouse.\n\n"
        "**ETF category scan** ('latest etf news', 'etf news sentiment'):\n"
        "  1. Call `run_etf_news_sentiment` for a full multi-category scan.\n\n"
        "**Price anomaly explanation** ('explain anomalies for GOLDBEES', 'why did the price spike/drop'):\n"
        "  1. ETFs/gold: Call `explain_price_anomalies(symbol)` + `plot_price_chart(symbol)` in parallel.\n"
        "  2. Stocks: Call `search_anomaly_events(symbol)` + `plot_price_chart(symbol)` in parallel.\n\n"
        "**PDF export** (only when user says 'save as PDF', 'publish report', 'export PDF'):\n"
        "  1. Call `publish_consolidated_pdf(report_markdown=<full_output>)`. "
        "Auto-detects symbols and charts. Report the saved file path.\n\n"
        "## Output format\n"
        "Always present results as a Markdown table:\n"
        "| Title | Source | Date | Sentiment |\n\n"
        "After the table, write 2-3 sentences summarising:\n"
        "- Dominant sentiment (bullish / bearish / mixed)\n"
        "- Key themes or events driving the news\n"
        "- Any actionable observation (e.g. 'FII selling pressure visible in 3 of 5 articles')\n\n"
        "## Rules\n"
        "- Never invent headlines — only report what the tools return.\n"
        "- If both GNews and NewsAPI return results, merge and deduplicate by title similarity.\n"
        "- Truncate long titles to 80 characters in the table."
    )

    def _get_tools(self) -> list:
        from src.tools.company_resolver import resolve_company
        from src.tools.news_search import get_stock_news, search_financial_news, get_db_news
        from src.tools.newsapi_search import get_newsapi_stock_news
        from src.tools.skills_tools import run_etf_news_sentiment, explain_price_anomalies
        from src.tools.chart_tools import plot_price_chart
        from src.tools.market.equity import search_anomaly_events
        from src.tools.market.correlation_tools import find_anomaly_correlations
        from src.tools.report_publisher import publish_research_pdf, publish_consolidated_pdf
        return [
            resolve_company,
            get_stock_news,
            get_newsapi_stock_news,
            search_financial_news,
            get_db_news,
            run_etf_news_sentiment,
            explain_price_anomalies,
            search_anomaly_events,
            find_anomaly_correlations,
            plot_price_chart,
            publish_research_pdf,
            publish_consolidated_pdf,
        ]
