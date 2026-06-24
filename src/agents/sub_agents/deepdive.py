"""
src/agents/sub_agents/deepdive.py
─────────────────────────────────
US equity research sub-agent: SEC 10-K/10-Q filings, XBRL financials,
peer valuation via the full deepdive pipeline.
"""
from __future__ import annotations

from src.agents.sub_agents.base import _SubAgent


class DeepDiveSubAgent(_SubAgent):
    """
    US equity research: SEC 10-K/10-Q filings, XBRL financials, peer valuation.

    Uses the full deepdive pipeline tool (run_deepdive_analysis) plus
    ClickHouse readers for previously stored structured data.

    Also carries ``resolve_company`` so the LLM can disambiguate company names
    before fetching SEC data.  If the company resolves to Indian (NSE/BSE) the
    agent will say so and advise using IndianEquityResearchSubAgent instead.
    """

    SYSTEM_PROMPT = (
        "You are a US equity research analyst specialising in SEC filing analysis. "
        "You have access to EDGAR data, XBRL financials, and Yahoo Finance market data. "
        "FIRST: Call `resolve_company` on any input ticker/name to confirm the symbol "
        "and verify the market is 'US'. Ticker symbols can change or be newly listed; "
        "always check if the output contains an 'error' field before proceeding. "
        "If `resolve_company` returns market='India', "
        "immediately reply: \"This stock is listed in India (NSE/BSE). "
        "Please use the Indian equity research path.\".  "
        "For US tickers: use `run_deepdive_analysis` to fetch SEC filings. "
        "If you need to summarize an existing deep-dive report, or if the user asks "
        "follow-up questions about a previously generated deep-dive report, "
        "use `read_deepdive_report` to load the full report content first. "
        "Use `query_clickhouse_db` to read deepdive_* tables in ClickHouse "
        "(always add FINAL). "
        "Use `get_yahoo_finance_data` for live price and valuation multiples. "
        "Present all data as Markdown tables. Never invent numbers."
    )

    def _get_tools(self) -> list:
        from src.tools.yahoo_finance import YAHOO_TOOLS
        from src.tools.earnings_scraper import EARNINGS_TOOLS
        from src.tools.skills_tools import run_deepdive_analysis, query_clickhouse_db, read_deepdive_report
        from src.tools.company_resolver import resolve_company
        from src.tools.report_publisher import publish_research_pdf, publish_consolidated_pdf
        return [resolve_company] + YAHOO_TOOLS + EARNINGS_TOOLS + [
            run_deepdive_analysis,
            query_clickhouse_db,
            read_deepdive_report,
            publish_research_pdf,
            publish_consolidated_pdf,
        ]
