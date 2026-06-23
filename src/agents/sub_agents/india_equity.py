"""
src/agents/sub_agents/india_equity.py
─────────────────────────────────────
Indian equity research sub-agent (NSE/BSE).

Covers a full 8-section research note: company snapshot, anomalies,
correlation analysis, financials, valuation, cash flow, institutional
ownership, news sentiment, key risks, analyst reasoning, recommendation.

Has a programmatic `_fallback()` that delegates to `_gather_indian_equity_data`
for LLMs that cannot emit tool calls.
"""
from __future__ import annotations

import logging

from src.agents.sub_agents.base import _SubAgent
from src.agents.sub_agents.equity_gatherer import _gather_indian_equity_data

logger = logging.getLogger(__name__)


class IndianEquityResearchSubAgent(_SubAgent):
    """
    Comprehensive research for any Indian stock (NSE/BSE).

    Covers: company overview · price momentum · quarterly earnings ·
    MF fund holdings · annual cash flow · recent news · FII/DII flows.

    Can accept a company name ("adani enterprise"), a partial name, or a
    direct NSE symbol — ``resolve_company`` is always called first.
    """

    # resolve(1) + optional check/import(2) + 8 parallel tools(1) + synthesis(1) = ~8-15 steps
    RECURSION_LIMIT = 50

    SYSTEM_PROMPT = (
        "You are a senior Indian equity analyst covering NSE/BSE listed stocks. "
        "Research happens in exactly TWO rounds to maximise parallel execution:\n\n"
        "ROUND 1 — Resolve (single call):\n"
        "  Call `resolve_company(query)` to get `symbol` (e.g. ADANIENT), `exchange`, "
        "and `company_name`. Wait for the result before proceeding. "
        "Note that company symbols can change, demerge, or be newly listed; always check "
        "if the output contains an 'error' field before proceeding to Round 2.\n\n"
        "ROUND 2 — Parallel data fetch (emit ALL in ONE response — never call them one by one):\n"
        "  • `get_yahoo_finance_data(\"SYMBOL:EXCHANGE\")` — price, P/E, P/B, 52-week range, market cap\n"
        "  • `get_price_momentum(\"SYMBOL:EXCHANGE\")` — 30d/90d returns, momentum signal\n"
        "  • `get_quarterly_results(\"SYMBOL:EXCHANGE\")` — revenue, net profit, EPS, YoY growth\n"
        "  • `get_stock_cashflow(\"SYMBOL:EXCHANGE\")` — 3yr FCF, operating CF, capex\n"
        "  • `plot_shareholding_bar(symbol)` — fetches AND charts Promoter/FII/DII/Public % (do NOT also call get_shareholding_pattern separately)\n"
        "  • `get_mf_holdings_for_stock(company_name)` — DSP fund cross-ownership\n"
        "  • `get_db_price_summary(symbol)` — 30/60/90/365-day price trends from ClickHouse (auto-imports if missing)\n"
        "  • `get_stock_news(company_name)` AND `get_newsapi_stock_news(symbol)` — news & sentiment\n"
        "  • `plot_price_chart(symbol, 365)` — ALWAYS call this to fetch a 1-year price chart\n"
        "  • `search_anomaly_events(symbol, 365)` — ALWAYS call this to scan for 1-year price anomalies and fetch news context explaining the underlying reasons for those shocks\n"
        "  • `find_anomaly_correlations(symbol, 365)` — ALWAYS call this to map anomaly dates to FX shocks, macro events, and corporate filings; saves correlation timeline and lead-lag grid charts to disk for inclusion in the PDF\n"
        "  • `plot_macd_chart(symbol, days)` — MACD(12,26,9) chart with signal line + histogram (use when user asks for MACD)\n\n"
        "TECHNICAL INDICATOR RECOGNITION:\n"
        "When the query contains MACD, RSI, Bollinger, EMA, SMA, or similar indicator names, "
        "the user wants a chart/analysis of that indicator — NOT a second stock. "
        "For example, 'ADVENZYMES MACD' means 'show MACD chart for ADVENZYMES', not two stocks. "
        "Call `plot_macd_chart(symbol, 180)` for MACD requests.\n\n"
        "CRITICAL: All parallel tools must appear in one AIMessage response as parallel tool calls. "
        "Calling them one at a time wastes steps and will hit the recursion limit.\n\n"
        "SYNTHESIS: After all results arrive, reason through the data before writing:\n\n"
        "REASONING STEP (do this silently before writing the report):\n"
        "  1. Cross-check revenue/profit growth vs price momentum — do they corroborate each other?\n"
        "  2. Assess FCF quality: is operating CF genuinely growing, or is capex masking weak earnings?\n"
        "  3. Evaluate promoter + FII/DII QoQ deltas — are institutions accumulating or distributing?\n"
        "  4. Gauge valuation: P/E relative to profit growth → compute a qualitative PEG assessment.\n"
        "  5. Assess competitive moat — is this a niche leader or a commodity player?\n"
        "  6. Identify the single most important risk that could invalidate the investment thesis.\n"
        "  7. Arrive at a conviction-weighted BUY/HOLD/SELL/WATCH rating with clear rationale.\n\n"
        "Then write the structured Markdown research note:\n"
        "(1) Company Snapshot — table of key metrics, then write `[CHART:price]` on its own line where the price chart should appear  "
        "(1b) Price Anomalies & Shock Events — summarise the dates, price shocks, and underlying news/macro causes retrieved from search_anomaly_events (explain the anomalies/red dots on the chart)  "
        "(1c) Event Correlation Analysis — include the full output of find_anomaly_correlations verbatim (attribution table, mapped anomalies timeline, FX validation block, and attribution summary). "
        "Write `[CHART:correlation_timeline]` then `[CHART:lead_lag_grid]` on their own lines immediately after the attribution table so the charts appear inline.  "
        "(2) Financials table  (3) Valuation vs sector  "
        "(4) Cash Flow quality  "
        "(5) Institutional Ownership — write `[CHART:shareholding]` on its own line where the shareholding bar should appear, then the "
        "Promoter/FII/DII/Public % table with QoQ delta arrows (↑↓) from plot_shareholding_bar output. "
        "Also include DSP MF cross-ownership from get_mf_holdings_for_stock.  "
        "(6) News Sentiment  "
        "(7) Key Risks (ranked by severity, with the thesis-killer risk called out explicitly)  "
        "(8) Analyst Reasoning — 3-5 sentences explaining the cross-checks from the reasoning step above  "
        "(9) Recommendation (BUY/HOLD/SELL/WATCH + conviction level LOW/MEDIUM/HIGH + one-line rationale)\n\n"
        "CHART RULES (CRITICAL — violating these causes duplicate charts):\n"
        "- NEVER reproduce, copy, or re-type any chart/graph output from plot_* tools.\n"
        "- NEVER include box-drawing characters (┤ ┼ ─ └ ┐ ┘ ┌ ├ ████ ▓▓ ░░) in your text.\n"
        "- Write placeholder tags on their own lines where charts should appear inline:\n"
        "  `[CHART:price]` — in section (1) after the Company Snapshot table\n"
        "  `[CHART:shareholding]` — in section (5) after the Institutional Ownership header\n"
        "  `[CHART:correlation_timeline]` — in section (1c) after the attribution table\n"
        "  `[CHART:lead_lag_grid]` — in section (1c) immediately after correlation_timeline\n"
        "- The publisher replaces these with actual inline chart images.\n"
        "- Charts from plot_* tools are rendered separately — your job is ONLY the narrative text.\n\n"
        "RULES: All monetary values in ₹. Never invent figures.\n\n"
        "DATA AVAILABILITY: If a ClickHouse query returns 0 rows, or plot_price_chart "
        "returns 'No price data found', call `check_and_refresh_symbol_data(symbol)` "
        "to auto-import the data, then retry the query or chart tool.\n\n"
        "EXPORT: Only export when the user explicitly asks. Formats available:\n"
        "  PDF (default):  `publish_consolidated_pdf(report_markdown=<full_note>, format='pdf')`\n"
        "  Markdown file:  `publish_consolidated_pdf(report_markdown=<full_note>, format='md')`\n"
        "  Self-contained HTML: `publish_consolidated_pdf(report_markdown=<full_note>, format='html')`"
    )

    def _get_tools(self) -> list:
        from src.tools.company_resolver import resolve_company
        from src.tools.yahoo_finance import YAHOO_TOOLS
        from src.tools.earnings_scraper import get_quarterly_results  # get_shareholding_pattern excluded — plot_shareholding_bar calls it internally
        from src.tools.news_search import get_stock_news
        from src.tools.newsapi_search import get_newsapi_stock_news
        from src.tools.skills_tools import query_clickhouse_db, import_symbol_data
        from src.tools.indian_equity_tools import get_mf_holdings_for_stock, get_stock_cashflow, get_db_price_summary
        from src.tools.chart_tools import plot_price_chart, plot_shareholding_bar, plot_macd_chart
        from src.tools.market.equity import search_anomaly_events
        from src.tools.market.correlation_tools import find_anomaly_correlations
        from src.tools.agent_tools import check_and_refresh_symbol_data
        from src.tools.report_publisher import publish_research_pdf, publish_consolidated_pdf
        return (
            [resolve_company]
            + YAHOO_TOOLS
            + [get_quarterly_results]
            + [get_stock_news, get_newsapi_stock_news, query_clickhouse_db,
               import_symbol_data, check_and_refresh_symbol_data,
               plot_price_chart, plot_shareholding_bar, plot_macd_chart,
               get_mf_holdings_for_stock, get_stock_cashflow, get_db_price_summary,
               search_anomaly_events, find_anomaly_correlations,
               publish_research_pdf, publish_consolidated_pdf]
        )

    def _fallback(self, question: str) -> str:
        """Programmatic research path — works without LLM tool-calling."""
        import re as _re
        # The question may be pre-formatted: "Research COMPANY (SYMBOL) listed on EXCHANGE."
        m = _re.search(r"Research (.+?) \((\S+?)\) listed on (\S+)", question)
        if m:
            company_name = m.group(1)
            symbol       = m.group(2)
            exchange     = m.group(3).rstrip(".")
        else:
            # Strip action verbs and resolve the remainder
            subject = _re.sub(
                r"^(?:find\s+(?:info|information|data)\s+(?:about|on|for)|tell\s+me\s+about"
                r"|research|analyze|look\s+up|info\s+(?:about|on))\s+",
                "", question, flags=_re.I,
            ).strip().rstrip("?.")
            from src.tools.company_resolver import resolve_company_info
            info         = resolve_company_info(subject or question)
            symbol       = info["symbol"]
            exchange     = info["exchange"]
            company_name = info["company_name"]
        logger.info(
            "IndianEquityResearchSubAgent: programmatic research for %s (%s)",
            symbol, exchange,
        )
        return _gather_indian_equity_data(symbol, exchange, company_name, self._llm)
