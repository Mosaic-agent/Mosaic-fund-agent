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
import re

from src.agents.sub_agents.base import _SubAgent
from src.agents.sub_agents.equity_gatherer import _gather_indian_equity_data

logger = logging.getLogger(__name__)

# ── Quick-stat fast path ────────────────────────────────────────────────────
# A bare "ITC dividend yield" / "TCS P/E ratio" doesn't need the full 8-section
# research note (12 parallel tools). If the question is a narrow single-metric
# ask, answer it from one get_yahoo_finance_data call instead of routing
# through the full ReAct agent below.

_HEAVY_KEYWORDS_RE = re.compile(
    r"\b(?:chart|plot|graph|report|research|analysis|analyse|compare|comparison"
    r"|quarterly|earnings|cash\s*flow|shareholding|news|anomaly|anomalies"
    r"|correlation|correlate|financials?|risks?|thesis|recommend|recommendation"
    r"|deep\s*dive|summary|summarize|summarise|momentum|history|historical)\b",
    re.I,
)

# (pattern, get_yahoo_finance_data() dict key, human label). Order matters —
_QUICK_STAT_FIELDS: list[tuple] = [
    (re.compile(
        r"\b(?:large|mid|small|micro)\s*cap\b|"
        r"\bmarket\s*cap(?:itali[sz]ation)?\s*categor\w*\b|"
        r"\bcap\s*(?:category|class|classification)\b|"
        r"\bwhat\s+cap\b|\bwhich\s+cap\b|"
        r"\bis\s+\w+\s+(?:a\s+)?(?:large|mid|small|micro)\s*cap\b",
        re.I,
    ), "cap_category", "Market cap category"),
    (re.compile(r"\bp\W?/?\W?e\s*ratio\b|\bprice.?to.?earnings\b", re.I), "pe_ratio", "P/E ratio"),
    (re.compile(r"\bp\W?/?\W?b\s*ratio\b|\bprice.?to.?book\b", re.I), "pb_ratio", "P/B ratio"),
    # Tolerates common typos ("dividiend", "yeild") — the LLM router already
    # saw the raw text before any spell-correction happens.
    (re.compile(r"\bdivid\w*\s+y[ie]{1,2}ld\w*\b", re.I), "dividend_yield_pct", "Dividend yield"),
    (re.compile(r"\bmarket\s*cap(?:italisation|italization)?\b|\bmcap\b", re.I), "market_cap_formatted", "Market cap"),
    (re.compile(r"\b52.?week\s*high\b|\b52\s*wk\s*high\b", re.I), "52_week_high", "52-week high"),
    (re.compile(r"\b52.?week\s*low\b|\b52\s*wk\s*low\b", re.I), "52_week_low", "52-week low"),
    (re.compile(r"\bsector\b", re.I), "sector", "Sector"),
    (re.compile(r"\bindustry\b", re.I), "industry", "Industry"),
    (re.compile(
        r"\b(?:current|share|stock|cmp|ltp)\s*price\b|\bprice\s+of\b|\bwhat.?s\s+the\s+price\b",
        re.I,
    ), "current_price_inr", "Current price"),
]


def try_quick_stat_answer(question: str) -> str | None:
    """
    Answer narrow single-metric questions directly from one Yahoo Finance
    call, bypassing the full 19-tool research agent below.

    Returns None when the question isn't a narrow single-metric ask, or
    resolution/fetch fails — the caller should fall back to the full agent.
    """
    if _HEAVY_KEYWORDS_RE.search(question):
        return None

    matched = [(field, label) for pat, field, label in _QUICK_STAT_FIELDS if pat.search(question)]
    if not matched:
        return None

    from src.tools.company_resolver import resolve_company_info
    info = resolve_company_info(question)
    symbol = info.get("symbol")
    if not symbol or info.get("error"):
        return None

    from src.tools.yahoo_finance import get_yahoo_finance_data, get_market_cap_category
    exchange = info.get("exchange") or "NSE"

    cap_data = None
    if any(field == "cap_category" for field, _ in matched):
        cap_data = get_market_cap_category.invoke({"input_str": f"{symbol}:{exchange}"})

    data = get_yahoo_finance_data.invoke({"input_str": f"{symbol}:{exchange}"})
    if not data.get("current_price_inr") and not cap_data:
        return None  # fetch failed — let the full agent retry with more tools

    if cap_data:
        data = {**data, **cap_data}

    lines = [f"**{info.get('company_name', symbol)} ({symbol}:{exchange})**"]
    for field, label in matched:
        value = data.get(field)
        if value is None:
            continue
        if field in ("pe_ratio", "pb_ratio"):
            lines.append(f"- {label}: {value:.2f}")
        elif field == "dividend_yield_pct":
            lines.append(f"- {label}: {value:.2f}%")
        elif field in ("current_price_inr", "52_week_high", "52_week_low"):
            lines.append(f"- {label}: ₹{value:,.2f}")
        else:
            lines.append(f"- {label}: {value}")
    logger.info("try_quick_stat_answer: answered %r without invoking full agent", question[:60])
    return "\n".join(lines)


# ── Chart-only fast path ─────────────────────────────────────────────────────
# A bare "NUVOCO price chart 2 month" doesn't need the 14-tool research
# playbook (batch_enrichment fan-out in config/agents/india_equity.yaml) —
# it just needs one plot_price_chart call. Fire only when the question is
# a pure chart/plot request with no other analysis keyword — those need
# the real data the full playbook gathers (financials, anomalies, news).

_CHART_ONLY_RE = re.compile(r"\b(?:chart|plot|graph)\b", re.I)

# anomaly/anomalies and volume are NOT excluded — plot_price_chart already
# overlays anomaly red-dots and a volume panel in its single ASCII output,
# so "volume chart" / "anomaly chart" route to the same fast tool call as a
# plain price chart. macd/rsi get their own dedicated ASCII tool below.
_CHART_EXCLUDE_RE = re.compile(
    r"\b(?:report|research|analysis|analyse|compare|comparison"
    r"|quarterly|earnings|cash\s*flow|shareholding|news"
    r"|correlation|correlate|financials?|risks?|thesis|recommend|recommendation"
    r"|deep\s*dive|summary|summarize|summarise|momentum"
    r"|bollinger|\bema\b|\bsma\b)\b",
    re.I,
)

_CHART_KIND_PATTERNS: list[tuple] = [
    (re.compile(r"\bmacd\b", re.I), "macd"),
    (re.compile(r"\brsi\b", re.I), "rsi"),
]


def _resolve_chart_kinds(question: str) -> list[str]:
    """
    Return every chart kind mentioned in *question*, in price/macd/rsi order.

    Plain "chart"/"volume chart"/"anomaly chart" (no macd/rsi keyword) still
    default to just ["price"] — volume/anomaly are panels already bundled
    inside plot_price_chart. "price" is only added alongside macd/rsi when
    the word "price" is explicitly present (e.g. "price and RSI chart").
    """
    has_macd = any(pat.search(question) for pat, kind in _CHART_KIND_PATTERNS if kind == "macd")
    has_rsi = any(pat.search(question) for pat, kind in _CHART_KIND_PATTERNS if kind == "rsi")
    has_price = bool(re.search(r"\bprice\b", question, re.I)) or not (has_macd or has_rsi)
    return [k for k, present in (("price", has_price), ("macd", has_macd), ("rsi", has_rsi)) if present]

_CHART_DAYS_PATTERNS: list[tuple] = [
    (re.compile(r"\b(\d+)\s*year\w*\b", re.I), 365),
    (re.compile(r"\b(\d+)\s*month\w*\b", re.I), 30),
    (re.compile(r"\b(\d+)\s*week\w*\b", re.I), 7),
    (re.compile(r"\b(\d+)\s*day\w*\b", re.I), 1),
]


def _parse_chart_days(question: str) -> int:
    for pat, mult in _CHART_DAYS_PATTERNS:
        m = pat.search(question)
        if m:
            return int(m.group(1)) * mult
    if re.search(r"\b1\s*yr\b|\byearly\b", question, re.I):
        return 365
    return 60  # plot_price_chart's own default


_CHART_KIND_LABELS = {"price": "Price", "macd": "MACD", "rsi": "RSI"}


def try_chart_only_fast_path(question: str) -> str | None:
    """
    Answer a bare "<symbol> <price|MACD|RSI>[, <kind>...] chart <lookback>"
    question directly with one plot_*_chart call per requested kind,
    bypassing the full research playbook. Volume and anomaly markers ride
    along for free — they're already panels/overlays inside plot_price_chart's
    single ASCII output.

    Returns None when the question isn't a pure chart request, or every
    requested kind failed to resolve/fetch — the caller should fall back to
    the full agent.
    """
    if not _CHART_ONLY_RE.search(question) or _CHART_EXCLUDE_RE.search(question):
        return None

    from src.tools.company_resolver import resolve_company_info
    info = resolve_company_info(question)
    symbol = info.get("symbol")
    if not symbol or info.get("error"):
        return None

    days = _parse_chart_days(question)
    kinds = _resolve_chart_kinds(question)
    from src.tools.chart_tools import plot_price_chart, plot_macd_chart, plot_rsi_chart
    tool_fns = {"price": plot_price_chart, "macd": plot_macd_chart, "rsi": plot_rsi_chart}

    sections: list[str] = []
    for kind in kinds:
        chart = tool_fns[kind].invoke({"symbol": symbol, "days": days})
        lowered = (chart or "").strip().lower()
        if not chart or "no price data found" in lowered or lowered.startswith(("error", "insufficient")):
            logger.debug("try_chart_only_fast_path: %s chart failed for %s, skipping", kind, symbol)
            continue
        sections.append(f"### {_CHART_KIND_LABELS[kind]} Chart\n{chart}")

    if not sections:
        return None  # let the full agent retry (e.g. auto-import missing data)

    company_name = info.get("company_name", symbol)
    logger.info("try_chart_only_fast_path: answered %r (%s) without invoking full agent", question[:60], "+".join(kinds))
    header = f"**{company_name} ({symbol}) — {days}d Chart{'s' if len(sections) > 1 else ''}**"
    return header + "\n\n" + "\n\n".join(sections)


class IndianEquityResearchSubAgent(_SubAgent):
    """
    Comprehensive research for any Indian stock (NSE/BSE).

    Covers: company overview · price momentum · quarterly earnings ·
    MF fund holdings · annual cash flow · recent news · FII/DII flows.

    Can accept a company name ("adani enterprise"), a partial name, or a
    direct NSE symbol — ``resolve_company`` is always called first.

    Uses the StateGraph workflow (src/workflows/india_equity.py) as the primary
    path (~61% token savings vs ReAct). Falls back to the ReAct agent on failure.
    """

    def run(self, question: str, llm_override: Any = None, callbacks: list | None = None) -> str:
        """Try the StateGraph workflow first, fall back to ReAct loop.

        Set MOSAIC_USE_WORKFLOWS=0 to force the ReAct agent for debugging.
        """
        import os
        if os.getenv("MOSAIC_USE_WORKFLOWS", "1") != "0":
            try:
                from src.workflows.india_equity import run as _wf_run
                logger.info("IndianEquityResearchSubAgent: routing → StateGraph workflow")
                return _wf_run(question, callbacks=callbacks)
            except Exception as exc:
                logger.warning(
                    "IndianEquityResearchSubAgent: workflow failed (%s), falling back to ReAct — track this", exc
                )
        return super().run(question, llm_override=llm_override, callbacks=callbacks)

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
        "  • `get_stock_news(company_name)`, `get_newsapi_stock_news(symbol)`, AND `get_yahoo_stock_news(\"SYMBOL:EXCHANGE\")` — news & sentiment (3 independent sources)\n"
        "  • `get_nse_announcements(symbol)` — official NSE disclosures (board outcomes, M&A, credit ratings, management changes) — ALWAYS call this too, it's the ground-truth source for material corporate events\n"
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
        "(1d) Volume & Liquidity Pattern — summarise average daily volume (30d ADV), recent volume vs 20d MA expansion/contraction ratio, volume Z-scores, and institutional block volume probability (`p_institutional` from search_anomaly_events).  "
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
        from src.tools.yahoo_finance import YAHOO_TOOLS, YAHOO_NEWS_TOOLS
        from src.tools.earnings_scraper import get_quarterly_results  # get_shareholding_pattern excluded — plot_shareholding_bar calls it internally
        from src.tools.news_search import get_stock_news
        from src.tools.newsapi_search import get_newsapi_stock_news
        from src.tools.nse_announcements import get_nse_announcements
        from src.tools.skills_tools import query_clickhouse_db, import_symbol_data, import_symbol_announcements
        from src.tools.indian_equity_tools import get_mf_holdings_for_stock, get_stock_cashflow, get_db_price_summary
        from src.tools.chart_tools import plot_price_chart, plot_shareholding_bar, plot_macd_chart
        from src.tools.market.equity import search_anomaly_events
        from src.tools.market.correlation_tools import find_anomaly_correlations
        from src.tools.agent_tools import check_and_refresh_symbol_data
        from src.tools.report_publisher import publish_research_pdf, publish_consolidated_pdf
        return (
            [resolve_company]
            + YAHOO_TOOLS
            + YAHOO_NEWS_TOOLS
            + [get_quarterly_results]
            + [get_stock_news, get_newsapi_stock_news, get_nse_announcements, query_clickhouse_db,
               import_symbol_data, import_symbol_announcements, check_and_refresh_symbol_data,
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
