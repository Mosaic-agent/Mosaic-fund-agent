"""
src/agents/sub_agents.py
────────────────────────
Specialised LangChain sub-agents for focused task domains.

Each sub-agent wraps a ``create_react_agent`` with a curated tool subset and
a domain-specific system prompt.  The main MosaicFundAgent auto-routes to the
appropriate sub-agent based on keyword intent detection.

Sub-agents
----------
DeepDiveSubAgent  — US stock SEC filings, XBRL financials, peer valuation
SignalSubAgent    — ETF signals, GOLDBEES ML pipeline, risk governor
MacroSubAgent     — COMEX, FII/DII flows, macro theme scanner

Usage (internal)
----------------
    from src.agents.sub_agents import route_intent, get_subagent

    intent = route_intent(question)          # 'deepdive' | 'signal' | 'macro' | 'main'
    if intent != 'main':
        answer = get_subagent(intent).run(question)
"""
from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

# ── Intent routing ─────────────────────────────────────────────────────────────

_DEEPDIVE_RE = re.compile(
    r"deep.?dive|10.?k filing|10.?q filing|sec filing|edgar|xbrl|annual report"
    r"|adsk|autodesk|aapl|apple|msft|microsoft|nvda|nvidia|amzn|amazon|googl|meta|tsla",
    re.I,
)
_SIGNAL_RE = re.compile(
    r"\bsignal|goldbees|kelly weight|composite score|inav premium|inav discount"
    r"|etf recommendation|buy signal|sell signal|risk governor|blended weight\b",
    re.I,
)
_MACRO_RE = re.compile(
    r"\bcomex|macro theme|macro scan|fii flow|dii flow|institutional flow"
    r"|gold price|silver price|copper price|crude oil|fed rate|rbi rate"
    r"|usd.?inr|cot report|geopolit|war risk|tariff|trade war\b",
    re.I,
)
# Broad research-intent phrases — used to detect "find info about X" style queries
_GENERAL_RESEARCH_RE = re.compile(
    r"(?:"
    r"find\s+(?:info|information|data)\s+(?:about|on|for)\s+"
    r"|(?:get|show|give)\s+(?:me\s+)?(?:info|data|details)\s+(?:about|on|for)\s+"
    r"|tell\s+me\s+about\s+"
    r"|(?:research|analyze|analyse)\s+(?!market|portfolio|holdings|etf)"
    r"|look\s+up\s+"
    r"|overview\s+of\s+"
    r"|details?\s+(?:about|on|for)\s+"
    r")(.+)",
    re.I,
)

# Queries that benefit from large-context cloud LLM (deep analysis, long reports)
_CLOUD_NEEDED_RE = re.compile(
    r"deep.?dive|10.?k\b|10.?q\b|sec filing|annual report|edgar"
    r"|portfolio analysis|full report|compare.*holdings"
    r"|explain.*over.*year|full.*analysis|comprehensive",
    re.I,
)


def _needs_cloud(question: str) -> bool:
    """Return True when the query needs a large-context or heavy-reasoning model."""
    return bool(_CLOUD_NEEDED_RE.search(question))


def route_intent(question: str) -> str:
    """
    Determine which sub-agent should handle this question.

    Returns
    -------
    'deepdive' | 'india_equity' | 'signal' | 'macro' | 'main'
    """
    if _DEEPDIVE_RE.search(question):
        return "deepdive"
    if _SIGNAL_RE.search(question):
        return "signal"
    if _MACRO_RE.search(question):
        return "macro"
    # General research intent — resolve company locally (no network) to avoid slowdown
    m = _GENERAL_RESEARCH_RE.search(question)
    if m:
        subject = m.group(1).strip().rstrip("?.")
        try:
            from src.tools.company_resolver import _local_indian_lookup, resolve_company_info
            if _local_indian_lookup(subject):
                return "india_equity"
            # Fall through to full resolver only if subject looks short (likely a ticker/name)
            if len(subject.split()) <= 5:
                info = resolve_company_info(subject)
                if info["source"] != "fallback":
                    return "india_equity" if info["market"] == "India" else "deepdive"
        except Exception:
            pass
    # Last resort: bare company/ticker name with no action verb
    # e.g. "adani enterprise", "RELIANCE", "hdfc bank"
    bare = question.strip().rstrip("?.")
    if len(bare.split()) <= 4:
        try:
            from src.tools.company_resolver import _local_indian_lookup
            if _local_indian_lookup(bare):
                return "india_equity"
        except Exception:
            pass
    return "main"


# ── Base sub-agent ─────────────────────────────────────────────────────────────

class _SubAgent:
    """
    Lazy-initialised sub-agent base.

    The LangGraph ReAct agent (and the LLM) are built on the first call to
    ``run()`` to avoid unnecessary startup cost when the sub-agent is never
    invoked in a session.
    """

    #: Override in subclass
    SYSTEM_PROMPT: str = "You are a helpful assistant."
    #: Override in subclass — property or class-level list
    TOOLS: list = []

    def __init__(self) -> None:
        self._agent: Any = None
        self._llm: Any = None

    def _build(self, llm_override: Any = None) -> None:
        """Lazily build the LangGraph ReAct agent.

        Parameters
        ----------
        llm_override:
            When provided (e.g. a cloud LLM), use it instead of the default
            local model.  This is set by ``get_subagent_for(question)`` when
            ``_needs_cloud(question)`` is True.

        Gemma 4 supports native function calling, but a ReAct loop with 5+ tool
        calls accumulates ~2500 tokens of history, leaving no room in a 4k context
        for the model to write a real answer.  For small context windows we skip
        the agent entirely and use the programmatic _fallback() path: Python
        collects all data, then a single LLM synthesis call produces the narrative.

        Threshold: 12 000 tokens gives enough headroom for the sub-agent system
        prompt + all tool payloads + final synthesis.
        """
        from src.agents.mosaic_fund_agent import MosaicFundAgent
        tmp = object.__new__(MosaicFundAgent)
        tmp._checkpointer = None

        if llm_override is not None:
            self._llm = llm_override
        else:
            self._llm = tmp._build_llm()

        from config.settings import settings
        effective_window = getattr(self._llm, "_context_window", None) or settings.llm_context_window
        if llm_override is None and settings.llm_context_window < 12000:
            # Local model context too small for a ReAct loop — try cloud LLM instead.
            cloud_llm = tmp._build_cloud_llm()
            if cloud_llm is not None:
                logger.info(
                    "%s: local context_window=%d < 12000 — upgrading to cloud LLM",
                    self.__class__.__name__, settings.llm_context_window,
                )
                self._llm = cloud_llm
            else:
                logger.info(
                    "%s: local context_window=%d < 12000 and no cloud LLM configured — falling back",
                    self.__class__.__name__, settings.llm_context_window,
                )
                return  # leave self._agent = None → _confirm_fallback() path

        try:
            from langgraph.prebuilt import create_react_agent, ToolNode
            if self._llm is None:
                logger.warning("%s: LLM unavailable — sub-agent disabled", self.__class__.__name__)
                return
            tools = self._get_tools()
            # ToolNode runs all tool calls returned in a single AIMessage concurrently
            # via its internal ThreadPoolExecutor — no extra configuration required.
            tool_node = ToolNode(tools)
            self._agent = create_react_agent(
                model=self._llm,
                tools=tool_node,
                prompt=self.SYSTEM_PROMPT,
            )
            logger.info("%s: agent built with parallel ToolNode (%d tools)", self.__class__.__name__, len(tools))
        except Exception as exc:
            logger.error("%s: build failed: %s", self.__class__.__name__, exc)

    def _get_tools(self) -> list:
        """Return the tool list.  Subclasses can override for lazy imports."""
        return self.TOOLS

    def run(self, question: str, llm_override: Any = None, callbacks: list | None = None) -> str:
        """Invoke the sub-agent and return its text response.

        Parameters
        ----------
        llm_override:
            Cloud LLM to use instead of the default local model.
        callbacks:
            LangChain callbacks list (e.g. [RichConsoleCallbackHandler()]) for
            verbose tool-call tracing.  Passed directly to agent.invoke().
        """
        if self._agent is None:
            self._build(llm_override=llm_override)
        if self._agent is None:
            return self._confirm_fallback(question)

        from langchain_core.messages import HumanMessage, ToolMessage
        config: dict = {}
        if callbacks:
            config["callbacks"] = callbacks
        try:
            result = self._agent.invoke({"messages": [HumanMessage(content=question)]}, config=config or None)
            msgs = result.get("messages", [])

            # Collect tool outputs; skip pure-plumbing calls (e.g. resolve_company
            # which returns a symbol dict with no display value).
            _SKIP_KEYS = ('"symbol"', '"nse_symbol"', '"yf_symbol"')
            tool_sections = []
            for m in msgs:
                if not isinstance(m, ToolMessage):
                    continue
                content = str(m.content).strip()
                if not content:
                    continue
                # Skip symbol-resolution tool output (internal plumbing)
                if content.startswith("{") and any(k in content for k in _SKIP_KEYS):
                    continue
                tool_sections.append(content)

            if tool_sections:
                # If the last message is a non-empty AIMessage, the LLM already
                # produced a synthesis — prefer that over raw tool concatenation.
                from langchain_core.messages import AIMessage
                last_ai = next(
                    (m for m in reversed(msgs) if isinstance(m, AIMessage) and str(m.content).strip()),
                    None,
                )
                if last_ai:
                    logger.info(
                        "%s: returning LLM synthesis (%d chars)",
                        self.__class__.__name__, len(str(last_ai.content)),
                    )
                    return str(last_ai.content)

                logger.info(
                    "%s: merged %d tool outputs programmatically",
                    self.__class__.__name__, len(tool_sections),
                )
                return "\n\n---\n\n".join(tool_sections)

            # No tool calls — return the last AI message directly.
            return msgs[-1].content if msgs else "No response from sub-agent."
        except Exception as exc:
            err = str(exc).lower()
            if any(k in err for k in ("tool", "400", "invalid_request", "function", "tool_calls", "not support")):
                logger.info(
                    "%s: LLM tool-calling failed (%s), using programmatic fallback",
                    self.__class__.__name__, type(exc).__name__,
                )
                return self._confirm_fallback(question)
            logger.error("%s.run() failed: %s", self.__class__.__name__, exc)
            return f"Research incomplete: {exc}"

    def _confirm_fallback(self, question: str) -> str:
        """Prompt the user before switching to the programmatic data-gathering path."""
        import sys
        prompt = "\n[mosaic] LLM tool-calling unavailable — use programmatic data gathering instead? [Y/n] "
        try:
            # Open /dev/tty directly so the prompt works even when Rich's Live display
            # is active and has captured stdout/stdin.
            with open("/dev/tty", "r+") as tty:
                tty.write(prompt)
                tty.flush()
                ans = tty.readline().strip().lower()
        except OSError:
            # Non-interactive environment (piped, tests) — default to yes.
            sys.stdout.write(prompt + "\n")
            sys.stdout.flush()
            ans = ""
        if ans in ("", "y", "yes"):
            return self._fallback(question)
        return "Aborted. To enable full capability, configure a tool-calling LLM in your .env."

    def _fallback(self, question: str) -> str:
        """Programmatic fallback for when the LLM cannot call tools.  Override in subclasses."""
        return (
            "Your configured LLM does not support tool-calling.  "
            "Set `LLM_PROVIDER=openai` or `LLM_PROVIDER=anthropic` in your .env for full capability.  "
            "Alternatively use `./mosaic.sh chat` for the interactive REPL."
        )


# ── Programmatic Indian equity data gatherer (no LLM tool-calling required) ──────────

def _gather_indian_equity_data(symbol: str, exchange: str, company_name: str, llm: Any = None) -> str:
    """
    Gather comprehensive Indian equity data via direct Python function calls.

    This is the tool-calling-free fallback path for models like gemma4 that
    do not support function/tool use.  Calls each data-source function directly
    and assembles a formatted Markdown research note.
    """
    from datetime import date as _date
    parts: list[str] = [
        f"# {company_name} ({symbol})\n"
        f"*Exchange: {exchange} \u2022 Research date: {_date.today()}*\n"
    ]

    # 1. Yahoo Finance overview + price momentum
    try:
        from src.tools.yahoo_finance import fetch_yahoo_data, fetch_price_history
        yf   = fetch_yahoo_data(symbol, exchange)
        hist = fetch_price_history(symbol, exchange, "3mo")
        mc   = f"₹{yf.market_cap / 1e7:,.0f} Cr" if yf.market_cap else "N/A"
        parts.append(
            f"## Company Snapshot\n"
            f"| Metric | Value |\n|---|---|\n"
            f"| Sector | {yf.sector or 'N/A'} |\n"
            f"| Industry | {yf.industry or 'N/A'} |\n"
            f"| Market Cap | {mc} |\n"
            f"| P/E (Trailing) | {round(yf.pe_ratio, 1) if yf.pe_ratio else 'N/A'} |\n"
            f"| P/B | {round(yf.pb_ratio, 1) if yf.pb_ratio else 'N/A'} |\n"
            f"| Current Price | ₹{yf.current_price:,.0f} |\n"
            f"| 52-Week High | ₹{yf.fifty_two_week_high:,.0f} |\n"
            f"| 52-Week Low | ₹{yf.fifty_two_week_low:,.0f} |\n"
        )
        if yf.description:
            parts.append(f"**Business:** {yf.description[:500]}…")
        if len(hist) >= 22:
            latest  = hist[-1]["close"]
            prev30  = hist[-22]["close"]
            prev90  = hist[0]["close"]
            r30 = round((latest - prev30) / prev30 * 100, 2) if prev30 else 0
            r90 = round((latest - prev90) / prev90 * 100, 2) if prev90 else 0
            sig = "BULLISH" if r30 > 5 else "BEARISH" if r30 < -5 else "NEUTRAL"
            parts.append(f"**Price Momentum:** 30d {r30:+.2f}% \u2502 90d {r90:+.2f}% \u2502 Signal: **{sig}**")
    except Exception as exc:
        parts.append(f"## Company Snapshot\n*Yahoo Finance unavailable: {exc}*")

    # 2. Quarterly results
    try:
        from src.tools.earnings_scraper import fetch_from_screener, fetch_from_yahoo_financials
        q = fetch_from_screener(symbol) or fetch_from_yahoo_financials(symbol, exchange)
        if q:
            parts.append(
                f"## Latest Quarterly Results ({q.period})\n"
                f"| Metric | Value |\n|---|---|\n"
                f"| Revenue | ₹{q.revenue_cr:,.0f} Cr |\n"
                f"| Net Profit | ₹{q.net_profit_cr:,.0f} Cr |\n"
                f"| EPS | ₹{q.eps:.2f} |\n"
                f"| Revenue Growth YoY | {q.revenue_yoy_pct:+.1f}% |\n"
                f"| Profit Growth YoY | {q.profit_yoy_pct:+.1f}% |\n"
            )
        else:
            parts.append("## Quarterly Results\n*Not available via Screener.in for this symbol.*")
    except Exception as exc:
        parts.append(f"## Quarterly Results\n*Unavailable: {exc}*")

    # 3. Cash flow
    try:
        from src.tools.indian_equity_tools import get_stock_cashflow
        cf_result = get_stock_cashflow.invoke({"input_str": f"{symbol}:{exchange}"})
        if isinstance(cf_result, dict) and cf_result.get("annual_cashflows"):
            rows = cf_result["annual_cashflows"]
            lines = ["## Annual Cash Flows\n| FY End | FCF (\u20b9M) | Op CF (\u20b9M) | Capex (\u20b9M) |", "|---|---|---|---|"]
            for r in rows:
                lines.append(
                    f"| {r['fiscal_year_end']} "
                    f"| {r.get('free_cash_flow_usd_m') or 'N/A'} "
                    f"| {r.get('operating_cash_flow_usd_m') or 'N/A'} "
                    f"| {r.get('capex_usd_m') or 'N/A'} |"
                )
            parts.append("\n".join(lines))
        else:
            parts.append("## Cash Flow\n*No cash flow data available.*")
    except Exception as exc:
        parts.append(f"## Cash Flow\n*Unavailable: {exc}*")

    # 4. DSP mutual fund holdings
    try:
        from src.tools.indian_equity_tools import get_mf_holdings_for_stock
        mf_result = get_mf_holdings_for_stock.invoke({"company_name_or_symbol": company_name})
        parts.append(
            f"## DSP Mutual Fund Holdings\n{mf_result}"
            if mf_result and "Error" not in mf_result
            else f"## DSP Mutual Fund Holdings\n*{mf_result or 'No data'}*"
        )
    except Exception as exc:
        parts.append(f"## DSP Fund Holdings\n*Unavailable: {exc}*")

    # 5. Recent news
    try:
        from src.tools.news_search import fetch_news_for_symbol
        news = fetch_news_for_symbol(symbol, company_name)
        if news:
            lines = ["## Recent News\n| Headline | Source | Sentiment |", "|---|---|---|"]
            for n in news[:8]:
                lines.append(f"| {n.title[:70]} | {n.source} | {n.sentiment.value} |")
            parts.append("\n".join(lines))
        else:
            parts.append("## Recent News\n*No recent news found.*")
    except Exception as exc:
        parts.append(f"## News\n*Unavailable: {exc}*")

    # 6. FII/DII institutional flows
    try:
        from src.tools.indian_equity_tools import get_fii_dii_summary
        fii_result = get_fii_dii_summary.invoke({"days": 7})
        parts.append(
            f"## FII/DII Institutional Flows (7 days)\n{fii_result}"
            if fii_result and "Error" not in fii_result
            else "## FII/DII Flows\n*Data unavailable from ClickHouse.*"
        )
    except Exception as exc:
        parts.append(f"## FII/DII Flows\n*Unavailable: {exc}*")

    raw_data = "\n\n".join(parts)

    # LLM synthesis — only attempted when the model has enough context headroom.
    # For small-context models (e.g. gemma4 at 4k) we return the raw tables directly;
    # the data is already complete and actionable without an extra LLM call.
    from config.settings import settings
    if llm is not None and settings.llm_context_window >= 12000:
        try:
            from langchain_core.messages import HumanMessage, SystemMessage
            budget = settings.llm_prompt_budget  # chars
            data_budget = max(500, budget - 300)
            truncated = raw_data[:data_budget]
            if len(raw_data) > data_budget:
                truncated += "\n\n*[data truncated to fit context window]*"

            synthesis_prompt = (
                f"You are a senior Indian equity analyst. Below is live market data for "
                f"{company_name} ({symbol}, {exchange}). "
                f"In 3-5 concise paragraphs synthesise: (1) business quality, "
                f"(2) valuation vs sector, (3) cash flow trend, "
                f"(4) institutional sentiment, (5) a clear BUY/HOLD/SELL/WATCH verdict with one-line rationale. "
                f"Never invent numbers — use only the data provided.\n\n"
                f"--- DATA ---\n{truncated}"
            )
            res = llm.invoke([
                SystemMessage(content="You are a concise Indian equity research analyst."),
                HumanMessage(content=synthesis_prompt),
            ])
            synthesis = str(res.content).strip()
            parts.append(f"## Analyst Synthesis\n{synthesis}")
            logger.info("_gather_indian_equity_data: LLM synthesis complete (%d chars)", len(synthesis))
        except Exception as exc:
            logger.warning("_gather_indian_equity_data: LLM synthesis failed: %s", exc)
        return "\n\n".join(parts)

    return raw_data


# ── DeepDive sub-agent ─────────────────────────────────────────────────────────

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
        "and verify the market is 'US'. If `resolve_company` returns market='India', "
        "immediately reply: \"This stock is listed in India (NSE/BSE). "
        "Please use the Indian equity research path.\".  "
        "For US tickers: use `run_deepdive_analysis` to fetch SEC filings. "
        "Use `query_clickhouse_db` to read deepdive_* tables in ClickHouse "
        "(always add FINAL). "
        "Use `get_yahoo_finance_data` for live price and valuation multiples. "
        "Present all data as Markdown tables. Never invent numbers."
    )

    def _get_tools(self) -> list:
        from src.tools.yahoo_finance import YAHOO_TOOLS
        from src.tools.earnings_scraper import EARNINGS_TOOLS
        from src.tools.skills_tools import run_deepdive_analysis, query_clickhouse_db
        from src.tools.company_resolver import resolve_company
        return [resolve_company] + YAHOO_TOOLS + EARNINGS_TOOLS + [
            run_deepdive_analysis, query_clickhouse_db,
        ]


# ── Indian Equity Research sub-agent ──────────────────────────────────────────

class IndianEquityResearchSubAgent(_SubAgent):
    """
    Comprehensive research for any Indian stock (NSE/BSE).

    Covers: company overview · price momentum · quarterly earnings ·
    MF fund holdings · annual cash flow · recent news · FII/DII flows.

    Can accept a company name ("adani enterprise"), a partial name, or a
    direct NSE symbol — ``resolve_company`` is always called first.
    """

    SYSTEM_PROMPT = (
        "You are a senior Indian equity analyst covering NSE/BSE listed stocks. "
        "Research happens in exactly TWO rounds to maximise parallel execution:\n\n"
        "ROUND 1 — Resolve (single call):\n"
        "  Call `resolve_company(query)` to get `symbol` (e.g. ADANIENT), `exchange`, "
        "and `company_name`. Wait for the result before proceeding.\n\n"
        "ROUND 2 — Parallel data fetch (call ALL seven tools simultaneously in one response):\n"
        "  • `get_yahoo_finance_data(\"SYMBOL:EXCHANGE\")` — price, P/E, P/B, 52-week range, market cap\n"
        "  • `get_price_momentum(\"SYMBOL:EXCHANGE\")` — 30d/90d returns, momentum signal\n"
        "  • `get_quarterly_results(\"SYMBOL:EXCHANGE\")` — revenue, net profit, EPS, YoY growth\n"
        "  • `get_stock_cashflow(\"SYMBOL:EXCHANGE\")` — 3yr FCF, operating CF, capex\n"
        "  • `get_mf_holdings_for_stock(company_name)` — DSP fund holdings & weights\n"
        "  • `get_stock_news(company_name)` AND `get_newsapi_stock_news(symbol)` — news & sentiment\n"
        "  • `get_fii_dii_summary(7)` — 7-day FII/DII institutional flows\n\n"
        "IMPORTANT: Emit all seven tool calls in a single response (not one at a time). "
        "LangGraph will execute them concurrently.\n\n"
        "SYNTHESIS: After all results arrive, write a structured Markdown research note:\n"
        "(1) Company Snapshot  (2) Financials table  (3) Valuation vs sector  "
        "(4) Cash Flow quality  (5) Institutional Ownership  (6) News Sentiment  "
        "(7) Key Risks  (8) Recommendation (BUY/HOLD/SELL/WATCH + one-line rationale)\n\n"
        "RULES: All monetary values in ₹. Never invent figures."
    )

    def _get_tools(self) -> list:
        from src.tools.company_resolver import resolve_company
        from src.tools.yahoo_finance import YAHOO_TOOLS
        from src.tools.earnings_scraper import EARNINGS_TOOLS
        from src.tools.news_search import get_stock_news
        from src.tools.newsapi_search import get_newsapi_stock_news
        from src.tools.skills_tools import query_clickhouse_db
        from src.tools.indian_equity_tools import INDIAN_EQUITY_TOOLS
        return (
            [resolve_company]
            + YAHOO_TOOLS
            + EARNINGS_TOOLS
            + [get_stock_news, get_newsapi_stock_news, query_clickhouse_db]
            + INDIAN_EQUITY_TOOLS
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


# ── Signal sub-agent ───────────────────────────────────────────────────────────

class SignalSubAgent(_SubAgent):
    """
    ETF signal pipeline: composite scores, GOLDBEES ML, Kelly weights, risk governor.
    """

    SYSTEM_PROMPT = (
        "You are a quantitative signal analyst for Indian ETF markets (NSE). "
        "Use `run_daily_signal_composite` to compute unified 0-100 composite scores "
        "for all tracked ETFs across macro, news, NAV Z-score, FII/DII, and ML pillars. "
        "Use `run_goldbees_pipeline` for the GOLDBEES ML prediction: report "
        "prob_up, expected_return_pct, regime_signal, and weights.blended_50 verbatim. "
        "Use `run_risk_governor_analysis` for GARCH volatility-targeted position sizing. "
        "Use `run_etf_news_sentiment` for ETF category news sentiment. "
        "CRITICAL: Never invent composite scores or labels like ACCUMULATE/STRONG BUY. "
        "Use regime_signal and blended_50 exactly as the pipeline outputs them. "
        "Format all signal tables in clean Markdown."
    )

    def _get_tools(self) -> list:
        from src.tools.skills_tools import (
            run_daily_signal_composite,
            run_goldbees_pipeline,
            run_etf_news_sentiment,
            run_risk_governor_analysis,
            query_clickhouse_db,
        )
        return [
            run_daily_signal_composite,
            run_goldbees_pipeline,
            run_etf_news_sentiment,
            run_risk_governor_analysis,
            query_clickhouse_db,
        ]


# ── Macro sub-agent ────────────────────────────────────────────────────────────

class MacroSubAgent(_SubAgent):
    """
    Macro analysis: COMEX commodity signals, FII/DII flows, macro theme scanner.
    """

    SYSTEM_PROMPT = (
        "You are a macro analyst covering Indian and global commodity markets. "
        "Use `run_macro_scanner` to scan live macro/geopolitical events and map "
        "their directional impact to ETFs. "
        "Use `run_comex_analysis` for COMEX gold/silver/copper pre-market price signals. "
        "Use `query_clickhouse_db` to read `market_data.fii_dii_flows FINAL` and "
        "`market_data.cot_gold FINAL` for institutional positioning data. "
        "Score interpretation: ≥+16 = strong bullish | +8 to +15 = moderate bullish "
        "| ≤−16 = strong bearish. "
        "CRITICAL: Only cite prices and flows from live tool output — never from "
        "training-time knowledge. Gold, FII, USDINR change daily."
    )

    def _get_tools(self) -> list:
        from src.tools.skills_tools import (
            run_macro_scanner,
            run_comex_analysis,
            query_clickhouse_db,
        )
        return [run_macro_scanner, run_comex_analysis, query_clickhouse_db]


# ── Registry ──────────────────────────────────────────────────────────────────

_registry: dict[str, _SubAgent] = {}


def get_subagent(name: str) -> _SubAgent:
    """Return (lazily creating) a sub-agent by name."""
    if name not in _registry:
        cls_map: dict[str, type[_SubAgent]] = {
            "deepdive":     DeepDiveSubAgent,
            "india_equity": IndianEquityResearchSubAgent,
            "signal":       SignalSubAgent,
            "macro":        MacroSubAgent,
        }
        cls = cls_map.get(name)
        if cls is None:
            raise ValueError(f"Unknown sub-agent: {name!r}  (valid: {list(cls_map)})")
        _registry[name] = cls()
    return _registry[name]


def run_subagent_for(intent: str, question: str, callbacks: list | None = None) -> str:
    """Run a named sub-agent, automatically routing to cloud LLM when needed.

    Parameters
    ----------
    callbacks:
        Pass [RichConsoleCallbackHandler()] to see live tool-call output.
    """
    import os
    cloud_llm = None
    if _needs_cloud(question):
        try:
            from src.agents.mosaic_fund_agent import MosaicFundAgent
            tmp = object.__new__(MosaicFundAgent)
            tmp._checkpointer = None
            cloud_llm = tmp._build_cloud_llm()
            if cloud_llm is not None:
                logger.info("run_subagent_for: using cloud LLM for %r", question[:60])
        except Exception as exc:
            logger.warning("run_subagent_for: could not build cloud LLM: %s", exc)
    if callbacks is None and os.getenv("VERBOSE") == "1":
        from src.agents.mosaic_fund_agent import RichConsoleCallbackHandler
        callbacks = [RichConsoleCallbackHandler()]
    return get_subagent(intent).run(question, llm_override=cloud_llm, callbacks=callbacks)
