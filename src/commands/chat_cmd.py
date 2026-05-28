"""
src/commands/chat_cmd.py
────────────────────────
Interactive REPL chat loop for the Mosaic-fund-agent.

Invoked by:
    python src/main.py chat          (explicit)
    ./mosaic.sh                      (default when no args given)

Features
--------
- Infinite prompt loop; exits on 'quit' / Ctrl-C
- In-session conversation memory via LangGraph MemorySaver (cleared on exit)
- Intent-based sub-agent auto-routing (deepdive / signal / macro / main)
- Slash commands for direct dispatch and utility actions
- Rich spinner while waiting; Markdown-rendered responses
"""
from __future__ import annotations

import logging
import os
import re
import uuid
from typing import Any


from rich.console import Console
from rich.markdown import Markdown
from rich.panel import Panel
from src.utils.markdown_renderer import render_markdown_to_group

logger = logging.getLogger(__name__)

# @agent override map — short aliases accepted
_AT_AGENT_MAP: dict[str, str] = {
    "signal":    "signal",
    "macro":     "macro",
    "news":      "news",
    "equity":    "india_equity",
    "india":     "india_equity",
    "intl_etf":  "intl_etf",
    "intl":      "intl_etf",
    "database":  "database",
    "db":        "database",
    "code":      "code",
    "deepdive":  "deepdive",
    "deep":      "deepdive",
    "research":  "research",
    "main":      "main",
}

# Follow-up phrases that should route to the same agent as the previous turn.
_FOLLOWUP_RE = re.compile(
    r"^(?:compare(?:\s+with|\s+to)?|vs\.?|versus|against"
    r"|and\s+(?:what\s+about|also|now)"
    r"|what\s+about|how\s+about"
    r"|now\s+(?:compare|look\s+at|show|check)"
    r"|also\s+(?:check|show|get))\b",
    re.I,
)


# ── prompt_toolkit input session ──────────────────────────────────────────────

def _build_prompt_session():
    """
    Build a prompt_toolkit PromptSession with:
      • Persistent history (↑/↓ navigation across restarts)
      • Bracketed-paste support — large pastes land as a single block, not
        individual keystrokes, so multi-line pastes never trigger premature submit
      • Alt+Enter  → insert newline (for deliberately typed multi-line questions)
      • Enter      → submit (unchanged default)
    Falls back to None when prompt_toolkit is unavailable (Windows CI, etc.).
    """
    try:
        from prompt_toolkit import PromptSession
        from prompt_toolkit.history import FileHistory
        from prompt_toolkit.key_binding import KeyBindings

        kb = KeyBindings()

        @kb.add("escape", "enter")   # Alt+Enter on macOS/Linux
        @kb.add("c-o")               # Ctrl+O  — secondary shortcut
        def _newline(event):
            event.current_buffer.insert_text("\n")

        return PromptSession(
            history=FileHistory(os.path.expanduser("~/.mosaic_chat_history")),
            key_bindings=kb,
            multiline=False,         # Enter submits; newlines from paste are kept as-is
            enable_open_in_editor=False,
        )
    except ImportError:
        return None


# ── Pre-execution plan builder ────────────────────────────────────────────────

_INTENT_STEPS: dict[str, list[str]] = {
    "india_equity": [
        "Resolve symbol for '{subject}'",
        "Fetch current price, NAV, 52-week range, momentum",
        "Get quarterly results (Screener.in / BSE fallback)",
        "Check DSP Mutual Fund holdings",
        "Fetch recent news & sentiment",
        "Plot 30-day price chart → plot_price_chart('{subject}', 30)",
        "Synthesise research note",
    ],
    "signal": [
        "Run composite ETF signal aggregator",
        "Fetch LightGBM 5-day ML prediction",
        "Compute GARCH volatility & Risk Governor weight",
        "Compute Kelly-optimal position size",
        "Plot signal scores → plot_signal_scores()",
        "Return regime signal + blended weight",
    ],
    "macro": [
        "Scan live macro / geopolitical events",
        "Fetch COMEX gold / silver / copper pre-market prices",
        "Query FII/DII institutional flows (7 days)",
        "Plot FII/DII flow trend → plot_fii_dii_chart(30)",
        "Map events → ETF directional impact scores",
    ],
    "intl_etf": [
        "Load 3-year price + NAV data for MAFANG, HNGSNGBEES, MON100, MASPTOP50, MAHKTECH, MONQ50",
        "Run requested analysis: performance / premium / regime / seasonality / correlation / drawdowns / LightGBM",
        "Plot chart → plot_intl_etf_performance() or plot_intl_etf_premium(symbol)",
        "Summarise key insight (regime, premium opportunity, best month)",
    ],
    "news": [
        "Resolve '{subject}' to NSE symbol (if company/ETF)",
        "Fetch news from GNews + NewsAPI in parallel",
        "Query saved news from ClickHouse news_articles",
        "Deduplicate and sort by date",
        "Present as table + sentiment summary",
    ],
    "database": [
        "Identify target table(s) for '{subject}'",
        "Describe table schema — confirm column names",
        "Write and execute SQL query (FINAL on all tables)",
        "Format results as Markdown table",
        "Plot chart if time-series or score-set → plot_price_chart / plot_fii_dii_chart / plot_signal_scores",
    ],
    "code": [
        "Understand code request: '{subject}'",
        "Search codebase for relevant files / patterns",
        "Write or execute Python code",
        "Validate output and report results",
    ],
    "deepdive": [
        "Resolve ticker for '{subject}'",
        "Fetch SEC 10-K / 10-Q filings from EDGAR",
        "Analyse XBRL financials and peer valuation",
        "Generate deep-dive research report",
    ],
    "main": [
        "Analyse query: '{subject}'",
        "Call relevant tools (portfolio, prices, news, ClickHouse)",
        "Synthesise and return answer",
    ],
}


# ── AI planner ────────────────────────────────────────────────────────────────

_VALID_AGENTS = frozenset(
    ["signal", "macro", "news", "equity", "database", "code", "deepdive", "research", "main"]
)

_PLANNER_PROMPT = """\
You are the Mosaic routing planner for an Indian equity & commodity intelligence platform.
Analyse the user query and reply in EXACTLY this format — no other text:

AGENT: <one of: signal | macro | news | equity | database | code | deepdive | research | main>
PLAN:
1. <specific step tailored to THIS query>
2. <specific step>
3. <specific step>
4. <optional step>
5. <optional step>

═══════════════════════════════════════════════
AGENT GUIDE — read every entry before choosing
═══════════════════════════════════════════════

── signal ──────────────────────────────────────
When: ETF composite scores, GOLDBEES ML pipeline, Kelly / blended position weights,
      GARCH risk governor, live iNAV / NAV queries, premium alerts, ETF category news sentiment.
Key tools: run_goldbees_pipeline · run_daily_signal_composite · run_risk_governor_analysis ·
           get_live_inav(symbol) · run_etf_news_sentiment · run_premium_alerts · plot_signal_scores ·
           plot_signal_breakdown · plot_weight_recommendations · plot_garch_volatility_chart
Examples:
  "GOLDBEES signal today"        → 1. run_goldbees_pipeline() — report prob_up, regime_signal, blended_50
  "composite scores all ETFs"    → 1. run_daily_signal_composite()  2. plot_signal_scores()
  "GOLDBEES position size"       → 1. run_risk_governor_analysis()  2. plot_garch_volatility_chart("GOLDBEES")
  "iNAV premium alerts"          → 1. run_premium_alerts()
  "what is iNAV of SILVERBEES"   → 1. get_live_inav("SILVERBEES")
  "GOLDBEES current NAV"         → 1. get_live_inav("GOLDBEES")
  "is HNGSNGBEES at premium"     → 1. get_live_inav("HNGSNGBEES")
  "ETF news sentiment"           → 1. run_etf_news_sentiment()

── macro ────────────────────────────────────────
When: ANY geopolitical event (Iran, Russia, China, Ukraine, Israel, Gaza, Pakistan, OPEC),
      sanctions, war, conflict, crude oil/energy, gold/silver price drivers, COMEX pre-market,
      FII/DII institutional flows, RBI/Fed rate decisions, USD/INR, COT reports.
Key tools: run_macro_scanner · run_comex_analysis · query_clickhouse_db (fii_dii_flows / cot_gold) ·
           plot_fii_dii_chart
Examples:
  "comex gold signal"            → 1. run_comex_analysis()
  "iran sanctions oil impact"    → 1. run_macro_scanner()
  "FII DII flows last 30 days"   → 1. query_clickhouse_db("SELECT trade_date, fii_net_cr, dii_net_cr FROM market_data.fii_dii_flows FINAL ORDER BY trade_date DESC LIMIT 30")  2. plot_fii_dii_chart(30)
  "COT gold positioning"         → 1. query_clickhouse_db("SELECT report_date, mm_long, mm_short, mm_net FROM market_data.cot_gold FINAL ORDER BY report_date DESC LIMIT 10")

── news ─────────────────────────────────────────
When: Latest news for a SPECIFIC Indian listed company or ETF symbol.
      NEVER use news for countries, commodities, or broad market topics — those are macro.
Key tools: resolve_company · get_stock_news · get_newsapi_stock_news · search_financial_news ·
           get_db_news · run_etf_news_sentiment
Examples:
  "news on HDFC Bank"            → 1. resolve_company("HDFC Bank")  2. get_stock_news("HDFCBANK|HDFC Bank") + get_newsapi_stock_news("HDFCBANK|HDFC Bank")
  "GOLDBEES latest news"         → 1. get_stock_news("GOLDBEES|Goldbees ETF")  2. get_newsapi_stock_news("GOLDBEES|Goldbees ETF")
  "gold ETF news sentiment"      → 1. run_etf_news_sentiment()
  "saved news bearish"           → 1. get_db_news(category="gold", sentiment="bearish")

── equity ───────────────────────────────────────
When: Research on a specific Indian NSE/BSE listed stock — price, valuation, earnings,
      cash flow, MF holding pattern, institutional ownership. NOT for US stocks (→ deepdive).
Key tools: resolve_company · get_yahoo_finance_data · get_price_momentum · get_quarterly_results ·
           get_stock_cashflow · get_mf_holdings_for_stock · get_fii_dii_summary · get_stock_news ·
           plot_price_chart · plot_fund_holdings_chart
Examples:
  "RELIANCE fundamentals"        → 1. resolve_company("RELIANCE")  2. get_yahoo_finance_data("RELIANCE:NSE")  3. get_quarterly_results("RELIANCE:NSE")  4. get_mf_holdings_for_stock("Reliance")
  "HDFC Bank cashflow"           → 1. resolve_company("HDFC Bank")  2. get_stock_cashflow("HDFCBANK:NSE")
  "TCS MF holdings trend"        → 1. get_mf_holdings_for_stock("TCS")  2. plot_fund_holdings_chart("DSP Top 100", 10)
  "TATASTEEL price momentum"     → 1. resolve_company("TATASTEEL")  2. get_price_momentum("TATASTEEL:NSE")  3. plot_price_chart("TATASTEEL", 90)

── intl_etf ─────────────────────────────────────
When: Analysis of the 6 NSE-listed overseas ETFs — NEVER for importing their data.
      Symbols: MAFANG (NYSE FANG+ / Mirae) · HNGSNGBEES (Hang Seng / Nippon) ·
               MON100 (Nasdaq 100 / Motilal) · MASPTOP50 (S&P 500 Top 50 / Mirae) ·
               MAHKTECH (HK Tech / Mirae) · MONQ50 (Nasdaq 50 / Motilal)
Key tools: get_intl_etf_performance · get_intl_etf_premium · get_intl_etf_regimes ·
           get_intl_etf_seasonality · get_intl_etf_correlation · get_intl_etf_drawdowns ·
           get_intl_etf_lgbm · plot_intl_etf_performance · plot_intl_etf_premium · plot_price_chart
Examples:
  "HNGSNGBEES scarcity premium"  → 1. get_intl_etf_premium("HNGSNGBEES")  2. plot_intl_etf_premium("HNGSNGBEES")
  "compare international ETFs"   → 1. get_intl_etf_performance()  2. plot_intl_etf_performance()  3. get_intl_etf_regimes()
  "MAFANG vs MON100 correlation" → 1. get_intl_etf_correlation()  2. get_intl_etf_performance()
  "best month to buy MON100"     → 1. get_intl_etf_seasonality()
  "MON100 drawdown history"      → 1. get_intl_etf_drawdowns()
  "intl ETF ML features"         → 1. get_intl_etf_lgbm()

── database ─────────────────────────────────────
When: User explicitly asks to query/inspect ClickHouse data — "show me", "query", "how many rows",
      "SELECT", "describe table", "last watermark", "schema".
      NEVER for import/refresh/sync/backfill/update — those go to main.
      MANDATORY: step 3 must be the raw SQL starting with SELECT (no markdown, no explanation).
Key tools: execute_db_query · describe_db_table · list_db_tables · sample_db_table ·
           get_db_watermarks · plot_price_chart · plot_fii_dii_chart
Examples:
  "show fii flows last 7 days"   → 1. Identify table  2. Confirm schema  3. SELECT trade_date, fii_net_cr, dii_net_cr FROM market_data.fii_dii_flows FINAL ORDER BY trade_date DESC LIMIT 7
  "how many rows in daily_prices"→ 1. SELECT count() FROM market_data.daily_prices FINAL
  "last import watermarks"       → 1. SELECT source, symbol, last_date FROM market_data.import_watermarks FINAL ORDER BY updated_at DESC LIMIT 20
  "describe mf_holdings table"   → 1. describe_db_table("mf_holdings")

── code ─────────────────────────────────────────
When: Writing/running Python scripts, debugging errors, building new tools/fetchers/signal sources,
      ad-hoc data analysis with code, running existing scripts.
Key tools: execute_python_snippet · write_project_file · read_project_file · run_existing_script ·
           search_project_code · list_project_scripts · query_clickhouse_db
Examples:
  "write script to calc rolling vol" → 1. write_project_file("src/scripts/market/rolling_vol.py", ...)  2. run_existing_script("src/scripts/market/rolling_vol.py")
  "run the goldbees backtest"        → 1. list_project_scripts()  2. run_existing_script("src/scripts/ml/...")
  "debug the import error"           → 1. read_project_file("src/importer/...")  2. execute_python_snippet(...)
  "add new signal source"            → 1. read_project_file("src/agents/signal_sources.py")  2. write_project_file(...)

── deepdive ─────────────────────────────────────
When: SEC 10-K / 10-Q filings, EDGAR data, annual reports for US-listed stocks.
      Symbols: AAPL, MSFT, NVDA, ADSK, TSLA, GOOG, AMZN, META, etc.
      NEVER for Indian stocks — use equity for those.
Key tools: resolve_company · run_deepdive_analysis · get_yahoo_finance_data · query_clickhouse_db
Examples:
  "NVIDIA 10-K analysis"         → 1. resolve_company("NVIDIA")  2. run_deepdive_analysis("NVDA")
  "MSFT vs AAPL valuation"       → 1. run_deepdive_analysis("MSFT")  2. run_deepdive_analysis("AAPL")
  "ADSK free cash flow trend"    → 1. run_deepdive_analysis("ADSK")

── research ─────────────────────────────────────
When: Multi-domain autonomous investigation — use when the query crosses two or more of:
      fundamentals + macro + signals + news + MF holdings + ML + correlation.
      Trigger phrases: "deep research", "comprehensive analysis", "investigate X",
      "full analysis of X", "why is X moving/rising/falling", "MF holding pattern for X",
      "predict price using ML", "full thesis on X".
      NOT for single-domain queries (e.g. "GOLDBEES signal" → signal, "news on TCS" → news).
Key tools: All tools + check_and_refresh_symbol_data · delegate_to_signal_agent ·
           delegate_to_macro_agent · delegate_to_intl_etf_agent · delegate_to_news_agent ·
           delegate_to_india_equity_agent · execute_python_snippet
Examples:
  "deep research on HDFC Bank"    → 1. check_and_refresh_symbol_data("HDFCBANK")  2. resolve_company + fundamentals  3. get_mf_holdings_for_stock  4. delegate_to_macro_agent  5. delegate_to_news_agent
  "why is GOLDBEES rising"        → 1. check_and_refresh_symbol_data("GOLDBEES")  2. get_price_momentum  3. delegate_to_signal_agent  4. delegate_to_macro_agent  5. synthesise thesis
  "MF holding pattern for TCS"    → 1. get_mf_holdings_for_stock("TCS")  2. query_clickhouse_db(mf_holdings trend)  3. get_fii_dii_summary  4. delegate_to_news_agent
  "predict NIFTYBEES price ML"    → 1. check_and_refresh_symbol_data("NIFTYBEES")  2. execute_python_snippet(LightGBM features)  3. delegate_to_signal_agent

── main ─────────────────────────────────────────
When: Zerodha portfolio / holdings, general questions, AND ALL data import/refresh operations.
      Import trigger words: import, refresh, sync, update, backfill + any data category.
      Valid import categories: etfs · stocks · mf · fii_dii · cot · fx_rates · inav
Key tools: fetch_portfolio_holdings · run_data_engineering_importer · run_goldbees_pipeline ·
           analyze_portfolio_with_llm
Examples:
  "import --category etfs"       → 1. Import ETF daily prices → run_data_engineering_importer(category="etfs")
  "import --category fii_dii"    → 1. Import FII/DII flows → run_data_engineering_importer(category="fii_dii")
  "import --category mf"         → 1. Import MF NAV data → run_data_engineering_importer(category="mf")
  "import --category cot"        → 1. Import COMEX COT positioning → run_data_engineering_importer(category="cot")
  "import --full"                → 1. Full backfill → run_data_engineering_importer(category="etfs,stocks,mf,fii_dii,cot,fx_rates,inav", full=True)
  "import inav"                  → 1. Live NSE iNAV snapshot → run_data_engineering_importer(category="inav")
  "refresh nav data"             → 1. run_data_engineering_importer(category="etfs")
  "sync fii dii"                 → 1. run_data_engineering_importer(category="fii_dii")
  "import all data"              → 1. run_data_engineering_importer(category="etfs,stocks,mf,fii_dii,cot,fx_rates")
  "import HNGSNGBEES 1 year"     → 1. Symbol-specific import → import_symbol_data(symbol="HNGSNGBEES", days=365)
  "import GOLDBEES 6 months"     → 1. Symbol-specific import → import_symbol_data(symbol="GOLDBEES", days=180)
  "import RELIANCE 2 years"      → 1. Symbol-specific import → import_symbol_data(symbol="RELIANCE", days=730)
NOTE: ONE symbol + custom range → import_symbol_data(symbol, days) | bulk category → run_data_engineering_importer(category)
  "show my portfolio"            → 1. fetch_portfolio_holdings()  2. analyze_portfolio_with_llm(...)

═══════════════════════════════════════════════
DISAMBIGUATION RULES — apply when in doubt
═══════════════════════════════════════════════
• "import / refresh / sync / update / backfill" + any data word → ALWAYS main (never database)
• Country / commodity / rate event → macro  (not news, not equity)
• Specific Indian company news → news  (not equity, not macro)
• GOLDBEES / ETF signal / Kelly / GARCH → signal  (not database, not research)
• MAFANG / HNGSNGBEES / MON100 / MASPTOP50 / MAHKTECH / MONQ50 analysis → intl_etf  (not equity)
• US ticker (AAPL, MSFT, NVDA, TSLA…) → deepdive  (not equity)
• "SELECT … FROM" or "query the db" → database  (not code)
• Write/run/debug Python code → code  (not database)
• Single-domain question → use the specialist agent, not research
• Multi-domain or "why is X" or "full analysis" → research

ClickHouse schema (database = market_data, all tables use ReplacingMergeTree — always add FINAL):
  daily_prices        : symbol(String), category(String), trade_date(Date), open, high, low, close(Float64), volume
  mf_holdings         : scheme_code, fund_name, as_of_month(Date), isin, security_name, asset_type, market_value_cr, pct_of_nav
  mf_nav              : symbol, scheme_code, nav_date(Date), nav(Float64)
  fii_dii_flows       : trade_date(Date), fii_net_cr, dii_net_cr, fii_gross_buy_cr, fii_gross_sell_cr
  fii_dii_fno_daily   : trade_date(Date), fii_fut_net_oi, fii_opt_call_net_oi, fii_opt_put_net_oi, nifty_close
  signal_composite    : as_of(Date), etf_symbol, composite_score(Float32), action, macro_score, ml_score
  ml_predictions      : as_of(Date), expected_return_pct, regime_signal, cv_r2_mean, goldbees_close
  weight_checkpoints  : as_of(Date), symbol, method, recommended_weight, garch_vol_pct, regime
  inav_snapshots      : symbol, snapshot_at(DateTime), inav(Float64), market_price(Float64), premium_discount_pct(Float64), source(String)
  -- NOTE: for current iNAV use get_live_inav(symbol) NOT raw SQL — it auto-refreshes from NSE if DB is stale
  cot_gold            : report_date(Date), mm_long, mm_short, mm_net, open_interest
  fx_rates            : trade_date(Date), symbol, close(Float64)
  macro_indicators    : ref_year, country_code, indicator_code, indicator_name, value
  news_articles       : fetched_at(DateTime), category, sentiment, impact_tier, title, source
  import_watermarks   : source, symbol, last_date(Date), updated_at
  stock_valuation     : symbol, snapshot_date(Date), trailing_pe, forward_pe, price_to_book, market_cap
  deepdive_financials : ticker, report_date(Date), revenue_usd_m, net_income_usd_m, free_cash_flow_usd_m
  deepdive_valuation  : ticker, report_date(Date), pe_trailing, ev_ebitda, fcf_yield_pct

SQL rules (CRITICAL — never use placeholder values like YYYY-MM-DD):
  FINAL modifier : always add FINAL after table name
  Current date   : today()
  N days ago     : today() - 30
  Start of month : toStartOfMonth(today())
  Specific date  : toDate('2026-05-01')   ← use real dates only, never YYYY-MM-DD

Chart tools available (use when the query involves price, trend, pattern, comparison, flows, weights, or scores):
  plot_price_chart(symbol, days)              — line chart: price trend
  plot_multi_price_chart('SYM1,SYM2', days)  — normalised comparison
  plot_fii_dii_chart(days)                   — bar chart: FII/DII net flows
  plot_signal_scores()                       — bar chart: all ETF composite scores
  plot_signal_breakdown('SYM1,SYM2')         — grouped: pillar weights per ETF
  plot_fund_holdings_chart(fund, top_n)      — horizontal bar: holdings by pct_of_nav
  plot_weight_recommendations(method)        — horizontal bar: position weights
  plot_nav_chart(symbol_or_scheme, days)     — line chart: MF/ETF NAV trend (pass NSE symbol e.g. 'GOLDBEES' or numeric scheme code)
  plot_intl_etf_performance()                — bar chart: 3-year total return % for all 6 intl ETFs (intl_etf agent)
  plot_intl_etf_premium(symbol, days)        — line chart: scarcity premium/discount trend for one intl ETF (intl_etf agent)

IMPORTANT: Always include chart tools as explicit numbered plan steps when visualisation adds value.
Example: "5. Plot 90-day price trend → plot_price_chart('GOLDBEES', 90)"

User query: \"{query}\"\
"""

_plan_llm: "Any" = None   # lazy singleton


def _get_plan_llm() -> "Any":
    """Build (once) the LLM used for planning calls."""
    global _plan_llm
    if _plan_llm is not None:
        return _plan_llm
    try:
        from config.settings import settings
        budget = settings.llm_token_budget
        kw = dict(temperature=0, max_tokens=budget)
        if settings.llm_base_url:
            from langchain_openai import ChatOpenAI
            _plan_llm = ChatOpenAI(
                model=settings.llm_model,
                base_url=settings.llm_base_url,
                api_key=settings.openai_api_key or "local",
                **kw,
            )
        elif settings.llm_provider == "anthropic":
            from langchain_anthropic import ChatAnthropic
            _plan_llm = ChatAnthropic(
                model=settings.llm_model,
                api_key=settings.anthropic_api_key,
                extra_headers={"anthropic-beta": "prompt-caching-2024-07-31"},
                **kw,
            )
        elif settings.llm_provider == "google":
            from langchain_google_genai import ChatGoogleGenerativeAI
            _plan_llm = ChatGoogleGenerativeAI(
                model=settings.llm_model,
                google_api_key=settings.google_api_key,
                temperature=0,
                max_output_tokens=budget,
            )
        else:
            from langchain_openai import ChatOpenAI
            _plan_llm = ChatOpenAI(
                model=settings.llm_model,
                api_key=settings.openai_api_key,
                **kw,
            )
    except Exception as exc:
        logger.warning("_get_plan_llm: could not build LLM: %s", exc)
    return _plan_llm


def _build_ai_plan(question: str, regex_intent: str, locked: bool = False) -> tuple[str, str, str | None]:
    """
    Ask the LLM to produce a specific execution plan for *question*.

    Parameters
    ----------
    locked : when True the regex_intent is authoritative — the LLM may still
             generate a richer plan but cannot change the agent choice.

    Returns
    -------
    (intent, plan_rich_text, sql_hint)
        intent        — LLM-chosen agent name (falls back to regex_intent on failure)
        plan_rich_text — numbered Rich-markup plan string
        sql_hint      — raw SQL extracted from the plan if intent is database, else None
    """
    llm = _get_plan_llm()
    if llm is not None:
        try:
            from langchain_core.messages import HumanMessage
            prompt = _PLANNER_PROMPT.format(query=question[:300])
            raw = str(llm.invoke([HumanMessage(content=prompt)]).content).strip()

            # Parse AGENT line — honour lock if set
            ai_intent = regex_intent
            if not locked:
                for line in raw.splitlines():
                    if line.upper().startswith("AGENT:"):
                        candidate = line.split(":", 1)[1].strip().lower()
                        if candidate == "equity":
                            candidate = "india_equity"
                        if candidate in _VALID_AGENTS or candidate == "india_equity":
                            ai_intent = candidate
                        break

            # Parse PLAN lines (numbered list after "PLAN:")
            steps: list[str] = []
            in_plan = False
            for line in raw.splitlines():
                if line.upper().startswith("PLAN:"):
                    in_plan = True
                    continue
                if in_plan:
                    stripped = line.strip()
                    if stripped and stripped[0].isdigit():
                        text = stripped.lstrip("0123456789. )")
                        if text:
                            steps.append(text)

            if steps:
                sql_hint: str | None = None
                lines = []
                for i, s in enumerate(steps, 1):
                    upper = s.upper().lstrip()
                    is_sql = upper.startswith(("SELECT", "WITH", "SHOW", "DESCRIBE"))
                    if is_sql:
                        lines.append(f"  [cyan]{i}.[/cyan] [green]{s}[/green]")
                        if sql_hint is None:
                            sql_hint = s.strip()
                    else:
                        lines.append(f"  [cyan]{i}.[/cyan] {s}")
                plan_text = "\n".join(lines)
                return ai_intent, plan_text, sql_hint

        except Exception as exc:
            logger.debug("_build_ai_plan: LLM call failed (%s) — using fallback", exc)

    # ── Deterministic fallback ─────────────────────────────────────────────
    return regex_intent, _build_fallback_plan(question, regex_intent), None


def _build_fallback_plan(question: str, intent: str) -> str:
    """Static template plan — used when the LLM planner is unavailable."""
    subject = question.strip()
    try:
        from src.tools.company_resolver import _local_indian_lookup
        sym = _local_indian_lookup(subject)
        subject = sym if sym else " ".join(question.split()[:4]) + ("…" if len(question.split()) > 4 else "")
    except Exception:
        subject = " ".join(question.split()[:4])

    steps = _INTENT_STEPS.get(intent, _INTENT_STEPS["main"])
    return "\n".join(
        f"  [cyan]{i}.[/cyan] {s.format(subject=subject)}"
        for i, s in enumerate(steps, 1)
    )


# ── ML status display ────────────────────────────────────────────────────────

def _get_ml_status() -> str:
    """
    Return a Markdown summary of live ML model state from ClickHouse.
    Falls back to a static capability overview if the DB is unavailable.
    """
    lines: list[str] = [
        "## ML Capabilities — Mosaic Fund Agent\n",
        "| Model | Purpose | Output |",
        "|---|---|---|",
        "| LightGBM classifier | 5-day up/down probability | `prob_up` (0–1) |",
        "| LightGBM regressor  | 5-day expected return     | `expected_return_pct` |",
        "| GARCH(1,1)          | Annualised volatility      | `garch_vol_pct` |",
        "| Isolation Forest    | Price anomaly / regime     | `regime_signal` |",
        "| Kelly criterion     | Optimal position size      | `weights.kelly` |",
        "| Risk Governor       | Vol-targeted position      | `weights.blended_50` |",
        "| Signal aggregator   | 5-pillar ETF score 0-100   | `composite_score` |",
        "",
        "### Where ML is used",
        "- **GOLDBEES pipeline**: LightGBM → Kelly → Risk Governor → blended weight",
        "- **Signal composite**: ML score is 1 of 5 pillars (macro/sentiment/val/flow/ML)",
        "- **Anomaly detection**: GARCH + Isolation Forest flags abnormal price regimes",
        "- **Position sizing**: GARCH vol feeds the Risk Governor weight calculation",
        "",
        "### ML-powered prompts",
        "```",
        "goldbees signal          — full ML prediction + Kelly weight",
        "goldbees kelly weight    — position sizing from GARCH + Kelly",
        "risk governor analysis   — GARCH volatility targeting",
        "composite score for all etfs  — 5-pillar ML-enhanced scores",
        "plot signal breakdown    — visualise ML vs other pillars",
        "plot weight recommendations   — chart of recommended positions",
        "```",
    ]

    # Append live prediction from ClickHouse if available
    try:
        from src.db.pool import query_df
        df = query_df("""
            SELECT as_of, expected_return_pct, regime_signal,
                   cv_r2_mean, goldbees_close, confidence_low, confidence_high
            FROM market_data.ml_predictions FINAL
            ORDER BY as_of DESC LIMIT 1
        """)
        if not df.empty:
            row = df.iloc[0]
            lines += [
                "",
                "### Latest ML Prediction (GOLDBEES)",
                f"| Field | Value |",
                f"|---|---|",
                f"| as_of | {row['as_of']} |",
                f"| expected_return_pct | {row['expected_return_pct']:.2f}% |",
                f"| confidence_band | [{row['confidence_low']:.2f}%, {row['confidence_high']:.2f}%] |",
                f"| regime_signal | **{row['regime_signal']}** |",
                f"| cv_r2_mean | {row['cv_r2_mean']:.4f} |",
                f"| goldbees_close | ₹{row['goldbees_close']:.2f} |",
            ]

        wdf = query_df("""
            SELECT symbol, method, recommended_weight, garch_vol_pct, regime, as_of
            FROM market_data.weight_checkpoints FINAL
            WHERE method = 'blended_50'
            ORDER BY as_of DESC LIMIT 5
        """)
        if not wdf.empty:
            lines += ["", "### Latest Blended Weights (blended_50)"]
            lines.append("| Symbol | Weight | GARCH vol% | Regime | as_of |")
            lines.append("|---|---|---|---|---|")
            for _, r in wdf.iterrows():
                lines.append(
                    f"| {r['symbol']} | {r['recommended_weight']:.3f} "
                    f"| {r['garch_vol_pct']:.1f}% | {r['regime']} | {r['as_of']} |"
                )
    except Exception as exc:
        lines.append(f"\n*Live ML data unavailable: {exc}*")

    return "\n".join(lines)


# ── Contextual prompt suggestions ────────────────────────────────────────────

_SUGGESTION_PROMPT = """\
You are a helpful assistant for an Indian equity & commodity investment platform.

Based on this conversation turn, suggest exactly 3 concise follow-up questions the user might ask next.
Make suggestions specific to the data/symbols mentioned — not generic.

Draw suggestions from these known-good prompts when relevant:
  signal: "goldbees signal", "composite score for all etfs", "plot signal scores", "inav premium alert"
  ml:     "show GARCH chart", "goldbees ml prediction", "plot weight recommendations blended_50"
  equity: "compare X with Y", "DSP fund holdings for X", "plot X last 90 days"
  macro:  "comex gold today", "plot fii dii chart", "iran news"
  intl:   "international ETF performance", "MAFANG premium chart", "intl ETF regimes"
  db:     "query fii flows last 30 days", "watermarks", "how many rows in mf_holdings"
  chart:  "plot price chart X", "compare X GOLDBEES", "plot nav chart"

User asked ({intent}): {question}
Answer summary: {answer_summary}

Reply with exactly 3 lines — one question per line, no numbering, no bullet points, no preamble.
"""


def _get_suggestions(question: str, answer: str, intent: str) -> list[str]:
    """
    Generate 3 contextual follow-up prompt suggestions via the LLM.
    Returns an empty list on failure (suggestions are best-effort).
    """
    llm = _get_plan_llm()
    if llm is None:
        return []
    try:
        from langchain_core.messages import HumanMessage
        answer_summary = answer[:300] + "…" if len(answer) > 300 else answer
        prompt = _SUGGESTION_PROMPT.format(
            intent=intent,
            question=question[:200],
            answer_summary=answer_summary,
        )
        raw = str(llm.invoke([HumanMessage(content=prompt)]).content).strip()
        suggestions = [
            ln.strip().lstrip("•-–—123456789.)> ").strip()
            for ln in raw.splitlines()
            if ln.strip() and not ln.strip().startswith("#")
        ]
        # Keep only non-empty, question-like lines (max 3)
        return [s for s in suggestions if len(s) > 8][:3]
    except Exception as exc:
        logger.debug("_get_suggestions failed: %s", exc)
        return []


# ── Prompt library ────────────────────────────────────────────────────────────

PROMPT_LIBRARY: dict[str, list[tuple[str, str]]] = {
    "signal": [
        ("goldbees signal",                          "Full ML pipeline — prob_up, Kelly weight, regime"),
        ("goldbees kelly weight",                    "Position sizing from GARCH + Kelly criterion"),
        ("composite score for all etfs",             "Signal aggregator: 18 ETFs scored 0-100"),
        ("inav premium alert",                       "Scarcity premium Z-score alerts"),
        ("risk governor analysis",                   "GARCH volatility targeting decision"),
        ("plot signal scores",                       "Bar chart of all ETF composite scores"),
        ("plot signal breakdown GOLDBEES",           "Pillar-level weights: macro/sentiment/flow/ML"),
        ("plot weight recommendations blended_50",   "Recommended position sizes"),
    ],
    "ml": [
        ("/ml",                                      "Live ML model status + latest prediction"),
        ("show GARCH chart",                         "GARCH vol trend vs vol-target line"),
        ("goldbees ml prediction",                   "LightGBM 5-day forecast with confidence band"),
        ("what is the regime signal today",          "BUY/HOLD/SELL from ML model"),
        ("show ml prediction",                       "Expected return % + confidence band"),
        ("plot GARCH volatility last 180 days",      "Historical volatility trend"),
    ],
    "equity": [
        ("analyse HDFC bank",                        "Price, P/E, earnings, MF holdings, news"),
        ("research reliance industries",             "Full equity research note"),
        ("compare ICICI with HDFC bank",             "Side-by-side valuation and momentum"),
        ("DSP fund holdings for infosys",            "Cross-fund institutional ownership"),
        ("news on tata motors",                      "Latest news + sentiment"),
        ("plot NIFTYBEES last 90 days",              "Price trend chart"),
    ],
    "macro": [
        ("macro scan",                               "Live geopolitical events → ETF impact scores"),
        ("comex gold today",                         "COMEX pre-market gold/silver/copper signals"),
        ("fii flows this week",                      "FII/DII institutional net flows"),
        ("iran news",                                "Geopolitical impact on Indian ETFs"),
        ("plot fii dii chart",                       "30-day net flow trend chart"),
        ("usd inr correlation",                      "USDINR and ETF correlation"),
    ],
    "intl_etf": [
        ("international ETF performance",            "3-year return, vol, Sharpe for 6 intl ETFs"),
        ("MAFANG premium chart",                     "China Tech scarcity premium trend"),
        ("intl ETF regimes",                         "Bull/Sideways/Bear regimes per ETF"),
        ("intl ETF seasonality",                     "Best/worst months per ETF"),
        ("MON100 drawdowns",                         "Major loss episodes in Nasdaq 100 ETF"),
        ("LightGBM feature importance intl ETF",     "What drives each ETF's 5-day return"),
        ("HNGSNGBEES vs MON100 correlation",         "Return correlation analysis"),
    ],
    "database": [
        ("show all tables",                          "List all ClickHouse tables with row counts"),
        ("watermarks",                               "Last import date for each data source"),
        ("query fii flows last 30 days",             "Raw FII/DII net flows from DB"),
        ("how many rows in mf_holdings",             "Row count and freshness check"),
        ("SELECT close FROM daily_prices WHERE symbol='GOLDBEES' ORDER BY trade_date DESC LIMIT 10",
                                                     "Direct SQL — latest GOLDBEES prices"),
    ],
    "chart": [
        ("plot GOLDBEES last 30 days",               "Price trend line chart"),
        ("compare GOLDBEES SILVERBEES NIFTYBEES",    "Normalised multi-ETF comparison"),
        ("plot fii dii chart",                       "Institutional flow bar chart"),
        ("plot nav chart GOLDBEES 90",               "NAV trend with sparkline"),
        ("plot DSP multi asset holdings",            "Top holdings by % of NAV"),
        ("plot intl ETF performance",                "International ETF 3-year bar chart"),
    ],
    "code": [
        ("write a script to plot GOLDBEES 90-day returns",       "Generate and save analysis script"),
        ("list all scripts",                                      "Browse src/scripts/ directory"),
        ("execute python: show last 5 rows of mf_holdings",      "Ad-hoc ClickHouse query in Python"),
        ("add a new signal source for bond spreads",             "Scaffold new SignalSource subclass"),
        ("debug this error in whale_tracker.py",                 "Diagnose and fix code issues"),
    ],
    "import": [
        ("import today's data",                          "Delta sync all categories since last import"),
        ("import all data",                              "Full sync — ETFs, stocks, MF, FII/DII, COT, FX"),
        ("import --category etfs",                       "Import only ETF OHLCV prices"),
        ("import --category fii_dii",                    "Import only institutional flow data"),
        ("import --category mf",                         "Import mutual fund NAV data"),
        ("import --category cot",                        "Import CFTC COT gold positioning data"),
        ("import --category fx_rates",                   "Import USDINR and other FX rates"),
        ("import --category stocks",                     "Import NSE equity prices"),
        ("import --dry-run",                             "Preview what would be imported (no writes)"),
        ("watermarks",                                   "Check last import date per source / symbol"),
        ("refresh data",                                 "Alias for import today's data"),
    ],
}

_CATEGORY_ALIASES = {
    "signals": "signal", "etf": "signal", "gold": "signal",
    "machine learning": "ml", "garch": "ml", "lightgbm": "ml",
    "stocks": "equity", "stock": "equity", "nse": "equity",
    "geo": "macro", "geopolitical": "macro", "comex": "macro", "flows": "macro",
    "international": "intl_etf", "intl": "intl_etf", "global": "intl_etf",
    "charts": "chart", "plots": "chart", "visualise": "chart",
    "sql": "database", "db": "database", "clickhouse": "database",
    "scripts": "code", "python": "code",
    "sync": "import", "refresh": "import", "data": "import",
}


def _show_prompts(console: "Console", category: str = "") -> None:
    """Render the prompt library for one category or all categories."""
    from rich.table import Table

    cat = category.strip().lower()
    cat = _CATEGORY_ALIASES.get(cat, cat)

    if cat and cat in PROMPT_LIBRARY:
        categories = {cat: PROMPT_LIBRARY[cat]}
    else:
        categories = PROMPT_LIBRARY

    for name, prompts in categories.items():
        t = Table(title=f"[bold cyan]{name.upper()}[/bold cyan]",
                  show_header=True, header_style="bold", expand=False)
        t.add_column("Prompt", style="green", no_wrap=True)
        t.add_column("What it does", style="dim")
        for prompt, desc in prompts:
            t.add_row(prompt, desc)
        console.print(t)

    console.print(
        "[dim]  Usage: type the prompt above, or type [bold]/prompts signal[/bold] "
        "to filter by category.[/dim]\n"
        "[dim]  Categories: signal · ml · equity · macro · intl_etf · database · chart · code[/dim]\n"
    )


def _starter_suggestions(n: int = 3) -> list[str]:
    """Pick n diverse starter prompts (one per category) to show at agent load."""
    import random
    picks = []
    cats  = list(PROMPT_LIBRARY.keys())
    random.shuffle(cats)
    for cat in cats[:n]:
        prompt, _ = PROMPT_LIBRARY[cat][0]
        picks.append(prompt)
    return picks


# ── Answer renderer ───────────────────────────────────────────────────────────

_CHART_CHARS = ("┤", "┼", "─", "└", "┐", "┘", "┌", "├", "┬", "┴", "╮", "╰", "╭")


def _print_answer(console: "Console", answer: Any) -> None:
    """
    Render the agent's final answer.

    If the answer contains an embedded ASCII chart (from a chart tool whose
    output was quoted verbatim by the LLM), split it out and render it in its
    own no-wrap panel so the box-drawing characters don't collide with Rich's
    Panel border.
    """
    from rich.text import Text

    if isinstance(answer, list):
        texts = []
        for block in answer:
            if isinstance(block, dict):
                if block.get("type") == "text":
                    texts.append(block.get("text", ""))
            elif isinstance(block, str):
                texts.append(block)
        answer = "\n".join(texts)
    elif not isinstance(answer, str):
        answer = str(answer) if answer is not None else ""

    # Detect a chart block: 3+ consecutive lines that start with box chars or spaces+box.
    lines = answer.splitlines()
    chart_lines: list[int] = [
        i for i, ln in enumerate(lines)
        if any(c in ln for c in _CHART_CHARS)
    ]

    # Only split if there's a meaningful run of chart lines (at least 5)
    if len(chart_lines) >= 5:
        # Find the contiguous chart block
        first, last = chart_lines[0], chart_lines[-1]
        text_before = "\n".join(lines[:first]).strip()
        chart_block  = "\n".join(lines[first:last + 1])
        text_after   = "\n".join(lines[last + 1:]).strip()

        if text_before:
            console.print(Panel(render_markdown_to_group(text_before), border_style="green"))
        _t = Text.from_ansi(chart_block)
        _t.no_wrap = True
        console.print(Panel(_t, border_style="blue", title="Chart", expand=False))
        if text_after:
            console.print(Panel(render_markdown_to_group(text_after), border_style="green"))
    else:
        console.print(Panel(render_markdown_to_group(answer), border_style="green"))


# ── Banner & help ──────────────────────────────────────────────────────────────

_BANNER = """[bold blue]
╔══════════════════════════════════════════════════════════╗
║        Mosaic-fund-agent  •  Interactive Chat            ║
║    Indian Equity & US Research Intelligence              ║
╚══════════════════════════════════════════════════════════╝
[/bold blue]
Type your question, or use a slash command:

  [cyan]/analyze [--max N][/cyan]   — full Zerodha portfolio analysis
  [cyan]/signals[/cyan]             — ETF composite signal dashboard
  [cyan]/ml[/cyan]                  — ML model status + live prediction
  [cyan]/prompts [category][/cyan]  — browse prompt library (signal·ml·equity·macro·intl_etf·chart·code·database·import)
  [cyan]/deepdive TICKER[/cyan]     — US stock SEC deep-dive (e.g. /deepdive ADSK)
  [cyan]/macro[/cyan]              — macro events + COMEX + FII/DII scan
  [cyan]/cache[/cyan]              — show LLM cache stats  ([cyan]/cache clear[/cyan] to wipe)
  [cyan]/clear[/cyan]              — reset conversation memory (fresh thread)
  [cyan]/help[/cyan]               — show this help text
  [dim]quit / exit / Ctrl-C[/dim]  — exit

Auto-routing (no slash needed):
  "deep-dives adsk"         →  DeepDive sub-agent
  "goldbees signal"         →  Signal sub-agent
  "comex gold"              →  Macro sub-agent
  "write a script to..."    →  Code sub-agent

ML models running:  LightGBM 5-day forecast · GARCH volatility · Isolation Forest anomaly
Type [cyan]/ml[/cyan] to see live model state and ML-powered prompts.
Tool calls and logs are shown live for every turn.
"""

_HELP_MD = """
## Mosaic Chat — Quick Reference

### Slash Commands

| Command | Action |
|---|---|
| `/analyze [--max N]` | Full Zerodha portfolio analysis (use --max 3 for quick test) |
| `/signals` | ETF composite signal aggregator (all 18 ETFs) |
| `/ml` | ML model status — LightGBM prediction, GARCH vol, anomaly regime |
| `/deepdive TICKER` | US stock SEC 10-K deep-dive (e.g. `/deepdive ADSK`) |
| `/macro` | Live macro events + COMEX + FII/DII institutional flows |
| `/cache` | Show LLM cache stats; `/cache clear` wipes cached responses |
| `/clear` | Reset session memory — next question starts a fresh thread |
| `/help` | This help text |
| `quit` / `exit` / `q` | Exit the chat |

### ML Capabilities

| Model | What it does | How to trigger |
|---|---|---|
| **LightGBM classifier** | 5-day directional probability for GOLDBEES | `goldbees signal` or `/ml` |
| **LightGBM regressor** | Expected 5-day return % + confidence band | `goldbees signal` |
| **GARCH(1,1)** | Annualised volatility for position sizing | `risk governor` or `/ml` |
| **Isolation Forest** | Price anomaly detection + regime label | `goldbees signal` |
| **Kelly criterion** | Optimal position fraction from edge + vol | `goldbees kelly weight` |
| **Risk Governor blend** | 50% Kelly + 50% vol-target weight | `goldbees pipeline` |
| **Signal aggregator** | 5-pillar composite score (0-100) for 18 ETFs | `/signals` |

### ML Output Fields
```
prob_up            — LightGBM up-probability (0–1)
expected_return_pct — predicted 5-day log return
confidence_band    — [low%, high%] quantile bounds
regime_signal      — BUY / WATCH_LONG / HOLD / WATCH_SHORT / SELL
cv_auc             — model AUC (>0.55 = useful signal)
cv_skill           — AUC − 0.5 (≤0 = no edge, Kelly disabled)
hit_ratio          — directional accuracy from walk-forward CV
weights.blended_50 — recommended weight (50% RG + 50% Kelly)
```

### Auto-Routing Keywords

| Keywords | Routes to |
|---|---|
| `deep-dive`, `10-K`, `AAPL`, `MSFT` … | DeepDive (US stocks) |
| `signal`, `goldbees`, `Kelly`, `iNAV` … | Signal + ML agent |
| `COMEX`, `macro`, `FII`, `DII`, `iran` … | Macro agent |
| `news on HDFC`, `etf news` … | News agent |
| `query database`, `SELECT …`, `clickhouse` | Database agent |
| `write a script`, `execute python` … | Code agent |
| Everything else | Main portfolio agent |

### Tips
- Type `1` `2` `3` after any response to pick a follow-up suggestion.
- Sub-agents share the same LLM; `/clear` resets all context.
- Charts auto-render for price, NAV, FII/DII, signal scores, and weights.
"""


# ── Slash command dispatcher ───────────────────────────────────────────────────

def _dispatch_slash(
    raw: str,
    console: Console,
    agent: Any,       # MosaicFundAgent
    thread_id: str,
    conv_history: list | None = None,
) -> tuple[str, str]:
    """
    Parse and handle a slash command.

    Returns
    -------
    (answer, new_thread_id)
    If answer is empty string the handler already printed its own output.
    """
    parts     = raw.lstrip("/").split()
    name      = parts[0].lower() if parts else ""

    # ── /help ──────────────────────────────────────────────────────────────
    if name == "help":
        console.print(Panel(Markdown(_HELP_MD), border_style="blue", title="[bold]Help[/bold]"))
        return "", thread_id

    # ── /clear ─────────────────────────────────────────────────────────────
    if name == "clear":
        new_id = str(uuid.uuid4())
        if conv_history is not None:
            conv_history.clear()
        console.print("[yellow]Memory cleared — new conversation thread started.[/yellow]")
        return "", new_id

    # ── /analyze [--max N] ─────────────────────────────────────────────────
    if name == "analyze":
        import os
        max_n = 0
        p = parts[1:]
        while p:
            if p[0] == "--max" and len(p) > 1:
                try:
                    max_n = int(p[1])
                except ValueError:
                    pass
                p = p[2:]
            else:
                p = p[1:]
        if max_n > 0:
            os.environ["MAX_HOLDINGS_PER_RUN"] = str(max_n)
        with console.status("[yellow]Running full portfolio analysis…[/yellow]", spinner="dots"):
            try:
                report = agent.run_full_analysis(console=console)
            except Exception as exc:
                console.print(f"[bold red]✗ Analysis failed:[/bold red] {exc}")
                return "", thread_id
        if report:
            from src.formatters.output import print_report_to_console
            print_report_to_console(report, console=console)
        return "", thread_id

    # ── /signals ───────────────────────────────────────────────────────────
    if name == "signals":
        return agent.chat("Run the daily ETF composite signal aggregator and show results", thread_id=thread_id), thread_id

    # ── /prompts [category] ────────────────────────────────────────────────
    if name in ("prompts", "prompt", "examples"):
        cat = parts[1] if len(parts) > 1 else ""
        _show_prompts(console, cat)
        return "", thread_id

    # ── /ml ────────────────────────────────────────────────────────────────
    if name == "ml":
        ml_md = _get_ml_status()
        console.print(Panel(Markdown(ml_md), border_style="magenta", title="[bold magenta]ML Model Status[/bold magenta]"))
        return "", thread_id

    # ── /deepdive TICKER ───────────────────────────────────────────────────
    if name == "deepdive":
        ticker = parts[1].upper() if len(parts) > 1 else ""
        if not ticker:
            return "Usage: `/deepdive TICKER`  — e.g. `/deepdive ADSK`", thread_id
        return agent.chat(f"deep-dive {ticker}", thread_id=thread_id), thread_id

    # ── /macro ─────────────────────────────────────────────────────────────
    if name == "macro":
        return agent.chat("Run the macro scanner and show COMEX signals plus FII/DII flows", thread_id=thread_id), thread_id

    # ── /cache [clear] ─────────────────────────────────────────────────────
    if name == "cache":
        from src.utils.llm_cache import get_cache
        cache = get_cache()
        if cache is None:
            return "LLM cache is **disabled** (set `LLM_CACHE_ENABLED=true` in .env to enable).", thread_id
        if len(parts) > 1 and parts[1].lower() == "clear":
            cache.clear()
            return "LLM cache cleared.", thread_id
        s = cache.stats()
        return (
            f"**LLM Cache** (`output/.cache/llm_cache.db`)\n\n"
            f"| Stat | Value |\n|---|---|\n"
            f"| Live entries | {s['live_entries']} |\n"
            f"| Total entries | {s['total_entries']} |\n"
            f"| DB size | {s['db_size_kb']} kB |\n\n"
            f"Use `/cache clear` to wipe all cached responses."
        ), thread_id

    # Unknown
    return f"Unknown command: `/{name}` — type `/help` for the full list.", thread_id


# ── Main loop ─────────────────────────────────────────────────────────────────

def run_chat_loop(console: Console | None = None) -> None:
    """
    Start the interactive REPL.

    Runs until the user types 'quit' / 'exit' / 'q' or presses Ctrl-C.
    """
    if console is None:
        console = Console()

    # Build agent with in-session memory
    from langgraph.checkpoint.memory import MemorySaver
    from src.agents.mosaic_fund_agent import MosaicFundAgent

    console.print(_BANNER)

    with console.status("[yellow]Loading agent…[/yellow]", spinner="dots"):
        agent        = MosaicFundAgent(checkpointer=MemorySaver())
        thread_id    = str(uuid.uuid4())
        _pt_session  = _build_prompt_session()

    # Conversation history buffer — (user_msg, answer, intent) triples.
    # Injected as a prefix into EVERY turn (main + sub-agents) so context is
    # never lost when switching between agents mid-session.
    _conv_history: list[tuple[str, str, str]] = []
    CONTEXT_TURNS = 4            # how many prior turns to inject
    _last_suggestions: list[str] = []   # shown after last response

    from config.settings import settings
    _backend = "ollama" if "11434" in settings.llm_base_url else ("local" if settings.llm_base_url else settings.llm_provider)
    _multiline_hint = "  [dim][Alt+↵ = newline  |  Ctrl+O = newline  |  ↑↓ = history][/dim]" if _pt_session else ""
    console.print(f"[dim]Agent ready  [bold]{settings.llm_model}[/bold] @ {_backend}.  Type your first question.[/dim]{_multiline_hint}\n")

    # Show 3 diverse starter suggestions
    from rich.text import Text as _RText
    _starters = _starter_suggestions(3)
    _sug_line = _RText("  Try: ")
    for i, s in enumerate(_starters):
        _sug_line.append(f"[{i+1}] {s}", style="dim cyan")
        if i < len(_starters) - 1:
            _sug_line.append("  ·  ", style="dim")
    _sug_line.append("  or  ", style="dim")
    _sug_line.append("/prompts", style="bold cyan")
    _sug_line.append(" to browse all", style="dim")
    console.print(_sug_line)
    console.print()

    while True:
        # ── Read input ─────────────────────────────────────────────────────
        try:
            if _pt_session is not None:
                raw = _pt_session.prompt("You: ").strip()
            else:
                raw = input("You: ").strip()
        except (EOFError, KeyboardInterrupt):
            console.print("\n[dim]Goodbye.[/dim]")
            break

        if not raw:
            continue

        # Number shortcut: "1" / "2" / "3" selects the last suggestion
        if raw in ("1", "2", "3") and _last_suggestions:
            idx = int(raw) - 1
            if idx < len(_last_suggestions):
                raw = _last_suggestions[idx]
                console.print(f"[dim]→ {raw}[/dim]\n")

        if raw.lower() in ("quit", "exit", "bye", "q"):
            console.print("[dim]Goodbye.[/dim]")
            break

        # ── Slash commands ─────────────────────────────────────────────────
        if raw.startswith("/"):
            answer, thread_id = _dispatch_slash(raw, console, agent, thread_id, _conv_history)
            if answer:
                console.print(Panel(render_markdown_to_group(answer), border_style="cyan"))
            continue

        # ── @agent override ────────────────────────────────────────────────
        # Syntax: @signal <question>  |  @macro <question>  |  @db <question>  etc.
        # Strips the @tag, forces the named agent, locks the AI planner.
        _at_intent: str | None = None
        _at_tag: str | None = None
        if raw.startswith("@"):
            _parts = raw.split(None, 1)
            _tag = _parts[0][1:].lower()
            if _tag in _AT_AGENT_MAP:
                _at_intent = _AT_AGENT_MAP[_tag]
                _at_tag = _tag
                raw = _parts[1].strip() if len(_parts) > 1 else ""
                if not raw:
                    _valid = " | ".join(
                        f"@{k}" for k in sorted(_AT_AGENT_MAP)
                    )
                    console.print(
                        f"[yellow]Usage: @{_tag} <your question>[/yellow]\n"
                        f"[dim]Valid agents: {_valid}[/dim]"
                    )
                    continue
            else:
                _valid = " | ".join(f"@{k}" for k in sorted(_AT_AGENT_MAP))
                console.print(
                    f"[yellow]Unknown agent '@{_tag}'.[/yellow]\n"
                    f"[dim]Valid: {_valid}[/dim]"
                )
                continue

        # ── Normal chat turn ───────────────────────────────────────────────
        try:
            import os
            from config.settings import settings
            from src.agents.sub_agents import route_intent

            # @agent override bypasses route_intent entirely and locks the planner
            if _at_intent:
                _intent = _at_intent
            else:
                _intent = route_intent(raw)

            _LABEL_MAP = {
                "intl_etf":     "intl ETF agent",
                "news":         "news agent",
                "database":     "database agent",
                "code":         "code agent",
                "signal":       "signal agent",
                "macro":        "macro agent",
                "deepdive":     "deepdive agent",
                "research":     "research agent",
                "india_equity": "equity agent",
                "main":         "main agent",
            }
            _agent_label = _LABEL_MAP.get(_intent, _intent)
            if _at_tag:
                _agent_label += f" (@{_at_tag})"

            _backend = "ollama" if "11434" in settings.llm_base_url else ("local" if settings.llm_base_url else settings.llm_provider)
            if _intent == "code" and settings.code_llm_provider:
                _model_tag  = settings.code_llm_model or settings.llm_model
                _model_back = settings.code_llm_provider
            else:
                _model_tag  = settings.llm_model
                _model_back = _backend

            # @agent override is always locked — planner generates a plan but cannot change the agent.
            # Regex routing for macro/deepdive/intl_etf/research is also locked.
            from src.agents.sub_agents import _IMPORT_RE
            _locked = bool(_at_intent) or _intent in ("macro", "deepdive", "intl_etf", "research") or (
                _intent == "main" and bool(_IMPORT_RE.search(raw))
            )
            _ai_intent, _plan_text, _sql_hint = _build_ai_plan(raw, _intent, locked=_locked)
            if _ai_intent != _intent:
                logger.info(
                    "AI planner overrode routing: %s → %s", _intent, _ai_intent
                )
                _intent = _ai_intent
                _agent_label = {
                    "news":         "news agent",
                    "database":     "database agent",
                    "code":         "code agent",
                    "signal":       "signal agent",
                    "macro":        "macro agent",
                    "deepdive":     "deepdive agent",
                    "research":     "research agent",
                    "india_equity": "equity agent",
                    "main":         "main agent",
                }.get(_intent, _intent)

            console.print(Panel(
                _plan_text,
                title=(
                    f"[bold cyan]Plan[/bold cyan]  "
                    f"[dim]{_model_tag} @ {_model_back}  →  {_agent_label}[/dim]"
                ),
                border_style="cyan",
                padding=(0, 1),
            ))

            # ── Follow-up routing ──────────────────────────────────────────
            # Short follow-up phrases like "compare with HDFC", "vs ICICI",
            # "what about SBIN" should stay on the same agent as the last turn
            # instead of falling through to main.
            if _intent == "main" and _FOLLOWUP_RE.match(raw.strip()) and _conv_history:
                for _, _, _prev_intent in reversed(_conv_history):
                    if _prev_intent not in ("main", ""):
                        logger.info(
                            "Follow-up detected: routing to previous agent '%s'", _prev_intent
                        )
                        _intent = _prev_intent
                        _agent_label = {
                            "news":         "news agent",
                            "database":     "database agent",
                            "code":         "code agent",
                            "signal":       "signal agent",
                            "macro":        "macro agent",
                            "deepdive":     "deepdive agent",
                            "research":     "research agent",
                            "india_equity": "equity agent",
                            "main":         "main agent",
                        }.get(_intent, _intent)
                        break

            # ── Build effective query ──────────────────────────────────────
            _effective_query = raw

            # Inject prior conversation context into ALL turns — both main
            # and sub-agents. Main agent also has MemorySaver but that only
            # covers main-agent turns; cross-agent context lives here.
            if _conv_history:
                recent = _conv_history[-CONTEXT_TURNS:]
                ctx_lines = [
                    "[Session context — prior turns in this conversation]"
                ]
                for _u, _a, _i in recent:
                    _a_short = _a[:400] + "…" if len(_a) > 400 else _a
                    ctx_lines.append(f"User ({_i}): {_u}")
                    ctx_lines.append(f"Assistant: {_a_short}")
                ctx_lines.append("[End of context]\n")
                _effective_query = "\n".join(ctx_lines) + raw

            # DB optimisation: planner already wrote the SQL — inject it so
            # the agent executes directly without re-generating the query.
            if _intent == "database" and _sql_hint:
                _effective_query += (
                    f"\n\nExecute this SQL directly (do not regenerate it):\n"
                    f"```sql\n{_sql_hint}\n```"
                )
                logger.info("DB optimisation: injecting planner SQL into agent query")

            _prev_verbose = os.environ.get("VERBOSE")
            os.environ["VERBOSE"] = "1"
            try:
                answer = agent.chat(_effective_query, thread_id=thread_id, forced_intent=_intent)
            finally:
                if _prev_verbose is None:
                    os.environ.pop("VERBOSE", None)
                else:
                    os.environ["VERBOSE"] = _prev_verbose

            # Persist this turn — store the intent so follow-up routing can reuse it.
            _conv_history.append((raw, answer[:600] + "…" if len(answer) > 600 else answer, _intent))

            _print_answer(console, answer)

            # ── Stale-data import prompt ───────────────────────────────────
            try:
                from src.tools.db_tools import get_stale_hint
                _hint = get_stale_hint()
                if _hint and _hint.get("days_ago", 0) > 0:
                    _days   = _hint["days_ago"]
                    _table  = _hint["table"]
                    _cat    = _hint["category"]
                    _dated  = _hint["last_date"]
                    console.print(
                        f"\n[yellow]⚠ Data freshness warning:[/yellow] "
                        f"[bold]{_table}[/bold] last imported [bold]{_dated}[/bold] "
                        f"({_days} day{'s' if _days != 1 else ''} ago)."
                    )
                    from rich.prompt import Confirm
                    if Confirm.ask(
                        f"  [cyan]Import [bold]{_cat}[/bold] data now?[/cyan]",
                        default=True,
                    ):
                        from src.tools.skills_tools import run_data_engineering_importer
                        with console.status(
                            f"[yellow]Importing {_cat}…[/yellow]", spinner="dots"
                        ):
                            _import_result = run_data_engineering_importer.invoke(
                                {"category": _cat, "full": False}
                            )
                        console.print(f"[green]✓ Import complete.[/green]")
                        # Re-run the original question with fresh data
                        console.print("[dim]Re-running query with fresh data…[/dim]")
                        _rerun_answer = agent.chat(
                            _effective_query, thread_id=thread_id, forced_intent=_intent
                        )
                        _print_answer(console, _rerun_answer)
                        _conv_history.append((
                            raw,
                            _rerun_answer[:600] + "…" if len(_rerun_answer) > 600 else _rerun_answer,
                            _intent,
                        ))
            except Exception as _fe:
                logger.debug("stale-data check failed: %s", _fe)

            # ── Contextual follow-up suggestions
            _last_suggestions = _get_suggestions(raw, answer, _intent)
            if _last_suggestions:
                from rich.text import Text as _RText
                sug = _RText("\n")
                for i, s in enumerate(_last_suggestions, 1):
                    sug.append(f"  [{i}] ", style="bold cyan")
                    sug.append(s + "\n", style="dim")
                console.print(sug)

        except KeyboardInterrupt:
            console.print("\n[yellow]Interrupted.[/yellow]")
        except Exception as exc:
            console.print(f"[bold red]✗ Error:[/bold red] {exc}")
            logger.exception("chat turn failed")
