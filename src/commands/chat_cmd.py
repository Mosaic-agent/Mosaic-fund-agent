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
- Persistent conversation memory via LangGraph SqliteSaver (saved in output/checkpoints.db)
- Intent-based sub-agent auto-routing (deepdive / signal / macro / main)
- Slash commands for direct dispatch and utility actions
- Rich spinner while waiting; Markdown-rendered responses
"""
from __future__ import annotations

import logging
import os
import re
import uuid
import warnings
warnings.filterwarnings("ignore", category=UserWarning)
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

# Short positive/negative confirmations responding to agent prompts/questions.
_CONFIRMATION_RE = re.compile(
    r"^(?:yes|y|no|n|sure|please|ok|okay|yeah|yep|nah|yup|go\s+ahead|do\s+it|indeed|fine|cancel|nevermind|of\s+course|sure\s+thing)\b",
    re.I,
)

# Report-specific follow-up keywords/phrases (e.g. for deepdive, research, equity reports)
_REPORT_FOLLOWUP_RE = re.compile(
    r"\b(?:summarise|summarize|summary|takeaway|takeaways|risk|risks|red\s*flags?|competitor|competitors|financial|financials|valuation|multiple|multiples|sec|filing|filings|detail|details|elaborate|explain|narrative|key|outlook|highlight|highlights)\b"
    r"|\b(?:this|it|its|the|that)\s+(?:report|analysis|company|stock|ticker|filing|filings)\b",
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

# Step items can be:
#   str                          — regular numbered step
#   ("∥", [substeps], "label")  — parallel group rendered as a tree branch
_INTENT_STEPS: dict[str, list] = {
    "india_equity": [
        "Resolve symbol for '{subject}'",
        ("∥", [
            "Fetch price, NAV, 52-week range, momentum",
            "Get quarterly results  [Screener.in / BSE fallback]",
            "Check DSP Mutual Fund holdings",
            "Fetch recent news & sentiment  [GNews + NewsAPI]",
        ], "all fire in parallel"),
        "Plot 30-day price chart → plot_price_chart('{subject}', 30)",
        "Synthesise research note",
    ],
    "signal": [
        "explain_price_anomalies('{subject}', days=90)",
        ("∥", [
            "GARCH fit on full history  [MAD-Z → GARCH(1,1) → Isolation Forest → PELT]",
            "news search per anomaly date  [GNews + NewsAPI]",
            "ml_prediction_asof(date)  [point-in-time ML forecast]",
            "signal_composite_asof(date)  [composite score as-of]",
        ], "ThreadPoolExecutor — all fire simultaneously"),
        "COMEX futures chart  [GC=F correlation]",
        "GARCH volatility trend chart",
        "Synthesise anomaly report",
    ],
    "macro": [
        ("∥", [
            "Scan live macro / geopolitical events",
            "Fetch COMEX gold / silver / copper pre-market",
            "Query FII/DII institutional flows  [7 days]",
            "Fetch DXY (US Dollar Index) trend  [get_dxy_context(30)]",
        ], "all fire in parallel"),
        "Plot FII/DII flow trend → plot_fii_dii_chart(30)",
        "Plot DXY trend chart → plot_dxy_chart(days)  [use days=365 for 1-year]",
        "Map events → ETF directional impact scores",
    ],
    "intl_etf": [
        "Load 3-year price + NAV data  [MAFANG · HNGSNGBEES · MON100 · MASPTOP50 · MAHKTECH · MONQ50]",
        "Run analysis: performance / premium / regime / seasonality / correlation / drawdowns / LightGBM",
        "Plot chart → plot_intl_etf_performance() or plot_intl_etf_premium(symbol)",
        "Summarise key insight  [regime, premium opportunity, best month]",
    ],
    "news": [
        "Resolve '{subject}' to NSE symbol",
        ("∥", [
            "Fetch news from GNews",
            "Fetch news from NewsAPI",
            "Query saved news from ClickHouse news_articles",
        ], "all fire in parallel"),
        "Deduplicate, sort by date, compute sentiment summary",
    ],
    "database": [
        "Identify target table(s) for '{subject}'",
        "Describe schema — confirm column names (FINAL on all tables)",
        "Write and execute SQL query",
        "Format results as Markdown table",
        "Plot chart if time-series → plot_price_chart / plot_fii_dii_chart / plot_signal_scores",
    ],
    "code": [
        "Understand code request: '{subject}'",
        "Search codebase for relevant files / patterns",
        "Write or execute Python code",
        "Validate output and report results",
    ],
    "deepdive": [
        "Resolve ticker for '{subject}'",
        ("∥", [
            "Fetch SEC 10-K / 10-Q from EDGAR",
            "Parse XBRL financials + peer valuation",
            "Fetch exec comp and hiring trends",
        ], "all fire in parallel"),
        "Generate deep-dive research report",
    ],
    "main": [
        "Analyse query: '{subject}'",
        "Call relevant tools  [portfolio, prices, news, ClickHouse]",
        "Synthesise and return answer",
    ],
}


# ── AI planner ────────────────────────────────────────────────────────────────

_VALID_AGENTS = frozenset(
    ["signal", "macro", "news", "equity", "database", "code", "deepdive", "research", "main"]
)

try:
    from pathlib import Path
    _PLANNER_PROMPT = Path("src/prompts/planner_prompt.txt").read_text(encoding="utf-8")
except Exception:
    logger.warning("Could not load planner_prompt.txt — using static fallback")
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
      GARCH risk governor, live iNAV / NAV queries, premium alerts, ETF category news sentiment,
      explaining price anomalies / chart spikes / daily return shocks.
Key tools: run_goldbees_pipeline · run_daily_signal_composite · run_risk_governor_analysis ·
           get_live_inav(symbol) · run_etf_news_sentiment · run_premium_alerts ·
           explain_price_anomalies(symbol, days) · plot_signal_scores ·
           plot_signal_breakdown · plot_weight_recommendations · plot_garch_volatility_chart
Examples:
  "GOLDBEES signal today"        → 1. run_goldbees_pipeline() — report prob_up, regime_signal, blended_50
  "composite scores all ETFs"    → 1. run_daily_signal_composite()  2. plot_signal_scores()
  "GOLDBEES position size"       → 1. run_risk_governor_analysis()  2. plot_garch_volatility_chart("GOLDBEES")
  "iNAV premium alerts"          → 1. run_premium_alerts()
  "premium 6 months"             → run_premium_alerts(lookback=6, lookback_unit="months")
  "premium 1 year"               → run_premium_alerts(lookback=1, lookback_unit="years")
  "premium 90 days"              → run_premium_alerts(lookback=90, lookback_unit="days")
  "what is iNAV of SILVERBEES"   → 1. get_live_inav("SILVERBEES")
  "GOLDBEES current NAV"         → 1. get_live_inav("GOLDBEES")
  "is HNGSNGBEES at premium"     → 1. get_live_inav("HNGSNGBEES")
  "ETF news sentiment"           → 1. run_etf_news_sentiment()
  "explain GOLDBEES price anomalies last 30 days" → 1. explain_price_anomalies(symbol="GOLDBEES", days=30)

── macro ────────────────────────────────────────
When: ANY geopolitical event (Iran, Russia, China, Ukraine, Israel, Gaza, Pakistan, OPEC),
      sanctions, war, conflict, crude oil/energy, gold/silver price drivers, COMEX pre-market,
      FII/DII institutional flows, RBI/Fed rate decisions, USD/INR, DXY / dollar index trend,
      COT reports.
Key tools: run_macro_scanner · run_comex_analysis · get_dxy_context · query_clickhouse_db (fii_dii_flows / cot_gold) ·
           plot_fii_dii_chart
Examples:
  "comex gold signal"            → 1. run_comex_analysis()
  "iran sanctions oil impact"    → 1. run_macro_scanner()
  "FII DII flows last 30 days"   → 1. query_clickhouse_db("SELECT trade_date, fii_net_cr, dii_net_cr FROM market_data.fii_dii_flows FINAL ORDER BY trade_date DESC LIMIT 30")  2. plot_fii_dii_chart(30)
  "COT gold positioning"         → 1. query_clickhouse_db("SELECT report_date, mm_long, mm_short, mm_net FROM market_data.cot_gold FINAL ORDER BY report_date DESC LIMIT 10")
  "dxy trend"                    → 1. get_dxy_context(days=30)  2. run_macro_scanner()
  "dollar index strength"        → 1. get_dxy_context(days=30)

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
           plot_price_chart · plot_fund_holdings_chart · plot_macd_chart
Examples:
  "RELIANCE fundamentals"        → 1. resolve_company("RELIANCE")  2. get_yahoo_finance_data("RELIANCE:NSE")  3. get_quarterly_results("RELIANCE:NSE")  4. get_mf_holdings_for_stock("Reliance")
  "HDFC Bank cashflow"           → 1. resolve_company("HDFC Bank")  2. get_stock_cashflow("HDFCBANK:NSE")
  "TCS MF holdings trend"        → 1. get_mf_holdings_for_stock("TCS")  2. plot_fund_holdings_chart("DSP Top 100", 10)
  "TATASTEEL price momentum"     → 1. resolve_company("TATASTEEL")  2. get_price_momentum("TATASTEEL:NSE")  3. plot_price_chart("TATASTEEL", 90)
  "INFY MACD chart"              → 1. resolve_company("INFY")  2. plot_macd_chart("INFY", 180)

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
  "import GOLDBEES 2019"         → 1. Symbol-specific import → import_symbol_data(symbol="GOLDBEES", start_date="2019-01-01", end_date="2019-12-31")
  "show goldbees price of 2026 to 2019" → 1. import_symbol_data(symbol="GOLDBEES", start_date="2019-01-01", end_date="2026-12-31")  2. plot_price_chart("GOLDBEES", start_date="2019-01-01", end_date="2026-12-31")
  "goldbees 2019"                → 1. import_symbol_data(symbol="GOLDBEES", start_date="2019-01-01", end_date="2019-12-31")  2. plot_price_chart("GOLDBEES", start_date="2019-01-01", end_date="2019-12-31")
  "goldbees trend in 2019"       → 1. import_symbol_data(symbol="GOLDBEES", start_date="2019-01-01", end_date="2019-12-31")  2. plot_price_chart("GOLDBEES", start_date="2019-01-01", end_date="2019-12-31")
NOTE: ONE symbol + custom range → import_symbol_data(symbol, days, start_date, end_date) | plot_price_chart(symbol, days, start_date, end_date) | bulk category → run_data_engineering_importer(category)
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
  tijori_macro_indicators : as_of_date(Date), indicator_code, indicator_name, parent_code, value(Float64), unit
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
  plot_dxy_chart(days)                       — line chart: DXY (US Dollar Index) trend (default 365 = 1 year)
  plot_signal_scores()                       — bar chart: all ETF composite scores
  plot_signal_breakdown('SYM1,SYM2')         — grouped: pillar weights per ETF
  plot_fund_holdings_chart(fund, top_n)      — horizontal bar: holdings by pct_of_nav
  plot_weight_recommendations(method)        — horizontal bar: position weights
  plot_nav_chart(symbol_or_scheme, days)     — line chart: MF/ETF NAV trend (pass NSE symbol e.g. 'GOLDBEES' or numeric scheme code)
  plot_intl_etf_performance()                — bar chart: 3-year total return % for all 6 intl ETFs (intl_etf agent)
  plot_intl_etf_premium(symbol, days)        — line chart: scarcity premium/discount trend for one intl ETF (intl_etf agent)

IMPORTANT: Always include chart tools as explicit numbered plan steps when visualisation adds value.
If a chart is requested but no specific chart tool exists in the list above, route to the `code` or `research` agent and plan to write Python code to build/plot the chart using `plotext` at run time.
Example: "5. Plot 90-day price trend → plot_price_chart('GOLDBEES', 90)"

Today's Date: {today}
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
        if settings.llm_provider == "openrouter":
            from langchain_openai import ChatOpenAI
            _plan_llm = ChatOpenAI(
                model=settings.llm_model,
                base_url="https://openrouter.ai/api/v1",
                api_key=settings.openrouter_api_key,
                request_timeout=30,
                timeout=30,
                **kw,
            )
        elif settings.llm_base_url:
            from langchain_openai import ChatOpenAI
            extra_body = {"options": {"num_ctx": settings.llm_context_window}}
            if settings.llm_think:
                extra_body["think"] = True
            _plan_llm = ChatOpenAI(
                model=settings.llm_model,
                base_url=settings.llm_base_url,
                api_key=settings.openai_api_key or "local",
                request_timeout=120,  # Prevent permanent hangs on local endpoint
                timeout=120,
                extra_body=extra_body,
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
                request_timeout=15,  # Prevent permanent hangs on cloud endpoint
                **kw,
            )
    except Exception as exc:
        logger.warning("_get_plan_llm: could not build LLM: %s", exc)
    return _plan_llm


def parse_query_date_range(query: str) -> tuple[str, str]:
    """
    Dynamically extract date ranges (years, year ranges, month ranges, explicit dates)
    from a user query to help guide planning.
    """
    import re
    import calendar
    from datetime import datetime, date
    
    query_clean = query.lower().strip()
    
    # 1. Match year ranges: "YYYY to YYYY", "YYYY-YYYY", etc.
    range_match = re.search(r'\b(20\d{2})\s*(?:to|and|-)\s*(20\d{2})\b', query_clean)
    if range_match:
        y1, y2 = int(range_match.group(1)), int(range_match.group(2))
        start_year = min(y1, y2)
        end_year = max(y1, y2)
        return f"{start_year}-01-01", f"{end_year}-12-31"
        
    # 2. Match month and year: e.g. "June 2019", "jun 2019", "06/2019", "06-2019"
    months_pattern = r'\b(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\b'
    month_year_match = re.search(months_pattern + r'\s+(20\d{2})\b', query_clean)
    if month_year_match:
        m_name = month_year_match.group(1)
        year = int(month_year_match.group(2))
        months_map = {
            'jan': 1, 'january': 1, 'feb': 2, 'february': 2, 'mar': 3, 'march': 3,
            'apr': 4, 'april': 4, 'may': 5, 'jun': 6, 'june': 6, 'jul': 7, 'july': 7,
            'aug': 8, 'august': 8, 'sep': 9, 'september': 9, 'oct': 10, 'october': 10,
            'nov': 11, 'november': 11, 'dec': 12, 'december': 12
        }
        m_num = months_map[m_name]
        _, last_day = calendar.monthrange(year, m_num)
        return f"{year}-{m_num:02d}-01", f"{year}-{m_num:02d}-{last_day:02d}"
        
    # 3. Match single year: "2019", "in 2019", "of 2019"
    year_match = re.search(r'\b(20\d{2})\b', query_clean)
    if year_match:
        year = year_match.group(1)
        # Verify it's not part of a date format like YYYY-MM-DD or YYYY-MM
        if not re.search(r'\b20\d{2}-\d{2}-\d{2}\b', query_clean) and not re.search(r'\b20\d{2}-\d{2}\b', query_clean):
            return f"{year}-01-01", f"{year}-12-31"
            
    # 4. Match explicit dates: YYYY-MM-DD to YYYY-MM-DD
    date_matches = re.findall(r'\b(20\d{2}-\d{2}-\d{2})\b', query_clean)
    if len(date_matches) >= 2:
        d1, d2 = date_matches[0], date_matches[1]
        try:
            dt1 = datetime.strptime(d1, "%Y-%m-%d").date()
            dt2 = datetime.strptime(d2, "%Y-%m-%d").date()
            if dt1 > dt2:
                d1, d2 = d2, d1
            return d1, d2
        except ValueError:
            pass
    elif len(date_matches) == 1:
        return date_matches[0], ""
        
    return "", ""


_RAG_PLAN_TEMPLATES = [
    (
        "GOLDBEES signal today / ML prediction",
        "signal",
        ["run_goldbees_pipeline() — report prob_up, expected_return_pct, blended_50, and regime_signal"]
    ),
    (
        "composite scores all ETFs today",
        "signal",
        ["run_daily_signal_composite()", "plot_signal_scores()"]
    ),
    (
        "position size / risk governor for GOLDBEES",
        "signal",
        ["run_risk_governor_analysis()", "plot_garch_volatility_chart('{symbol}')"]
    ),
    (
        "premium alerts 90 days",
        "signal",
        ["run_premium_alerts(lookback={days}, lookback_unit='days')"]
    ),
    (
        "what is iNAV of GOLDBEES",
        "signal",
        ["get_live_inav('{symbol}')"]
    ),
    (
        "explain GOLDBEES price anomalies last 30 days",
        "signal",
        ["explain_price_anomalies(symbol='{symbol}', days={days})"]
    ),
    (
        "comex gold signal",
        "macro",
        ["run_comex_analysis()"]
    ),
    (
        "iran sanctions crude oil impact",
        "macro",
        ["run_macro_scanner()", "search_financial_news('Iran sanctions crude oil impact')"]
    ),
    (
        "FII DII flows this week",
        "macro",
        ["Query FII/DII institutional flows from market_data.fii_dii_flows", "plot_fii_dii_chart(30)"]
    ),
    (
        "what is USD-INR doing / DXY context",
        "macro",
        ["get_dxy_context(30)", "plot_dxy_chart(365)"]
    ),
    (
        "latest news on RELIANCE",
        "news",
        ["Resolve symbol for '{symbol}'", "Fetch news from GNews & NewsAPI", "Query saved news from ClickHouse news_articles", "Deduplicate and compute sentiment summary"]
    ),
    (
        "deep dive Apple / annual report EDGAR",
        "deepdive",
        ["Resolve ticker for '{symbol}'", "Fetch SEC 10-K / 10-Q from EDGAR", "Parse XBRL financials & Peer comparison", "Generate deep-dive report"]
    ),
    (
        "import GOLDBEES / refresh stock data",
        "main",
        ["import_symbol_data(symbol='{symbol}', days={days})"]
    ),
    (
        "import fresh FII and DII data",
        "main",
        ["run_data_engineering_importer(category='fii_dii')"]
    ),
    (
        "refresh ETF prices / import indices",
        "main",
        ["run_data_engineering_importer(category='etfs,indices')"]
    ),
    (
        "sync mutual fund holdings",
        "main",
        ["run_data_engineering_importer(category='mf_holdings')"]
    ),
]

_rag_plan_tfidf = None
_rag_plan_embeddings = None


def _build_rag_plan(question: str) -> tuple[str | None, str | None]:
    """
    Build a plan dynamically using local RAG (Embeddings + TF-IDF) over query templates.
    """
    global _rag_plan_tfidf, _rag_plan_embeddings
    import re
    
    # 1. Prepare candidate list
    candidates = [(tmpl[0], str(idx)) for idx, tmpl in enumerate(_RAG_PLAN_TEMPLATES)]
    
    best_idx = None
    best_score = 0.0
    
    # A. Try Ollama Embedding similarity
    try:
        from src.ml.correlation.news_rag import embed_text
        import numpy as np
        
        q_vec = embed_text(question)
        if any(q_vec):
            # Compute embeddings for templates (lazy load & cache in memory)
            if _rag_plan_embeddings is None:
                _rag_plan_embeddings = []
                for tmpl in _RAG_PLAN_TEMPLATES:
                    _rag_plan_embeddings.append(embed_text(tmpl[0]))
                    
            q_norm = np.linalg.norm(q_vec)
            if q_norm > 0:
                for idx, tmpl_vec in enumerate(_rag_plan_embeddings):
                    t_norm = np.linalg.norm(tmpl_vec)
                    if t_norm > 0:
                        sim = np.dot(q_vec, tmpl_vec) / (q_norm * t_norm)
                        if sim > best_score:
                            best_score = sim
                            best_idx = idx
    except Exception as exc:
        logger.debug("_build_rag_plan: Embedding match failed: %s", exc)
        
    # B. Try TF-IDF fallback if embedding score is low
    if best_score < 0.82:
        try:
            from src.agents.intent_router import SimpleTFIDF
            if _rag_plan_tfidf is None:
                _rag_plan_tfidf = SimpleTFIDF(candidates)
            matched_idx_str, tfidf_score = _rag_plan_tfidf.similarity(question)
            if tfidf_score >= 0.45:
                best_idx = int(matched_idx_str)
                best_score = tfidf_score
                logger.info("RAG Planner (TF-IDF): matched template %r (score=%.3f)", _RAG_PLAN_TEMPLATES[best_idx][0], best_score)
        except Exception as exc:
            logger.debug("_build_rag_plan: TF-IDF match failed: %s", exc)
            
    if best_idx is None or best_score < 0.45:
        return None, None
        
    matched_template = _RAG_PLAN_TEMPLATES[best_idx]
    intent = matched_template[1]
    steps_template = matched_template[2]
    
    # 2. Extract parameters (symbol, days)
    symbol = "GOLDBEES"  # default
    try:
        from src.tools.company_resolver import _local_indian_lookup
        words = [w.strip("?,.!") for w in question.split()]
        for w in words:
            if w.isupper() and len(w) >= 3:
                symbol = w
                break
            sym_lookup = _local_indian_lookup(w)
            if sym_lookup:
                symbol = sym_lookup
                break
    except Exception:
        pass
        
    days = 90  # default
    match = re.search(r"\b(\d+)\s*(?:day|days|month|months|year|years|d|m|y)\b", question, re.I)
    if match:
        days = int(match.group(1))
    else:
        match_num = re.search(r"\b(\d+)\b", question)
        if match_num:
            days = int(match_num.group(1))
            
    # 3. Format plan steps
    formatted_steps = []
    for step in steps_template:
        formatted_steps.append(step.replace("{symbol}", symbol).replace("{days}", str(days)))
        
    plan_text = _render_plan_steps(formatted_steps, symbol)
    logger.info("RAG Planner: generated plan for %s / %s", symbol, intent)
    return intent, plan_text


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
            from datetime import date
            today_str = date.today().strftime("%Y-%m-%d")
            prompt = _PLANNER_PROMPT.format(today=today_str, query=question[:300])
            
            # Dynamic Date range detection system hint
            start_date, end_date = parse_query_date_range(question)
            if start_date:
                prompt += (
                    f"\n[SYSTEM HINT - DYNAMIC DATE RANGE DETECTED]\n"
                    f"The user query implies a specific date boundary:\n"
                    f"  - start_date: '{start_date}'\n"
                    f"  - end_date: '{end_date or ''}'\n"
                    f"In your generated plan, make sure all tool calls like `import_symbol_data(...)` "
                    f"and `plot_price_chart(...)` use these exact dates as parameters "
                    f"(e.g. `import_symbol_data(symbol='...', start_date='{start_date}', end_date='{end_date}')` "
                    f"and `plot_price_chart('...', start_date='{start_date}', end_date='{end_date}')`).\n"
                )
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

    # ── RAG-based plan search ──────────────────────────────────────────────
    try:
        rag_intent, rag_plan_text = _build_rag_plan(question)
        if rag_plan_text:
            return rag_intent or regex_intent, rag_plan_text, None
    except Exception as exc:
        logger.debug("_build_ai_plan: RAG plan lookup failed (%s)", exc)

    # ── Deterministic fallback ─────────────────────────────────────────────
    return regex_intent, _build_fallback_plan(question, regex_intent), None


def _render_plan_steps(steps: list, subject: str) -> str:
    """
    Render a plan step list as Rich markup.

    Items can be:
      str                        → numbered step  "  1. text"
      ("∥", [substeps], label)  → parallel tree branch with label on the middle row
    """
    lines: list[str] = []
    num = 1
    for step in steps:
        if isinstance(step, tuple) and step[0] == "∥":
            _, substeps, label = step[0], step[1], step[2] if len(step) > 2 else ""
            n = len(substeps)
            mid = n // 2  # row where the label annotation appears
            for i, sub in enumerate(substeps):
                if n == 1:
                    branch = "──"
                elif i == 0:
                    branch = "┌─"
                elif i == n - 1:
                    branch = "└─"
                else:
                    branch = "├─"
                annotation = f"  [dim italic]◀ {label}[/dim italic]" if (i == mid and label) else ""
                lines.append(
                    f"     [dim]{branch}[/dim] [dim]{sub.format(subject=subject)}[/dim]{annotation}"
                )
        else:
            lines.append(f"  [cyan]{num}.[/cyan] {step.format(subject=subject)}")
            num += 1
    return "\n".join(lines)


def _build_fallback_plan(question: str, intent: str) -> str:
    """Static template plan — used when the LLM planner is unavailable."""
    subject = question.strip()
    sym = None
    try:
        from src.tools.company_resolver import _local_indian_lookup
        sym = _local_indian_lookup(subject)
        subject = sym if sym else " ".join(question.split()[:4]) + ("…" if len(question.split()) > 4 else "")
    except Exception:
        subject = " ".join(question.split()[:4])

    # Dynamic date range checking for fallback
    start_date, end_date = parse_query_date_range(question)
    if (sym or subject.upper() in {"GOLDBEES", "NIFTYBEES", "BANKBEES", "SILVERBEES"}) and start_date:
        resolved_sym = sym or subject.upper()
        steps = [
            f"import_symbol_data(symbol='{resolved_sym}', start_date='{start_date}', end_date='{end_date}')",
            f"plot_price_chart('{resolved_sym}', start_date='{start_date}', end_date='{end_date}')"
        ]
        return _render_plan_steps(steps, subject)

    steps = _INTENT_STEPS.get(intent, _INTENT_STEPS["main"])
    if intent == "india_equity" and "macd" in question.lower():
        steps = list(steps)
        # Find index of "Synthesise research note" to insert before it
        try:
            idx = steps.index("Synthesise research note")
            steps.insert(idx, "Plot MACD chart and analyze momentum → plot_macd_chart('{subject}', 180)")
        except ValueError:
            steps.append("Plot MACD chart and analyze momentum → plot_macd_chart('{subject}', 180)")

    return _render_plan_steps(steps, subject)


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
                   cv_r2_mean, goldbees_close, confidence_low, confidence_high,
                   prob_up, cv_auc_mean
            FROM market_data.ml_predictions FINAL
            ORDER BY as_of DESC LIMIT 1
        """)
        if not df.empty:
            row = df.iloc[0]
            prob_up = row.get("prob_up", 0.5)
            cv_auc_mean = row.get("cv_auc_mean", 0.5)
            lines += [
                "",
                "### Latest ML Prediction (GOLDBEES)",
                f"| Field | Value | Detail |",
                f"|---|---|---|",
                f"| as_of | {row['as_of']} | Date of model execution |",
                f"| expected_return_pct | **{row['expected_return_pct']:.2f}%** | Expected 5-day return |",
                f"| probability_up | **{prob_up:.1%}** | Classifier probability GOLDBEES goes up |",
                f"| confidence_band | [{row['confidence_low']:.2f}%, {row['confidence_high']:.2f}%] | 5-day quantile bounds |",
                f"| regime_signal | **{row['regime_signal']}** | Dynamic regime signal |",
                f"| Model AUC (CV mean) | {cv_auc_mean:.4f} | Validation AUC (>0.50 = edge) |",
                f"| cv_r2_mean (skill) | {row['cv_r2_mean']:.4f} | Centred skill score (AUC - 0.5) |",
                f"| goldbees_close | ₹{row['goldbees_close']:.2f} | Benchmark close at forecast time |",
            ]

        # Calculate validation history
        try:
            import numpy as np
            import pandas as pd
            preds_all = query_df("""
                SELECT
                    as_of,
                    horizon_days,
                    expected_return_pct,
                    regime_signal,
                    goldbees_close AS start_price
                FROM market_data.ml_predictions FINAL
                ORDER BY as_of ASC
            """)
            prices_all = query_df("""
                SELECT
                    trade_date,
                    argMax(close, imported_at) AS close
                FROM market_data.daily_prices
                WHERE symbol = 'GOLDBEES' AND category = 'etfs'
                GROUP BY trade_date
                ORDER BY trade_date ASC
            """)
            if not preds_all.empty and not prices_all.empty:
                preds_all["as_of"]       = pd.to_datetime(preds_all["as_of"])
                prices_all["trade_date"] = pd.to_datetime(prices_all["trade_date"])

                price_idx = pd.DatetimeIndex(prices_all["trade_date"])
                price_map = prices_all.set_index("trade_date")["close"].to_dict()

                results = []
                for _, prow in preds_all.iterrows():
                    as_of = prow["as_of"]
                    horizon = int(prow["horizon_days"])
                    start_price = float(prow["start_price"])

                    if start_price <= 0:
                        continue

                    entry_pos = price_idx.searchsorted(as_of, side="left")
                    exit_pos  = entry_pos + horizon
                    if exit_pos >= len(price_idx):
                        continue

                    end_date  = price_idx[exit_pos]
                    end_price = float(price_map[end_date])

                    actual_logret    = np.log(end_price / start_price) * 100
                    predicted_logret = float(prow["expected_return_pct"])

                    pred_sign   = np.sign(predicted_logret)
                    actual_sign = np.sign(actual_logret)
                    hit = int(pred_sign == actual_sign) if pred_sign != 0 else None

                    results.append({
                        "as_of":     as_of.date(),
                        "end_date":  end_date.date(),
                        "regime":    prow["regime_signal"],
                        "predicted": predicted_logret,
                        "actual":    actual_logret,
                        "hit":       hit,
                    })

                if results:
                    eval_df = pd.DataFrame(results)
                    directional = eval_df[eval_df["hit"].notna()]
                    hit_ratio   = directional["hit"].mean() if not directional.empty else float("nan")
                    mae         = eval_df["predicted"].sub(eval_df["actual"]).abs().mean()
                    rmse        = np.sqrt(eval_df["predicted"].sub(eval_df["actual"]).pow(2).mean())

                    lines += [
                        "",
                        "### Model Validation & Out-of-Sample Backtest History",
                        f"| Metric | Value | Reference / Threshold |",
                        f"|---|---|---|",
                        f"| **Out-of-Sample Hit Ratio** | **{hit_ratio:.1%}** | >50% indicates directional edge |",
                        f"| **Mean Absolute Error (MAE)** | {mae:.2f}% | Average forecast magnitude error |",
                        f"| **Root Mean Squared Error (RMSE)** | {rmse:.2f}% | Penalizes larger forecast errors |",
                        f"| **Realised Predictions (n)** | {len(eval_df)} | Completed trading-day horizons |",
                        "",
                        "#### Recent Realised Out-of-Sample Outcomes",
                        "| As Of | Target Date | Regime | Predicted Return | Realised Return | Hit |",
                        "|---|---|---|---|---|---|",
                    ]
                    for _, r in eval_df.tail(6).iterrows():
                        hit_str = "✓" if r["hit"] == 1 else "✗" if r["hit"] == 0 else "—"
                        lines.append(
                            f"| {r['as_of']} | {r['end_date']} | {r['regime']} | "
                            f"{r['predicted']:+.2f}% | {r['actual']:+.2f}% | {hit_str} |"
                        )
        except Exception as eval_exc:
            lines.append(f"\n*Validation metrics compilation failed: {eval_exc}*")

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


def _is_numeric_choice_prompt(answer: str) -> bool:
    """
    Detect if the assistant's answer is a prompt asking the user to choose
    from numbered options (e.g., 1, 2, or 3 for data source selection).
    """
    if not answer:
        return False
    lower_ans = answer.lower()
    
    # Check if there are common choice prompt phrases
    choice_phrases = [
        "which source", 
        "data_source_required", 
        "enter 1, 2", 
        "enter 1, 2, or 3", 
        "choose 1, 2", 
        "select 1, 2",
        "which of the following",
        "choose an option"
    ]
    if any(phrase in lower_ans for phrase in choice_phrases):
        return True
        
    # Check if there are numbered options 1, 2, 3 and words indicating a question/choice
    has_numbers = "1" in lower_ans and "2" in lower_ans and "3" in lower_ans
    has_indicators = any(word in lower_ans for word in ("enter", "choose", "select", "prefer", "option", "source", "shoonya", "yfinance"))
    if has_numbers and has_indicators:
        # Check if they are structured as options (e.g., "1." or "1)" or "1 ")
        import re
        if re.search(r'\b1[\s\.\)]+\w+', lower_ans) and re.search(r'\b2[\s\.\)]+\w+', lower_ans) and re.search(r'\b3[\s\.\)]+\w+', lower_ans):
            return True
            
    return False


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

    # Convert LaTeX math notation the LLM sometimes emits into Unicode equivalents
    # so they render correctly in the terminal instead of showing raw LaTeX.
    _LATEX_MAP = [
        (r"$\leftarrow$",  "←"),
        (r"$\rightarrow$", "→"),
        (r"$\uparrow$",    "↑"),
        (r"$\downarrow$",  "↓"),
        (r"$\times$",      "×"),
        (r"$\leq$",        "≤"),
        (r"$\geq$",        "≥"),
        (r"$\neq$",        "≠"),
        (r"$\approx$",     "≈"),
        (r"$\infty$",      "∞"),
        (r"$\sigma$",      "σ"),
        (r"$\alpha$",      "α"),
        (r"$\beta$",       "β"),
        (r"$\omega$",      "ω"),
        (r"\leftarrow",    "←"),
        (r"\rightarrow",   "→"),
        (r"\times",        "×"),
    ]
    for latex, uni in _LATEX_MAP:
        answer = answer.replace(latex, uni)

    # Detect a chart block: 3+ consecutive lines that start with box chars or spaces+box.
    lines = answer.splitlines()
    chart_lines: list[int] = [
        i for i, ln in enumerate(lines)
        if any(c in ln for c in _CHART_CHARS)
    ]

    # Find all contiguous ranges of chart lines
    blocks = []
    if chart_lines:
        current_block = [chart_lines[0]]
        for idx in chart_lines[1:]:
            # If the line is within 3 lines of the previous chart line, group it
            if idx - current_block[-1] <= 3:
                current_block.append(idx)
            else:
                blocks.append(current_block)
                current_block = [idx]
        blocks.append(current_block)

    # Filter out blocks that have fewer than 5 chart lines total (too small to be a real chart)
    valid_blocks = [blk for blk in blocks if len(blk) >= 5]

    if valid_blocks:
        extended_blocks = []
        for i, blk in enumerate(valid_blocks):
            first, last = blk[0], blk[-1]
            limit = valid_blocks[i+1][0] if i + 1 < len(valid_blocks) else len(lines)
            
            import re
            while last + 1 < limit:
                next_line = lines[last + 1].strip()
                if next_line and (
                    any(c in next_line for c in ("/", "→", "₹", "Cr", "$")) or
                    re.search(r'^\d+(\s+\d+)*$', next_line) or
                    re.search(r'^\d{2}/\d{2}', next_line) or
                    re.search(r'^[0-9\s\-\:\.\/]+$', next_line)
                ):
                    last += 1
                else:
                    break
            extended_blocks.append((first, last))

        current_idx = 0
        for first, last in extended_blocks:
            # Print text before this chart block
            if first > current_idx:
                text_block = "\n".join(lines[current_idx:first]).strip()
                if text_block:
                    console.print(Panel(render_markdown_to_group(text_block), border_style="green"))
            
            # Print this chart block
            chart_block = "\n".join(lines[first:last + 1])
            _t = Text.from_ansi(chart_block)
            _t.no_wrap = True
            console.print(Panel(_t, border_style="blue", title="Chart", expand=False))
            
            current_idx = last + 1
            
        # Print remaining text after the last chart block
        if current_idx < len(lines):
            text_block = "\n".join(lines[current_idx:]).strip()
            if text_block:
                console.print(Panel(render_markdown_to_group(text_block), border_style="green"))
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
| `/caveman [level]` | Toggle Caveman mode (`lite`/`full`/`ultra`/`wenyan`/`off`) |
| `/cache` | Show LLM cache stats; `/cache clear` wipes cached responses |
| `/telemetry` | View telemetry; `/telemetry on` or `off` toggles turn overlay |
| `/clear` | Reset session memory — next question starts a fresh thread |
| `/list thread` | List all previous conversation threads with summaries |
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
        console.print(f"[yellow]Memory cleared — new conversation thread started:[/yellow] [bold cyan]{new_id}[/bold cyan]")
        return "", new_id

    # ── /list thread / /threads ───────────────────────────────────────────
    if name in ("threads", "thread") or (name == "list" and len(parts) > 1 and parts[1].lower() in ("thread", "threads")):
        checkpointer = getattr(agent, "_checkpointer", None)
        if checkpointer is None:
            console.print("[yellow]No checkpoints database found (thread history is unavailable).[/yellow]")
            return "", thread_id

        from rich.table import Table
        table = Table(title="[bold cyan]Conversation Threads[/bold cyan]", border_style="cyan")
        table.add_column("Thread ID", style="cyan", no_wrap=True)
        table.add_column("Last Active", style="green")
        table.add_column("Messages", style="magenta")
        table.add_column("Initial Query / Summary", style="white")

        threads = {}
        try:
            for cp_tuple in checkpointer.list(None):
                cfg = cp_tuple.config
                tid = cfg.get("configurable", {}).get("thread_id")
                if not tid:
                    continue
                
                msgs = cp_tuple.checkpoint.get("channel_values", {}).get("messages", [])
                ts = cp_tuple.checkpoint.get("ts")
                
                if tid not in threads or len(msgs) > len(threads[tid]["messages"]):
                    threads[tid] = {
                        "ts": ts,
                        "messages": msgs
                    }
        except Exception as exc:
            console.print(f"[bold red]Error loading thread history:[/bold red] {exc}")
            return "", thread_id

        if not threads:
            console.print("[yellow]No conversation history found.[/yellow]")
            return "", thread_id

        # Sort threads by timestamp descending
        def get_sort_key(item):
            val = item[1]["ts"]
            return str(val) if val is not None else ""

        sorted_threads = sorted(threads.items(), key=get_sort_key, reverse=True)

        for tid, data in sorted_threads:
            ts_val = data["ts"]
            ts_str = ""
            if ts_val:
                if hasattr(ts_val, "strftime"):
                    ts_str = ts_val.strftime("%Y-%m-%d %H:%M:%S")
                else:
                    ts_str = str(ts_val).split(".")[0].replace("T", " ")
                    
            msgs = data["messages"]
            num_msgs = len(msgs)
            
            first_human = None
            for m in msgs:
                if m.__class__.__name__ == "HumanMessage" or getattr(m, "type", None) == "human":
                    first_human = m.content
                    break
                    
            if first_human:
                summary = first_human.strip()
                if "[End of context]\n" in summary:
                    summary = summary.split("[End of context]\n", 1)[1].strip()
                if len(summary) > 80:
                    summary = summary[:77] + "..."
            else:
                summary = "[No user queries]"
                
            table.add_row(tid, ts_str, f"{num_msgs} msgs", summary)

        console.print(table)
        console.print(f"\n[dim]To resume a thread, restart the chat with: [cyan]--thread-id <id>[/cyan] or [cyan]-t <id>[/cyan][/dim]")
        return "", thread_id

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

    # ── /telemetry [on/off] ────────────────────────────────────────────────
    if name == "telemetry":
        sub_arg = parts[1].lower() if len(parts) > 1 else ""
        if sub_arg in ("on", "enable", "start"):
            import os
            os.environ["MOSAIC_AUTO_TELEMETRY"] = "1"
            console.print("[green]✓ Auto telemetry overlay enabled. Will display system stats after each query.[/green]\n")
            return "", thread_id
        elif sub_arg in ("off", "disable", "stop"):
            import os
            os.environ.pop("MOSAIC_AUTO_TELEMETRY", None)
            console.print("[yellow]✓ Auto telemetry overlay disabled.[/yellow]\n")
            return "", thread_id
        else:
            from src.scripts.portfolio.system_telemetry import get_dashboard_renderable
            console.print(get_dashboard_renderable())
            return "", thread_id

    # ── /caveman [level] ───────────────────────────────────────────────────
    if name == "caveman":
        import os
        level = parts[1].lower() if len(parts) > 1 else "full"
        if level in ("off", "stop", "normal", "disabled"):
            os.environ.pop("CAVEMAN_LEVEL", None)
            return "Caveman mode **disabled**. Reverted to normal prose.", thread_id
        else:
            valid_levels = ("lite", "full", "ultra", "wenyan", "wenyan-lite", "wenyan-full", "wenyan-ultra")
            if level not in valid_levels:
                return f"Invalid caveman level. Valid levels: {', '.join(valid_levels)} or 'off'.", thread_id
            os.environ["CAVEMAN_LEVEL"] = level
            return f"Caveman mode **enabled** (level: `{level}`). Less waffle, more speed.", thread_id

    # Unknown
    return f"Unknown command: `/{name}` — type `/help` for the full list.", thread_id


def extract_company_subject(question: str, intent: str) -> str | None:
    """Extract a candidate company name from the query if it targets stock/news research."""
    import re
    from src.agents.sub_agents import _GENERAL_RESEARCH_RE
    
    clean = question.strip().rstrip("?.")
    
    # Strip common command verbs from the start of the query
    clean = re.sub(r"^(import|refresh|sync|update|backfill)\s+", "", clean, flags=re.I)
    
    # If it is news intent, strip news-related prefixes/suffixes
    if intent == "news":
        clean = re.sub(r"\b(latest\s+)?news(\s+(for|on|about|of))?\b", "", clean, flags=re.I)
        clean = re.sub(r"\bheadlines(\s+(for|on|about|of))?\b", "", clean, flags=re.I)
        clean = clean.strip()
        
    m = _GENERAL_RESEARCH_RE.search(clean)
    if m:
        subj = m.group(1).strip().rstrip("?.")
        # Strip leading prepositions (e.g. "on HDFC Bank" -> "HDFC Bank")
        subj = re.sub(r"^(on|about|for|of|to)\s+", "", subj, flags=re.I)
        if len(subj.split()) <= 4:
            if not subj.startswith("-") and subj.lower() not in ("etfs", "stocks", "mf", "fii_dii", "cot", "fx_rates", "inav"):
                return subj
            
    # For short queries, check if it's a potential company name
    if len(clean.split()) <= 3:
        # Avoid resolving generic commands or common non-company queries
        if clean.lower() not in ("quit", "exit", "bye", "q", "help", "/help", "/prompts", "/goal", "/schedule", "/grill-me"):
            # Skip numbers, dates, flags and generic category names
            if not clean.isdigit() and not clean.startswith("-"):
                if clean.lower() not in ("etfs", "stocks", "mf", "fii_dii", "cot", "fx_rates", "inav"):
                    return clean
            
    return None


# ── Main loop ─────────────────────────────────────────────────────────────────

def run_chat_loop(console: Console | None = None, thread_id: str | None = None) -> None:
    """
    Start the interactive REPL.

    Runs until the user types 'quit' / 'exit' / 'q' or presses Ctrl-C.
    """
    if console is None:
        console = Console()

    # Ensure output directory exists for checkpoints database
    import os
    os.makedirs("output", exist_ok=True)

    from langgraph.checkpoint.sqlite import SqliteSaver
    with SqliteSaver.from_conn_string("output/checkpoints.db") as checkpointer:
        _run_chat_loop_inner(console=console, checkpointer=checkpointer, thread_id=thread_id)


def _run_chat_loop_inner(console: Console, checkpointer: Any, thread_id: str | None = None) -> None:
    # Set environment variable to indicate interactive session for resolver prompts
    import os
    os.environ["MOSAIC_INTERACTIVE_CHAT"] = "1"

    # Build agent with persistent sqlite checkpointer
    from src.agents.mosaic_fund_agent import MosaicFundAgent

    console.print(_BANNER)

    with console.status("[yellow]Loading agent…[/yellow]", spinner="dots"):
        agent        = MosaicFundAgent(checkpointer=checkpointer)
        is_resuming  = (thread_id is not None)
        if not thread_id:
            thread_id = str(uuid.uuid4())
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

    if is_resuming:
        console.print(f"[green]Resuming conversation thread:[/green] [bold cyan]{thread_id}[/bold cyan]\n")
    else:
        console.print(f"[green]Active conversation thread:[/green] [bold cyan]{thread_id}[/bold cyan] [dim](Use --thread-id {thread_id} to resume later)[/dim]\n")


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
            import os
            _caveman_level = os.environ.get("CAVEMAN_LEVEL")
            _prompt_prefix = f"You (🪨 {_caveman_level}): " if _caveman_level else "You: "
            if _pt_session is not None:
                raw = _pt_session.prompt(_prompt_prefix).strip()
            else:
                raw = input(_prompt_prefix).strip()
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

        if raw.lower() in ("stop caveman", "normal mode"):
            import os
            os.environ.pop("CAVEMAN_LEVEL", None)
            console.print("[yellow]Caveman mode disabled. Reverted to normal prose.[/yellow]\n")
            continue

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

            # ── Pre-resolve company name if applicable ──────────────────────
            # This allows the interactive prompt to run and get user confirmation
            # BEFORE the AI planner generates its plan, ensuring the visual plan
            # panel contains the correct symbol/company.
            _subject = extract_company_subject(raw, _intent)
            if _subject and _intent in ("india_equity", "research", "deepdive", "news", "signal", "main", "intl_etf"):
                try:
                    from src.tools.company_resolver import resolve_company_info
                    _info = resolve_company_info(_subject, auto_import=True)
                    if _info and not _info.get("error") and _info.get("symbol"):
                        _symbol = _info["symbol"]
                        # Replace the subject in our raw query with the resolved symbol
                        # (e.g. "tata" becomes "TATAPOWER")
                        import re
                        raw = re.sub(r'\b' + re.escape(_subject) + r'\b', _symbol, raw, flags=re.I)
                        logger.info("Chat pre-resolved company subject '%s' to '%s'", _subject, _symbol)
                        # Re-calculate intent on the cleaned query to be precise
                        if not _at_intent:
                            _intent = route_intent(raw)
                except Exception as exc:
                    logger.debug("Chat pre-resolution failed: %s", exc)

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

            _ctx_size = settings.code_llm_context_window or settings.llm_context_window if _intent == "code" else settings.llm_context_window
            console.print(Panel(
                _plan_text,
                title=(
                    f"[bold cyan]Plan[/bold cyan]  "
                    f"[dim]{_model_tag} @ {_model_back} ({_ctx_size} ctx)  →  {_agent_label}[/dim]"
                ),
                border_style="cyan",
                padding=(0, 1),
            ))

            # ── Follow-up routing ──────────────────────────────────────────
            # Short follow-up phrases like "compare with HDFC", "vs ICICI",
            # "what about SBIN" should stay on the same agent as the last turn
            # instead of falling through to main.
            # ALSO: Short answers/responses (like "yes", "no", "y", "n", "sure", "ok")
            # or any response when the previous assistant message ended with a question
            # should stay on the previous agent.
            _is_followup = False
            if _intent == "main" and _conv_history:
                _prev_raw, _prev_answer, _prev_intent = _conv_history[-1]
                if _FOLLOWUP_RE.match(raw.strip()):
                    _is_followup = True
                elif _CONFIRMATION_RE.match(raw.strip()) and len(raw.split()) <= 4:
                    _is_followup = True
                elif _is_numeric_choice_prompt(_prev_answer):
                    _cleaned_input = raw.strip().lower()
                    if _cleaned_input.isdigit() or _cleaned_input in ("shoonya", "nse", "yfinance"):
                        _is_followup = True
                elif _prev_intent in ("deepdive", "research", "india_equity"):
                    if _REPORT_FOLLOWUP_RE.search(raw.strip()):
                        _is_followup = True
                else:
                    _cleaned_answer = _prev_answer.strip().rstrip("`").strip().rstrip("*").strip()
                    if _cleaned_answer.endswith("?") and len(raw.split()) <= 8:
                        _is_followup = True

            if _is_followup:
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
                    if _i in ("deepdive", "research", "india_equity"):
                        _a_short = _a
                    else:
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
            _conv_history.append((raw, answer, _intent))

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
            if _is_numeric_choice_prompt(answer):
                if any(w in answer.lower() for w in ("shoonya", "yfinance", "nse")):
                    _last_suggestions = ["Shoonya", "NSE", "yfinance"]
                else:
                    _last_suggestions = ["1", "2", "3"]
            else:
                _last_suggestions = _get_suggestions(raw, answer, _intent)
            if _last_suggestions:
                from rich.text import Text as _RText
                sug = _RText("\n")
                for i, s in enumerate(_last_suggestions, 1):
                    sug.append(f"  [{i}] ", style="bold cyan")
                    sug.append(s + "\n", style="dim")
                console.print(sug)

            # ── Telemetry Overlay ──────────────────────────────────────────
            if os.environ.get("MOSAIC_AUTO_TELEMETRY") == "1":
                try:
                    from src.scripts.portfolio.system_telemetry import get_compact_telemetry_renderable
                    console.print(get_compact_telemetry_renderable())
                except Exception as _te:
                    logger.debug("telemetry overlay print failed: %s", _te)

        except KeyboardInterrupt:
            console.print("\n[yellow]Interrupted.[/yellow]")
        except Exception as exc:
            console.print(f"[bold red]✗ Error:[/bold red] {exc}")
            logger.exception("chat turn failed")
