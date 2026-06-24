"""
agent_tools.py — cross-agent tools for the AutonomousResearchAgent.

Provides:
  - check_and_refresh_symbol_data  : check ClickHouse price freshness and auto-import if stale
  - delegate_to_signal_agent       : run the Signal sub-agent (ETF signals, Kelly, GARCH)
  - delegate_to_macro_agent        : run the Macro sub-agent (COMEX, FII/DII, geopolitical)
  - delegate_to_intl_etf_agent     : run the IntlETF sub-agent (premium, regime, seasonality)
  - delegate_to_news_agent         : run the News sub-agent (multi-source news + sentiment)
  - delegate_to_india_equity_agent : run the IndiaEquity sub-agent (full stock research note)

All imports from src.agents.sub_agents are lazy (inside function bodies) to avoid
circular imports — the same pattern used in every _get_tools() method in sub_agents.py.
"""

from langchain_core.tools import tool

# NSE symbol → import category.  Mirrors the ETFS list in src/importer/registry.py.
_SYMBOL_CATEGORY: dict[str, str] = {
    "NIFTYBEES":  "etfs",
    "JUNIORBEES": "etfs",
    "GOLDBEES":   "etfs",
    "LIQUIDBEES": "etfs",
    "BANKBEES":   "etfs",
    "PSUBNKBEES": "etfs",
    "SILVERBEES": "etfs",
    "HNGSNGBEES": "etfs",
    "MAFANG":     "etfs",
    "MON100":     "etfs",
    "MAHKTECH":   "etfs",
    "MASPTOP50":  "etfs",
    "MONQ50":     "etfs",
    "HDFCNIFTY":  "etfs",
    "SETFNIF50":  "etfs",
    "ICICIB22":   "etfs",
    "GOLDCASE":   "etfs",
    "SILVERCASE": "etfs",
    "TOP100CASE": "etfs",
    "MID150CASE": "etfs",
    "SMALLCAP":   "etfs",
    "LTGILTCASE": "etfs",
}

# Staleness thresholds (calendar days).
# ETFs trade weekdays only — Friday close shows 3 days old on Monday, 4 on Tuesday.
# 7 days tolerates normal weekends + Indian market holidays without false-positive imports.
_ETF_STALE_DAYS   = 7
_STOCK_STALE_DAYS = 4


# ── Data availability + auto-import ───────────────────────────────────────────

@tool
def check_and_refresh_symbol_data(symbol: str, auto_import: bool = True) -> str:
    """
    Check whether price data for an NSE symbol exists and is fresh in ClickHouse
    (market_data.daily_prices). If missing or stale, optionally trigger the data
    engineering importer to refresh it before analysis begins.

    Staleness thresholds (weekday-only sources):
      ETFs   — 7 calendar days  (covers weekends + Indian market holidays)
      Stocks — 4 calendar days

    Call this BEFORE running price-based analysis (momentum, correlations, GARCH)
    for any symbol the user explicitly named. Do NOT call it for every ETF in a
    broad scan — only for the 1–3 primary symbols under study.

    Args:
        symbol:      NSE symbol to check (e.g. "GOLDBEES", "HNGSNGBEES", "RELIANCE").
                     Uppercased automatically.
        auto_import: If True (default), run the importer when data is missing or stale.
                     Set to False to only report status without triggering an import.

    Returns:
        A status string with one of these prefixes:
          FRESH:          data is current — proceed with analysis
          REFRESHED:      import ran and last trade_date advanced — proceed
          UNCHANGED:      import ran but no new rows (weekend/holiday) — proceed
          STALE:          stale and auto_import=False — manual import needed
          IMPORT_FAILED:  import raised an error — proceed with caveat in report
          UNKNOWN_SYMBOL: symbol not in price database — use Yahoo Finance instead
    """
    sym = symbol.strip().upper()

    # Resolve category from static map first (fast path)
    category: str | None = _SYMBOL_CATEGORY.get(sym)

    try:
        from src.db.pool import query_df
        import pandas as pd

        # Fallback: detect category from daily_prices itself (handles new symbols)
        if category is None:
            cat_df = query_df(
                f"SELECT DISTINCT category FROM market_data.daily_prices FINAL "
                f"WHERE symbol = '{sym}' LIMIT 1"
            )
            if not cat_df.empty:
                category = str(cat_df.iloc[0]["category"])
            else:
                return (
                    f"UNKNOWN_SYMBOL: {sym} — not found in price database or known ETF "
                    f"registry. Use Yahoo Finance (get_yahoo_finance_data) for price data. "
                    f"To add: run `import --category etfs` or `import --category stocks`."
                )

        stale_days = _ETF_STALE_DAYS if category == "etfs" else _STOCK_STALE_DAYS

        # Query actual last trade_date for this symbol
        fresh_df = query_df(
            f"SELECT max(trade_date) AS last_date "
            f"FROM market_data.daily_prices FINAL "
            f"WHERE symbol = '{sym}' AND category = '{category}'"
        )
        if fresh_df.empty or fresh_df.iloc[0]["last_date"] is None:
            last_date = None
            days_old = 999
        else:
            last_date = fresh_df.iloc[0]["last_date"]
            days_old = (pd.Timestamp.now().date() - pd.Timestamp(last_date).date()).days

    except Exception as exc:
        return f"IMPORT_FAILED: {sym} — could not query daily_prices: {exc}. Proceeding with Yahoo Finance."

    # Data is fresh — nothing to do
    if last_date is not None and days_old <= stale_days:
        return (
            f"FRESH: {sym} — last trade_date {str(last_date)[:10]} "
            f"({days_old} day(s) ago). Data is current."
        )

    status_label = "MISSING" if last_date is None else f"STALE ({days_old} days old, last: {str(last_date)[:10]})"

    if not auto_import:
        return (
            f"STALE: {sym} — {status_label}. auto_import=False. "
            f"Run: `import --category {category}` to refresh."
        )

    # Trigger the importer
    try:
        from src.tools.skills_tools import run_data_engineering_importer
        run_data_engineering_importer.invoke({"category": category, "full": False})

        # Verify: re-check last_date after import
        verify_df = query_df(
            f"SELECT max(trade_date) AS last_date "
            f"FROM market_data.daily_prices FINAL "
            f"WHERE symbol = '{sym}' AND category = '{category}'"
        )
        new_last = None if verify_df.empty else verify_df.iloc[0]["last_date"]
        new_last_str = str(new_last)[:10] if new_last else "none"
        old_last_str = str(last_date)[:10] if last_date else "none"

        if new_last and new_last_str != old_last_str:
            return (
                f"REFRESHED: {sym} — import completed. "
                f"New last trade_date: {new_last_str} (was: {old_last_str})."
            )
        else:
            return (
                f"UNCHANGED: {sym} — import ran but last trade_date is still "
                f"{new_last_str}. Normal on weekends or market holidays. "
                f"Proceeding with existing data."
            )

    except Exception as exc:
        return (
            f"IMPORT_FAILED: {sym} — {status_label}, importer raised: {exc}. "
            f"Proceeding with available data."
        )


# ── Sub-agent delegation tools ────────────────────────────────────────────────

@tool
def delegate_to_signal_agent(question: str) -> str:
    """
    Delegate to the specialist Signal Sub-Agent, which runs the full ETF signal
    pipeline: GOLDBEES LightGBM ML prediction (prob_up, expected_return_pct,
    regime_signal, blended Kelly weights), composite scores across all 18 ETFs,
    GARCH volatility-targeted position sizing (risk governor), and ETF category
    news sentiment.

    Use when the research task requires:
    - Today's GOLDBEES ML pipeline output (prob_up, blended_50 weight, regime)
    - Composite signal scores for all 18 tracked Indian ETFs
    - GARCH risk governor analysis and Kelly position sizing
    - Signal breakdown charts or weight recommendation charts

    The signal agent has tools this research agent does not: run_goldbees_pipeline,
    plot_signal_scores, plot_signal_breakdown, plot_weight_recommendations.

    Args:
        question: Full question or task for the signal agent. Be specific, e.g.:
                  "Run the GOLDBEES ML pipeline and report prob_up, expected_return_pct,
                  regime_signal, and blended_50 weight."

    Returns:
        The signal agent's complete analysis as a Markdown string.
    """
    try:
        from src.tools.company_resolver import rewrite_delegation_question
        question = rewrite_delegation_question(question)
    except Exception:
        pass
    try:
        from src.agents.sub_agents import run_subagent_for
        return run_subagent_for("signal", question)
    except Exception as exc:
        return f"Signal agent error: {exc}"


@tool
def delegate_to_macro_agent(question: str) -> str:
    """
    Delegate to the specialist Macro Sub-Agent, which covers COMEX commodity
    pre-market signals (gold/silver/copper), FII/DII institutional flow data,
    COT positioning, and macro/geopolitical theme scanning with directional
    impact scores mapped to Indian ETFs.

    Use when the research task requires:
    - COMEX pre-market gold, silver, or copper signals with positioning data
    - Active macro/geopolitical themes (crude, Fed/RBI rates, Iran/Russia, OPEC)
      mapped to specific ETF impact scores
    - FII/DII net flow charts and trend analysis
    - COT (Commitment of Traders) gold positioning

    The macro agent has tools this research agent does not: run_comex_analysis,
    plot_fii_dii_chart in macro context, full geopolitical theme scanner output.

    Args:
        question: Full question or task for the macro agent.

    Returns:
        The macro agent's complete analysis as a Markdown string.
    """
    try:
        from src.tools.company_resolver import rewrite_delegation_question
        question = rewrite_delegation_question(question)
    except Exception:
        pass
    try:
        from src.agents.sub_agents import run_subagent_for
        return run_subagent_for("macro", question)
    except Exception as exc:
        return f"Macro agent error: {exc}"


@tool
def delegate_to_intl_etf_agent(question: str) -> str:
    """
    Delegate to the specialist International ETF Sub-Agent covering the 6
    NSE-listed overseas ETFs: MAFANG (China Tech), HNGSNGBEES (Hang Seng),
    MON100 (Nasdaq 100), MASPTOP50 (S&P 500 Top 50), MAHKTECH (HK Tech),
    MONQ50 (Nasdaq 50).

    Provides 7 analytical lenses not available in the research agent directly:
    - Scarcity premium/discount (RBI overseas investment cap arbitrage)
    - KMeans Bull/Sideways/Bear regime detection
    - Monthly return seasonality (best/worst months per ETF)
    - Return correlations + USDINR currency sensitivity
    - LightGBM feature importance for 5-day return prediction
    - Major drawdown episodes (> 10% from peak)
    - 3-year performance table (total return, annualised vol, Sharpe)

    Use when the research involves any of the 6 intl ETFs and needs premium,
    regime, seasonality, drawdown, or LightGBM analysis — not just price data.

    Args:
        question: Full question or task for the intl ETF agent.
                  Example: "Analyse HNGSNGBEES scarcity premium and current regime."

    Returns:
        The intl ETF agent's complete analysis as a Markdown string.
    """
    try:
        from src.tools.company_resolver import rewrite_delegation_question
        question = rewrite_delegation_question(question)
    except Exception:
        pass
    try:
        from src.agents.sub_agents import run_subagent_for
        return run_subagent_for("intl_etf", question)
    except Exception as exc:
        return f"IntlETF agent error: {exc}"


@tool
def delegate_to_news_agent(question: str) -> str:
    """
    Delegate to the specialist News Sub-Agent, which aggregates financial news
    from Google News (GNews), NewsAPI.org, and the ClickHouse news_articles table,
    deduplicates by title, sorts by date, and produces a sentiment summary.

    Use when the research task requires:
    - A thorough news sweep for a specific company or ETF across multiple sources
    - Sentiment classification (bullish/bearish/mixed) with article-level detail
    - Querying saved ETF news articles from ClickHouse by category or sentiment
    - A full ETF category news sentiment scan

    Args:
        question: Full question or task for the news agent.
                  Example: "Get the latest 14 days of news for GOLDBEES and
                  summarise the dominant sentiment and key themes."

    Returns:
        A Markdown table of headlines + 2–3 sentence sentiment summary.
    """
    try:
        from src.tools.company_resolver import rewrite_delegation_question
        question = rewrite_delegation_question(question)
    except Exception:
        pass
    try:
        from src.agents.sub_agents import run_subagent_for
        return run_subagent_for("news", question)
    except Exception as exc:
        return f"News agent error: {exc}"


@tool
def delegate_to_india_equity_agent(question: str) -> str:
    """
    Delegate to the specialist Indian Equity Research Sub-Agent for a full
    8-section research note on any NSE/BSE listed stock.

    The india_equity agent gathers in parallel: company snapshot (Yahoo Finance),
    price momentum, quarterly results (Screener.in / BSE fallback), annual cash
    flows, DSP mutual fund cross-ownership trend, recent news, and FII/DII flows.
    It then synthesises a BUY / HOLD / SELL / WATCH verdict with rationale.

    Use when the research task needs a complete equity sub-report on a specific
    Indian company — especially when this research agent is already orchestrating
    a multi-asset or comparative study and wants to avoid duplicating effort.

    Args:
        question: Full question or task including the company name or NSE symbol.
                  Example: "Research RELIANCE (NSE: RELIANCE) — provide a full
                  investment note covering fundamentals, MF ownership, and outlook."

    Returns:
        A structured Markdown research note covering all 8 sections.
    """
    try:
        from src.tools.company_resolver import rewrite_delegation_question
        question = rewrite_delegation_question(question)
    except Exception:
        pass
    try:
        from src.agents.sub_agents import run_subagent_for
        return run_subagent_for("india_equity", question)
    except Exception as exc:
        return f"India equity agent error: {exc}"


@tool
def delegate_to_mf_agent(question: str) -> str:
    """
    Delegate to the specialist Mutual Fund Sub-Agent for Indian MF holdings,
    NAV returns, cross-fund consensus, and fund imports.

    The MF agent covers:
    - MoM / YoY position changes for any tracked multi-asset fund
      (DSP, Nippon, Bajaj, Quant, ICICI — use canonical fund names)
    - Cross-fund consensus: which securities 3+ funds are collectively adding or trimming
    - Asset-class rotation signals (gold/equity/cash shifts across all 7 funds)
    - NAV MoM returns for any Indian MF by scheme code or name
    - Whale tracker: theme exposure across funds (gold, silver, nuclear, infra)
    - Reverse lookup: which funds hold a specific stock
    - Import / refresh fund holdings:
        "import all multi asset funds"     → run_all_multi_asset_importers (DSP + Nippon + ICICI)
        "import DSP holdings"              → run_dsp_multi_asset_importer
        "import Nippon holdings"           → run_nippon_importer
        "import ICICI holdings"            → run_icici_importer

    Use when:
    - The research needs institutional smart-money context (which active funds
      hold or recently added a stock)
    - The user asks about MF holdings, NAV returns, fund consensus, or whale tracking
    - The user wants to import/refresh fund portfolio disclosures

    Args:
        question: Full question or task for the MF agent.
                  Example: "What is DSP Multi Asset buying this month?"

    Returns:
        The MF agent's complete analysis as a Markdown string.
    """
    try:
        from src.agents.sub_agents import run_subagent_for
        return run_subagent_for("mf", question)
    except Exception as exc:
        return f"MF agent error: {exc}"


# ── Export list ───────────────────────────────────────────────────────────────

AGENT_TOOLS = [
    check_and_refresh_symbol_data,
    delegate_to_signal_agent,
    delegate_to_macro_agent,
    delegate_to_intl_etf_agent,
    delegate_to_news_agent,
    delegate_to_india_equity_agent,
    delegate_to_mf_agent,
]
