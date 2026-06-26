"""
src/agents/golden_pairs.py
──────────────────────────
Golden reference dataset of queries and their mapped intents.
Used by the intent router's local RAG/semantic search and validated in unit tests.
"""

GOLDEN_PAIRS: list[tuple[str, str]] = [
    # ── main (default / import / general) ─────────────────────────────────────
    ("import nav of GOLDBEES", "main"),
    ("refresh stock prices", "main"),
    ("sync all data", "main"),
    ("backfill etf prices", "main"),
    ("import --category stocks", "main"),
    ("import dxy, comex", "main"),
    ("import dxy", "main"),
    ("refresh GOLDBEES, NIFTYBEES", "main"),

    # ── signal ────────────────────────────────────────────────────────────────
    ("what is the composite signal for GOLDBEES?", "signal"),
    ("run goldbees pipeline", "signal"),
    ("today's gold signal", "signal"),
    ("show kelly weight for GOLDBEES", "signal"),
    ("what is the blended weight?", "signal"),
    ("GARCH volatility chart", "signal"),
    ("ml prediction for etfs", "signal"),
    ("regime signal", "signal"),
    ("buy signal for etfs", "signal"),
    ("plot price chart", "signal"),
    ("plot returns chart", "signal"),

    # ── macro ─────────────────────────────────────────────────────────────────
    ("what are the macro themes today?", "macro"),
    ("comex pre-market analysis", "macro"),
    ("FII flows this week", "macro"),
    ("DII flow trend", "macro"),
    ("gold price outlook", "macro"),
    ("crude oil impact on Indian markets", "macro"),
    ("what is usd-inr doing?", "macro"),
    ("is there a war risk from Iran?", "macro"),
    ("tariff impact on equities", "macro"),
    ("COT report for gold", "macro"),

    # ── deepdive ──────────────────────────────────────────────────────────────
    ("deep dive ADSK", "deepdive"),
    ("10-K filing for Apple", "deepdive"),
    ("SEC filing analysis for NVDA", "deepdive"),
    ("EDGAR annual report MSFT", "deepdive"),

    # ── news ──────────────────────────────────────────────────────────────────
    ("latest news on RELIANCE", "news"),
    ("market headlines today", "news"),
    ("etf news sentiment", "news"),
    ("what's happening with TCS?", "news"),
    ("breaking news for IT sector", "news"),

    # ── code ──────────────────────────────────────────────────────────────────
    ("write a python script to backtest momentum", "code"),
    ("create a new fetcher for NSE data", "code"),
    ("execute python code to analyze returns", "code"),

    # ── database ──────────────────────────────────────────────────────────────
    ("query the database for GOLDBEES prices", "database"),
    ("show me all tables in clickhouse", "database"),
    ("SELECT count() FROM market_data.daily_prices", "database"),
    ("describe table daily_prices", "database"),
    ("what are the watermarks?", "database"),

    # ── intl_etf ──────────────────────────────────────────────────────────────
    ("international etf performance", "intl_etf"),
    ("MAFANG ETF analysis", "intl_etf"),
    ("Hang Seng ETF regime", "intl_etf"),
    ("HNGSNGBEES premium", "intl_etf"),

    # ── research ──────────────────────────────────────────────────────────────
    ("autonomous research on gold ETFs", "research"),
    ("comprehensive analysis of HDFC Bank", "research"),
    ("deep research into pharma sector", "research"),
    ("full thesis on renewable energy stocks", "research"),
    ("why is GOLDBEES falling today?", "research"),
]
