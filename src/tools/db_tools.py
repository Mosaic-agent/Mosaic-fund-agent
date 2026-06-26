"""
src/tools/db_tools.py
─────────────────────
LangChain tools for the DatabaseSubAgent.

Provides schema discovery, table sampling, watermark inspection, and an
enhanced query executor that wraps query_clickhouse_db with richer context.
"""

from __future__ import annotations

import re

from langchain_core.tools import tool

# ── SQLite → ClickHouse auto-corrector ────────────────────────────────────────

_SQLITE_FIXES: list[tuple[re.Pattern, str]] = [
    # date('now', '-N days')            →  today() - N
    (re.compile(r"date\s*\(\s*'now'\s*,\s*'-\s*(\d+)\s*days?'\s*\)", re.I),
     lambda m: f"today() - {m.group(1)}"),
    # date('now', '+N days')            →  today() + N
    (re.compile(r"date\s*\(\s*'now'\s*,\s*'\+?\s*(\d+)\s*days?'\s*\)", re.I),
     lambda m: f"today() + {m.group(1)}"),
    # date('now')                       →  today()
    (re.compile(r"date\s*\(\s*'now'\s*\)", re.I), "today()"),
    # datetime('now')                   →  now()
    (re.compile(r"datetime\s*\(\s*'now'\s*\)", re.I), "now()"),
    # current_date()  / CURRENT_DATE    →  today()
    (re.compile(r"\bcurrent_date\s*\(\s*\)", re.I), "today()"),
    (re.compile(r"\bCURRENT_DATE\b", re.I), "today()"),
    # current_timestamp() / NOW()       →  now()
    (re.compile(r"\bcurrent_timestamp\s*\(\s*\)", re.I), "now()"),
    # date_trunc('month', x)            →  toStartOfMonth(x)
    (re.compile(r"date_trunc\s*\(\s*'month'\s*,\s*([^)]+)\)", re.I),
     lambda m: f"toStartOfMonth({m.group(1).strip()})"),
    # date_trunc('year', x)             →  toStartOfYear(x)
    (re.compile(r"date_trunc\s*\(\s*'year'\s*,\s*([^)]+)\)", re.I),
     lambda m: f"toStartOfYear({m.group(1).strip()})"),
    # date_trunc('week', x)             →  toStartOfWeek(x)
    (re.compile(r"date_trunc\s*\(\s*'week'\s*,\s*([^)]+)\)", re.I),
     lambda m: f"toStartOfWeek({m.group(1).strip()})"),
    # strftime('%Y', col)               →  toYear(col)
    (re.compile(r"strftime\s*\(\s*'%Y'\s*,\s*(\w+)\s*\)", re.I),
     lambda m: f"toYear({m.group(1)})"),
    # strftime('%m', col)               →  toMonth(col)
    (re.compile(r"strftime\s*\(\s*'%m'\s*,\s*(\w+)\s*\)", re.I),
     lambda m: f"toMonth({m.group(1)})"),
    # JULIANDAY diff                    →  dateDiff('day', a, b)
    (re.compile(r"julianday\s*\((\w+)\)\s*-\s*julianday\s*\((\w+)\)", re.I),
     lambda m: f"dateDiff('day', {m.group(2)}, {m.group(1)})"),
    # IFNULL(a, b)                      →  ifNull(a, b)
    (re.compile(r"\bIFNULL\s*\(", re.I), "ifNull("),
    # Wrong column names → ClickHouse equivalents (table-aware via context)
    # close_price  →  close
    (re.compile(r"\bclose_price\b", re.I), "close"),
]

# Per-table date column map — used to patch bare 'date' column references
_TABLE_DATE_COL: dict[str, str] = {
    "daily_prices":    "trade_date",
    "mf_nav":          "nav_date",
    "mf_holdings":     "as_of_month",
    "fii_dii_flows":   "trade_date",
    "fii_dii_monthly": "month_date",
    "fii_dii_fno_daily": "trade_date",
    "signal_composite": "as_of",
    "ml_predictions":  "as_of",
    "weight_checkpoints": "as_of",
    "inav_snapshots":  "snapshot_at",
    "cot_gold":        "report_date",
    "fx_rates":        "trade_date",
    "news_articles":   "fetched_at",
    "import_watermarks": "last_date",
    "stock_valuation": "snapshot_date",
    "stock_earnings":  "earnings_date",
    "stock_insider_trades": "transaction_date",
    "corporate_actions": "ex_date",
    "deepdive_financials": "report_date",
    "deepdive_valuation":  "report_date",
    "deepdive_prices":     "trade_date",
}


def _auto_fix_sql(sql: str) -> tuple[str, list[str]]:
    """
    Apply known SQLite/PostgreSQL→ClickHouse pattern fixes.

    Returns (fixed_sql, list_of_changes_made).
    """
    changes: list[str] = []
    result = sql

    # Standard pattern replacements
    for pattern, replacement in _SQLITE_FIXES:
        new = pattern.sub(replacement, result) if not callable(replacement) \
              else pattern.sub(replacement, result)
        if new != result:
            changes.append(f"{pattern.pattern[:50]}… → fixed")
            result = new

    # Table-aware bare `date` column fix:
    # Detect which table is being queried and replace bare `date` references
    # with the correct ClickHouse column name for that table.
    table_match = re.search(
        r"\bFROM\s+(?:market_data\.)?(\w+)", result, re.I
    )
    if table_match:
        table = table_match.group(1).lower()
        correct_col = _TABLE_DATE_COL.get(table)
        if correct_col:
            # Only replace standalone `date` (not inside function calls like toDate())
            bare_date = re.compile(r"\bdate\b(?!\s*\()", re.I)
            new = bare_date.sub(correct_col, result)
            if new != result:
                changes.append(f"date → {correct_col} (table: {table})")
                result = new

    return result, changes


# ── Data freshness detection ──────────────────────────────────────────────────

# Thread-safe flag: set by _run_sql when data is stale; read and cleared by chat loop.
import threading as _threading
_stale_flag: _threading.local = _threading.local()

# Table → import category mapping (used to suggest the right import command)
_TABLE_IMPORT_CATEGORY: dict[str, str] = {
    "daily_prices":      "etfs,stocks",
    "mf_nav":            "mf",
    "mf_holdings":       "mf",
    "fii_dii_flows":     "fii_dii",
    "fii_dii_monthly":   "fii_dii",
    "fii_dii_fno_daily": "fii_dii",
    "signal_composite":  "etfs",
    "ml_predictions":    "etfs",
    "cot_gold":          "cot",
    "fx_rates":          "fx_rates",
    "news_articles":     "etfs",
    "corporate_actions": "stocks",
    "stock_earnings":    "stocks",
    "stock_insider_trades": "stocks",
    "stock_valuation":   "stocks",
    "weight_checkpoints": "etfs",
}

STALE_THRESHOLD_DAYS = 2   # flag as stale if last import > this many days ago


def _check_table_freshness(sql: str) -> dict | None:
    """
    Look up the import watermark for the primary table in *sql*.
    Returns a dict with {table, category, last_date, days_ago} or None.
    """
    m = re.search(r"\bFROM\s+(?:market_data\.)?(\w+)", sql, re.I)
    if not m:
        return None
    table = m.group(1).lower()
    category = _TABLE_IMPORT_CATEGORY.get(table)
    if not category:
        return None
    try:
        from src.db.pool import query_df
        df = query_df(f"""
            SELECT max(last_date) AS last_date,
                   dateDiff('day', max(last_date), today()) AS days_ago
            FROM market_data.import_watermarks FINAL
            WHERE source ILIKE '%{table.replace('_', '%')}%'
               OR source ILIKE '%{category.split(',')[0]}%'
        """)
        if df.empty or df.iloc[0]["last_date"] is None:
            return {"table": table, "category": category,
                    "last_date": "never", "days_ago": 999}
        days_ago = int(df.iloc[0]["days_ago"])
        return {
            "table": table,
            "category": category,
            "last_date": str(df.iloc[0]["last_date"])[:10],
            "days_ago": days_ago,
        }
    except Exception:
        return None


def get_stale_hint() -> dict | None:
    """Called by the chat loop to check if a freshness warning was raised this turn."""
    hint = getattr(_stale_flag, "hint", None)
    _stale_flag.hint = None   # clear after reading
    return hint


# ── Schema reference ──────────────────────────────────────────────────────────
# Condensed column list for the LLM — used in describe_db_table fallback.

_TABLE_SCHEMA: dict[str, list[str]] = {
    # ── Indian market ─────────────────────────────────────────────────────────
    "daily_prices":       ["symbol", "category", "trade_date", "open", "high", "low", "close", "volume", "imported_at"],
    "mf_nav":             ["symbol", "scheme_code", "nav_date", "nav", "imported_at"],
    "mf_holdings":        ["scheme_code", "fund_name", "as_of_month", "isin", "security_name", "asset_type", "market_value_cr", "pct_of_nav", "imported_at"],
    "fii_dii_flows":      ["trade_date", "fii_gross_buy_cr", "fii_gross_sell_cr", "fii_net_cr", "dii_gross_buy_cr", "dii_gross_sell_cr", "dii_net_cr", "imported_at"],
    "fii_dii_monthly":    ["month_date", "fii_buy_cr", "fii_sell_cr", "fii_net_cr", "dii_buy_cr", "dii_sell_cr", "dii_net_cr", "nifty_close", "nifty_change_pct", "imported_at"],
    "fii_dii_fno_daily":  ["trade_date", "fii_fut_net_oi", "fii_opt_call_net_oi", "fii_opt_put_net_oi", "nifty_close", "banknifty_close", "imported_at"],
    "signal_composite":   ["as_of", "etf_symbol", "macro_score", "sentiment_score", "valuation_score", "flow_score", "ml_score", "composite_score", "action", "rationale", "imported_at"],
    "ml_predictions":     ["as_of", "horizon_days", "expected_return_pct", "confidence_low", "confidence_high", "regime_signal", "cv_r2_mean", "goldbees_close", "created_at"],
    "weight_checkpoints": ["as_of", "symbol", "method", "recommended_weight", "expected_return_pct", "garch_vol_pct", "regime", "composite_score", "cv_r2", "created_at"],
    "inav_snapshots":     ["symbol", "snapshot_at", "inav", "market_price", "premium_discount_pct", "source"],
    "news_articles":      ["fetched_at", "published_at", "source_type", "category", "etfs_impacted", "sentiment", "impact_tier", "title", "source", "url", "imported_at"],
    "cot_gold":           ["report_date", "mm_long", "mm_short", "mm_net", "comm_long", "comm_short", "comm_net", "open_interest", "source"],
    "fx_rates":           ["trade_date", "symbol", "open", "high", "low", "close", "source", "imported_at"],
    "macro_indicators":   ["ref_year", "country_code", "indicator_code", "indicator_name", "value", "source", "is_forecast", "imported_at"],
    "tijori_macro_indicators": ["as_of_date", "indicator_code", "indicator_name", "parent_code", "value", "unit", "imported_at"],
    "etf_aum":            ["trade_date", "symbol", "aum_usd", "price", "implied_tonnes", "source"],
    "cb_gold_reserves":   ["ref_period", "country_code", "country_name", "reserves_tonnes", "source"],
    "stock_valuation":    ["symbol", "snapshot_date", "market_cap", "trailing_pe", "forward_pe", "price_to_book", "debt_to_equity", "return_on_equity", "profit_margin", "recommendation", "imported_at"],
    "stock_earnings":     ["symbol", "earnings_date", "eps_estimate", "eps_actual", "surprise_pct", "imported_at"],
    "import_watermarks":  ["source", "symbol", "last_date", "updated_at"],
    # ── User portfolio ────────────────────────────────────────────────────────
    "user_holdings":      ["tradingsymbol", "exchange", "quantity", "average_price", "last_price", "pnl", "day_change_percentage", "imported_at"],
    "user_positions":     ["tradingsymbol", "exchange", "product", "quantity", "average_price", "last_price", "pnl", "imported_at"],
    "user_margins":       ["segment", "cash", "available_balance", "utilised_debits", "imported_at"],
    "user_orders":        ["order_id", "status", "tradingsymbol", "transaction_type", "quantity", "price", "average_price", "order_timestamp", "imported_at"],
    # ── US deep-dive ──────────────────────────────────────────────────────────
    "deepdive_filings":   ["ticker", "report_date", "form_type", "filed_date", "filing_url", "company_name", "cik"],
    "deepdive_financials":["ticker", "report_date", "fiscal_year", "revenue_usd_m", "gross_profit_usd_m", "net_income_usd_m", "free_cash_flow_usd_m", "gross_margin_pct", "operating_margin_pct"],
    "deepdive_valuation": ["ticker", "report_date", "market_cap_usd_b", "pe_trailing", "pe_forward", "ev_revenue", "ev_ebitda", "fcf_yield_pct"],
    "deepdive_segments":  ["ticker", "report_date", "fiscal_year", "segment_name", "revenue_usd_m", "yoy_growth_pct"],
    "deepdive_headcount": ["ticker", "report_date", "fiscal_period", "total_headcount", "yoy_change_pct"],
    "deepdive_exec_comp": ["ticker", "report_date", "fiscal_year", "exec_name", "position", "salary_usd", "total_usd"],
    "deepdive_prices":    ["ticker", "trade_date", "open", "high", "low", "close", "volume"],
    "deepdive_reports":   ["ticker", "report_date", "section_key", "section_heading", "content_md"],
}


def _run_sql(sql: str) -> str:
    """Execute SQL and return a markdown table or error string."""
    try:
        from src.db.pool import query_df
        df = query_df(sql.strip())
        if df.empty:
            # Check watermark — data might just not have been imported yet
            hint = _check_table_freshness(sql)
            if hint:
                _stale_flag.hint = hint
                days = hint["days_ago"]
                msg = (
                    f"No data found ({hint['table']})."
                    f" Last import: **{hint['last_date']}** ({days} day{'s' if days != 1 else ''} ago)."
                )
                if days > STALE_THRESHOLD_DAYS:
                    msg += f"\n\n⚠ Data may be stale. Consider: `import --category {hint['category']}`"
                return msg
            return "Query returned 0 rows."

        # Check if most-recent row is stale even when data exists
        date_cols = [c for c in df.columns
                     if any(k in c.lower() for k in ("date", "as_of", "snapshot", "fetched"))]
        if date_cols:
            try:
                import pandas as pd
                latest = pd.to_datetime(df[date_cols[0]]).max()
                days_old = (pd.Timestamp.now() - latest).days
                if days_old > STALE_THRESHOLD_DAYS:
                    hint = _check_table_freshness(sql)
                    if hint:
                        _stale_flag.hint = {**hint, "days_ago": days_old,
                                             "last_date": str(latest)[:10]}
            except Exception:
                pass

        n = len(df)
        out = df.head(200).to_markdown(index=False)
        if n > 200:
            out += f"\n\n*[truncated — {n} total rows, showing 200]*"
        return out
    except Exception as exc:
        return f"Query error: {exc}"


@tool
def list_db_tables() -> str:
    """
    List all tables in the market_data ClickHouse database with approximate row counts.
    Call this first to discover what data is available before writing queries.
    """
    sql = """
        SELECT
            name                                    AS table,
            formatReadableQuantity(total_rows)      AS rows,
            formatReadableSize(total_bytes)         AS size,
            max_date                                AS latest_date
        FROM system.tables
        WHERE database = 'market_data'
          AND engine NOT IN ('View','MaterializedView')
        ORDER BY name
    """
    try:
        from src.db.pool import query_df
        df = query_df(sql)
        if df.empty:
            return "No tables found in market_data database."
        return df.to_markdown(index=False)
    except Exception as exc:
        # Fallback: return static list from schema dict
        lines = ["| table | known_columns |", "|---|---|"]
        for tbl, cols in _TABLE_SCHEMA.items():
            lines.append(f"| {tbl} | {', '.join(cols[:5])}… |")
        return "\n".join(lines) + f"\n\n*(system.tables unavailable: {exc})*"


@tool
def describe_db_table(table_name: str) -> str:
    """
    Show column names, types, and a sample row count for a specific table.
    Use this before writing a query to confirm column names and types.

    Example: describe_db_table("mf_holdings")
    """
    clean = table_name.strip().lower().replace("market_data.", "")
    sql = f"DESCRIBE TABLE market_data.{clean}"
    try:
        from src.db.pool import query_df
        df = query_df(sql)
        result = df[["name", "type"]].to_markdown(index=False)
        # Append row count
        count_sql = f"SELECT count() AS rows FROM market_data.{clean} FINAL"
        try:
            cnt = query_df(count_sql).iloc[0, 0]
            result += f"\n\n**Total rows (FINAL):** {cnt:,}"
        except Exception:
            pass
        return result
    except Exception as exc:
        # Static fallback
        cols = _TABLE_SCHEMA.get(clean)
        if cols:
            lines = ["| name | type |", "|---|---|"]
            for c in cols:
                lines.append(f"| {c} | — |")
            return "\n".join(lines) + f"\n\n*(DESCRIBE failed: {exc})*"
        return f"Table '{clean}' not found. Run list_db_tables() to see available tables."


@tool
def sample_db_table(table_name: str, limit: int = 5) -> str:
    """
    Return sample rows from a table as a markdown table.
    Useful for understanding the data shape before writing aggregation queries.

    Example: sample_db_table("daily_prices", limit=3)
    """
    clean = table_name.strip().lower().replace("market_data.", "")
    sql = f"SELECT * FROM market_data.{clean} FINAL LIMIT {min(limit, 20)}"
    return _run_sql(sql)


@tool
def get_db_watermarks() -> str:
    """
    Show the last imported date for every (source, symbol) pair.
    Use this to check data freshness — whether today's data has been imported.
    """
    sql = """
        SELECT source, symbol, last_date, updated_at
        FROM market_data.import_watermarks FINAL
        ORDER BY updated_at DESC
        LIMIT 60
    """
    return _run_sql(sql)


@tool
def execute_db_query(sql: str) -> str:
    """
    Execute a read-only SQL query against the market_data ClickHouse database.

    Rules:
    - Only SELECT, SHOW, DESCRIBE, EXPLAIN, WITH are allowed.
    - Always add FINAL to ReplacingMergeTree tables:
        SELECT ... FROM market_data.daily_prices FINAL WHERE ...
    - ClickHouse date functions (NOT SQLite):
        today()                    — current date
        today() - 30               — 30 days ago
        toStartOfMonth(today())    — first day of current month
        toDate('2026-05-01')       — specific date
    - Column names: trade_date (not 'date'), nav_date, as_of_month, as_of
    - Results truncated to 200 rows.

    Example:
        SELECT symbol, trade_date, close
        FROM market_data.daily_prices FINAL
        WHERE symbol = 'GOLDBEES' AND trade_date >= today() - 30
        ORDER BY trade_date DESC LIMIT 10
    """
    clean = sql.strip()
    first = clean.split()[0].upper() if clean else ""
    if first not in ("SELECT", "SHOW", "DESCRIBE", "EXPLAIN", "WITH"):
        return "Only read-only queries are permitted (SELECT / SHOW / DESCRIBE / EXPLAIN / WITH)."

    # Auto-correct common SQLite patterns before hitting ClickHouse
    fixed, changes = _auto_fix_sql(clean)
    if changes:
        import logging
        logging.getLogger(__name__).info(
            "execute_db_query: auto-fixed %d SQL pattern(s): %s", len(changes), changes
        )

    result = _run_sql(fixed)

    # If the query still fails, return the error WITH the auto-corrected SQL so
    # the agent can see exactly what was tried and fix the remaining issues.
    if result.startswith("Query error:"):
        hint = (
            "\n\n**ClickHouse syntax reminder:**\n"
            "- Dates: `today()`, `today() - 30`, `toStartOfMonth(today())`\n"
            "- Column: `trade_date` (not `date`), `nav_date`, `as_of`\n"
            "- Always add `FINAL` after table name\n"
            f"\n**SQL that was executed:**\n```sql\n{fixed}\n```"
        )
        return result + hint

    return result


DB_TOOLS = [
    list_db_tables,
    describe_db_table,
    sample_db_table,
    get_db_watermarks,
    execute_db_query,
]
