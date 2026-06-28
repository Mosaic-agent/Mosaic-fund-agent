"""
src/ui/app.py
─────────────
Streamlit web UI for Mosaic Fund Agent data management.

Provides three tabs:
  📥 Import   — trigger historical data imports with live log output
  🔍 Query    — SQL editor against ClickHouse with presets and CSV export
  📊 Explorer — interactive charts (Gold price, GOLDBEES NAV vs price, premium/discount, iNAV)

Launch locally:
    streamlit run src/ui/app.py

Via Docker Compose:
    docker compose up ui
    then open http://localhost:8501
"""
from __future__ import annotations

import io
import os
import sys
from datetime import date

import pandas as pd
import streamlit as st

# matplotlib is an optional dependency for pandas Styler.background_gradient.
# Guard all gradient styling so the UI degrades gracefully when it's missing
# (e.g. stale Docker image built before matplotlib was added to requirements.txt).
try:
    import matplotlib  # noqa: F401
    _HAS_MATPLOTLIB = True
except ImportError:
    _HAS_MATPLOTLIB = False


def _gradient_dataframe(df: pd.DataFrame, style_fn, **st_kwargs) -> None:
    """Call st.dataframe with gradient styling when matplotlib is available,
    falling back to plain st.dataframe otherwise."""
    if _HAS_MATPLOTLIB:
        try:
            st.dataframe(style_fn(df.style), **st_kwargs)
            return
        except Exception:
            pass
    st.dataframe(df, **st_kwargs)

# Ensure project root is importable when running as `streamlit run src/ui/app.py`
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

# ── ClickHouse connection pool ────────────────────────────────────────────────
# Pool is a module-level singleton (src/db/pool.py). Streamlit's
# @st.cache_resource wraps get_pool() so the same CHPool instance is reused
# across all page reruns and concurrent users — no new TCP handshake per query.

@st.cache_resource
def _get_pool():
    from src.db.pool import get_pool
    return get_pool()


def _get_client():
    """Return the cached pool (kept for backward compat with call sites)."""
    return _get_pool()


def _query_df(sql: str) -> pd.DataFrame:
    return _get_pool().query_df(sql)


@st.cache_data(ttl=30)
def _ch_ok() -> bool:
    try:
        _get_pool().execute("SELECT 1")
        return True
    except Exception:
        return False


# ── ClickHouse connection constants (read-only; used in labels + legacy callers) ─
from config.settings import settings as _settings  # noqa: E402
CH_HOST = _settings.clickhouse_host
CH_PORT = _settings.clickhouse_port
CH_DB   = _settings.clickhouse_database
CH_USER = _settings.clickhouse_user
CH_PASS = _settings.clickhouse_password


@st.cache_resource
def _ensure_schema() -> None:
    """Create all market_data tables if they don't exist (idempotent DDL)."""
    try:
        from src.importer.clickhouse import ClickHouseImporter
        ch = ClickHouseImporter()   # uses pool singleton — no params needed
        ch.ensure_schema()
        ch.close()
    except Exception:
        pass  # ClickHouse may be unavailable; individual queries will surface errors


@st.cache_data(ttl=15)
def _table_stats() -> pd.DataFrame:
    # Use system.tables to only count tables that actually exist
    return _query_df("""
        SELECT name AS tbl,
               total_rows AS rows
        FROM system.tables
        WHERE database = 'market_data'
          AND name IN (
              'daily_prices', 'mf_nav', 'inav_snapshots',
              'import_watermarks', 'cot_gold', 'cb_gold_reserves', 'etf_aum', 'fx_rates'
          )
        ORDER BY name
    """)


def _init_macro_signals_from_db() -> None:
    if "macro_net_signal" not in st.session_state:
        try:
            # Query the latest batch of macro events from ClickHouse
            _latest_articles = _query_df("""
                SELECT category, impact_tier, title
                FROM market_data.news_articles
                WHERE source_type = 'macro_event'
                  AND fetched_at = (
                      SELECT max(fetched_at)
                      FROM market_data.news_articles
                      WHERE source_type = 'macro_event'
                  )
            """)
            if not _latest_articles.empty:
                from src.tools.macro_event_scanner import MACRO_THEMES
                theme_maps = {t["theme"]: t["impact_map"] for t in MACRO_THEMES}
                etf_net = {}
                themes_detected = set()
                for _, row in _latest_articles.iterrows():
                    theme = row["category"]
                    themes_detected.add(theme)
                    impact_map = theme_maps.get(theme, {})
                    for etf, direction in impact_map.items():
                        etf_net[etf] = etf_net.get(etf, 0) + direction
                st.session_state["macro_net_signal"] = etf_net
                st.session_state["macro_n_themes"] = len(themes_detected)
        except Exception:
            pass


# ── Page config ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Mosaic Data Hub",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.title("📈 Mosaic Data Hub")
    st.caption(f"ClickHouse  `{CH_HOST}:{CH_PORT}`")

    ok = _ch_ok()
    if ok:
        _ensure_schema()   # idempotent — creates any missing tables on first load
        st.success("ClickHouse connected", icon="✅")
        st.divider()
        st.subheader("Table stats")
        try:
            for _, row in _table_stats().iterrows():
                st.metric(row["tbl"], f"{int(row['rows']):,}")
        except Exception as e:
            st.warning(f"Stats error: {e}")
    else:
        st.error("ClickHouse unreachable", icon="❌")
        st.code("docker compose up clickhouse -d", language="bash")

    st.divider()
    if st.button("🔄 Refresh stats"):
        st.cache_data.clear()
        st.rerun()


# ── Tabs ──────────────────────────────────────────────────────────────────────

tab_import, tab_query, tab_explorer, tab_anomaly, tab_wis, tab_holdings, tab_etf_scan, tab_news, tab_signals, tab_kite, tab_deepdive, tab_intl_etf, tab_workflows, tab_reports = st.tabs(["📥 Import Data", "🔍 SQL Query", "📊 Explorer", "🔬 Anomaly Detection", "🕵️ Who Is Selling?", "📦 MF Holdings", "🏦 ETF Scanner", "📰 Market News", "🎛️ Signals", "🪁 Kite Dashboard", "🏢 Deep Dive", "🌍 Intl ETFs", "🤖 Workflows", "📁 Reports"])


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — IMPORT
# ══════════════════════════════════════════════════════════════════════════════

with tab_import:
    st.header("Import Historical Market Data")
    st.caption(
        "Fetches OHLCV from Yahoo Finance, MF NAV from MFAPI.in, and "
        "live iNAV from NSE into ClickHouse. Subsequent runs are delta-synced."
    )

    col_ctrl, col_log = st.columns([1, 2])

    with col_ctrl:
        st.subheader("Settings")

        ALL_CATS = ["stocks", "etfs", "commodities", "indices", "mf", "inav",
                    "cot", "cb_reserves", "etf_aum", "fx_rates", "mf_holdings"]
        CATEGORY_HELP = {
            "stocks":       "50 NSE large/mid-caps (Yahoo Finance)",
            "etfs":         "15 NSE ETFs — OHLCV (Yahoo Finance)",
            "commodities":  "Gold, Silver, Oil futures (Yahoo Finance)",
            "indices":      "Nifty50, Sensex, S&P500, Bank Nifty, etc. (Yahoo Finance + nselib)",
            "mf":           "ETF NAV history from MFAPI.in (AMFI official)",
            "inav":         "Live iNAV snapshot from NSE API",
            "cot":          "CFTC COT Gold — hedge fund & commercial positioning (weekly)",
            "cb_reserves":  "Central bank gold reserves — 9 countries via IMF IFS (monthly)",
            "etf_aum":      "Gold ETF AUM — GLD, IAU, SGOL, PHYS + implied tonnes (daily)",
            "fx_rates":     "USD FX rates — INR, CNY, AED, SAR, KWD daily OHLC (Yahoo Finance)",
            "mf_holdings":  "📦 Monthly portfolio holdings — DSP/Quant/ICICI/Bajaj Multi Asset (Morningstar)",
        }

        select_all = st.checkbox("All categories", value=False)
        selected_cats = (
            ALL_CATS if select_all
            else st.multiselect(
                "Categories",
                options=ALL_CATS,
                default=["etfs", "mf", "inav"],
                format_func=lambda c: f"{c}  —  {CATEGORY_HELP[c]}",
            )
        )

        st.divider()

        lookback = st.slider(
            "Lookback days (first run)",
            min_value=7, max_value=730, value=730, step=7,
            help="How many calendar days of history on the very first import. "
                 "Delta runs ignore this and only fetch new data.",
        )

        col_a, col_b = st.columns(2)
        full_reimport = col_a.toggle("Full re-import", value=False,
                                     help="Ignore watermarks; re-fetch full window.")
        dry_run       = col_b.toggle("Dry run",       value=False,
                                     help="Fetch data but do NOT write to ClickHouse.")

        # ── MF Holdings month picker (only shown when mf_holdings selected) ──
        mf_holdings_month = None
        if "mf_holdings" in (ALL_CATS if select_all else selected_cats):
            st.divider()
            st.markdown("**📦 MF Holdings — month to import**")
            _today = date.today()
            # Build list of first-of-month dates: Jan 2024 → current month
            import calendar as _cal
            _months: list[date] = []
            _yr, _mo = 2024, 1
            while (_yr, _mo) <= (_today.year, _today.month):
                _months.append(date(_yr, _mo, 1))
                _mo += 1
                if _mo > 12:
                    _mo, _yr = 1, _yr + 1
            _months.reverse()  # newest first
            mf_holdings_month = st.selectbox(
                "Holdings month",
                options=_months,
                index=0,
                format_func=lambda d: d.strftime("%B %Y"),
                help="Morningstar shows the latest published portfolio. "
                     "Pick the month label to tag the snapshot with.",
            )

        st.divider()
        run_btn = st.button(
            "▶  Start Import",
            type="primary",
            disabled=not ok or len(selected_cats) == 0,
            width="stretch",
        )

    with col_log:
        st.subheader("Import log")
        log_box    = st.empty()
        status_box = st.empty()

    if run_btn:
        log_box.info("Starting import…")
        buf = io.StringIO()
        try:
            from rich.console import Console as RichConsole
            rich_con = RichConsole(file=buf, no_color=True, width=110)
        except ImportError:
            rich_con = None

        try:
            from src.importer.cli import run_import
            run_import(
                categories=selected_cats,
                lookback_days=lookback,
                full_reimport=full_reimport,
                dry_run=dry_run,
                console=rich_con,
                clickhouse_host=CH_HOST,
                clickhouse_port=CH_PORT,
                clickhouse_database=CH_DB,
                clickhouse_user=CH_USER,
                clickhouse_password=CH_PASS,
                mf_holdings_month=mf_holdings_month,
            )
            log_box.code(buf.getvalue() or "Done.", language="")
            status_box.success(
                "✓ Import complete." + ("  *(dry run — nothing written)*" if dry_run else "")
            )
            st.cache_data.clear()
        except SystemExit as exc:
            log_box.code(buf.getvalue(), language="")
            status_box.error(f"Import stopped (exit {exc.code}). Is ClickHouse running?")
        except Exception as exc:
            log_box.code(buf.getvalue(), language="")
            status_box.error(f"Import error: {exc}")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — SQL QUERY
# ══════════════════════════════════════════════════════════════════════════════

PRESETS: dict[str, str] = {
    "— select a preset —": "",
    "Table row counts": """\
SELECT 'daily_prices' AS tbl, count() AS rows FROM market_data.daily_prices FINAL
UNION ALL SELECT 'mf_nav',          count() FROM market_data.mf_nav            FINAL
UNION ALL SELECT 'inav_snapshots',  count() FROM market_data.inav_snapshots    FINAL
UNION ALL SELECT 'import_watermarks', count() FROM market_data.import_watermarks FINAL""",

    "Symbols in daily_prices": """\
SELECT category, symbol, count() AS rows,
       min(trade_date) AS from_date, max(trade_date) AS to_date,
       round(argMax(close, trade_date), 2) AS latest_close
FROM market_data.daily_prices FINAL
GROUP BY category, symbol
ORDER BY category, symbol""",

    "GOLDBEES — NAV vs market price (last 30 days)": """\
SELECT
    p.trade_date,
    round(p.close, 4)                                                       AS market_close,
    round(n.nav_adj, 4)                                                     AS amfi_nav,
    if(n.nav_adj > 0, round((p.close - n.nav_adj) / n.nav_adj * 100, 3), NULL) AS premium_disc_pct
FROM (
    SELECT trade_date, close
    FROM market_data.daily_prices FINAL
    WHERE symbol = 'GOLDBEES' AND category = 'etfs'
) p
LEFT JOIN (
    SELECT nav_date AS trade_date,
           if(nav_date < '2019-12-23', nav / 100, nav) AS nav_adj
    FROM market_data.mf_nav FINAL
    WHERE symbol = 'GOLDBEES'
) n USING (trade_date)
ORDER BY trade_date DESC
LIMIT 30""",

    "COMEX Gold — daily close (last 60 days)": """\
SELECT trade_date, round(close, 2) AS close_usd,
       round(open, 2) AS open_usd, round(high, 2) AS high_usd,
       round(low, 2) AS low_usd, round(volume, 0) AS volume
FROM market_data.daily_prices FINAL
WHERE symbol = 'GOLD' AND category = 'commodities'
ORDER BY trade_date DESC
LIMIT 60""",

    "iNAV snapshots (all)": """\
SELECT symbol, snapshot_at,
       round(inav, 4) AS inav, round(market_price, 4) AS market_price,
       round(premium_discount_pct, 3) AS prem_disc_pct, source
FROM market_data.inav_snapshots FINAL
ORDER BY snapshot_at DESC
LIMIT 200""",

    "Import watermarks": """\
SELECT source, symbol, last_date, updated_at
FROM market_data.import_watermarks FINAL
ORDER BY source, symbol""",

    "MF NAV — latest per scheme": """\
SELECT symbol, scheme_code,
       argMax(nav_date, nav_date) AS latest_date,
       round(argMax(nav, nav_date), 4) AS latest_nav,
       count() AS total_rows
FROM market_data.mf_nav FINAL
GROUP BY symbol, scheme_code
ORDER BY symbol""",

    "Gold 30-day rolling avg vs daily close": """\
SELECT
    trade_date,
    round(close, 2) AS close_usd,
    round(avg(close) OVER (ORDER BY trade_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 2)
        AS ma30_usd
FROM market_data.daily_prices FINAL
WHERE symbol = 'GOLD' AND category = 'commodities'
ORDER BY trade_date DESC
LIMIT 60""",

    "COT — Hedge fund positioning (last 52 weeks)": """\
SELECT
    report_date,
    mm_long,
    mm_short,
    mm_net,
    open_interest,
    round(mm_net / open_interest * 100, 2)  AS mm_net_pct_oi,
    comm_net
FROM market_data.cot_gold FINAL
ORDER BY report_date DESC
LIMIT 52""",

    "COT — Extreme positioning (top 10 crowded longs/shorts)": """\
SELECT
    report_date,
    mm_net,
    open_interest,
    round(mm_net / open_interest * 100, 2) AS mm_net_pct_oi,
    CASE
        WHEN mm_net / open_interest > 0.25 THEN '🔴 Crowded Long — crash risk'
        WHEN mm_net / open_interest < -0.05 THEN '🟢 Extreme Short — squeeze fuel'
        ELSE '⚪ Neutral'
    END AS signal
FROM market_data.cot_gold FINAL
ORDER BY abs(mm_net / open_interest) DESC
LIMIT 10""",

    "Central bank reserves — latest per country": """\
SELECT
    country_name,
    country_code,
    argMax(ref_period, ref_period)      AS latest_period,
    round(argMax(reserves_tonnes, ref_period), 1) AS latest_tonnes,
    round(
        argMax(reserves_tonnes, ref_period) -
        argMin(reserves_tonnes, ref_period), 1
    ) AS change_since_2010
FROM market_data.cb_gold_reserves FINAL
GROUP BY country_name, country_code
ORDER BY latest_tonnes DESC""",

    "Central bank reserves — China & India quarterly trend": """\
SELECT
    toStartOfQuarter(ref_period)  AS quarter,
    country_name,
    round(argMax(reserves_tonnes, ref_period), 1) AS eop_tonnes
FROM market_data.cb_gold_reserves FINAL
WHERE country_code IN ('CN', 'IN')
GROUP BY quarter, country_name
ORDER BY quarter DESC, country_name
LIMIT 40""",

    "ETF AUM — GLD implied gold tonnes (last 60 days)": """\
SELECT
    trade_date,
    symbol,
    round(aum_usd / 1e9, 3)  AS aum_bn_usd,
    price,
    implied_tonnes
FROM market_data.etf_aum FINAL
WHERE symbol = 'GLD'
ORDER BY trade_date DESC
LIMIT 60""",

    "ETF AUM — all ETFs latest snapshot": """\
SELECT
    trade_date,
    symbol,
    round(aum_usd / 1e9, 3)  AS aum_bn_usd,
    price,
    implied_tonnes
FROM market_data.etf_aum FINAL
ORDER BY trade_date DESC, implied_tonnes DESC
LIMIT 20""",

    "FX Rates \u2014 USDINR daily close (last 90 days)": """\
SELECT trade_date, open, high, low, close
FROM market_data.fx_rates FINAL
WHERE symbol = 'USDINR'
ORDER BY trade_date DESC
LIMIT 90""",

    "FX Rates \u2014 rebased index (INR vs peers)": """\
-- Rebase all pairs to 100 at start; rising = USD stronger / local currency weaker
SELECT
    trade_date,
    symbol,
    round(close / first_value(close) OVER (
        PARTITION BY symbol ORDER BY trade_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED PRECEDING
    ) * 100, 4) AS rebased_index
FROM market_data.fx_rates FINAL
ORDER BY symbol, trade_date""",

    "FX Rates \u2014 all pairs 2yr change summary": """\
SELECT
    symbol,
    argMax(trade_date, trade_date)  AS latest_date,
    round(argMax(close, trade_date), 4) AS latest_close,
    round(argMin(close, trade_date), 4) AS close_2yr_ago,
    round((argMax(close, trade_date) - argMin(close, trade_date))
          / argMin(close, trade_date) * 100, 2) AS chg_pct_2yr
FROM market_data.fx_rates FINAL
GROUP BY symbol
ORDER BY symbol""",

    "ML Predictions \u2014 all logged forecasts": """\
SELECT
    as_of,
    horizon_days,
    round(prob_up, 4)              AS prob_up,
    round(expected_return_pct, 3)  AS expected_return_pct,
    confidence_low,
    confidence_high,
    regime_signal,
    round(cv_auc_mean, 4)          AS cv_auc,
    round(cv_r2_mean, 4)           AS cv_skill,
    n_training_rows,
    goldbees_close
FROM market_data.ml_predictions FINAL
ORDER BY as_of DESC, horizon_days""",

    "ML Predictions \u2014 accuracy check (predicted vs actual)": """\
-- Uses log return to match model target basis.
-- Exit price joined on exact calendar date (addDays). If horizon lands on a
-- weekend the row will be NULL — that's expected; use evaluate_ml_performance.py
-- for trading-day-aware evaluation.
SELECT
    m.as_of,
    m.horizon_days,
    round(m.prob_up, 3)                                              AS prob_up,
    round(m.expected_return_pct, 3)                                  AS predicted_logret_pct,
    m.regime_signal,
    m.goldbees_close                                                 AS close_at_pred,
    p.close                                                          AS close_at_expiry,
    round(log(p.close / m.goldbees_close) * 100, 3)                  AS actual_logret_pct,
    round(log(p.close / m.goldbees_close) * 100 - m.expected_return_pct, 3) AS error_pct,
    if(m.expected_return_pct * log(p.close / m.goldbees_close) > 0, 1, 0) AS hit
FROM (
    SELECT as_of, horizon_days, prob_up, expected_return_pct,
           regime_signal, goldbees_close
    FROM market_data.ml_predictions FINAL
) AS m
LEFT JOIN (
    SELECT trade_date, argMax(close, imported_at) AS close
    FROM market_data.daily_prices
    WHERE symbol = 'GOLDBEES' AND category = 'etfs'
    GROUP BY trade_date
) AS p ON p.trade_date = addDays(m.as_of, m.horizon_days)
ORDER BY m.as_of DESC""",

    "Weight Checkpoints \u2014 Kelly vs RG vs Blended": """\
SELECT
    as_of,
    method,
    round(recommended_weight * 100, 1)  AS weight_pct,
    round(cv_r2, 4)                     AS cv_skill,
    round(expected_return_pct, 3)       AS exp_ret_pct,
    round(garch_vol_pct, 2)             AS garch_vol,
    regime,
    rationale
FROM market_data.weight_checkpoints FINAL
ORDER BY as_of DESC, method
LIMIT 60""",

    "FII/DII \u2014 net flows last 60 days": """\
SELECT
    trade_date,
    round(fii_gross_buy_cr, 0)  AS fii_buy_cr,
    round(fii_gross_sell_cr, 0) AS fii_sell_cr,
    round(fii_net_cr, 0)        AS fii_net_cr,
    round(dii_gross_buy_cr, 0)  AS dii_buy_cr,
    round(dii_gross_sell_cr, 0) AS dii_sell_cr,
    round(dii_net_cr, 0)        AS dii_net_cr
FROM market_data.fii_dii_flows FINAL
ORDER BY trade_date DESC
LIMIT 60""",

    "FII/DII \u2014 5-day rolling cumulative net": """\
SELECT
    trade_date,
    round(fii_net_cr, 0)  AS fii_net_cr,
    round(dii_net_cr, 0)  AS dii_net_cr,
    round(sum(fii_net_cr) OVER (ORDER BY trade_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW), 0)
        AS fii_5d_rolling_cr,
    round(sum(dii_net_cr) OVER (ORDER BY trade_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW), 0)
        AS dii_5d_rolling_cr
FROM market_data.fii_dii_flows FINAL
ORDER BY trade_date DESC
LIMIT 90""",
}


def _load_preset() -> None:
    chosen = st.session_state.get("preset_sel", "")
    if chosen and chosen in PRESETS and PRESETS[chosen]:
        st.session_state["sql_editor"] = PRESETS[chosen]


with tab_query:
    st.header("SQL Query Runner")

    st.selectbox(
        "Preset queries",
        options=list(PRESETS.keys()),
        key="preset_sel",
        on_change=_load_preset,
    )

    if "sql_editor" not in st.session_state:
        st.session_state["sql_editor"] = ""

    sql = st.text_area(
        "SQL",
        key="sql_editor",
        height=180,
        placeholder="SELECT * FROM market_data.daily_prices FINAL LIMIT 10",
        label_visibility="collapsed",
    )

    btn_col, dl_col, hist_col = st.columns([1, 1, 4])
    run_q  = btn_col.button("▶ Run",  type="primary", disabled=not ok)
    clear_q = dl_col.button("✕ Clear")
    if clear_q:
        st.session_state["sql_editor"] = ""
        st.rerun()

    if run_q and sql.strip():
        try:
            with st.spinner("Querying ClickHouse…"):
                df = _query_df(sql.strip())
            st.caption(f"**{len(df):,} rows** returned")
            st.dataframe(df, width="stretch", height=400)

            st.download_button(
                "⬇ Download CSV",
                data=df.to_csv(index=False).encode(),
                file_name=f"query_{date.today()}.csv",
                mime="text/csv",
            )

            # Keep history
            hist: list[str] = st.session_state.get("qhistory", [])
            hist = [sql.strip()] + [h for h in hist if h != sql.strip()]
            st.session_state["qhistory"] = hist[:10]

        except Exception as exc:
            st.error(f"Query error: {exc}")

    hist = st.session_state.get("qhistory", [])
    if hist:
        with st.expander(f"History ({len(hist)} queries)", expanded=False):
            for i, h in enumerate(hist):
                label = h[:90] + "…" if len(h) > 90 else h
                if st.button(f"↩ {label}", key=f"hist_{i}"):
                    st.session_state["sql_editor"] = h
                    st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — EXPLORER
# ══════════════════════════════════════════════════════════════════════════════

with tab_explorer:
    st.header("Data Explorer")

    if not ok:
        st.warning("ClickHouse not connected.")
        st.stop()

    # ── 1. COMEX Gold ─────────────────────────────────────────────────────────
    with st.container():
        st.subheader("🪙 COMEX Gold — Daily Close (USD/troy oz)")
        try:
            gdf = _query_df("""
                SELECT trade_date, round(close, 2) AS close_usd
                FROM market_data.daily_prices FINAL
                WHERE symbol = 'GOLD' AND category = 'commodities'
                ORDER BY trade_date ASC
            """)
            if gdf.empty:
                st.info("No COMEX gold data. Run **Import → commodities**.")
            else:
                gdf["trade_date"] = pd.to_datetime(gdf["trade_date"])
                gdf = gdf.set_index("trade_date")
                st.line_chart(gdf["close_usd"], color="#FFD700", height=280)

                m1, m2, m3, m4 = st.columns(4)
                latest = gdf["close_usd"].iloc[-1]
                oldest = gdf["close_usd"].iloc[0]
                ret    = (latest - oldest) / oldest * 100
                m1.metric("Latest",       f"${latest:,.2f}")
                m2.metric("2-Year High",  f"${gdf['close_usd'].max():,.2f}")
                m3.metric("2-Year Low",   f"${gdf['close_usd'].min():,.2f}")
                m4.metric("2-Year Return", f"{ret:+.1f}%")
        except Exception as exc:
            st.error(f"Gold chart: {exc}")

    st.divider()

    # ── GOLDBEES discount alert ───────────────────────────────────────────────
    try:
        _alert_row = _query_df("""
            SELECT
                p.trade_date,
                round(p.close, 4)  AS market_close,
                round(n.nav_adj, 4)  AS amfi_nav,
                if(n.nav_adj > 0, round((p.close - n.nav_adj) / n.nav_adj * 100, 3), NULL) AS premium_disc_pct
            FROM (
                SELECT trade_date, close
                FROM market_data.daily_prices FINAL
                WHERE symbol = 'GOLDBEES' AND category = 'etfs'
            ) p
            LEFT JOIN (
                SELECT nav_date AS trade_date,
                       if(nav_date < '2019-12-23', nav / 100, nav) AS nav_adj
                FROM market_data.mf_nav FINAL
                WHERE symbol = 'GOLDBEES'
            ) n USING (trade_date)
            WHERE n.nav_adj > 0
            ORDER BY p.trade_date DESC
            LIMIT 1
        """)
        if not _alert_row.empty:
            _disc  = float(_alert_row["premium_disc_pct"].iloc[0])
            _price = float(_alert_row["market_close"].iloc[0])
            _nav   = float(_alert_row["amfi_nav"].iloc[0])
            _dt    = str(_alert_row["trade_date"].iloc[0])[:10]
            if _disc <= -1.0:
                st.error(
                    f"🚨 **GOLDBEES Discount Alert** — as of {_dt}  \n"
                    f"Market price **₹{_price:.2f}** is at **{_disc:+.3f}%** vs AMFI NAV ₹{_nav:.2f}  \n"
                    f"Discount exceeds −1% threshold — potential buying opportunity or liquidity stress."
                )
            elif _disc < 0:
                st.warning(
                    f"⚠️ **GOLDBEES at Discount** — as of {_dt}  \n"
                    f"Market price **₹{_price:.2f}** at **{_disc:+.3f}%** vs AMFI NAV ₹{_nav:.2f}"
                )
            else:
                st.success(
                    f"✅ **GOLDBEES at Premium** — as of {_dt}  \n"
                    f"Market price **₹{_price:.2f}** at **{_disc:+.3f}%** vs AMFI NAV ₹{_nav:.2f}"
                )
    except Exception:
        pass

    # ── 2. GOLDBEES market price vs NAV ──────────────────────────────────────
    with st.container():
        _gb_c1, _gb_c2 = st.columns([5, 1])
        _gb_c1.subheader("📊 GOLDBEES — Market Close vs AMFI NAV (₹)")
        _gb_range_map = {"1Y": 365, "3Y": 1095, "5Y": 1825, "10Y": 3650, "All": 9999}
        _gb_range = _gb_c2.selectbox(
            "Range", list(_gb_range_map.keys()), index=1,
            key="gb_nav_range", label_visibility="collapsed",
        )
        _gb_cutoff = (
            pd.Timestamp.today() - pd.Timedelta(days=_gb_range_map[_gb_range])
        ).strftime("%Y-%m-%d")
        try:
            gbdf = _query_df(f"""
                SELECT
                    p.trade_date,
                    round(p.market_close, 4)      AS market_close,
                    nullIf(round(n.nav_adj, 4), 0)    AS amfi_nav
                FROM (
                    SELECT trade_date, argMax(close, imported_at) AS market_close
                    FROM market_data.daily_prices
                    WHERE symbol = 'GOLDBEES' AND category = 'etfs'
                      AND trade_date >= toDate('{_gb_cutoff}')
                    GROUP BY trade_date
                ) p
                LEFT JOIN (
                    SELECT nav_date AS trade_date,
                           if(argMax(nav, imported_at) > 100,
                              argMax(nav, imported_at) / 100,
                              argMax(nav, imported_at)) AS nav_adj
                    FROM market_data.mf_nav
                    WHERE symbol = 'GOLDBEES'
                      AND nav_date >= toDate('{_gb_cutoff}')
                    GROUP BY nav_date
                ) n USING (trade_date)
                ORDER BY p.trade_date ASC
            """)
            if gbdf.empty:
                st.info("No GOLDBEES data. Run **Import → etfs + mf**.")
            else:
                gbdf["trade_date"] = pd.to_datetime(gbdf["trade_date"])
                gbdf = gbdf.set_index("trade_date")
                st.line_chart(gbdf[["market_close", "amfi_nav"]], height=280)
                st.caption(
                    "**market_close** = NSE last traded price (Yahoo Finance)  "
                    "·  **amfi_nav** = AMFI official NAV (MFAPI.in)  "
                    "·  *Gaps = Muhurat trading / holidays where AMFI did not publish NAV*"
                )
        except Exception as exc:
            st.error(f"GOLDBEES chart: {exc}")

    st.divider()

    # ── 3. Premium / Discount impact on GOLDBEES price ───────────────────────
    with st.container():
        st.subheader("↕ How Premium / Discount Impacts GOLDBEES Price")
        st.caption(
            "Premium = market buying pressure pushing price above NAV.  "
            "Discount = selling pressure dragging price below NAV.  "
            "The scatter and correlation views reveal whether today's spread predicts tomorrow's price move."
        )
        try:
            import altair as alt

            pddf = _query_df("""
                SELECT *
                FROM (
                    SELECT
                        p.trade_date,
                        round(p.close, 4)                              AS price,
                        nullIf(round(n.nav_adj, 4), 0)                AS nav,
                        if(n.nav_adj > 0, round((p.close - n.nav_adj) / n.nav_adj * 100, 3), NULL) AS premium_disc_pct
                    FROM (
                        SELECT trade_date, close
                        FROM market_data.daily_prices FINAL
                        WHERE symbol = 'GOLDBEES' AND category = 'etfs'
                    ) p
                    LEFT JOIN (
                        SELECT nav_date AS trade_date,
                               -- old-scale rows (nav > 1000) are pre-split; always normalise
                               if(nav_date < '2019-12-23' OR nav > 1000, nav / 100, nav) AS nav_adj
                        FROM market_data.mf_nav FINAL
                        WHERE symbol = 'GOLDBEES'
                    ) n USING (trade_date)
                )
                WHERE premium_disc_pct IS NULL OR abs(premium_disc_pct) <= 10
                ORDER BY trade_date ASC
            """)

            if not pddf.empty:
                pddf["trade_date"] = pd.to_datetime(pddf["trade_date"])

                # Derived columns
                pddf["next_day_return_pct"] = pddf["price"].pct_change(-1).mul(-100).round(3)  # tomorrow's return
                pddf["price_return_pct"]    = pddf["price"].pct_change().mul(100).round(3)
                pddf["signal"] = pddf["premium_disc_pct"].apply(
                    lambda v: "🟢 Premium" if v >= 0 else "🔴 Discount"
                )

                # ── Summary metrics ─────────────────────────────────────────
                avg = pddf["premium_disc_pct"].mean()
                mx  = pddf["premium_disc_pct"].max()
                mn  = pddf["premium_disc_pct"].min()
                days_disc = int((pddf["premium_disc_pct"] < -0.25).sum())
                days_prem = int((pddf["premium_disc_pct"] > +0.25).sum())
                corr_sameday  = pddf["premium_disc_pct"].corr(pddf["price_return_pct"])
                corr_nextday  = pddf["premium_disc_pct"].corr(pddf["next_day_return_pct"])

                m1, m2, m3, m4, m5, m6 = st.columns(6)
                m1.metric("Avg Spread",      f"{avg:+.3f}%")
                m2.metric("Max Premium",     f"{mx:+.3f}%")
                m3.metric("Max Discount",    f"{mn:+.3f}%")
                m4.metric("Days at Discount (>0.25%)", days_disc)
                m5.metric("Same-day corr",   f"{corr_sameday:+.3f}",
                           help="Pearson corr: spread vs same-day price return")
                m6.metric("Next-day corr",   f"{corr_nextday:+.3f}",
                           help="Pearson corr: today's spread vs tomorrow's price return (mean-reversion signal)")

                tab_overlay, tab_scatter, tab_rolling = st.tabs([
                    "📊 Price vs Spread overlay",
                    "🔵 Scatter: Spread → Next-day Return",
                    "📈 Rolling Correlation",
                ])

                # ── Tab 1: Dual-axis overlay ────────────────────────────────
                with tab_overlay:
                    st.caption(
                        "🟢 **Green bars** = premium (market > NAV, buying pressure up)  "
                        "·  🔴 **Red bars** = discount (selling pressure down)  "
                        "·  🟡 **Line** = GOLDBEES price  ·  Grey band = ±0.25% fair-value zone"
                    )
                    _zero_rule = alt.Chart(pd.DataFrame({"y": [0]})).mark_rule(
                        color="#666", strokeWidth=1, opacity=0.5
                    ).encode(y="y:Q")
                    _fair_band = alt.Chart(
                        pd.DataFrame({"y1": [-0.25], "y2": [0.25]})
                    ).mark_rect(opacity=0.07, color="#aaaaaa").encode(y="y1:Q", y2="y2:Q")

                    _bars = alt.Chart(pddf).mark_bar(size=4).encode(
                        x=alt.X("trade_date:T", title="Date",
                                axis=alt.Axis(format="%b %Y", labelAngle=-35)),
                        y=alt.Y("premium_disc_pct:Q",
                                title="Premium / Discount (%)",
                                scale=alt.Scale(zero=True),
                                axis=alt.Axis(titleColor="#E74C3C", labelColor="#E74C3C")),
                        color=alt.condition(
                            alt.datum.premium_disc_pct >= 0,
                            alt.value("#2ECC71"),
                            alt.value("#E74C3C"),
                        ),
                        tooltip=[
                            alt.Tooltip("trade_date:T", title="Date"),
                            alt.Tooltip("premium_disc_pct:Q", title="Spread %", format="+.3f"),
                            alt.Tooltip("price:Q", title="Price ₹", format=".2f"),
                            "signal:N",
                        ],
                    )

                    _price_line = alt.Chart(pddf).mark_line(
                        color="#FFD700", strokeWidth=2
                    ).encode(
                        x="trade_date:T",
                        y=alt.Y("price:Q", title="GOLDBEES Price (₹)",
                                scale=alt.Scale(zero=False),
                                axis=alt.Axis(titleColor="#FFD700", labelColor="#FFD700")),
                        tooltip=[
                            alt.Tooltip("trade_date:T", title="Date"),
                            alt.Tooltip("price:Q", title="Price ₹", format=".2f"),
                        ],
                    )

                    overlay = alt.layer(
                        _fair_band + _zero_rule + _bars,
                        _price_line,
                    ).resolve_scale(y="independent").properties(height=300).interactive()

                    st.altair_chart(overlay, width="stretch")

                # ── Tab 2: Scatter spread → next-day return ─────────────────
                with tab_scatter:
                    st.caption(
                        "Each dot = one trading day.  "
                        "**X** = today's premium/discount %,  "
                        "**Y** = next day's GOLDBEES price return %.  "
                        "A downward slope (negative corr) = **mean-reversion**: "
                        "discounts tend to be followed by price rebounds."
                    )
                    scatter_df = pddf[["trade_date", "premium_disc_pct",
                                       "next_day_return_pct", "signal"]].dropna()

                    scatter = alt.Chart(scatter_df).mark_circle(
                        opacity=0.55, size=55
                    ).encode(
                        x=alt.X("premium_disc_pct:Q", title="Today's Spread % (+ = premium)"),
                        y=alt.Y("next_day_return_pct:Q",
                                title="Next-day Price Return %"),
                        color=alt.Color("signal:N", scale=alt.Scale(
                            domain=["🟢 Premium", "🔴 Discount"],
                            range=["#2ECC71", "#E74C3C"],
                        )),
                        tooltip=[
                            alt.Tooltip("trade_date:T", title="Date"),
                            alt.Tooltip("premium_disc_pct:Q",  title="Spread %",      format="+.3f"),
                            alt.Tooltip("next_day_return_pct:Q", title="Next-day ret %", format="+.3f"),
                            "signal:N",
                        ],
                    )
                    # OLS trend line
                    trend = scatter.transform_regression(
                        "premium_disc_pct", "next_day_return_pct"
                    ).mark_line(color="#888888", strokeDash=[6, 3], strokeWidth=1.5)

                    st.altair_chart(
                        (scatter + trend).properties(height=320).interactive(),
                        width="stretch",
                    )
                    st.info(
                        f"Next-day return correlation with spread: **{corr_nextday:+.3f}**  \n"
                        f"{'↩ Mean-reversion present — discounts tend to be followed by price recovery.' if corr_nextday < -0.05 else ('↗ Momentum present — premiums tend to attract more buying.' if corr_nextday > 0.05 else '↔ No strong predictive relationship between spread and next-day return.')}"
                    )

                # ── Tab 3: Rolling 30-day correlation ───────────────────────
                with tab_rolling:
                    st.caption(
                        "Rolling 30-day Pearson correlation between the premium/discount spread "
                        "and the **same-day** GOLDBEES price return.  "
                        "**Positive** = premiums coincide with up-days (momentum).  "
                        "**Negative** = spread and price move in opposite directions (arbitrage pressure)."
                    )
                    roll_win = st.slider("Rolling window (days)", 10, 90, 30, 5,
                                         key="pd_roll_win")
                    roll_df = pddf[["trade_date", "premium_disc_pct",
                                    "price_return_pct"]].dropna().set_index("trade_date")
                    roll_corr = (
                        roll_df["premium_disc_pct"]
                        .rolling(roll_win)
                        .corr(roll_df["price_return_pct"])
                        .rename("rolling_corr")
                        .reset_index()
                        .dropna()
                    )
                    _zero_r = alt.Chart(pd.DataFrame({"y": [0]})).mark_rule(
                        color="#888", strokeDash=[4, 2], strokeWidth=1
                    ).encode(y="y:Q")
                    _corr_line = alt.Chart(roll_corr).mark_line(
                        color="#00B4D8", strokeWidth=2
                    ).encode(
                        x=alt.X("trade_date:T", title="Date",
                                axis=alt.Axis(format="%b %Y", labelAngle=-35)),
                        y=alt.Y("rolling_corr:Q", title="Rolling Correlation",
                                scale=alt.Scale(domain=[-1, 1])),
                        tooltip=[
                            alt.Tooltip("trade_date:T", title="Date"),
                            alt.Tooltip("rolling_corr:Q", title="Correlation", format="+.3f"),
                        ],
                    )
                    st.altair_chart(
                        (_zero_r + _corr_line).properties(height=260).interactive(),
                        width="stretch",
                    )

        except ImportError as exc:
            st.error(f"Missing dependency: {exc} — run `.venv/bin/pip install altair`")
        except Exception as exc:
            st.error(f"Premium/Discount chart: {exc}")

    st.divider()

    # ── 4. iNAV Snapshots ─────────────────────────────────────────────────────
    with st.container():
        st.subheader("⚡ NSE Live iNAV Snapshots")
        try:
            invdf = _query_df("""
                SELECT symbol, snapshot_at,
                       round(inav, 4)                AS inav,
                       round(market_price, 4)        AS market_price,
                       round(premium_discount_pct,3) AS prem_disc_pct,
                       source
                FROM market_data.inav_snapshots FINAL
                ORDER BY snapshot_at DESC
                LIMIT 500
            """)
            if invdf.empty:
                st.info(
                    "No iNAV snapshots yet. Run **Import → inav**.  \n"
                    "Schedule periodic runs during market hours to build a time series."
                )
            else:
                syms = sorted(invdf["symbol"].unique().tolist())
                sel_sym = st.selectbox("Symbol", options=syms, key="inav_sym")
                sym_df = invdf[invdf["symbol"] == sel_sym].copy()
                sym_df["snapshot_at"] = pd.to_datetime(sym_df["snapshot_at"])
                sym_df = sym_df.sort_values("snapshot_at").set_index("snapshot_at")

                st.dataframe(
                    sym_df[["inav", "market_price", "prem_disc_pct", "source"]],
                    width="stretch",
                    height=220,
                )
                if len(sym_df) > 1:
                    st.line_chart(sym_df[["inav", "market_price"]], height=220)
        except Exception as exc:
            st.error(f"iNAV chart: {exc}")

    st.divider()

    # ── 5. Symbol explorer ────────────────────────────────────────────────────
    with st.container():
        st.subheader("🔎 Symbol Price History")
        try:
            sym_list = _query_df("""
                SELECT concat(symbol, ' (', category, ')') AS label, symbol, category
                FROM (
                    SELECT DISTINCT symbol, category
                    FROM market_data.daily_prices FINAL
                    ORDER BY category, symbol
                )
            """)
            if sym_list.empty:
                st.info("No price data. Run Import first.")
            else:
                labels  = sym_list["label"].tolist()
                sel_lbl = st.selectbox("Symbol", options=labels, key="sym_sel")
                row     = sym_list[sym_list["label"] == sel_lbl].iloc[0]
                sym, cat = row["symbol"], row["category"]

                ph_df = _query_df(f"""
                    SELECT trade_date, round(open,2) AS open, round(high,2) AS high,
                           round(low,2) AS low, round(close,2) AS close,
                           round(volume,0) AS volume
                    FROM market_data.daily_prices FINAL
                    WHERE symbol = '{sym}' AND category = '{cat}'
                    ORDER BY trade_date ASC
                """)
                ph_df["trade_date"] = pd.to_datetime(ph_df["trade_date"])
                ph_df = ph_df.set_index("trade_date")

                st.line_chart(ph_df["close"], height=260)

                p1, p2, p3, p4, p5 = st.columns(5)
                c = ph_df["close"]
                p1.metric("Latest",  f"{c.iloc[-1]:,.2f}")
                p2.metric("High",    f"{c.max():,.2f}")
                p3.metric("Low",     f"{c.min():,.2f}")
                ret = (c.iloc[-1] - c.iloc[0]) / c.iloc[0] * 100
                p4.metric("Period Return", f"{ret:+.1f}%")
                p5.metric("Data points",  len(ph_df))
        except Exception as exc:
            st.error(f"Symbol explorer: {exc}")

    st.divider()

    # ── 6. CFTC COT — Managed Money Positioning ───────────────────────────────
    with st.container():
        _cot_hdr, _cot_rng = st.columns([5, 1])
        _cot_hdr.subheader("📋 CFTC COT — Managed Money Net Positioning (Gold)")
        _cot_range_map = {"1Y": 52, "3Y": 156, "5Y": 260, "10Y": 520, "All": 9999}
        _cot_range = _cot_rng.selectbox(
            "Range", list(_cot_range_map.keys()), index=2,
            key="cot_range", label_visibility="collapsed",
        )
        try:
            cot_df = _query_df("""
                SELECT
                    report_date,
                    mm_long,
                    mm_short,
                    mm_net,
                    open_interest,
                    round(mm_net / open_interest * 100, 2) AS mm_net_pct_oi,
                    comm_net
                FROM market_data.cot_gold FINAL
                ORDER BY report_date ASC
            """)
            if cot_df.empty:
                st.info("No COT data yet. Run **Import → cot**.")
            else:
                cot_df["report_date"] = pd.to_datetime(cot_df["report_date"])
                cot_df = cot_df.set_index("report_date")

                # Apply range filter
                _cot_n = _cot_range_map[_cot_range]
                cot_view = cot_df.tail(_cot_n)

                c1, c2, c3, c4 = st.columns(4)
                latest = cot_df.iloc[-1]
                c1.metric("MM Net (last week)",     f"{int(latest['mm_net']):,}")
                c2.metric("MM Net % OI",            f"{latest['mm_net_pct_oi']:+.1f}%")
                c3.metric("Open Interest",          f"{int(latest['open_interest']):,}")
                c4.metric("Commercial Net",         f"{int(latest['comm_net']):,}")

                st.caption("**MM Net % OI > +25%** = crowded long (crash risk)  ·  **< −5%** = extreme short (squeeze fuel)")
                st.line_chart(
                    cot_view[["mm_net", "comm_net"]],
                    height=240,
                    color=["#2196F3", "#FF5722"],
                )

                with st.expander("MM Net % of Open Interest"):
                    st.bar_chart(cot_view["mm_net_pct_oi"], height=200, color="#9C27B0")

                n_weeks = st.slider(
                    "Show last N weeks", 26, min(len(cot_view), 9999), min(len(cot_view), 104),
                    26, key="cot_weeks",
                )
                st.dataframe(
                    cot_view[["mm_long", "mm_short", "mm_net", "mm_net_pct_oi",
                               "comm_net", "open_interest"]]
                    .tail(n_weeks)
                    .sort_index(ascending=False)
                    .reset_index(),
                    width="stretch",
                    height=300,
                )
        except Exception as exc:
            st.error(f"COT chart: {exc}")

    st.divider()

    # ── 7. Gold ETF AUM ───────────────────────────────────────────────────────
    with st.container():
        st.subheader("🏦 Gold ETF AUM — Implied Gold Tonnes (GLD · IAU · SGOL · PHYS)")
        try:
            aum_df = _query_df("""
                SELECT trade_date, symbol,
                       round(aum_usd / 1e9, 3)  AS aum_bn_usd,
                       implied_tonnes
                FROM market_data.etf_aum FINAL
                ORDER BY trade_date ASC, symbol ASC
            """)
            if aum_df.empty:
                st.info("No ETF AUM data yet. Run **Import → etf_aum**.")
            else:
                aum_df["trade_date"] = pd.to_datetime(aum_df["trade_date"])

                # Latest snapshot metrics
                latest_date = aum_df["trade_date"].max()
                latest_snap = aum_df[aum_df["trade_date"] == latest_date]
                cols = st.columns(len(latest_snap))
                for i, (_, row) in enumerate(latest_snap.iterrows()):
                    cols[i].metric(
                        row["symbol"],
                        f"{row['implied_tonnes']:.0f} t",
                        f"${row['aum_bn_usd']:.1f}B",
                    )

                # Pivot for charting
                aum_pivot = aum_df.pivot(
                    index="trade_date", columns="symbol", values="implied_tonnes"
                ).fillna(0)
                st.caption("Implied gold tonnes = AUM / (spot price/oz × 32,150.7)")
                st.line_chart(aum_pivot, height=280)

                aum_tab = st.checkbox("Show raw AUM table", key="aum_raw")
                if aum_tab:
                    st.dataframe(
                        aum_df.sort_values(["trade_date", "symbol"], ascending=[False, True]),
                        width="stretch",
                        height=300,
                    )
        except Exception as exc:
            st.error(f"ETF AUM chart: {exc}")

    st.divider()

    # ── 8. Central Bank Gold Reserves ─────────────────────────────────────────
    with st.container():
        st.subheader("🏛 Central Bank Gold Reserves (metric tonnes)")
        try:
            cb_df = _query_df("""
                SELECT ref_period, country_code, country_name,
                       round(reserves_tonnes, 1) AS reserves_tonnes
                FROM market_data.cb_gold_reserves FINAL
                ORDER BY ref_period ASC, country_name ASC
            """)
            if cb_df.empty:
                st.info("No central bank data yet. Run **Import → cb_reserves**.")
            else:
                cb_df["ref_period"] = pd.to_datetime(cb_df["ref_period"])

                # Latest per country
                latest_cb = (
                    cb_df.sort_values("ref_period")
                    .groupby("country_code")
                    .last()
                    .reset_index()
                    .sort_values("reserves_tonnes", ascending=False)
                )

                c_lat, c_chart = st.columns([1, 2])
                with c_lat:
                    st.caption(f"As of {latest_cb['ref_period'].max().strftime('%b %Y')}")
                    st.dataframe(
                        latest_cb[["country_name", "reserves_tonnes"]],
                        width="stretch",
                        hide_index=True,
                        height=300,
                    )
                with c_chart:
                    countries = sorted(cb_df["country_name"].unique().tolist())
                    sel_countries = st.multiselect(
                        "Countries", options=countries,
                        default=[c for c in ["China", "India", "United States", "Germany"]
                                 if c in countries] or countries[:4],
                        key="cb_countries",
                    )
                    if sel_countries:
                        cb_pivot = (
                            cb_df[cb_df["country_name"].isin(sel_countries)]
                            .pivot(index="ref_period", columns="country_name",
                                   values="reserves_tonnes")
                            .fillna(method="ffill")
                        )
                        st.line_chart(cb_pivot, height=280)
        except Exception as exc:
            st.error(f"Central bank chart: {exc}")

    st.divider()

    # ── 9. FX Rates ───────────────────────────────────────────────────────────
    with st.container():
        st.subheader("💱 USD FX Rates — INR vs Peers")
        st.caption(
            "All pairs quoted as **USD/XXX** (how many local units buy $1). "
            "**Rising = local currency weakening vs USD.** "
            "Rebased index (= 100 at start) lets you compare across very different absolute values."
        )
        try:
            fx_df = _query_df("""
                SELECT trade_date, symbol, close
                FROM market_data.fx_rates FINAL
                ORDER BY symbol ASC, trade_date ASC
            """)
            if fx_df.empty:
                st.info("No FX data yet. Run **Import → fx_rates**.")
            else:
                fx_df["trade_date"] = pd.to_datetime(fx_df["trade_date"])

                pairs_avail = sorted(fx_df["symbol"].unique().tolist())
                sel_pairs = st.multiselect(
                    "Pairs", options=pairs_avail, default=pairs_avail,
                    key="fx_pairs_sel",
                )

                if sel_pairs:
                    filtered = fx_df[fx_df["symbol"].isin(sel_pairs)].copy()
                    fx_pivot_raw = filtered.pivot(
                        index="trade_date", columns="symbol", values="close"
                    )

                    # Rebase: each series ÷ its first valid value × 100
                    first_vals = fx_pivot_raw.bfill().iloc[0]
                    fx_rebased = fx_pivot_raw.div(first_vals) * 100

                    tab_idx, tab_raw, tab_corr = st.tabs([
                        "📊 Rebased index (= 100 at start)",
                        "📈 Raw close (USD/XXX)",
                        "🔗 Rolling correlation with USDINR",
                    ])

                    with tab_idx:
                        st.caption(
                            "**105** = currency is **5% weaker** vs USD since start date.  "
                            "AED/SAR near-flat lines = USD pegs."
                        )
                        st.line_chart(fx_rebased, height=300)
                        delta_cols = st.columns(len(sel_pairs))
                        for col, sym in zip(delta_cols, sorted(sel_pairs)):
                            if sym in fx_rebased.columns:
                                last = fx_rebased[sym].dropna().iloc[-1]
                                col.metric(
                                    sym, f"{last:.1f}",
                                    f"{last - 100:+.1f}% vs start",
                                    delta_color="inverse",
                                )

                    with tab_raw:
                        st.caption("Raw USD/XXX rates — not directly comparable across pairs.")
                        st.line_chart(fx_pivot_raw, height=280)
                        latest_fx = (
                            filtered.sort_values("trade_date")
                            .groupby("symbol").last().reset_index()
                        )
                        rcols = st.columns(len(latest_fx))
                        for rcol, (_, row) in zip(rcols, latest_fx.iterrows()):
                            rcol.metric(row["symbol"], f"{row['close']:.4f}",
                                        help=f"As of {row['trade_date'].strftime('%b %d, %Y')}")

                    with tab_corr:
                        st.caption(
                            "Rolling Pearson correlation of **daily returns** vs USDINR.  "
                            "**+1.0** = moves together (broad EM risk-off).  "
                            "**≈0** = idiosyncratic / pegged."
                        )
                        if "USDINR" not in sel_pairs:
                            st.info("Select **USDINR** to enable correlation chart.")
                        else:
                            corr_window = st.slider(
                                "Rolling window (days)", 20, 120, 60, 10, key="fx_corr_win"
                            )
                            returns = fx_pivot_raw.pct_change().dropna()
                            inr_ret = returns["USDINR"]
                            corr_df = pd.DataFrame({
                                sym: returns[sym].rolling(corr_window).corr(inr_ret)
                                for sym in sel_pairs
                                if sym != "USDINR" and sym in returns.columns
                            }).dropna()
                            if corr_df.empty:
                                st.info("Not enough data for rolling correlation.")
                            else:
                                st.line_chart(corr_df, height=260)
                                st.caption(
                                    "USDAED / USDSAR show near-zero correlation (USD-pegged).  "
                                    "USDCNY diverging from USDINR = China-specific factors."
                                )

                    with st.expander("📋 2-year change summary"):
                        summary = []
                        for sym in sorted(sel_pairs):
                            if sym in fx_pivot_raw.columns:
                                s = fx_pivot_raw[sym].dropna()
                                if len(s) >= 2:
                                    chg = (s.iloc[-1] - s.iloc[0]) / s.iloc[0] * 100
                                    summary.append({
                                        "Pair":     sym,
                                        "Start":    f"{s.iloc[0]:.4f}",
                                        "Latest":   f"{s.iloc[-1]:.4f}",
                                        "Change":   f"{chg:+.2f}%",
                                        "Signal":   "⬆ Weaker" if chg > 0.5
                                                    else ("⬇ Stronger" if chg < -0.5
                                                    else "↔ Pegged/Flat"),
                                    })
                        if summary:
                            st.dataframe(
                                pd.DataFrame(summary),
                                width="stretch", hide_index=True,
                            )
        except Exception as exc:
            st.error(f"FX chart: {exc}")

    st.divider()

    # ── 10. Global Anomaly Index vs GOLDBEES price ────────────────────────────
    with st.container():
        st.subheader("📊 Global Anomaly Index (last 180 days) vs GOLDBEES Price")
        st.caption(
            "Composite stress signal averaged across GOLD, GOLDBEES, NIFTY 50, S&P 500 and USDINR.  "
            "Each asset's MAD-based rolling Z-score of daily returns is computed; "
            "the **Global Anomaly Index** is the cross-asset mean of |Z|.  "
            "Spikes indicate broad market stress coinciding with gold ETF price moves."
        )
        try:
            import altair as alt
            from src.ml.anomaly import robust_zscore

            # Assets used to build the composite index
            _GAI_ASSETS = [
                ("GOLD",      "commodities"),
                ("GOLDBEES",  "etfs"),
                ("^NSEI",     "indices"),
                ("^GSPC",     "indices"),
                ("USDINR=X",  "fx_rates"),
            ]
            _since_180 = (pd.Timestamp.today() - pd.Timedelta(days=180)).strftime("%Y-%m-%d")

            # Build IN list safely from the fixed asset tuples
            _sym_in  = ", ".join(f"'{s}'" for s, _ in _GAI_ASSETS)
            _cat_in  = ", ".join(f"'{c}'" for _, c in _GAI_ASSETS)

            gai_raw = _query_df(f"""
                SELECT trade_date,
                       symbol,
                       category,
                       toFloat64(close) AS close
                FROM market_data.daily_prices FINAL
                WHERE (symbol, category) IN (
                    {', '.join(f"('{s}', '{c}')" for s, c in _GAI_ASSETS)}
                )
                  AND trade_date >= '{_since_180}'
                ORDER BY symbol, trade_date ASC
            """)

            # Also try fx_rates table for USDINR if not in daily_prices
            _usdinr_backup = _query_df(f"""
                SELECT trade_date,
                       'USDINR=X' AS symbol,
                       'fx_rates'  AS category,
                       toFloat64(close) AS close
                FROM market_data.fx_rates FINAL
                WHERE symbol = 'USDINR'
                  AND trade_date >= '{_since_180}'
                ORDER BY trade_date ASC
            """)

            gai_raw = pd.concat([gai_raw, _usdinr_backup], ignore_index=True)
            gai_raw["trade_date"] = pd.to_datetime(gai_raw["trade_date"])

            # Pivot: one column per symbol
            gai_pivot = gai_raw.pivot_table(
                index="trade_date", columns="symbol", values="close", aggfunc="last"
            ).sort_index()

            if gai_pivot.shape[1] < 2 or len(gai_pivot) < 20:
                st.info(
                    "Not enough data to compute Global Anomaly Index.  \n"
                    "Run **Import → commodities, etfs, indices, fx_rates** first."
                )
            else:
                # Compute daily returns → robust Z → |Z| per asset → average
                z_cols = []
                for col in gai_pivot.columns:
                    ret = gai_pivot[col].pct_change() * 100
                    ret = ret.replace([float("inf"), float("-inf")], float("nan"))
                    z = robust_zscore(ret, window=20).abs()
                    z.name = col
                    z_cols.append(z)

                z_df = pd.concat(z_cols, axis=1).dropna(how="all")
                # Global Anomaly Index = cross-asset mean of |Z|
                gai_series = z_df.mean(axis=1, skipna=True).rename("Global Anomaly Index")

                # GOLDBEES price for the overlay
                gai_pb = _query_df(f"""
                    SELECT trade_date, toFloat64(close) AS goldbees_close
                    FROM market_data.daily_prices FINAL
                    WHERE symbol = 'GOLDBEES' AND category = 'etfs'
                      AND trade_date >= '{_since_180}'
                    ORDER BY trade_date ASC
                """)
                gai_pb["trade_date"] = pd.to_datetime(gai_pb["trade_date"])
                gai_pb = gai_pb.set_index("trade_date")["goldbees_close"]

                # Align on common dates
                combined = pd.concat([gai_series, gai_pb], axis=1).dropna(how="any").reset_index()
                combined.columns = ["trade_date", "anomaly_index", "goldbees"]

                if combined.empty:
                    st.info("Not enough overlapping data between anomaly index and GOLDBEES.")
                else:
                    # ── Summary metrics ─────────────────────────────────────
                    m1, m2, m3, m4 = st.columns(4)
                    ai = combined["anomaly_index"]
                    pb = combined["goldbees"]
                    m1.metric("Latest Anomaly Index", f"{ai.iloc[-1]:.3f}")
                    m2.metric("180-day Peak",         f"{ai.max():.3f}",
                              help="Highest cross-asset stress reading in window")
                    m3.metric("GOLDBEES Latest",      f"₹{pb.iloc[-1]:.2f}")
                    corr = ai.corr(pb.pct_change())
                    m4.metric("Anomaly ↔ GB Return corr", f"{corr:+.3f}",
                              help="Pearson correlation of anomaly index with GOLDBEES daily returns")

                    # ── Dual-axis Altair chart ───────────────────────────────
                    base = alt.Chart(combined).encode(
                        x=alt.X("trade_date:T", title="Date",
                                axis=alt.Axis(format="%b %d", labelAngle=-35))
                    )

                    # Left axis — Global Anomaly Index (bar)
                    bar = base.mark_bar(opacity=0.55, color="#FF5722").encode(
                        y=alt.Y(
                            "anomaly_index:Q",
                            title="Global Anomaly Index",
                            scale=alt.Scale(zero=True),
                            axis=alt.Axis(titleColor="#FF5722", labelColor="#FF5722"),
                        ),
                        tooltip=[
                            alt.Tooltip("trade_date:T",    title="Date"),
                            alt.Tooltip("anomaly_index:Q", title="Anomaly Index", format=".3f"),
                        ],
                    )

                    # Right axis — GOLDBEES price (line)
                    line = base.mark_line(
                        color="#FFD700", strokeWidth=2.2,
                    ).encode(
                        y=alt.Y(
                            "goldbees:Q",
                            title="GOLDBEES Price (₹)",
                            scale=alt.Scale(zero=False),
                            axis=alt.Axis(titleColor="#FFD700", labelColor="#FFD700"),
                        ),
                        tooltip=[
                            alt.Tooltip("trade_date:T",  title="Date"),
                            alt.Tooltip("goldbees:Q",    title="GOLDBEES ₹", format=".2f"),
                        ],
                    )

                    chart = alt.layer(bar, line).resolve_scale(y="independent").properties(
                        height=340,
                    ).interactive()

                    st.altair_chart(chart, width="stretch")
                    st.caption(
                        "🟠 **Global Anomaly Index** (left axis) — cross-asset mean |MAD Z-score| "
                        "of daily returns across GOLD, GOLDBEES, NIFTY 50, S&P 500 and USDINR  ·  "
                        "🟡 **GOLDBEES price** (right axis, ₹)"
                    )

                    # ── Top stress days table ────────────────────────────────
                    with st.expander("📋 Top 10 stress days", expanded=False):
                        top_stress = (
                            combined.sort_values("anomaly_index", ascending=False)
                            .head(10)
                            .copy()
                            .reset_index(drop=True)
                        )
                        top_stress["trade_date"] = top_stress["trade_date"].dt.date
                        top_stress["anomaly_index"] = top_stress["anomaly_index"].round(4)
                        top_stress["goldbees"]      = top_stress["goldbees"].round(2)
                        top_stress.columns = ["Date", "Anomaly Index", "GOLDBEES ₹"]
                        st.dataframe(top_stress, width="stretch", hide_index=True)

        except ImportError as exc:
            st.error(
                f"Missing dependency: {exc}  \n"
                "Run: `.venv/bin/pip install altair scikit-learn`  then restart Streamlit."
            )
        except Exception as exc:
            st.error(f"Global Anomaly Index chart error: {exc}")

    # ── FII vs DII Institutional Flows ────────────────────────────────────────
    st.divider()
    with st.container():
        st.subheader("🏦 Institutional Flows — FII vs DII Net (Last 30 Days)")
        st.caption(
            "Daily cash-market net flows (₹ Crore) from NSE India.  "
            "🟢 Green bar = net buying (above zero) · 🔴 Red bar = net selling (below zero)."
        )
        try:
            import altair as alt
            fii_df = _query_df("""
                SELECT
                    trade_date,
                    round(fii_net_cr, 0) AS fii_net_cr,
                    round(dii_net_cr, 0) AS dii_net_cr
                FROM market_data.fii_dii_flows FINAL
                ORDER BY trade_date DESC
                LIMIT 30
            """)

            if fii_df.empty:
                st.info(
                    "No FII/DII data yet. "
                    "Run: **Import Data → select fii_dii** or "
                    "`mosaic import -c fii_dii`"
                )
            else:
                fii_df["trade_date"] = pd.to_datetime(fii_df["trade_date"])
                fii_df = fii_df.sort_values("trade_date").reset_index(drop=True)

                # ── KPI metrics ───────────────────────────────────────────────
                latest_row = fii_df.iloc[-1]
                fii_5d     = fii_df["fii_net_cr"].tail(5).sum()
                dii_5d     = fii_df["dii_net_cr"].tail(5).sum()
                net_5d     = fii_5d + dii_5d

                k1, k2, k3, k4, k5 = st.columns(5)
                k1.metric(
                    "FII Latest",
                    f"₹{latest_row['fii_net_cr']:+,.0f} Cr",
                    "buying" if latest_row["fii_net_cr"] >= 0 else "selling",
                    delta_color="normal" if latest_row["fii_net_cr"] >= 0 else "inverse",
                )
                k2.metric(
                    "DII Latest",
                    f"₹{latest_row['dii_net_cr']:+,.0f} Cr",
                    "buying" if latest_row["dii_net_cr"] >= 0 else "selling",
                    delta_color="normal" if latest_row["dii_net_cr"] >= 0 else "inverse",
                )
                k3.metric(
                    "FII 5-Day",
                    f"₹{fii_5d:+,.0f} Cr",
                    "net buying" if fii_5d >= 0 else "net selling",
                    delta_color="normal" if fii_5d >= 0 else "inverse",
                )
                k4.metric(
                    "DII 5-Day",
                    f"₹{dii_5d:+,.0f} Cr",
                    "net buying" if dii_5d >= 0 else "net selling",
                    delta_color="normal" if dii_5d >= 0 else "inverse",
                )
                k5.metric(
                    "Combined 5-Day",
                    f"₹{net_5d:+,.0f} Cr",
                    "market buying" if net_5d >= 0 else "market selling",
                    delta_color="normal" if net_5d >= 0 else "inverse",
                )

                # ── Grouped bar chart — last 15 trading days ─────────────────
                bar_df   = fii_df.tail(15).copy()
                bar_long = bar_df[["trade_date", "fii_net_cr", "dii_net_cr"]].melt(
                    id_vars="trade_date",
                    value_vars=["fii_net_cr", "dii_net_cr"],
                    var_name="_col",
                    value_name="net_cr",
                )
                bar_long["Investor"] = bar_long["_col"].map(
                    {"fii_net_cr": "FII", "dii_net_cr": "DII"}
                )

                _color_scale = alt.Scale(
                    domain=["FII", "DII"],
                    range=["#3498DB", "#E67E22"],
                )
                _zero_rule = (
                    alt.Chart(pd.DataFrame({"z": [0]}))
                    .mark_rule(color="#555555", strokeWidth=1.5, strokeDash=[3, 3])
                    .encode(y=alt.Y("z:Q"))
                )
                _bars = (
                    alt.Chart(bar_long)
                    .mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3, opacity=0.85)
                    .encode(
                        x=alt.X(
                            "trade_date:T",
                            title=None,
                            axis=alt.Axis(format="%d %b", labelAngle=-30, grid=False),
                            scale=alt.Scale(padding=0.3),
                        ),
                        y=alt.Y(
                            "net_cr:Q",
                            title="₹ Crore",
                            scale=alt.Scale(zero=True),
                            axis=alt.Axis(format=",.0f", grid=True, gridOpacity=0.2),
                        ),
                        xOffset=alt.XOffset("Investor:N"),
                        color=alt.Color(
                            "Investor:N",
                            scale=_color_scale,
                            legend=alt.Legend(
                                title=None, orient="top-left",
                                symbolType="square",
                                labelFontSize=12,
                            ),
                        ),
                        tooltip=[
                            alt.Tooltip("trade_date:T", title="Date",        format="%d %b %Y"),
                            alt.Tooltip("Investor:N",   title="Investor"),
                            alt.Tooltip("net_cr:Q",     title="Net (₹ Cr)", format="+,.0f"),
                        ],
                    )
                )

                st.altair_chart(
                    alt.layer(_zero_rule, _bars)
                    .properties(
                        title=alt.TitleParams(
                            "FII & DII Net Flows — Last 15 Days  (🔵 FII · 🟠 DII)",
                            fontSize=13,
                        ),
                        height=320,
                    )
                    .configure_view(strokeWidth=0)
                    .configure_title(anchor="start")
                    .interactive(),
                    width="stretch",
                )

                # 30-day trend line overview
                with st.expander("📈 30-day trend (line chart)", expanded=False):
                    fii_long = fii_df[["trade_date", "fii_net_cr", "dii_net_cr"]].melt(
                        id_vars="trade_date",
                        value_vars=["fii_net_cr", "dii_net_cr"],
                        var_name="_col",
                        value_name="net_cr",
                    )
                    fii_long["Investor"] = fii_long["_col"].map(
                        {"fii_net_cr": "FII", "dii_net_cr": "DII"}
                    )
                    _base30 = alt.Chart(fii_long).encode(
                        x=alt.X(
                            "trade_date:T", title=None,
                            axis=alt.Axis(format="%d %b", labelAngle=-30, grid=False),
                        ),
                        y=alt.Y(
                            "net_cr:Q", title="₹ Crore",
                            scale=alt.Scale(zero=True),
                            axis=alt.Axis(format=",.0f", grid=True, gridOpacity=0.2),
                        ),
                        color=alt.Color(
                            "Investor:N", scale=_color_scale,
                            legend=alt.Legend(
                                title=None, orient="top-left",
                                symbolType="stroke", symbolStrokeWidth=3, labelFontSize=12,
                            ),
                        ),
                        tooltip=[
                            alt.Tooltip("trade_date:T", title="Date",        format="%d %b %Y"),
                            alt.Tooltip("Investor:N",   title="Investor"),
                            alt.Tooltip("net_cr:Q",     title="Net (₹ Cr)", format="+,.0f"),
                        ],
                    )
                    _lines30 = _base30.mark_line(strokeWidth=2).encode(
                        strokeDash=alt.condition(
                            alt.datum["Investor"] == "DII",
                            alt.value([6, 3]),
                            alt.value([1, 0]),
                        )
                    )
                    _dots30 = _base30.mark_circle(size=40, opacity=0.9)
                    _zero30 = (
                        alt.Chart(pd.DataFrame({"z": [0]}))
                        .mark_rule(color="#555555", strokeWidth=1.5, strokeDash=[3, 3])
                        .encode(y=alt.Y("z:Q"))
                    )
                    st.altair_chart(
                        alt.layer(_zero30, _lines30, _dots30)
                        .properties(
                            title=alt.TitleParams(
                                "30-Day Trend  (🔵 FII solid · 🟠 DII dashed)",
                                fontSize=13,
                            ),
                            height=260,
                        )
                        .configure_view(strokeWidth=0)
                        .configure_title(anchor="start")
                        .interactive(),
                        width="stretch",
                    )

                with st.expander("📋 Raw data", expanded=False):
                    show_df = fii_df[["trade_date", "fii_net_cr", "dii_net_cr"]].copy()
                    show_df["trade_date"] = show_df["trade_date"].dt.date
                    show_df["combined"]   = show_df["fii_net_cr"] + show_df["dii_net_cr"]
                    show_df = show_df.sort_values("trade_date", ascending=False)
                    show_df.columns = ["Date", "FII Net (₹ Cr)", "DII Net (₹ Cr)", "Combined (₹ Cr)"]
                    st.dataframe(show_df, use_container_width=True, hide_index=True)

        except ImportError as exc:
            st.error(
                f"Missing dependency: {exc}  \n"
                "Run: `.venv/bin/pip install altair`  then restart Streamlit."
            )
        except Exception as exc:
            if "60" in str(exc) and "UNKNOWN_TABLE" in str(exc):
                st.info(
                    "Table `market_data.fii_dii_flows` does not exist yet.  \n"
                    "Run the import to create it and load data:  \n"
                    "**Import Data → select `fii_dii`** or  \n"
                    "```\nmosaic import -c fii_dii\n```"
                )
            else:
                st.error(f"FII/DII chart error: {exc}")

    st.divider()

    # ── 7. Quant Scorecard ────────────────────────────────────────────────────
    with st.container():
        st.subheader("🎯 Quant Scorecard — GOLDBEES")
        st.caption(
            "Composite 0–100 signal across 4 quantitative pillars: "
            "**Macro** (DXY + Real Yield) · **Flows** (COT positioning) · "
            "**Valuation** (iNAV premium/discount) · **Momentum** (LightGBM 5-day pred).  \n"
            "Score < 33 = bearish, 33–66 = neutral, > 66 = bullish."
        )

        run_sc = st.button("▶ Compute Scorecard", key="run_scorecard", type="primary")
        if run_sc:
            st.session_state.pop("scorecard_result", None)  # force refresh

        # ── Compute / load from cache ──────────────────────────────────────
        if run_sc or "scorecard_result" in st.session_state:
            if run_sc or "scorecard_result" not in st.session_state:
                with st.spinner("Fetching DXY, COT, iNAV, ML prediction…"):
                    try:
                        from src.tools.quant_scorecard import compute_gold_scorecard
                        sc = compute_gold_scorecard(
                            ch_host=CH_HOST, ch_port=CH_PORT,
                            ch_user=CH_USER, ch_pass=CH_PASS,
                            ch_database=CH_DB,
                        )
                        st.session_state["scorecard_result"] = sc
                    except Exception as exc:
                        st.error(f"Scorecard computation failed: {exc}")
                        sc = None
            else:
                sc = st.session_state["scorecard_result"]

            if sc is not None:
                if sc.get("error"):
                    st.warning(f"⚠️ Partial data: {sc['error']}")

                composite = sc["composite_score"]
                as_of_str = str(sc["as_of"]) if sc["as_of"] else "unknown"

                # ── Plotly gauge ─────────────────────────────────────────────
                try:
                    import plotly.graph_objects as go

                    gauge_value = composite if composite is not None else 0
                    fig = go.Figure(go.Indicator(
                        mode="gauge+number",
                        value=gauge_value,
                        number={"suffix": " / 100", "font": {"size": 36}},
                        title={"text": f"Composite Gold Score<br><sub>as of {as_of_str}</sub>",
                               "font": {"size": 18}},
                        gauge={
                            "axis": {"range": [0, 100], "tickwidth": 1,
                                     "tickcolor": "#888"},
                            "bar": {"color": "#FFD700", "thickness": 0.25},
                            "bgcolor": "rgba(0,0,0,0)",
                            "borderwidth": 0,
                            "steps": [
                                {"range": [0,  33], "color": "#4a1010"},
                                {"range": [33, 66], "color": "#4a3a00"},
                                {"range": [66, 100], "color": "#0a3a0a"},
                            ],
                            "threshold": {
                                "line": {"color": "#FFD700", "width": 4},
                                "thickness": 0.75,
                                "value": gauge_value,
                            },
                        },
                    ))
                    fig.update_layout(
                        height=300,
                        margin={"t": 60, "b": 10, "l": 20, "r": 20},
                        paper_bgcolor="rgba(0,0,0,0)",
                        font={"color": "#FAFAFA"},
                    )
                    st.plotly_chart(fig, width="stretch")
                except ImportError:
                    if composite is not None:
                        st.metric("Composite Gold Score", f"{composite:.0f} / 100")
                    else:
                        st.metric("Composite Gold Score", "N/A")

                # ── Pillar breakdown ─────────────────────────────────────────
                c1, c2, c3, c4 = st.columns(4)
                def _fmt_score(v):
                    return f"{v:.0f}" if v is not None else "N/A"

                sigs = sc.get("signals", {})

                def _sig(val, fmt=".2f"):
                    return f"{val:{fmt}}" if val is not None else "N/A"

                c1.metric(
                    "🌍 Macro (30%)", _fmt_score(sc["macro_score"]),
                    help=(
                        f"DXY: {_sig(sigs.get('dxy_level'), '.2f')}  |  "
                        f"Real Yield: {_sig(sigs.get('real_yield_level'), '.3f')}  |  "
                        f"Δ5d: {_sig(sigs.get('real_yield_delta5'), '+.4f')}"
                    ),
                )
                c2.metric(
                    "📋 Flows (30%)", _fmt_score(sc["flows_score"]),
                    help=(
                        f"COT MM Net/OI: {_sig(sigs.get('cot_pct_oi'), '.1f')}%  |  "
                        "< 15% = oversold, > 25% = crowded"
                    ),
                )
                c3.metric(
                    "💰 Valuation (20%)", _fmt_score(sc["valuation_score"]),
                    help=(
                        f"iNAV spread: {_sig(sigs.get('inav_disc_pct'), '+.3f')}%  |  "
                        "Negative = discount (cheap)"
                    ),
                )
                c4.metric(
                    "⚡ Momentum (20%)", _fmt_score(sc["momentum_score"]),
                    help=(
                        f"LightGBM 5-day pred: {_sig(sigs.get('lgbm_return_pct'), '+.2f')}%"
                    ),
                )

                # ── Rolling GOLDBEES–DXY correlation chart ───────────────────
                gb_df  = sc.get("goldbees_prices",  pd.DataFrame())
                dxy_df = sc.get("dxy_prices",       pd.DataFrame())

                if not gb_df.empty and not dxy_df.empty:
                    try:
                        import altair as alt

                        merged = (
                            gb_df.rename(columns={"close": "goldbees"})
                            .merge(
                                dxy_df.rename(columns={"close": "dxy"}),
                                on="trade_date", how="inner",
                            )
                        )
                        merged = merged.sort_values("trade_date").reset_index(drop=True)

                        sc_roll_win = st.slider(
                            "Rolling correlation window (days)", 10, 60, 30, 5,
                            key="sc_roll_win",
                        )
                        merged["gb_ret"]  = merged["goldbees"].pct_change()
                        merged["dxy_ret"] = merged["dxy"].pct_change()
                        merged["rolling_corr"] = (
                            merged["gb_ret"]
                            .rolling(sc_roll_win)
                            .corr(merged["dxy_ret"])
                        )
                        corr_clean = merged[["trade_date", "rolling_corr"]].dropna()

                        latest_corr = (
                            float(corr_clean["rolling_corr"].iloc[-1])
                            if not corr_clean.empty else None
                        )
                        corr_note = ""
                        if latest_corr is not None:
                            if latest_corr < -0.3:
                                corr_note = "↩ **Negative correlation** — gold is hedging dollar strength (typical regime)."
                            elif latest_corr > 0.3:
                                corr_note = "⚠️ **Positive correlation** — gold and DXY moving together (regime decoupling — geopolitical bid?)."
                            else:
                                corr_note = "↔ **Near-zero correlation** — gold decoupled from dollar (macro uncertainty?)."

                        zero_rule = alt.Chart(
                            pd.DataFrame({"y": [0]})
                        ).mark_rule(color="#666", strokeDash=[3, 3], strokeWidth=1).encode(
                            y="y:Q"
                        )
                        corr_line = alt.Chart(corr_clean).mark_line(strokeWidth=2).encode(
                            x=alt.X("trade_date:T", title="Date",
                                    axis=alt.Axis(format="%d %b", labelAngle=-35)),
                            y=alt.Y("rolling_corr:Q",
                                    title="Rolling Correlation (GB returns vs DXY returns)",
                                    scale=alt.Scale(domain=[-1, 1])),
                            color=alt.condition(
                                alt.datum.rolling_corr < 0,
                                alt.value("#00B4D8"),
                                alt.value("#FF4B4B"),
                            ),
                            tooltip=[
                                alt.Tooltip("trade_date:T",    title="Date"),
                                alt.Tooltip("rolling_corr:Q",  title="Correlation", format="+.3f"),
                            ],
                        )
                        st.caption(
                            f"**{sc_roll_win}-day Rolling Correlation: GOLDBEES returns vs DXY returns**  |  "
                            f"Latest: **{latest_corr:+.3f}**  |  {corr_note}"
                            if latest_corr is not None
                            else f"**{sc_roll_win}-day Rolling Correlation: GOLDBEES vs DXY**"
                        )
                        st.altair_chart(
                            (zero_rule + corr_line).properties(height=240).interactive(),
                            width="stretch",
                        )
                    except ImportError as exc:
                        st.info(f"altair not installed: {exc}")
                    except Exception as exc:
                        st.error(f"Correlation chart error: {exc}")
                else:
                    st.info(
                        "Rolling correlation chart requires GOLDBEES price history in ClickHouse "
                        "and DXY data from Yahoo Finance. Run **Import → etfs** first."
                    )


# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — ANOMALY DETECTION (Robust Z + RF Residuals + Isolation Forest)
# ══════════════════════════════════════════════════════════════════════════════

with tab_anomaly:
    st.header("🔬 Composite Anomaly Detection")
    st.caption(
        "**Step 1** Robust Z-Score (MAD)  ·  "
        "**Step 2** GARCH(1,1) Standardised Residual Z-Score  ·  "
        "**Step 3** Isolation Forest Confidence Multiplier  \n"
        "**Final Z** = Z_robust × (1 + IF_confidence)"
    )

    if not ok:
        st.warning("ClickHouse not connected.")
    else:
        col_cfg, col_res = st.columns([1, 3])

        with col_cfg:
            st.subheader("Settings")

            @st.cache_data(ttl=3600)
            def _anomaly_sym_labels() -> list[str]:
                try:
                    df = _query_df("""
                        SELECT DISTINCT symbol, category
                        FROM market_data.daily_prices FINAL
                        WHERE category IN ('commodities', 'etfs')
                        ORDER BY category, symbol
                    """)
                    return (df["symbol"] + " (" + df["category"] + ")").tolist()
                except Exception:
                    return ["GOLD (commodities)"]

            _sym_labels = _anomaly_sym_labels()

            _default_idx = next(
                (i for i, lbl in enumerate(_sym_labels) if lbl.startswith("GOLD")), 0
            )
            iso_label = st.selectbox(
                "Symbol", _sym_labels, index=_default_idx, key="iso_sym"
            )
            iso_sym = iso_label.split(" (")[0]
            iso_cat = iso_label.split("(")[1].rstrip(")")

            import re as _re
            if not _re.fullmatch(r"[\w\-\^\.\=\&]+", iso_sym) or not _re.fullmatch(r"[a-z]+", iso_cat):
                st.error("Invalid symbol or category.")
                st.stop()

            contamination = st.slider(
                "IF Contamination (%)", min_value=1, max_value=20, value=5, step=1,
                help="Expected fraction of anomalous days for Isolation Forest.",
            ) / 100.0

            z_threshold = st.slider(
                "Final-Z alert threshold", min_value=1.0, max_value=5.0,
                value=2.5, step=0.5,
                help="Days where |Final Z| exceeds this are flagged.",
            )

            z_window = st.slider(
                "Z-score rolling window", min_value=10, max_value=60, value=30,
                help="Lookback period for rolling Median and MAD in the Robust Z calculation.",
            )

            run_btn = st.button(
                "▶ Run Analysis", type="primary", width="stretch"
            )

        with col_res:
            with st.expander("ℹ How it works", expanded=False):
                st.markdown("""
**Step 1 — Robust Z-Score (MAD)**
Standard Z-score inflates σ when prices trend, masking shocks.
MAD Z-score stays centred on the median and resists outliers.
`Z_robust = 0.6745 × (x − median) / MAD`

**Step 2 — GARCH(1,1) Standardised Residual Z-Score**
GARCH models conditional volatility σ_t (volatility clustering).
Standardised residual **e_t = r_t / σ_t** isolates truly unexpected moves:
quiet-period σ is small → moderate returns flag; volatile-period σ is large → only extreme returns flag.
Student-t distribution captures gold's fat-tailed returns.  Fire rate ≈ 5% vs old RF's 21%.

| Z_robust | Z_resid | COT crowding | Regime |
|---|---|---|---|
| — | High | — | ⚡ Flash Crash / Black Swan — EXIT |
| High | High | — | 🔥 Volatile Breakout |
| High | Low | > top-quartile | ⚠️ Crowded Long (Squeeze Risk) |
| High | Low | — | 📈 Strong Trend — HODL |
| Low | Low | — | ✅ Normal |

**Step 3 — Isolation Forest Confidence Multiplier**
Features: daily_return, range_pct, z_volume + USDINR vol + COT crowding (when available).
`Final_Z = Z_robust × (1 + IF_confidence)`
This *boosts* only days suspicious to **both** algorithms.
                """)

            if run_btn:
                try:
                    import importlib, src.ml.anomaly as _anomaly_mod
                    importlib.reload(_anomaly_mod)
                    run_composite_anomaly = _anomaly_mod.run_composite_anomaly
                    import altair as alt

                    with st.spinner(f"Fetching {iso_sym} data from ClickHouse…"):
                        raw = _query_df(
                            f"SELECT trade_date,"
                            f" toFloat64(open) AS open, toFloat64(high) AS high,"
                            f" toFloat64(low) AS low, toFloat64(close) AS close,"
                            f" toFloat64(volume) AS volume"
                            f" FROM market_data.daily_prices FINAL"
                            f" WHERE symbol = '{iso_sym}' AND category = '{iso_cat}'"
                            f" ORDER BY trade_date ASC"
                        )

                    if len(raw) < 60:
                        st.warning(
                            f"Only {len(raw)} rows for {iso_sym} — need ≥ 60. "
                            "Run Import first."
                        )
                        st.stop()

                    raw["trade_date"] = pd.to_datetime(raw["trade_date"])

                    # ── Fetch cross-asset data (COT + USDINR) in parallel ─────
                    with st.spinner("Fetching cross-asset data (COT, FX)…"):
                        from concurrent.futures import ThreadPoolExecutor as _XATPE
                        def _fetch_cot():
                            return _query_df(
                                "SELECT report_date, mm_net, open_interest "
                                "FROM market_data.cot_gold"
                            )
                        def _fetch_fx():
                            return _query_df(
                                "SELECT symbol, trade_date, toFloat64(close) AS close "
                                "FROM market_data.fx_rates FINAL "
                                "WHERE symbol = 'USDINR'"
                            )
                        with _XATPE(max_workers=2) as _xapool:
                            _f_cot = _xapool.submit(_fetch_cot)
                            _f_fx  = _xapool.submit(_fetch_fx)
                            df_cot_raw = _f_cot.result()
                            df_fx_raw  = _f_fx.result()
                        if not df_cot_raw.empty:
                            df_cot_raw["report_date"] = pd.to_datetime(df_cot_raw["report_date"])
                        if not df_fx_raw.empty:
                            df_fx_raw["trade_date"] = pd.to_datetime(df_fx_raw["trade_date"])

                    with st.spinner(
                        f"Running GARCH(1,1) anomaly detection on {iso_sym}  "
                        "(Robust Z → GARCH residuals → Isolation Forest)…"
                    ):
                        df_if, flagged, garch_loglik = run_composite_anomaly(
                            raw,
                            contamination=contamination,
                            z_threshold=z_threshold,
                            z_window=z_window,
                            df_cot=df_cot_raw if not df_cot_raw.empty else None,
                            df_fx=df_fx_raw  if not df_fx_raw.empty  else None,
                        )

                    # ── Summary metrics ───────────────────────────────────────
                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("Rows analysed",              f"{len(df_if):,}")
                    c2.metric(f"|Final Z| > {z_threshold}", len(flagged))
                    c3.metric("Max Final Z",                f"{df_if['final_z_abs'].max():.2f}")
                    c4.metric("GARCH Log-Likelihood",       f"{garch_loglik:.0f}")

                    # ── Chart 1: Price + flagged markers by regime ────────────
                    st.subheader("Close Price — Flagged Days by Regime")
                    price_line = alt.Chart(df_if).mark_line(
                        color="#4C78A8", strokeWidth=1.5,
                    ).encode(
                        x=alt.X("trade_date:T", title="Date"),
                        y=alt.Y("close:Q",      title="Close Price"),
                        tooltip=["trade_date:T",
                                 alt.Tooltip("close:Q", format=".2f"),
                                 "regime:N"],
                    )
                    regime_dots = alt.Chart(flagged).mark_point(
                        filled=True, size=100, opacity=0.85,
                    ).encode(
                        x="trade_date:T",
                        y="close:Q",
                        color=alt.Color("regime:N",
                                        scale=alt.Scale(scheme="tableau10")),
                        tooltip=[
                            "trade_date:T",
                            alt.Tooltip("close:Q",         format=".2f"),
                            alt.Tooltip("final_z_abs:Q",   title="Final |Z|",  format=".3f"),
                            alt.Tooltip("z_robust:Q",      title="Robust Z",   format=".3f"),
                            alt.Tooltip("z_resid_abs:Q",   title="Resid |Z|",  format=".3f"),
                            alt.Tooltip("if_confidence:Q", title="IF Conf",    format=".3f"),
                            "regime:N",
                        ],
                    )
                    st.altair_chart(
                        (price_line + regime_dots).interactive().properties(height=300),
                        width="stretch",
                    )

                    # ── Chart 2: Z-score decomposition ────────────────────────
                    st.subheader("Z-Score Decomposition")
                    z_melt = df_if[["trade_date", "z_robust", "z_resid", "final_z"]].melt(
                        id_vars="trade_date", var_name="series", value_name="z"
                    )
                    z_lines = alt.Chart(z_melt).mark_line(opacity=0.75).encode(
                        x=alt.X("trade_date:T", title="Date"),
                        y=alt.Y("z:Q",          title="Z-Score"),
                        color=alt.Color("series:N", scale=alt.Scale(
                            domain=["z_robust", "z_resid", "final_z"],
                            range=["#4C78A8",   "#F58518",  "#E45756"],
                        )),
                        tooltip=["trade_date:T", "series:N",
                                 alt.Tooltip("z:Q", format=".3f")],
                    )
                    thresh_rules = alt.Chart(
                        pd.DataFrame({"y": [z_threshold, -z_threshold]})
                    ).mark_rule(
                        color="gray", strokeDash=[5, 3], opacity=0.5
                    ).encode(y="y:Q")
                    st.altair_chart(
                        (z_lines + thresh_rules).interactive().properties(height=240),
                        width="stretch",
                    )
                    st.caption(
                        "🔵 z_robust (MAD)  ·  🟠 z_resid (GARCH std resid)  ·  "
                        "🔴 final_z (boosted)  ·  dashed = ±threshold"
                    )

                    # ── Chart 3: IF Confidence area ───────────────────────────
                    st.subheader("Isolation Forest Confidence (0 → 1)")
                    if_area = alt.Chart(df_if).mark_area(
                        color="#E45756", opacity=0.35, line=True,
                    ).encode(
                        x=alt.X("trade_date:T", title="Date"),
                        y=alt.Y("if_confidence:Q", title="IF Confidence",
                                scale=alt.Scale(domain=[0, 1])),
                        tooltip=["trade_date:T",
                                 alt.Tooltip("if_confidence:Q", format=".4f")],
                    )
                    st.altair_chart(
                        if_area.interactive().properties(height=150),
                        width="stretch",
                    )

                    # ── Chart 4: GARCH dual-panel ─────────────────────────────
                    # Panel A — last 90 days zoomed so ±1σ/±2σ bands are visible.
                    # Panel B — full-history annualised vol % (the real signal).
                    st.subheader("GARCH(1,1) Conditional Volatility")
                    garch_annual_vol = float(df_if["garch_vol"].dropna().iloc[-1]) \
                        if "garch_vol" in df_if.columns else 0.0

                    _garch_all = df_if[
                        ["trade_date", "close", "garch_band_1s", "garch_band_2s", "garch_vol"]
                    ].dropna(subset=["garch_band_1s", "garch_band_2s"]).copy()
                    _garch_all["upper_2s"] = _garch_all["close"] + _garch_all["garch_band_2s"]
                    _garch_all["lower_2s"] = _garch_all["close"] - _garch_all["garch_band_2s"]
                    _garch_all["upper_1s"] = _garch_all["close"] + _garch_all["garch_band_1s"]
                    _garch_all["lower_1s"] = _garch_all["close"] - _garch_all["garch_band_1s"]

                    # Panel A: last 90 days — bands are ~2-3% of price, visible at this zoom
                    _garch_90 = _garch_all.tail(90).reset_index(drop=True)
                    _ya_min = float(_garch_90["lower_2s"].min())
                    _ya_max = float(_garch_90["upper_2s"].max())
                    _ya_pad = (_ya_max - _ya_min) * 0.08
                    _ya_scale = alt.Scale(domain=[_ya_min - _ya_pad, _ya_max + _ya_pad], zero=False)

                    _band2 = alt.Chart(_garch_90).mark_area(
                        opacity=0.20, color="#F58518",
                    ).encode(
                        x=alt.X("trade_date:T", title=None, axis=alt.Axis(labels=False)),
                        y=alt.Y("lower_2s:Q", title="Price", scale=_ya_scale),
                        y2="upper_2s:Q",
                        tooltip=[
                            "trade_date:T",
                            alt.Tooltip("lower_2s:Q", title="−2σ", format=".2f"),
                            alt.Tooltip("upper_2s:Q", title="+2σ", format=".2f"),
                        ],
                    )
                    _band1 = alt.Chart(_garch_90).mark_area(
                        opacity=0.35, color="#F58518",
                    ).encode(
                        x=alt.X("trade_date:T", axis=alt.Axis(labels=False)),
                        y=alt.Y("lower_1s:Q", scale=_ya_scale),
                        y2="upper_1s:Q",
                        tooltip=[
                            "trade_date:T",
                            alt.Tooltip("lower_1s:Q", title="−1σ", format=".2f"),
                            alt.Tooltip("upper_1s:Q", title="+1σ", format=".2f"),
                        ],
                    )
                    _close90 = alt.Chart(_garch_90).mark_line(
                        color="#4C78A8", strokeWidth=1.5,
                    ).encode(
                        x=alt.X("trade_date:T", axis=alt.Axis(labels=False)),
                        y=alt.Y("close:Q", scale=_ya_scale),
                        tooltip=[
                            "trade_date:T",
                            alt.Tooltip("close:Q", title="Close", format=".2f"),
                        ],
                    )
                    panel_a = (_band2 + _band1 + _close90).properties(
                        height=180,
                        title=alt.TitleParams(
                            "Last 90 days — price with ±1σ / ±2σ bands",
                            fontSize=12, color="gray",
                        ),
                    )

                    # Panel B: full history annualised vol %
                    _vol_df = _garch_all[["trade_date", "garch_vol"]].dropna()
                    _vol_line = alt.Chart(_vol_df).mark_area(
                        color="#E45756", opacity=0.35, line={"color": "#E45756", "strokeWidth": 1},
                    ).encode(
                        x=alt.X("trade_date:T", title="Date"),
                        y=alt.Y("garch_vol:Q", title="Ann. Vol %", scale=alt.Scale(zero=True)),
                        tooltip=[
                            "trade_date:T",
                            alt.Tooltip("garch_vol:Q", title="Ann. Vol %", format=".1f"),
                        ],
                    )
                    panel_b = _vol_line.properties(
                        height=120,
                        title=alt.TitleParams(
                            "Full history — GARCH annualised volatility %",
                            fontSize=12, color="gray",
                        ),
                    )

                    st.caption(
                        f"Latest GARCH annualised vol: **{garch_annual_vol:.1f}%**  ·  "
                        "🟠 bands = ±1σ (darker) / ±2σ (lighter)  ·  🔵 = close  ·  "
                        "🔴 panel = conditional vol regime"
                    )
                    st.altair_chart(
                        alt.vconcat(panel_a, panel_b).resolve_scale(x="shared").interactive(),
                        width="stretch",
                    )

                    # ── Top flagged days table ────────────────────────────────
                    n_show = min(25, len(flagged))
                    st.subheader(f"Top {n_show} Flagged Days — sorted by |Final Z|")
                    show_cols = ["trade_date", "close", "final_z_abs",
                                 "z_robust", "z_resid_abs", "if_confidence", "regime"]
                    top_tbl = (
                        flagged[show_cols]
                        .sort_values("final_z_abs", ascending=False)
                        .head(25)
                        .copy()
                        .reset_index(drop=True)
                    )
                    top_tbl["trade_date"] = top_tbl["trade_date"].dt.date
                    for col in ["final_z_abs", "z_robust", "z_resid_abs", "if_confidence"]:
                        top_tbl[col] = top_tbl[col].round(4)
                    top_tbl["close"] = top_tbl["close"].round(2)
                    st.dataframe(top_tbl, width="stretch")

                    st.download_button(
                        "⬇ Download flagged days CSV",
                        data=top_tbl.to_csv(index=False).encode(),
                        file_name=f"anomalies_{iso_sym}_{date.today()}.csv",
                        mime="text/csv",
                    )

                except ImportError as exc:
                    st.error(
                        f"Missing dependency: {exc}  \n"
                        "Run: `.venv/bin/pip install scikit-learn altair`  "
                        "then restart Streamlit."
                    )
                except Exception as exc:
                    st.error(f"Analysis error: {exc}")

            else:
                st.info(
                    "Configure settings on the left and click **▶ Run Analysis**.  \n\n"
                    "**Formula:**  \n"
                    "> `Final_Z = Z_robust × (1 + IF_confidence)`  \n\n"
                    "**Regimes:**  \n"
                    "- 📈 **Strong Trend** — high Z_robust, low Z_resid → HODL  \n"
                    "- ⚡ **Flash Crash** — low Z_robust, high Z_resid → EXIT  \n"
                    "- 🔥 **Volatile Breakout** — both high → caution  \n"
                    "- ✅ **Normal** — both low → no action  \n"
                )


# ══════════════════════════════════════════════════════════════════════════════
# TAB 5 — WHO IS SELLING?
# ══════════════════════════════════════════════════════════════════════════════

_REGIME_COLORS = {
    "RETAIL_PANIC":       "#FF4B4B",
    "INSTITUTIONAL_EXIT": "#FF8C00",
    "OVERLEVERED_LONGS":  "#FFD700",
    "SHORT_SQUEEZE_SETUP":"#00B4D8",
    "CB_ACCUMULATION":    "#4CAF50",
    "MIXED":              "#9C27B0",
    "NEUTRAL":            "#888888",
}
_SIGNAL_ICONS = {
    "PANIC":          "🔴",
    "STRESSED":       "🟠",
    "DISCOUNT":       "🟡",
    "EXIT":           "🔴",
    "CROWDED_LONG":   "🔴",
    "ELEVATED_LONG":  "🟡",
    "EXTREME_SHORT":  "🔵",
    "ACCUMULATING":   "🟢",
    "PARTIAL":        "🟡",
    "WEAK":           "🔴",
    "INFLOW":         "🟢",
    "NEUTRAL":        "🟢",
    "unknown":        "⚪",
    "error":          "❌",
}

with tab_wis:
    st.header("🕵️ Who Is Selling?")
    st.caption(
        "Identifies *which* market segment is driving a gold sell-off by checking "
        "4 independent signal streams in real time.  \n"
        "**Sources:** ClickHouse (fx_rates, cot_gold, daily_prices, mf_nav) + yfinance (GLD, USDCNY, CL=F)"
    )

    if not ok:
        st.warning("ClickHouse not connected.")
        st.stop()

    col_run, col_info = st.columns([1, 3])
    with col_run:
        run_wis = st.button("🔍 Analyse Now", type="primary", width="stretch")
        st.caption("Calls yfinance + ClickHouse live — takes ~5 seconds.")

    with col_info:
        with st.expander("Signal logic", expanded=False):
            st.markdown("""
| Signal | Trigger | Interpretation |
|--------|---------|----------------|
| 🇮🇳 **Retail Panic** | USDINR +3% in 60d **AND** GOLDBEES discount < −1% | Indian retail panic-selling |
| 🏦 **Institutional Exit** | GLD AUM proxy −3% in 30d | Western hedge funds redeeming |
| 📋 **Speculator Crowding** | MM Net / OI > 25% | Leveraged longs at crash risk |
| 🌍 **CB Accumulation** | USDCNY stable (<1.5%) **AND** WTI > $80 | China + Gulf absorbing selling |
            """)

    if run_wis:
        with st.spinner("Running 4 signal checks…"):
            try:
                from src.tools.who_is_selling_agent import fetch_who_is_selling
                result = fetch_who_is_selling(verbose=False)

                regime  = result["regime"]
                color   = _REGIME_COLORS.get(regime, "#888888")
                signals = result["signals"]

                # ── Regime banner ───────────────────────────────────────────
                st.markdown(
                    f"<div style='background:{color}22;border-left:5px solid {color};"
                    f"padding:12px 18px;border-radius:6px;margin-bottom:12px'>"
                    f"<span style='font-size:1.3em;font-weight:700;color:{color}'>"
                    f"REGIME: {regime}</span><br/>"
                    f"<span style='font-size:0.9em;color:#ccc'>{result['summary']}</span>"
                    f"</div>",
                    unsafe_allow_html=True,
                )

                # ── Recommendation ──────────────────────────────────────────
                st.info(f"💡 **Recommendation:** {result['recommendation']}")

                # ── Signal cards ────────────────────────────────────────────
                st.subheader("Signal Details")
                c1, c2, c3, c4 = st.columns(4)
                for col, (key, label, detail_keys) in zip(
                    [c1, c2, c3, c4],
                    [
                        ("retail",      "🇮🇳 Retail (India)",
                         [("USDINR 60d", "usdinr_60d_pct", "{:+.2f}%"),
                          ("GOLDBEES disc", "goldbees_disc_pct", "{:+.3f}%"),
                          ("INR rate", "usdinr_latest", "{:.4f}")]),
                        ("institution", "🏦 Institutions",
                         [("GLD AUM", "gld_aum_usd", "${:.1f}B", 1e9),
                          ("GLD 30d Δ", "gld_30d_chg_pct", "{:+.1f}%"),
                          ("GLD price", "gld_price", "${:.2f}")]),
                        ("speculator",  "📋 Speculators",
                         [("MM Net%OI", "mm_net_pct_oi", "{:+.1f}%"),
                          ("MM Net", "mm_net", "{:,}"),
                          ("Open Int", "open_interest", "{:,}")]),
                        ("cb",          "🌍 Central Banks",
                         [("USDCNY 30d", "usdcny_30d_pct", "{:+.2f}%"),
                          ("CNY rate", "usdcny_now", "{:.4f}"),
                          ("WTI Crude", "crude_price", "${:.1f}")]),
                    ],
                ):
                    sig = signals[key]
                    status = sig.get("status", "unknown")
                    icon   = _SIGNAL_ICONS.get(status, "⚪")
                    col.markdown(f"**{label}**")
                    col.markdown(f"{icon} `{status}`")
                    for row in detail_keys:
                        field_label, field_key = row[0], row[1]
                        fmt = row[2] if len(row) > 2 else "{}"
                        divisor = row[3] if len(row) > 3 else 1
                        val = sig.get(field_key)
                        if val is not None:
                            try:
                                display = fmt.format(val / divisor)
                            except Exception:
                                display = str(val)
                            col.caption(f"{field_label}: **{display}**")

                # ── Detail expanders ─────────────────────────────────────────
                st.subheader("Signal Narratives")
                for key, name in [
                    ("retail",      "🇮🇳 Retail Panic (India)"),
                    ("institution", "🏦 Institutional Exit (GLD)"),
                    ("speculator",  "📋 Speculator Over-Leverage (COT)"),
                    ("cb",          "🌍 Central Bank Strength"),
                ]:
                    sig = signals[key]
                    status = sig.get("status", "unknown")
                    icon   = _SIGNAL_ICONS.get(status, "⚪")
                    with st.expander(f"{icon} {name} — **{status}**", expanded=True):
                        st.write(sig.get("detail", "No detail available."))

                # ── Global Anomaly Index chart ────────────────────────────────
                st.subheader("📊 Global Anomaly Index (last 180 days)")
                try:
                    gai_df = _query_df("""
                        SELECT
                            p.trade_date AS trade_date,
                            round((p.close - n.nav) / n.nav * 100, 3) AS retail_disc_pct,
                            round(cot.mm_net / cot.open_interest * 100, 2)  AS mm_net_pct_oi,
                            f.close AS usdinr
                        FROM (
                            SELECT trade_date, close FROM market_data.daily_prices FINAL
                            WHERE symbol = 'GOLDBEES' AND category = 'etfs'
                              AND trade_date >= today() - INTERVAL '180' DAY
                        ) p
                        JOIN (
                            SELECT nav_date AS trade_date, nav
                            FROM market_data.mf_nav FINAL
                            WHERE symbol = 'GOLDBEES'
                        ) n ON p.trade_date = n.trade_date
                        LEFT JOIN (
                            SELECT
                                d.trade_date,
                                argMax(c.mm_net, c.report_date)        AS mm_net,
                                argMax(c.open_interest, c.report_date) AS open_interest
                            FROM (
                                SELECT DISTINCT trade_date
                                FROM market_data.daily_prices FINAL
                                WHERE symbol = 'GOLDBEES' AND category = 'etfs'
                                  AND trade_date >= today() - INTERVAL '180' DAY
                            ) d
                            CROSS JOIN market_data.cot_gold c
                            WHERE c.report_date <= d.trade_date
                            GROUP BY d.trade_date
                        ) cot ON p.trade_date = cot.trade_date
                        LEFT JOIN (
                            SELECT trade_date, close FROM market_data.fx_rates FINAL
                            WHERE symbol = 'USDINR'
                        ) f ON p.trade_date = f.trade_date
                        ORDER BY p.trade_date ASC
                    """)
                    if not gai_df.empty:
                        gai_df["trade_date"] = pd.to_datetime(gai_df["trade_date"])
                        gai_df = gai_df.set_index("trade_date")

                        tab_disc, tab_cot, tab_inr = st.tabs([
                            "GOLDBEES Discount %",
                            "COT MM Net % OI",
                            "USDINR",
                        ])
                        with tab_disc:
                            st.caption("Negative = discount (retail selling pressure). Below −1% = panic zone.")
                            st.bar_chart(gai_df["retail_disc_pct"].dropna(), height=220, color="#FF4B4B")
                        with tab_cot:
                            st.caption("Above 25% = crowded long (crash risk). Below −5% = short-squeeze fuel.")
                            st.line_chart(gai_df["mm_net_pct_oi"].dropna(), height=220, color="#9C27B0")
                        with tab_inr:
                            st.caption("Rising = rupee weakening vs USD.")
                            st.line_chart(gai_df["usdinr"].dropna(), height=220, color="#FF8C00")
                except Exception as exc:
                    st.warning(f"Anomaly index chart: {exc}")

            except Exception as exc:
                st.error(f"Who Is Selling analysis failed: {exc}")
    else:
        st.info("Click **🔍 Analyse Now** to run the real-time signal check.")

    # ══════════════════════════════════════════════════════════════════════════
    # ML FORECAST — LightGBM 5-day forward return predictor
    # Independent of the expert-system button above.
    # ══════════════════════════════════════════════════════════════════════════
    st.divider()
    st.subheader("🤖 LightGBM 5-Day Forecast")
    st.caption(
        "Classifies **direction** (up/down) via walk-forward cross-validation, "
        "then sizes using Kelly formula.  \n"
        "**Target:** sign(log return over horizon) — binary classifier  ·  "
        "**Metric:** AUC (0.5 = random, >0.55 = useful edge)  ·  "
        "**Validation:** TimeSeriesSplit — no look-ahead leakage"
    )

    ml_cfg_col, ml_run_col = st.columns([2, 1])
    with ml_cfg_col:
        ml_horizon  = st.slider("Forecast horizon (trading days)", 3, 15, 5, 1, key="ml_horizon")
        ml_n_splits = st.slider("CV folds", 3, 8, 5, 1, key="ml_splits")
    with ml_run_col:
        st.write("")  # vertical spacer
        st.write("")
        run_ml = st.button("📊 Run ML Forecast", type="secondary",
                           width="stretch", key="ml_btn")
        st.caption("~5–10 seconds: assemble → engineer → train → predict")

    if run_ml:
        with st.spinner("Assembling master table → engineering features → walk-forward training → predicting…"):
            try:
                from src.ml.trend_predictor import run_trend_prediction
                ml = run_trend_prediction(
                    horizon=ml_horizon,
                    n_splits=ml_n_splits,
                    verbose=False,
                    ch_host=CH_HOST,
                    ch_port=CH_PORT,
                    ch_database=CH_DB,
                    ch_user=CH_USER,
                    ch_password=CH_PASS,
                )

                _ML_SIGNAL_COLORS = {
                    "BUY":         "#4CAF50",
                    "WATCH_LONG":  "#8BC34A",
                    "HOLD":        "#FFC107",
                    "WATCH_SHORT": "#FF9800",
                    "SELL":        "#F44336",
                }
                sig_color = _ML_SIGNAL_COLORS.get(ml["regime_signal"], "#888888")

                # Regime banner
                st.markdown(
                    f"<div style='background:{sig_color}22;border-left:5px solid {sig_color};"
                    f"padding:12px 18px;border-radius:6px;margin-bottom:12px'>"
                    f"<span style='font-size:1.2em;font-weight:700;color:{sig_color}'>"
                    f"ML SIGNAL: {ml['regime_signal']}</span><br/>"
                    f"<span style='font-size:0.9em;color:#ccc'>{ml['regime_rationale']}</span>"
                    f"</div>",
                    unsafe_allow_html=True,
                )

                # Key metrics — row 1: classifier outputs
                mc1, mc2, mc3, mc4 = st.columns(4)
                prob_up = ml.get("prob_up", 0.5)
                mc1.metric(
                    "Probability Up",
                    f"{prob_up:.1%}",
                    delta=f"{(prob_up - 0.5)*100:+.1f}pp vs 50%",
                    help="Classifier confidence that GOLDBEES closes higher in horizon_days.",
                )
                mc2.metric(
                    f"Expected {ml['horizon_days']}-Day Return",
                    f"{ml['expected_return_pct']:+.2f}%",
                    help="(2×prob_up − 1) × historical mean |return|. Calibrated from direction signal.",
                )
                mc3.metric(
                    "Confidence Band",
                    f"[{ml['confidence_low']:+.1f}%, {ml['confidence_high']:+.1f}%]",
                )
                mc4.metric("Training Rows", f"{ml['n_training_rows']:,}")

                # Row 2: model quality metrics
                mq1, mq2, mq3, mq4 = st.columns(4)
                cv_auc  = ml.get("cv_auc_mean",  ml.get("cv_r2_mean", 0) + 0.5)
                cv_skill = ml.get("cv_r2_mean", cv_auc - 0.5)
                mq1.metric(
                    "Model AUC",
                    f"{cv_auc:.4f}",
                    delta="above random" if cv_auc > 0.5 else "no edge",
                    help="AUC > 0.5 = directional edge. > 0.55 = strong edge. < 0.5 = worse than random.",
                )
                mq2.metric(
                    "Skill (AUC − 0.5)",
                    f"{cv_skill:+.4f}",
                    help="Centred skill score. ≤ 0 disables Kelly weight entirely.",
                )
                mq3.metric(
                    "Hit Ratio",
                    f"{ml.get('cv_hit_ratio_mean', 0)*100:.1f}%",
                    help="Directional accuracy. >52% = statistical edge, >55% = strong edge.",
                )
                mq4.metric(
                    "R² (legacy)",
                    f"{ml.get('cv_r2_legacy_mean', ml.get('cv_r2_mean', 0)):.4f}",
                    help="Regression R² on log-return target — diagnostic only. Negative is expected for noisy 5-day returns.",
                )

                # Feature importance bar chart
                st.subheader("Feature Importances")
                fi = ml["feature_importances"].copy()
                fi["feature"] = fi["feature"].str.replace("f_", "", regex=False)
                fi = fi.set_index("feature")
                st.bar_chart(fi["importance"], height=240, color="#2196F3")
                st.caption(
                    "Importance = average LightGBM split gain over last 3 CV folds. "
                    "Higher = the model relies on this signal more."
                )

                # Walk-forward AUC per fold
                with st.expander("Walk-Forward AUC per fold"):
                    _auc_list = ml.get("cv_auc_scores", [s + 0.5 for s in ml.get("cv_r2_scores", [])])
                    _skill_list = ml.get("cv_r2_scores", [])
                    folds_df = pd.DataFrame({
                        "fold":  [f"Fold {i+1}" for i in range(len(_auc_list))],
                        "AUC":   _auc_list,
                        "Skill (AUC−0.5)": _skill_list if _skill_list else [a - 0.5 for a in _auc_list],
                    }).set_index("fold")
                    st.bar_chart(folds_df["AUC"], height=160, color="#9C27B0")
                    st.caption(
                        "Each fold trains on earlier data only and tests on the unseen future fold.  "
                        "**AUC > 0.5** = model has directional edge on that period.  "
                        "**AUC < 0.5** = model was worse than random — Kelly disabled for those periods."
                    )

                # Walk-forward hit ratio per fold
                with st.expander("Walk-Forward Hit Ratio per fold"):
                    _hit_list = ml.get("cv_hit_ratios", [])
                    hr_df = pd.DataFrame({
                        "fold":      [f"Fold {i+1}" for i in range(len(_hit_list))],
                        "hit_ratio": [h * 100 for h in _hit_list],
                    }).set_index("fold")
                    st.bar_chart(hr_df["hit_ratio"], height=160, color="#4CAF50")
                    st.caption(
                        "Hit Ratio = % of test days where the model predicted direction (Up/Down) correctly.  "
                        "**50%** = random coin flip · **>52%** = statistical edge · **>55%** = strong edge.  "
                        "Even a modest hit ratio can be profitable if combined with proper position sizing."
                    )

            except ImportError:
                st.error(
                    "LightGBM not installed.  \n"
                    "Run: `.venv/bin/pip install lightgbm` then restart Streamlit."
                )
            except ValueError as exc:
                st.warning(str(exc))
            except Exception as exc:
                st.error(f"ML Forecast error: {exc}")

    else:
        with st.expander("ℹ Expert System vs LightGBM — when to use which"):
            st.markdown("""
| | Expert System | LightGBM Forecast |
|---|---|---|
| **Thresholds** | Hard (25% COT = crowded) | Soft (learned from data) |
| **Signal interaction** | Each signal checked independently | Models all signals jointly |
| **Explainability** | Full (rules visible) | Partial (feature importance) |
| **Data needed** | Just today's values | Historical training data (≥ 120 rows) |
| **Strengths** | Fast, interpretable, always runs | Captures non-linear cross-signal effects |
| **Weaknesses** | Misses cross-signal amplification | Needs history; can overfit |

**Use both together:** Expert system as an immediate sanity check; LightGBM for position-sizing decisions where the interaction between signals matters.
            """)

    # (scarcity premium alerts moved to 🏦 ETF Scanner tab)
    st.info("🌍 International ETF — Scarcity Premium Alerts has moved to the **🏦 ETF Scanner** tab.")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 6 — MF HOLDINGS
# ══════════════════════════════════════════════════════════════════════════════
with tab_holdings:
    st.header("📦 Multi-Asset Fund Holdings Tracker")

    _FUND_LABELS = {
        "DSP_MULTI_ASSET":   "DSP Multi Asset",
        "DSP_MULTI_ASSET_OMNI_FOF": "DSP Multi Asset Omni FoF",
        "QUANT_MULTI_ASSET": "Quant Multi Asset",
        "ICICI_MULTI_ASSET": "ICICI Pru Multi Asset",
        "BAJAJ_MULTI_ASSET": "Bajaj Multi Asset",
    }
    _ASSET_COLORS = {
        "equity": "#1976D2",
        "gold":   "#FFA726",
        "silver": "#90A4AE",
        "bond":   "#43A047",
        "cash":   "#78909C",
        "other":  "#AB47BC",
    }

    # ── Check data availability ────────────────────────────────────────────
    try:
        _h_count_df = _query_df("SELECT count() AS n FROM market_data.mf_holdings")
        _h_count = int(_h_count_df.iloc[0, 0]) if not _h_count_df.empty else 0
    except Exception:
        _h_count = 0

    if _h_count == 0:
        st.info(
            "No holdings data yet.  \n"
            "Run **📥 Import Data → mf_holdings** to fetch the latest monthly portfolio."
        )
        st.stop()

    # ── Available months — all months across mf_holdings (full history) ───
    _months_df = _query_df(
        "SELECT DISTINCT as_of_month FROM market_data.mf_holdings FINAL ORDER BY as_of_month DESC"
    )
    _available_months = list(_months_df.iloc[:, 0]) if not _months_df.empty else []
    if not _available_months:
        st.warning("No holdings data yet. Run **📥 Import Data → mf_holdings** first.")
        st.stop()

    # ── Controls ────────────────────────────────────────────────────────────
    col_fund, col_month = st.columns([2, 1])
    with col_fund:
        selected_funds = st.multiselect(
            "Funds",
            options=list(_FUND_LABELS.keys()),
            default=list(_FUND_LABELS.keys()),
            format_func=lambda k: _FUND_LABELS[k],
        )
    with col_month:
        selected_month = st.selectbox(
            "Month",
            options=_available_months,
            format_func=lambda d: d.strftime("%b %Y") if hasattr(d, "strftime") else str(d),
        )

    # Normalise to plain YYYY-MM-DD string — ClickHouse Date columns reject
    # datetime strings like '2026-04-01 00:00:00' with a TYPE_MISMATCH error.
    _month_str = (
        selected_month.strftime("%Y-%m-%d")
        if hasattr(selected_month, "strftime")
        else str(selected_month)[:10]
    )

    if not selected_funds:
        st.warning("Select at least one fund.")
        st.stop()

    # ── Load current month data ────────────────────────────────────────────
    _fund_filter = ", ".join(f"'{f}'" for f in selected_funds)
    _hold_df = _query_df(
        f"""
        SELECT scheme_code, fund_name, isin, security_name, asset_type,
               market_value_cr, pct_of_nav
        FROM market_data.mf_holdings FINAL
        WHERE fund_name IN ({_fund_filter})
          AND as_of_month = '{_month_str}'
        ORDER BY fund_name, pct_of_nav DESC
        """
    )
    _hold_df.columns = ["scheme_code", "fund_name", "isin", "security_name",
                        "asset_type", "market_value_cr", "pct_of_nav"]

    # ── Fallback: for funds with no data in selected month, load their latest ─
    _sel_month_label = selected_month.strftime("%b %Y") if hasattr(selected_month, "strftime") else str(selected_month)[:7]
    _funds_with_data = set(_hold_df["fund_name"].unique())
    _missing_keys = [k for k in selected_funds if k not in _funds_with_data]
    _fallback_month_map: dict = {}  # fund_name → display label

    if _missing_keys:
        _missing_filter = ", ".join(f"'{k}'" for k in _missing_keys)
        _fb_months_df = _query_df(
            f"SELECT fund_name, max(as_of_month) AS latest_month"
            f" FROM market_data.mf_holdings FINAL"
            f" WHERE fund_name IN ({_missing_filter})"
            f" GROUP BY fund_name"
        )
        if not _fb_months_df.empty:
            _fb_months_df.columns = ["fund_name", "latest_month"]
            _fb_chunks = []
            for _, _fb_row in _fb_months_df.iterrows():
                _fn  = _fb_row["fund_name"]
                _lm  = _fb_row["latest_month"]
                _lm_str   = _lm.strftime("%Y-%m-%d") if hasattr(_lm, "strftime") else str(_lm)[:10]
                _lm_label = _lm.strftime("%b %Y")    if hasattr(_lm, "strftime") else str(_lm)[:7]
                _fb_fund_df = _query_df(
                    f"""
                    SELECT scheme_code, fund_name, isin, security_name, asset_type,
                           market_value_cr, pct_of_nav
                    FROM market_data.mf_holdings FINAL
                    WHERE fund_name = '{_fn}' AND as_of_month = '{_lm_str}'
                    ORDER BY pct_of_nav DESC
                    """
                )
                if not _fb_fund_df.empty:
                    _fb_fund_df.columns = ["scheme_code", "fund_name", "isin", "security_name",
                                           "asset_type", "market_value_cr", "pct_of_nav"]
                    _fb_chunks.append(_fb_fund_df)
                    _fallback_month_map[_fn] = _lm_label
            if _fb_chunks:
                import pandas as _pd
                _hold_df = _pd.concat([_hold_df] + _fb_chunks, ignore_index=True)
                _notes = [f"**{_FUND_LABELS[fn]}** → {lbl}" for fn, lbl in _fallback_month_map.items()]
                st.info(f"No {_sel_month_label} data — showing latest available for: {', '.join(_notes)}")

    if _hold_df.empty:
        st.warning("No holdings data found for any selected fund.")
        st.stop()

    # fund_label: append "(fallback month)" suffix for funds not on selected month
    def _make_fund_label(fn: str) -> str:
        label = _FUND_LABELS.get(fn, fn)
        if fn in _fallback_month_map:
            label = f"{label} ({_fallback_month_map[fn]})"
        return label

    _hold_df["fund_label"] = _hold_df["fund_name"].apply(_make_fund_label)

    # ══ 1. Asset allocation pie per fund ══════════════════════════════════
    st.subheader("Asset Allocation")
    pie_cols = st.columns(len(selected_funds))
    for i, fund_key in enumerate(selected_funds):
        with pie_cols[i]:
            _fd = _hold_df[_hold_df["fund_name"] == fund_key]
            _alloc = _fd.groupby("asset_type")["pct_of_nav"].sum().reset_index()
            _alloc.columns = ["asset_type", "weight"]
            if _alloc.empty:
                st.caption(f"_{_FUND_LABELS[fund_key]}_")
                st.info("No data")
            else:
                import plotly.express as px  # type: ignore[import]
                fig_pie = px.pie(
                    _alloc,
                    values="weight",
                    names="asset_type",
                    title=_make_fund_label(fund_key),
                    color="asset_type",
                    color_discrete_map=_ASSET_COLORS,
                    hole=0.35,
                )
                fig_pie.update_layout(
                    margin=dict(t=40, b=0, l=0, r=0),
                    legend=dict(orientation="h", y=-0.1),
                    height=320,
                )
                st.plotly_chart(fig_pie, width="stretch")

    # ══ 2. Holdings table ══════════════════════════════════════════════════
    st.subheader("Holdings Detail")
    _disp_df = _hold_df[["fund_label", "security_name", "asset_type", "market_value_cr", "pct_of_nav", "isin"]].rename(columns={
        "fund_label":     "Fund",
        "security_name":  "Security",
        "asset_type":     "Type",
        "market_value_cr": "Mkt Val (₹Cr)",
        "pct_of_nav":     "Weight (%)",
        "isin":           "ISIN",
    })
    st.dataframe(
        _disp_df.style.format({"Weight (%)": "{:.2f}", "Mkt Val (₹Cr)": "{:.1f}"}),
        width="stretch",
        height=420,
    )

    # ══ 3. Month-over-month drift ══════════════════════════════════════════
    # Each fund compares its own two most recent available months so fallback
    # funds (showing an earlier month) still produce meaningful drift.
    st.subheader("Month-over-Month Drift")

    # Which months are being compared per fund?
    _cmp_df = _query_df(
        f"""
        SELECT fund_name,
               maxIf(as_of_month, rn = 1) AS cur_month,
               maxIf(as_of_month, rn = 2) AS prev_month
        FROM (
            SELECT fund_name, as_of_month,
                   row_number() OVER (PARTITION BY fund_name ORDER BY as_of_month DESC) AS rn
            FROM (
                SELECT DISTINCT fund_name, as_of_month
                FROM market_data.mf_holdings FINAL
                WHERE fund_name IN ({_fund_filter})
            ) AS t1
        ) AS t2
        GROUP BY fund_name
        HAVING prev_month != toDate('1970-01-01')
        """
    )

    if _cmp_df.empty:
        st.info("Need at least 2 months of data per fund to show drift.")
    else:
        _cmp_df.columns = ["fund_name", "cur_month", "prev_month"]
        _cmp_labels = {
            row["fund_name"]: (
                f"{row['cur_month'].strftime('%b %Y') if hasattr(row['cur_month'], 'strftime') else str(row['cur_month'])[:7]}"
                f" vs "
                f"{row['prev_month'].strftime('%b %Y') if hasattr(row['prev_month'], 'strftime') else str(row['prev_month'])[:7]}"
            )
            for _, row in _cmp_df.iterrows()
        }
        _cmp_caption = "  |  ".join(f"**{_FUND_LABELS.get(fn, fn)}**: {lbl}" for fn, lbl in _cmp_labels.items())
        st.caption(_cmp_caption)

        _drift_df = _query_df(
            f"""
            WITH
            months_ranked AS (
                SELECT fund_name, as_of_month,
                       row_number() OVER (PARTITION BY fund_name ORDER BY as_of_month DESC) AS rn
                FROM (
                    SELECT DISTINCT fund_name, as_of_month
                    FROM market_data.mf_holdings FINAL
                    WHERE fund_name IN ({_fund_filter})
                ) AS t1
            ),
            cur AS (
                SELECT fund_name, isin, security_name, asset_type, pct_of_nav
                FROM market_data.mf_holdings FINAL
                WHERE (fund_name, as_of_month) IN (
                    SELECT fund_name, as_of_month FROM months_ranked WHERE rn = 1
                )
            ),
            prev AS (
                SELECT fund_name, isin, security_name, pct_of_nav
                FROM market_data.mf_holdings FINAL
                WHERE (fund_name, as_of_month) IN (
                    SELECT fund_name, as_of_month FROM months_ranked WHERE rn = 2
                )
            )
            SELECT *
            FROM (
                SELECT
                    coalesce(cur.fund_name, prev.fund_name)           AS fund_name,
                    coalesce(cur.isin, prev.isin)                     AS isin,
                    coalesce(cur.security_name, prev.security_name)   AS security_name,
                    coalesce(cur.asset_type, '')                      AS asset_type,
                    coalesce(cur.pct_of_nav, 0.0)                     AS pct_cur,
                    coalesce(prev.pct_of_nav, 0.0)                    AS pct_prev,
                    coalesce(cur.pct_of_nav, 0.0) - coalesce(prev.pct_of_nav, 0.0) AS drift,
                    CASE
                        WHEN prev.isin IS NULL OR prev.isin = '' THEN 'ENTERED'
                        WHEN cur.isin  IS NULL OR cur.isin  = '' THEN 'EXITED'
                        WHEN abs(cur.pct_of_nav - prev.pct_of_nav) >= 2               THEN 'CHANGED'
                        ELSE 'UNCHANGED'
                    END AS event
                FROM cur
                FULL OUTER JOIN prev ON cur.fund_name = prev.fund_name AND cur.isin = prev.isin
            ) AS joined
            WHERE event != 'UNCHANGED'
            ORDER BY fund_name, event, abs(drift) DESC
            """
        )

        if _drift_df.empty:
            st.success("No significant changes vs prior month (no ENTERED/EXITED, all weight shifts < 2 pp).")
        else:
            _drift_df.columns = ["fund_name", "isin", "security_name", "asset_type",
                                  "pct_cur", "pct_prev", "drift", "event"]
            _drift_df["fund_label"] = _drift_df["fund_name"].map(_FUND_LABELS).fillna(_drift_df["fund_name"])
            _event_color = {"ENTERED": "🟢", "EXITED": "🔴", "CHANGED": "🟡"}
            _drift_df["🔔"] = _drift_df["event"].map(_event_color).fillna("")
            st.dataframe(
                _drift_df[["🔔", "fund_label", "security_name", "asset_type",
                           "pct_prev", "pct_cur", "drift", "event"]]
                .rename(columns={
                    "fund_label":    "Fund",
                    "security_name": "Security",
                    "asset_type":    "Type",
                    "pct_prev":      "Prev (%)",
                    "pct_cur":       "Cur (%)",
                    "drift":         "Δ (%)",
                    "event":         "Event",
                })
                .style.format({"Prev (%)": "{:.2f}", "Cur (%)": "{:.2f}", "Δ (%)": "{:+.2f}"}),
                width="stretch",
                height=380,
            )

    # ══ 4. Asset allocation trend over time ═══════════════════════════════
    if len(_available_months) >= 2:
        st.subheader("Allocation Trend Over Time")
        _trend_df = _query_df(
            f"""
            SELECT as_of_month, fund_name, asset_type, sum(pct_of_nav) AS weight
            FROM market_data.mf_holdings FINAL
            WHERE fund_name IN ({_fund_filter})
            GROUP BY as_of_month, fund_name, asset_type
            ORDER BY as_of_month, fund_name, asset_type
            """
        )
        if not _trend_df.empty:
            _trend_df.columns = ["month", "fund_name", "asset_type", "weight"]
            import plotly.express as px  # noqa: F811
            _trend_df["fund_label"] = _trend_df["fund_name"].map(_FUND_LABELS).fillna(_trend_df["fund_name"])
            
            # Stacked Area Chart
            fig_trend = px.area(
                _trend_df,
                x="month",
                y="weight",
                color="asset_type",
                facet_col="fund_label",
                facet_col_wrap=len(selected_funds),
                color_discrete_map=_ASSET_COLORS,
                labels={"month": "", "weight": "Weight (%)", "asset_type": "Type"},
                height=380,
                markers=True,
            )
            fig_trend.update_layout(
                margin=dict(t=30, b=0),
                legend=dict(orientation="h", y=-0.2)
            )
            st.plotly_chart(fig_trend, width="stretch")

    # ══ 5. Common Holdings & Overlap (all asset types) ════════════════════
    if len(selected_funds) >= 1:
        st.subheader("Top Holdings by Aggregate Weight")
        _common_df = _query_df(
            f"""
            SELECT
                security_name,
                any(asset_type)                                                    AS asset_type,
                count(DISTINCT fund_name)                                          AS n_funds,
                sum(pct_of_nav)                                                    AS total_weight,
                groupArray(concat(fund_name, ' (', toString(round(pct_of_nav, 1)), '%)')) AS breakdown
            FROM market_data.mf_holdings FINAL
            WHERE fund_name IN ({_fund_filter})
              AND as_of_month = '{_month_str}'
            GROUP BY security_name
            HAVING total_weight > 0.5
            ORDER BY total_weight DESC
            LIMIT 20
            """
        )
        
        if not _common_df.empty:
            _common_df.columns = ["Security", "Type", "Funds", "Total Weight (%)", "Breakdown"]
            _sel_month_label = selected_month.strftime("%b %Y") if hasattr(selected_month, "strftime") else str(selected_month)

            col_chart, col_table = st.columns([1, 1])
            with col_chart:
                import plotly.express as px # noqa: F811
                fig_common = px.bar(
                    _common_df,
                    x="Total Weight (%)",
                    y="Security",
                    orientation="h",
                    color="Type",
                    color_discrete_map=_ASSET_COLORS,
                    title=f"Aggregate Exposure — All Asset Types ({_sel_month_label})",
                )
                fig_common.update_layout(yaxis={"categoryorder": "total ascending"}, height=500, margin=dict(l=0, r=0, t=40, b=0))
                st.plotly_chart(fig_common, width="stretch")

            with col_table:
                st.dataframe(
                    _common_df,
                    column_config={
                        "Breakdown": st.column_config.ListColumn("Fund Breakdown")
                    },
                    hide_index=True,
                    width="stretch",
                    height=500,
                )
        else:
            st.info("No holdings above 0.5% weight found for the selected funds/month.")


# ══════════════════════════════════════════════════════════════════════════════
# TAB — 🏦 ETF SCANNER (Domestic Premium / Discount)
# ══════════════════════════════════════════════════════════════════════════════

with tab_etf_scan:
    st.header("🏦 Domestic ETF — Premium / Discount Scanner")
    st.caption(
        "Arbitrage desks close domestic ETF mispricing quickly. "
        "This scanner computes a Z-score of today's premium vs its rolling mean — "
        "flagging ETFs trading unusually expensive or cheap relative to their iNAV."
    )

    if not ok:
        st.warning("ClickHouse not connected.")
        st.stop()

    from src.tools.domestic_etf_scanner import scan_domestic_etfs, DOMESTIC_ETF_SYMBOLS

    col_ctrl, col_run = st.columns([2, 1])
    with col_ctrl:
        lookback_days  = st.slider("Lookback window (days)", 7, 90, 30, key="etfscan_lookback")
        z_threshold    = st.slider("Z-score threshold", 0.5, 3.0, 1.5, step=0.25, key="etfscan_z")
        min_snapshots  = st.number_input("Min hourly buckets required", 1, 50, 3, 1, key="etfscan_min_snaps",
                                         help="Lower this if data is still building up (import runs recently)")
        custom_syms    = st.text_input(
            "Custom symbols (comma-separated, leave blank for default)",
            value="",
            key="etfscan_syms",
        )
        tax_slab = st.radio(
            "Your income-tax slab (for STCG on Gold/Debt ETFs)",
            options=["20% slab  →  effective 20.8%", "30% slab  →  effective 31.2%"],
            index=0,
            horizontal=True,
            key="etfscan_tax_slab",
        )
        _slab_rate = 0.208 if tax_slab.startswith("20%") else 0.312
    with col_run:
        st.write("")
        st.write("")
        run_scan = st.button("▶ Run Scanner", use_container_width=True, key="etfscan_run")

    if run_scan:
        sym_list = (
            [s.strip().upper() for s in custom_syms.split(",") if s.strip()]
            if custom_syms.strip()
            else DOMESTIC_ETF_SYMBOLS
        )

        with st.spinner(f"Computing Z-scores for {len(sym_list)} symbols…"):
            try:
                with _get_pool().acquire() as _scan_client:
                    results = scan_domestic_etfs(
                        ch_client=_scan_client,
                        symbols=sym_list,
                        lookback_days=lookback_days,
                        z_high=z_threshold,
                        z_low=-z_threshold,
                        z_mild_high=z_threshold - 0.5,
                        z_mild_low=-(z_threshold - 0.5),
                        min_snapshots=int(min_snapshots),
                    )
            except Exception as exc:
                st.error(f"Scan failed: {exc}")
                st.stop()

        if not results:
            st.warning("No results — ensure iNAV snapshots are imported (Import → inav).")
            st.stop()

        import plotly.graph_objects as go

        # ── Signal summary cards ───────────────────────────────────────────────
        actionable = [r for r in results if r["z_score"] is not None]
        n_high     = sum(1 for r in actionable if "HIGH PREMIUM"   in r["signal"])
        n_mild_h   = sum(1 for r in actionable if "MILD PREMIUM"   in r["signal"])
        n_fair     = sum(1 for r in actionable if "FAIR VALUE"     in r["signal"])
        n_mild_l   = sum(1 for r in actionable if "MILD DISCOUNT"  in r["signal"])
        n_good     = sum(1 for r in actionable if "GOOD DISCOUNT"  in r["signal"])

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("🔴 High Premium",  n_high)
        c2.metric("🟡 Mild Premium",  n_mild_h)
        c3.metric("⚪ Fair Value",     n_fair)
        c4.metric("🟡 Mild Discount", n_mild_l)
        c5.metric("🟢 Good Discount", n_good)

        st.divider()

        # ── Z-score bar chart ──────────────────────────────────────────────────
        syms   = [r["symbol"]  for r in actionable]
        zscores = [r["z_score"] for r in actionable]
        colors  = []
        for r in actionable:
            sig = r["signal"]
            if   "HIGH PREMIUM"  in sig: colors.append("#ef4444")
            elif "MILD PREMIUM"  in sig: colors.append("#f59e0b")
            elif "GOOD DISCOUNT" in sig: colors.append("#22c55e")
            elif "MILD DISCOUNT" in sig: colors.append("#eab308")
            else:                        colors.append("#94a3b8")

        fig_z = go.Figure(go.Bar(
            x=syms, y=zscores,
            marker_color=colors,
            text=[f"{z:+.2f}" for z in zscores],
            textposition="outside",
        ))
        fig_z.add_hline(y=z_threshold,  line_dash="dash", line_color="red",   annotation_text=f"+{z_threshold} (High Premium)")
        fig_z.add_hline(y=-z_threshold, line_dash="dash", line_color="green", annotation_text=f"-{z_threshold} (Good Discount)")
        fig_z.update_layout(
            title=f"Premium Z-Score  (vs {lookback_days}d mean)",
            yaxis_title="Z-Score",
            xaxis_title="",
            height=380,
            margin=dict(t=50, b=20),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(fig_z, use_container_width=True)

        # ── Scatter: latest premium vs rolling mean ────────────────────────────
        x_vals = [r["mean_premium"]   for r in actionable if r["mean_premium"] is not None]
        y_vals = [r["latest_premium"] for r in actionable if r["latest_premium"] is not None]
        s_syms = [r["symbol"]         for r in actionable if r["mean_premium"] is not None]

        if x_vals:
            fig_sc = go.Figure(go.Scatter(
                x=x_vals, y=y_vals, mode="markers+text",
                text=s_syms, textposition="top center",
                marker=dict(size=12, color=colors[:len(x_vals)], line=dict(width=1, color="white")),
            ))
            mn = min(min(x_vals), min(y_vals)) - 0.5
            mx = max(max(x_vals), max(y_vals)) + 0.5
            fig_sc.add_shape(type="line", x0=mn, y0=mn, x1=mx, y1=mx,
                             line=dict(dash="dot", color="grey", width=1))
            fig_sc.update_layout(
                title="Latest Premium vs Rolling Mean  (diagonal = fair value)",
                xaxis_title=f"{lookback_days}d Avg Premium (%)",
                yaxis_title="Latest Premium (%)",
                height=380,
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
            )
            st.plotly_chart(fig_sc, use_container_width=True)

        # ── Full results table ────────────────────────────────────────────────
        st.subheader("Full Results")
        rows_display = []
        signal_icons = {
            "HIGH PREMIUM": "🔴", "MILD PREMIUM": "🟡",
            "FAIR VALUE": "⚪", "MILD DISCOUNT": "🟡", "GOOD DISCOUNT": "🟢",
        }
        for r in results:
            icon = next((v for k, v in signal_icons.items() if k in r["signal"]), "⚠")
            rows_display.append({
                "Symbol":        r["symbol"],
                "Latest (%)":    f"{r['latest_premium']:+.3f}" if r["latest_premium"] is not None else "—",
                f"{lookback_days}d Avg (%)": f"{r['mean_premium']:+.3f}" if r["mean_premium"] is not None else "—",
                "Std Dev":       f"{r['std_premium']:.4f}"    if r["std_premium"]    is not None else "—",
                "Z-Score":       f"{r['z_score']:+.3f}"       if r["z_score"]        is not None else "—",
                "Snapshots":     r["n_snapshots"],
                "Signal":        f"{icon} {r['signal']}",
                "Note":          r["error"] or "",
            })
        st.dataframe(pd.DataFrame(rows_display), use_container_width=True, hide_index=True)

        # ── STCG Post-Tax Viability Analysis ──────────────────────────────────
        st.divider()
        st.subheader("📊 Short-Term Trade Viability (Post-Tax)")

        _STCG_RATES = {
            "equity":    0.208,   # 20% base + 4% cess  (Budget July 2024)
            "commodity": _slab_rate,
            "debt":      _slab_rate,
        }
        _LTCG_RATES = {
            "equity":    0.130,   # 12.5% + cess  (> 12 months)
            "commodity": 0.208,   # 20% + cess (indexed; simplified)
            "debt":      _slab_rate,
        }
        _ROUND_TRIP_COST = 0.10  # % — brokerage + STT + exchange charges + stamp duty

        _tax_rows = []
        for _r in results:
            if _r["z_score"] is None:
                continue
            _sig   = _r["signal"]
            _rev   = _r.get("expected_reversion_pct")
            _tcls  = _r.get("tax_class", "equity")
            _stcg  = _STCG_RATES[_tcls]
            _ltcg  = _LTCG_RATES[_tcls]

            # Only compute viability where there is a directional signal
            if _rev is None:
                continue
            # For discount signals: expected gain is positive (price rises to mean)
            # For premium signals: expected loss-avoidance (sell before reversion)
            _expected_gross = abs(_rev)  # reversion magnitude
            _direction = "BUY (discount)" if _rev > 0 else "SELL / AVOID (premium)"

            _stcg_cost  = _expected_gross * _stcg
            _net_stcg   = _expected_gross * (1 - _stcg) - _ROUND_TRIP_COST
            _net_ltcg   = _expected_gross * (1 - _ltcg) - _ROUND_TRIP_COST
            _breakeven  = _ROUND_TRIP_COST / (1 - _stcg)   # min gross gain to be STCG-profitable

            _stcg_viable = "✅ YES" if _net_stcg > 0 else "❌ NO"
            _ltcg_viable = "✅ YES" if _net_ltcg > 0 else "❌ NO"

            _tax_rows.append({
                "Symbol":             _r["symbol"],
                "Signal":             _sig,
                "Tax Class":          _tcls.capitalize(),
                "Direction":          _direction,
                "Expected Reversion %": f"{_rev:+.3f}%",
                "STCG Rate":          f"{_stcg*100:.1f}%",
                "Net Gain (STCG) %":  f"{_net_stcg:+.3f}%",
                "Net Gain (LTCG>12M) %": f"{_net_ltcg:+.3f}%",
                "Min Gross for STCG %":  f"{_breakeven:.3f}%",
                "STCG Viable?":       _stcg_viable,
            })

        if _tax_rows:
            st.caption(
                f"Round-trip transaction cost assumed: **{_ROUND_TRIP_COST:.2f}%** "
                "(brokerage + STT 0.001% sell + exchange + stamp duty). "
                "STCG rates post Budget July 23, 2024: equity **20.8%**, "
                f"gold/silver/debt at your **{_slab_rate*100:.1f}% effective slab rate**. "
                "LTCG (>12 months): equity **13.0%**, commodity ~**20.8%**."
            )
            _viable_df = pd.DataFrame(_tax_rows)
            # Highlight rows based on STCG viability
            def _highlight_viability(row: pd.Series) -> list[str]:
                if row["STCG Viable?"] == "✅ YES":
                    return ["background-color: rgba(34,197,94,0.12)"] * len(row)
                return ["background-color: rgba(239,68,68,0.08)"] * len(row)
            st.dataframe(
                _viable_df.style.apply(_highlight_viability, axis=1),
                use_container_width=True,
                hide_index=True,
            )

            st.info(
                "💡 **Key insight:** With STCG at 20.8%, a 1% discount-to-NAV only nets you "
                f"**~{1.0 * (1 - 0.208) - _ROUND_TRIP_COST:.2f}%** after tax and costs. "
                "Short-term arbitrage is most worthwhile when: "
                "(1) you have capital losses to offset gains, "
                "(2) the discount is ≥ 2% for meaningful net gain, or "
                "(3) you hold ≥ 12 months and pay LTCG at 13.0% instead."
            )
        else:
            st.info("Run the scanner first to see post-tax viability.")
    st.divider()
    st.subheader("🌍 International ETF — Scarcity Premium Alerts")
    st.caption(
        "The RBI $7B overseas investment cap creates a structural premium on international ETFs. "
        "A deeply negative Z-score means the ETF is currently trading cheap relative to its own "
        "history — a potential entry point before the premium normalises."
    )

    _pa_col1, _pa_col2, _pa_col3 = st.columns([1, 1, 2])
    with _pa_col1:
        _pa_lookback = st.slider("Lookback days", 7, 90, 30, 1, key="pa_lookback")
    with _pa_col2:
        _pa_z_thresh = st.slider("Z threshold (BUY)", -3.0, -0.5, -1.5, 0.1, key="pa_z_thresh")
    with _pa_col3:
        _pa_min_snaps = st.number_input(
            "Min snapshots required", 1, 50, 1, 1, key="pa_min_snaps"
        )

    if st.button("📡 Scan Premiums", key="pa_scan_btn"):
        with st.spinner("Fetching iNAV snapshots and computing Z-scores…"):
            try:
                from src.tools.premium_alerts import check_premium_alerts, INTL_ETF_SYMBOLS

                _pa_client = _get_pool().get_client()  # unmanaged; closed after alerts
                _pa_results = check_premium_alerts(
                    ch_client=_pa_client,
                    symbols=INTL_ETF_SYMBOLS,
                    lookback_days=_pa_lookback,
                    z_threshold=_pa_z_thresh,
                    good_entry_threshold=_pa_z_thresh + 0.5,
                    min_snapshots=int(_pa_min_snaps),
                )
                _pa_client.close()

                # ── Signal summary cards ──────────────────────────────────────
                _pa_buy   = [r for r in _pa_results if "SCREAMING" in r["action"]]
                _pa_entry = [r for r in _pa_results if "ENTRY"     in r["action"]]
                _pa_noact = [r for r in _pa_results if "NO ACTION" in r["action"]]
                _pa_flat  = [r for r in _pa_results if "FLAT"      in r["action"]]

                _sc1, _sc2, _sc3, _sc4 = st.columns(4)
                _sc1.metric("🟢 SCREAMING BUY", len(_pa_buy))
                _sc2.metric("🟡 GOOD ENTRY",    len(_pa_entry))
                _sc3.metric("🔴 NO ACTION",      len(_pa_noact))
                _sc4.metric("⚪ FLAT PREMIUM",   len(_pa_flat),
                            help="Market holiday or stale iNAV — no spread variation")

                # ── Z-score bar chart (only symbols with a computed z-score) ──
                import plotly.graph_objects as _go_pa

                _valid      = [r for r in _pa_results if r["z_score"]        is not None]
                _has_prem   = [r for r in _pa_results if r["latest_premium"] is not None]
                if _valid:
                    _bar_colors = []
                    for _r in _valid:
                        _z = _r["z_score"]
                        if _z <= _pa_z_thresh:
                            _bar_colors.append("#4CAF50")
                        elif _z <= _pa_z_thresh + 0.5:
                            _bar_colors.append("#FFC107")
                        else:
                            _bar_colors.append("#F44336")

                    _fig_pa = _go_pa.Figure()
                    _fig_pa.add_trace(_go_pa.Bar(
                        x=[r["symbol"]  for r in _valid],
                        y=[r["z_score"] for r in _valid],
                        marker_color=_bar_colors,
                        text=[f"Z={r['z_score']:+.2f}" for r in _valid],
                        textposition="outside",
                        hovertemplate=(
                            "<b>%{x}</b><br>"
                            "Z-Score: %{y:.3f}<br>"
                            "Latest premium: %{customdata[0]:+.3f}%<br>"
                            f"{_pa_lookback}d avg: %{{customdata[1]:+.3f}}%<br>"
                            "Std dev: %{customdata[2]:.4f}"
                            "<extra></extra>"
                        ),
                        customdata=[
                            [r["latest_premium"] or 0,
                             r["mean_premium"]   or 0,
                             r["std_premium"]    or 0]
                            for r in _valid
                        ],
                    ))
                    _fig_pa.add_hline(
                        y=_pa_z_thresh,
                        line_dash="dash", line_color="#4CAF50", line_width=1.5,
                        annotation_text="SCREAMING BUY threshold",
                        annotation_font_color="#4CAF50",
                    )
                    _fig_pa.add_hline(
                        y=_pa_z_thresh + 0.5,
                        line_dash="dot", line_color="#FFC107", line_width=1.5,
                        annotation_text="GOOD ENTRY threshold",
                        annotation_font_color="#FFC107",
                    )
                    _fig_pa.add_hline(
                        y=0, line_dash="solid", line_color="#888888", line_width=0.8,
                    )
                    _fig_pa.update_layout(
                        title=f"Premium Z-Score vs {_pa_lookback}d Mean  (negative = cheap relative to history)",
                        yaxis_title="Z-Score",
                        xaxis_title="Symbol",
                        plot_bgcolor="rgba(0,0,0,0)",
                        paper_bgcolor="rgba(0,0,0,0)",
                        height=380,
                        margin=dict(t=50, b=40, l=60, r=20),
                        font=dict(size=13),
                    )
                    st.plotly_chart(_fig_pa, use_container_width=True)

                # ── Premium level chart (all symbols with premium data) ───────
                if _has_prem:
                    _prem_colors = []
                    for _r in _has_prem:
                        if "SCREAMING" in _r["action"]:  _prem_colors.append("#4CAF50")
                        elif "ENTRY"   in _r["action"]:  _prem_colors.append("#FFC107")
                        elif "FLAT"    in _r["action"]:  _prem_colors.append("#90A4AE")
                        else:                            _prem_colors.append("#F44336")

                    _fig_prem = _go_pa.Figure()
                    _fig_prem.add_trace(_go_pa.Bar(
                        name=f"{_pa_lookback}d Avg Premium",
                        x=[r["symbol"]       for r in _has_prem],
                        y=[r["mean_premium"] for r in _has_prem],
                        marker_color="#90A4AE",
                        opacity=0.6,
                    ))
                    _fig_prem.add_trace(_go_pa.Scatter(
                        name="Latest Premium",
                        x=[r["symbol"]         for r in _has_prem],
                        y=[r["latest_premium"] for r in _has_prem],
                        mode="markers",
                        marker=dict(size=14, color=_prem_colors, symbol="diamond"),
                    ))
                    _fig_prem.update_layout(
                        title="Latest Premium vs 30d Average  (diamond = today, bar = mean)",
                        yaxis_title="Premium / Discount (%)",
                        xaxis_title="Symbol",
                        plot_bgcolor="rgba(0,0,0,0)",
                        paper_bgcolor="rgba(0,0,0,0)",
                        height=340,
                        margin=dict(t=50, b=40, l=60, r=20),
                        legend=dict(orientation="h", y=1.12),
                        font=dict(size=13),
                    )
                    _fig_prem.add_hline(
                        y=0, line_dash="solid", line_color="#888888", line_width=0.8,
                        annotation_text="iNAV parity",
                    )
                    st.plotly_chart(_fig_prem, use_container_width=True)

                # ── Detail table ──────────────────────────────────────────────
                with st.expander("📋 Full Signal Table"):
                    import pandas as _pd_pa
                    _pa_rows = []
                    for _r in _pa_results:
                        _pa_rows.append({
                            "Symbol":          _r["symbol"],
                            "Latest Prem (%)": f"{_r['latest_premium']:+.3f}" if _r["latest_premium"] is not None else "—",
                            f"{_pa_lookback}d Avg (%)": f"{_r['mean_premium']:+.3f}" if _r["mean_premium"] is not None else "—",
                            "Std Dev":         f"{_r['std_premium']:.4f}"   if _r["std_premium"]    is not None else "—",
                            "Z-Score":         f"{_r['z_score']:+.3f}"      if _r["z_score"]        is not None else "—",
                            "Snapshots":       _r["n_snapshots"],
                            "Action":          _r["action"],
                            "Note":            _r["error"] or "",
                        })
                    st.dataframe(_pd_pa.DataFrame(_pa_rows), use_container_width=True, hide_index=True)

            except Exception as _exc_pa:
                st.error(f"Premium alerts error: {_exc_pa}")
    else:
        st.info("Click **📡 Scan Premiums** to compute Z-scores and render charts.")


    st.write("")
    st.write("")
    st.divider()
    st.subheader("⚙️ ETF Volume-Volatility Setups & Trends")
    st.caption("Identify Squeezes, Breakouts, and Trend Postures across all 28 tracked ETFs.")
    sub_tab_setups, sub_tab_trends = st.tabs(["⚡ Setup Scanner", "📈 Trend Lookbacks"])
    with sub_tab_setups:
        with st.expander("ℹ️ Understanding Setup Patterns (Breakouts, Exhaustions & Squeezes)"):
            st.markdown("""
            * **🚀 Volatile Breakout**: Price makes a large move accompanied by **heavy volume expansion** (>1.5x 20d MA). This indicates strong institutional momentum and a continuation of the trend.
            * **⚠️ Volume Exhaustion**: Price drops or rises sharply on **very dry volume** (<0.7x 20d MA). This indicates a liquidity vacuum rather than actual selling pressure—sellers have exhausted their supply, setting up a high-probability **contrarian mean-reversion bounce**.
            * **📦 Volatility Squeeze**: Volatility contracts to extreme historical lows (bottom 25% of its 60-day range) with low volume. The market is coiling like a spring; a **massive, explosive breakout is imminent**. Watch for a volume surge to trade the breakout direction.
            """)

        col_setups_ctrl, col_setups_run = st.columns([3, 1])
        with col_setups_ctrl:
            lookback_vol = st.slider("Setup Lookback window (days)", 20, 180, 90, key="volscan_lookback")
        with col_setups_run:
            st.write("")
            st.write("")
            run_setups = st.button("▶ Run Setup Scan", use_container_width=True, key="volscan_run")
            
        if run_setups:
            from src.tools.etf_setup_scanner import run_etf_setup_scan
            with st.spinner("Scanning volume-volatility configurations…"):
                setup_results = run_etf_setup_scan(lookback_days=lookback_vol)
                
            if not setup_results:
                st.warning("No setup scan results found.")
            else:
                import pandas as pd
                import plotly.express as px
                
                df_setups = pd.DataFrame(setup_results)
                
                df_plot = df_setups.copy()
                df_plot["return_ratio"] = df_plot["daily_return"] / df_plot["volatility_20d"]
                
                fig_setups = px.scatter(
                    df_plot,
                    x="volume_vs_ma",
                    y="return_ratio",
                    text="symbol",
                    color="pattern",
                    color_discrete_map={
                        "🚀 Volatile Breakout": "#ff3366",
                        "⚠️ Volume Exhaustion": "#00ffcc",
                        "📦 Volatility Squeeze": "#9933ff",
                        "Normal": "#94a3b8"
                    },
                    labels={
                        "volume_vs_ma": "Volume Ratio (vs 20d MA)",
                        "return_ratio": "Return Ratio (Return / 20d Vol)",
                        "pattern": "Setup Classification"
                    },
                    title="ETF Volume-Volatility Space Map"
                )
                fig_setups.update_traces(textposition="top center", marker=dict(size=12))
                fig_setups.add_vline(x=1.5, line_dash="dash", line_color="#ff3366", opacity=0.3)
                fig_setups.add_vline(x=0.7, line_dash="dash", line_color="#00ffcc", opacity=0.3)
                fig_setups.add_hline(y=1.5, line_dash="dot", line_color="#ffcc00", opacity=0.2)
                fig_setups.add_hline(y=-1.5, line_dash="dot", line_color="#ffcc00", opacity=0.2)
                
                # Add background quadrant shapes & labels for intuitive human reading
                fig_setups.update_layout(
                    shapes=[
                        # Bullish Breakout (Top-Right)
                        dict(
                            type="rect", xref="x", yref="y",
                            x0=1.5, y0=1.5, x1=3.5, y1=3.5,
                            fillcolor="rgba(34, 197, 94, 0.05)", line_width=0, layer="below"
                        ),
                        # Bearish Liquidation (Bottom-Right)
                        dict(
                            type="rect", xref="x", yref="y",
                            x0=1.5, y0=-3.5, x1=3.5, y1=-1.5,
                            fillcolor="rgba(239, 68, 68, 0.05)", line_width=0, layer="below"
                        ),
                        # Liquidity Vacuum Drop (Bottom-Left)
                        dict(
                            type="rect", xref="x", yref="y",
                            x0=0.0, y0=-3.5, x1=0.7, y1=-1.5,
                            fillcolor="rgba(6, 182, 212, 0.05)", line_width=0, layer="below"
                        ),
                        # Volatility Squeeze (Center-Left)
                        dict(
                            type="rect", xref="x", yref="y",
                            x0=0.0, y0=-1.0, x1=0.8, y1=1.0,
                            fillcolor="rgba(147, 51, 234, 0.05)", line_width=0, layer="below"
                        )
                    ],
                    annotations=[
                        dict(
                            x=2.5, y=2.5, xref="x", yref="y",
                            text="🟢 BULLISH MOMENTUM", showarrow=False,
                            font=dict(color="rgba(34, 197, 94, 0.5)", size=12, family="Courier New")
                        ),
                        dict(
                            x=2.5, y=-2.5, xref="x", yref="y",
                            text="🔴 PANIC / LIQUIDATION", showarrow=False,
                            font=dict(color="rgba(239, 68, 68, 0.5)", size=12, family="Courier New")
                        ),
                        dict(
                            x=0.35, y=-2.5, xref="x", yref="y",
                            text="⚠️ LIQUIDITY VACUUM", showarrow=False,
                            font=dict(color="rgba(6, 182, 212, 0.5)", size=10, family="Courier New")
                        ),
                        dict(
                            x=0.4, y=0.0, xref="x", yref="y",
                            text="📦 VOLATILITY SQUEEZE", showarrow=False,
                            font=dict(color="rgba(147, 51, 234, 0.5)", size=10, family="Courier New")
                        )
                    ],
                    plot_bgcolor="rgba(0,0,0,0)",
                    paper_bgcolor="rgba(0,0,0,0)",
                    height=500
                )
                st.plotly_chart(fig_setups, use_container_width=True)
                
                # ── Categorized Cards Grid (prevents overlap) ─────────────────────
                st.write("")
                st.subheader("🎯 Active Opportunities Grouped by Pattern")
                
                g_c1, g_c2, g_c3, g_c4 = st.columns(4)
                
                # Helper to format cards
                def make_card_html(row_data, border_color):
                    ret_val = row_data["daily_return"]
                    ret_color = "#22c55e" if ret_val >= 0 else "#ef4444"
                    return f"""
                    <div style="background-color: #1e1e24; border-left: 4px solid {border_color}; padding: 12px; border-radius: 6px; margin-bottom: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.15);">
                        <div style="font-size: 15px; font-weight: bold; color: #ffffff;">{row_data['symbol']}</div>
                        <div style="font-size: 13px; color: #888888; margin-top: 4px;">
                            Price: <span style="color: #ffffff;">₹{row_data['close']:.2f}</span>
                        </div>
                        <div style="font-size: 13px; color: #888888;">
                            Return: <span style="color: {ret_color}; font-weight: bold;">{ret_val:+.2f}%</span>
                        </div>
                        <div style="font-size: 12px; color: #aaaaaa; margin-top: 4px;">
                            Volume Ratio: <span style="font-weight: bold; color: #ffffff;">{row_data['volume_vs_ma']:.2f}x</span>
                        </div>
                    </div>
                    """
                
                with g_c1:
                    st.markdown("<h4 style='color: #ff3366; margin-bottom: 10px;'>🚀 Breakouts</h4>", unsafe_allow_html=True)
                    bo_list = df_setups[df_setups["pattern"] == "🚀 Volatile Breakout"]
                    if bo_list.empty:
                        st.caption("No active breakouts")
                    else:
                        for _, r_data in bo_list.iterrows():
                            st.markdown(make_card_html(r_data, "#ff3366"), unsafe_allow_html=True)
                            
                with g_c2:
                    st.markdown("<h4 style='color: #00ffcc; margin-bottom: 10px;'>⚠️ Exhaustions</h4>", unsafe_allow_html=True)
                    ex_list = df_setups[df_setups["pattern"] == "⚠️ Volume Exhaustion"]
                    if ex_list.empty:
                        st.caption("No active exhaustions")
                    else:
                        for _, r_data in ex_list.iterrows():
                            st.markdown(make_card_html(r_data, "#00ffcc"), unsafe_allow_html=True)
                            
                with g_c3:
                    st.markdown("<h4 style='color: #9933ff; margin-bottom: 10px;'>📦 Squeezes</h4>", unsafe_allow_html=True)
                    sq_list = df_setups[df_setups["pattern"] == "📦 Volatility Squeeze"]
                    if sq_list.empty:
                        st.caption("No active squeezes")
                    else:
                        for _, r_data in sq_list.iterrows():
                            st.markdown(make_card_html(r_data, "#9933ff"), unsafe_allow_html=True)
                            
                with g_c4:
                    st.markdown("<h4 style='color: #94a3b8; margin-bottom: 10px;'>⚪ Consolidation</h4>", unsafe_allow_html=True)
                    co_list = df_setups[df_setups["pattern"] == "Normal"]
                    if co_list.empty:
                        st.caption("No active consolidations")
                    else:
                        # Only show top 4 normal ones to save space, sorted by volume ratio
                        for _, r_data in co_list.head(4).iterrows():
                            st.markdown(make_card_html(r_data, "#94a3b8"), unsafe_allow_html=True)
                        if len(co_list) > 4:
                            st.caption(f"+ {len(co_list) - 4} more in table below")
                
                st.write("")
                st.subheader("📋 Full Signal Table")
                st.dataframe(
                    df_setups[["symbol", "close", "daily_return", "volatility_20d", "volume_vs_ma", "pattern", "details"]],
                    column_config={
                        "symbol": "ETF",
                        "close": "Close (₹)",
                        "daily_return": st.column_config.NumberColumn("Daily Return", format="%+.2f%%"),
                        "volatility_20d": st.column_config.NumberColumn("20d Volatility", format="%.2f%%"),
                        "volume_vs_ma": st.column_config.NumberColumn("Volume vs 20d MA", format="%.2fx"),
                        "pattern": "Setup Classification",
                        "details": "Details"
                    },
                    use_container_width=True,
                    hide_index=True
                )

    with sub_tab_trends:
        st.write("")
        run_trends = st.button("▶ Run Trend Scan", use_container_width=True, key="trendscan_run")
        
        if run_trends:
            from src.tools.etf_setup_scanner import run_etf_trend_scan
            with st.spinner("Calculating trend lookbacks…"):
                trend_results = run_etf_trend_scan()
                
            if not trend_results:
                st.warning("No trend scan results found.")
            else:
                import pandas as pd
                df_trends = pd.DataFrame(trend_results)
                
                st.dataframe(
                    df_trends[["symbol", "close", "return_5d", "return_20d", "return_60d", "status"]],
                    column_config={
                        "symbol": "ETF",
                        "close": "Close (₹)",
                        "return_5d": st.column_config.NumberColumn("5d Return", format="%+.2f%%"),
                        "return_20d": st.column_config.NumberColumn("20d Return", format="%+.2f%%"),
                        "return_60d": st.column_config.NumberColumn("60d Return", format="%+.2f%%"),
                        "status": "Trend Status"
                    },
                    use_container_width=True,
                    hide_index=True
                )


# ══════════════════════════════════════════════════════════════════════════════
# TAB 8 — MARKET NEWS
# ══════════════════════════════════════════════════════════════════════════════

with tab_news:
    _init_macro_signals_from_db()
    st.header("📰 Market News")
    st.caption(
        "Parallel news scanner — ~5s per full scan.  "
        "Populate via CLI: `mosaic macro --save`  ·  `mosaic etf-news --save`  "
        "or use the **Refresh** buttons below."
    )

    _SENT_ICON_N  = {"POSITIVE": "🟢", "NEGATIVE": "🔴", "NEUTRAL": "⚪"}
    _CONV_COLOR_N = {"HIGH": "🔴", "MEDIUM": "🟡", "LOW": "⚪"}
    _IMP_C_N      = {"HIGH": "🔴", "MEDIUM": "🟡", "LOW": "⚪"}
    _ETF_CATS_N   = [
        "All", "Gold ETFs", "Nifty ETFs", "Bank ETFs", "IT ETFs",
        "PSU ETFs", "Mid/Small Cap ETFs", "Pharma ETFs",
        "International ETFs", "Debt / Liquid ETFs", "Auto ETFs",
    ]

    # ── Quant Overlay — always visible, loaded from ClickHouse ────────────────
    try:
        _qo_fii = _query_df("""
            SELECT sum(fii_net_cr) AS fii_net_5d, sum(dii_net_cr) AS dii_net_5d
            FROM market_data.fii_dii_flows FINAL
            WHERE trade_date >= today() - INTERVAL 5 DAY
        """)
        _qo_px = _query_df("""
            SELECT trade_date, toFloat64(argMax(close, imported_at)) AS close
            FROM market_data.daily_prices
            WHERE symbol='GOLDBEES' AND category='etfs'
            GROUP BY trade_date ORDER BY trade_date DESC LIMIT 55
        """)
        _qo_garch = _query_df("""
            SELECT garch_vol_pct FROM market_data.weight_checkpoints FINAL
            WHERE symbol='GOLDBEES' AND garch_vol_pct > 0
            ORDER BY as_of DESC LIMIT 1
        """)
        _qo_ok = not _qo_px.empty
    except Exception:
        _qo_ok = False

    if _qo_ok:
        import numpy as _np_qo
        _qo_px = _qo_px.sort_values("trade_date").reset_index(drop=True)
        _qo_closes = _qo_px["close"].astype(float)
        _qo_latest = float(_qo_closes.iloc[-1])
        _qo_ema50  = float(_qo_closes.ewm(span=50, adjust=False).mean().iloc[-1])
        _qo_vs_ema = "above" if _qo_latest >= _qo_ema50 else "below"
        _qo_ret5d  = (
            round(float(_np_qo.log(_qo_latest / _qo_closes.iloc[-6])) * 100, 2)
            if len(_qo_closes) >= 6 else None
        )
        _qo_fii_val = float(_qo_fii["fii_net_5d"].iloc[0]) if not _qo_fii.empty else None
        _qo_dii_val = float(_qo_fii["dii_net_5d"].iloc[0]) if not _qo_fii.empty else None
        _qo_gv      = float(_qo_garch["garch_vol_pct"].iloc[0]) if not _qo_garch.empty else None

        with st.container(border=True):
            st.caption("📐 **Quant Overlay** — live ClickHouse ground truth for interpreting news signals")
            _qc1, _qc2, _qc3, _qc4 = st.columns(4)
            _ret5d_str = f"{_qo_ret5d:+.2f}%" if _qo_ret5d is not None else "—"
            _ema_delta = "▲ above EMA50" if _qo_vs_ema == "above" else "▼ below EMA50"
            _qc1.metric("GOLDBEES", f"₹{_qo_latest:.2f}", _ret5d_str, delta_color="normal")
            _qc2.metric("vs EMA50", _qo_vs_ema.upper(), _ema_delta,
                        delta_color="normal" if _qo_vs_ema == "above" else "inverse")
            _qc3.metric("GARCH Vol", f"{_qo_gv:.1f}%" if _qo_gv else "—",
                        "▲ elevated" if _qo_gv and _qo_gv > 15 else "✓ normal",
                        delta_color="inverse" if _qo_gv and _qo_gv > 15 else "normal")
            _fii_str = f"₹{_qo_fii_val:+,.0f} Cr" if _qo_fii_val is not None else "—"
            _fii_delta = "selling" if _qo_fii_val and _qo_fii_val < 0 else "buying"
            _qc4.metric("FII 5d Net", _fii_str, _fii_delta,
                        delta_color="inverse" if _qo_fii_val and _qo_fii_val < 0 else "normal")

        # Signal vs price contradiction callout
        if "macro_net_signal" in st.session_state:
            _gold_score = st.session_state["macro_net_signal"].get("GOLDBEES", 0)
            _n_themes   = st.session_state.get("macro_n_themes", 8)
            _strong_th  = max(4, (_n_themes * 4) // 2)
            if _gold_score >= _strong_th and _qo_vs_ema == "below":
                st.warning(
                    f"⚠️ **Signal vs Price divergence** — macro news is strongly bullish on GOLDBEES "
                    f"(net score +{_gold_score}), but price is **below EMA50** at ₹{_qo_latest:.2f}. "
                    "The thesis is priced into headlines, not yet into price action.",
                    icon="⚠️",
                )

    st.divider()
    news_col1, news_col2 = st.columns([1, 1])

    # ── LEFT: Macro Events ─────────────────────────────────────────────────────
    with news_col1:
        st.subheader("🌍 Macro & Geopolitical Events")
        _macro_from = st.date_input(
            "From date", value=pd.Timestamp.now() - pd.Timedelta(days=7),
            key="macro_from_date",
        )
        _macro_max_n = st.slider("Articles per theme", 2, 8, 4, key="macro_max_n")
        _refresh_macro = st.button(
            "🔄 Refresh macro (live → DB)", key="refresh_macro", type="primary",
        )

        if _refresh_macro:
            import time as _time_m
            _t0_m = _time_m.time()
            with st.spinner("Scanning 8 macro themes in parallel…"):
                try:
                    from src.tools.macro_event_scanner import (
                        scan_macro_events, save_macro_events_to_db,
                    )
                    from src.importer.clickhouse import ClickHouseImporter
                    _m_report = scan_macro_events(max_per_theme=_macro_max_n)
                    _m_ch = ClickHouseImporter(
                        host=CH_HOST, port=CH_PORT,
                        database="market_data",
                        username=CH_USER, password=CH_PASS,
                    )
                    _m_ch.ensure_schema()
                    _m_saved = save_macro_events_to_db(_m_report, _m_ch)
                    _m_ch.close()
                    _elapsed_m = _time_m.time() - _t0_m
                    # Persist net signal + theme count for contradiction callout
                    st.session_state["macro_net_signal"] = _m_report.etf_net_signal
                    st.session_state["macro_n_themes"]   = len(_m_report.themes_detected)
                    st.success(
                        f"✓ {len(_m_report.events)} events · {len(_m_report.themes_detected)} themes · "
                        f"{_m_saved} rows saved · {_elapsed_m:.1f}s"
                    )
                    st.rerun()
                except Exception as _exc_mref:
                    st.error(f"Refresh error: {_exc_mref}")

        # Net score bar chart — shown after refresh (from session_state)
        if "macro_net_signal" not in st.session_state:
            try:
                from src.tools.macro_event_scanner import MACRO_THEMES
                _latest_themes_db = _query_df("""
                    SELECT category, count() as cnt
                    FROM market_data.news_articles
                    WHERE source_type = 'macro_event'
                      AND fetched_at = (
                          SELECT max(fetched_at)
                          FROM market_data.news_articles
                          WHERE source_type = 'macro_event'
                      )
                    GROUP BY category
                """)
                if not _latest_themes_db.empty:
                    _etf_net = {}
                    _n_themes_set = set()
                    for _, _row in _latest_themes_db.iterrows():
                        _theme_name = _row["category"]
                        _cnt = int(_row["cnt"])
                        _theme_def = next((t for t in MACRO_THEMES if t["theme"] == _theme_name), None)
                        if _theme_def and "impact_map" in _theme_def:
                            _n_themes_set.add(_theme_name)
                            for _etf, _direction in _theme_def["impact_map"].items():
                                _etf_net[_etf] = _etf_net.get(_etf, 0) + (_direction * _cnt)
                    if _etf_net:
                        st.session_state["macro_net_signal"] = _etf_net
                        st.session_state["macro_n_themes"] = len(_n_themes_set)
            except Exception as _exc_init:
                pass

        if "macro_net_signal" in st.session_state:
            _mn_sig = st.session_state["macro_net_signal"]
            _mn_nt  = st.session_state.get("macro_n_themes", 8)
            if _mn_sig:
                _mn_df = pd.DataFrame(
                    sorted(_mn_sig.items(), key=lambda x: x[1], reverse=True),
                    columns=["ETF", "Net Score"],
                ).set_index("ETF")
                _strong_t = max(4, (_mn_nt * _macro_max_n) // 2)
                st.caption(
                    f"Net score per ETF · ≥+{_strong_t} = strong bullish · "
                    f"≤−{_strong_t} = strong bearish"
                )
                st.bar_chart(_mn_df, height=220, color="#4CAF50")

                # ── Macro to ETF Weighted Impact Network ──────────────────────
                st.write("")
                with st.expander("🌍 Active Macro Themes & Transmission Channels (Latest News)", expanded=True):
                    try:
                        from src.tools.macro_event_scanner import MACRO_THEMES
                        all_theme_names = [t["theme"] for t in MACRO_THEMES]
                        
                        # Fetch active themes and article counts from latest DB run
                        _latest_themes_df = _query_df("""
                            SELECT category, count() as cnt
                            FROM market_data.news_articles
                            WHERE source_type = 'macro_event'
                              AND fetched_at = (
                                  SELECT max(fetched_at)
                                  FROM market_data.news_articles
                                  WHERE source_type = 'macro_event'
                              )
                            GROUP BY category
                        """)
                        
                        if not _latest_themes_df.empty:
                            active_themes = list(_latest_themes_df["category"].unique())
                            theme_counts = dict(zip(_latest_themes_df["category"], _latest_themes_df["cnt"]))
                        else:
                            active_themes = ["Geopolitical / War", "Central Bank Policy (Fed / RBI)", "Global Risk-Off / Equity Sell-Off"]
                            theme_counts = {t: 0 for t in active_themes}
                        
                        selected_themes = st.multiselect(
                            "Filter themes in view",
                            options=all_theme_names,
                            default=[t for t in active_themes if t in all_theme_names],
                            key="network_selected_themes"
                        )
                        
                        if selected_themes:
                            theme_cards_html = []
                            
                            # Fetch articles details from latest DB run
                            _latest_articles_df = _query_df("""
                                SELECT category, title, source, sentiment, url, published_at
                                FROM market_data.news_articles
                                WHERE source_type = 'macro_event'
                                  AND fetched_at = (
                                      SELECT max(fetched_at)
                                      FROM market_data.news_articles
                                      WHERE source_type = 'macro_event'
                                  )
                                ORDER BY published_at DESC
                            """)
                            
                            from collections import defaultdict as _dd_m
                            theme_articles = _dd_m(list)
                            if not _latest_articles_df.empty:
                                for _, _row in _latest_articles_df.iterrows():
                                    theme_articles[_row["category"]].append({
                                        "title": _row["title"],
                                        "source": _row["source"],
                                        "sentiment": _row["sentiment"],
                                        "url": _row["url"],
                                        "published_at": _row["published_at"]
                                    })
                            
                            # Custom CSS stylesheet for glassmorphism layout
                            css_styles = """
                            <style>
                              .theme-grid {
                                display: grid;
                                grid-template-columns: repeat(auto-fill, minmax(320px, 1fr));
                                gap: 16px;
                                padding: 10px 0;
                                width: 100%;
                              }
                              .theme-card {
                                background: rgba(30, 41, 59, 0.4);
                                border: 1px solid rgba(71, 85, 105, 0.5);
                                border-radius: 12px;
                                padding: 16px;
                                transition: all 0.25s cubic-bezier(0.4, 0, 0.2, 1);
                                box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
                                backdrop-filter: blur(8px);
                                display: flex;
                                flex-direction: column;
                                justify-content: space-between;
                              }
                              .theme-card:hover {
                                transform: translateY(-3px);
                                border-color: rgba(59, 130, 246, 0.6);
                                background: rgba(30, 41, 59, 0.6);
                                box-shadow: 0 10px 20px -3px rgba(0, 0, 0, 0.4);
                              }
                              .theme-top {
                                margin-bottom: 12px;
                              }
                              .theme-header {
                                display: flex;
                                align-items: center;
                                gap: 8px;
                                margin-bottom: 8px;
                              }
                              .theme-icon {
                                font-size: 20px;
                              }
                              .theme-title {
                                font-weight: 700;
                                font-size: 15px;
                                color: #F8FAFC;
                                line-height: 1.3;
                              }
                              .theme-badges {
                                display: flex;
                                flex-wrap: wrap;
                                gap: 6px;
                                margin-bottom: 10px;
                              }
                              .badge {
                                font-size: 9.5px;
                                font-weight: 700;
                                text-transform: uppercase;
                                padding: 2px 7px;
                                border-radius: 9999px;
                                display: inline-block;
                              }
                              .badge.conviction.high {
                                background: rgba(239, 68, 68, 0.15);
                                color: #EF4444;
                                border: 1px solid rgba(239, 68, 68, 0.3);
                              }
                              .badge.conviction.medium {
                                background: rgba(245, 158, 11, 0.15);
                                color: #F59E0B;
                                border: 1px solid rgba(245, 158, 11, 0.3);
                              }
                              .badge.conviction.low {
                                background: rgba(148, 163, 184, 0.15);
                                color: #94A3B8;
                                border: 1px solid rgba(148, 163, 184, 0.3);
                              }
                              .badge.count {
                                background: rgba(59, 130, 246, 0.15);
                                color: #3B82F6;
                                border: 1px solid rgba(59, 130, 246, 0.3);
                              }
                              .badge.sentiment-positive {
                                background: rgba(16, 185, 129, 0.15);
                                color: #10B981;
                                border: 1px solid rgba(16, 185, 129, 0.3);
                              }
                              .badge.sentiment-negative {
                                background: rgba(239, 68, 68, 0.15);
                                color: #EF4444;
                                border: 1px solid rgba(239, 68, 68, 0.3);
                              }
                              .theme-transmission {
                                font-size: 11.5px;
                                color: #CBD5E1;
                                line-height: 1.45;
                                margin-bottom: 12px;
                                border-bottom: 1px solid rgba(71, 85, 105, 0.3);
                                padding-bottom: 10px;
                              }
                              .theme-news {
                                display: flex;
                                flex-direction: column;
                                gap: 10px;
                              }
                              .news-header {
                                font-size: 11px;
                                font-weight: 700;
                                color: #94A3B8;
                                text-transform: uppercase;
                                margin-bottom: 4px;
                                letter-spacing: 0.05em;
                              }
                              .news-item {
                                font-size: 11px;
                                line-height: 1.4;
                                padding-bottom: 8px;
                                border-bottom: 1px dashed rgba(71, 85, 105, 0.3);
                              }
                              .news-item:last-child {
                                border-bottom: none;
                                padding-bottom: 0;
                              }
                              .news-title-row {
                                display: flex;
                                align-items: flex-start;
                                gap: 6px;
                              }
                              .news-sent-icon {
                                font-size: 11px;
                                line-height: 1.4;
                              }
                              .news-title-link {
                                color: #60A5FA;
                                text-decoration: none;
                                font-weight: 500;
                                display: inline-block;
                              }
                              .news-title-link:hover {
                                color: #93C5FD;
                                text-decoration: underline;
                              }
                              .news-title-text {
                                color: #E2E8F0;
                                font-weight: 500;
                              }
                              .news-meta {
                                display: flex;
                                justify-content: space-between;
                                font-size: 9.5px;
                                color: #64748B;
                                margin-top: 4px;
                                padding-left: 17px;
                              }
                            </style>
                            """
                            theme_cards_html.append(css_styles)
                            theme_cards_html.append('<div class="theme-grid">')
                            
                            for theme_def in MACRO_THEMES:
                                theme_name = theme_def["theme"]
                                if theme_name not in selected_themes:
                                    continue
                                
                                t_icon = theme_def.get("icon", "🌍")
                                t_conv = theme_def.get("conviction", "MEDIUM")
                                t_transmission = theme_def.get("transmission", "")
                                count = int(theme_counts.get(theme_name, 0))
                                
                                # Calculate bias from articles
                                theme_art_list = theme_articles.get(theme_name, [])
                                pos_count = sum(1 for a in theme_art_list if a["sentiment"] == "POSITIVE")
                                neg_count = sum(1 for a in theme_art_list if a["sentiment"] == "NEGATIVE")
                                
                                if pos_count > neg_count:
                                    bias_badge = "<span class='badge sentiment-positive'>🟢 Bullish Bias</span>"
                                elif neg_count > pos_count:
                                    bias_badge = "<span class='badge sentiment-negative'>🔴 Bearish Bias</span>"
                                else:
                                    bias_badge = "<span class='badge conviction low'>⚪ Neutral Bias</span>"
                                
                                conv_badge = f"<span class='badge conviction {t_conv.lower()}'>{t_conv}</span>"
                                count_badge = f"<span class='badge count'>{count} Articles</span>"
                                
                                # Build news section HTML
                                import html as _html_m
                                news_items_html = []
                                if theme_art_list:
                                    for _art in theme_art_list[:3]:
                                        _art_sent = _art.get("sentiment", "NEUTRAL")
                                        _art_icon = _SENT_ICON_N.get(_art_sent, "⚪")
                                        _raw_date = str(_art.get("published_at", ""))
                                        _pub_date = _raw_date[:16] if len(_raw_date) >= 16 else _raw_date
                                        
                                        _escaped_title = _html_m.escape(_art.get("title", ""))
                                        _escaped_source = _html_m.escape(_art.get("source", "Unknown"))
                                        
                                        if _art.get("url"):
                                            _escaped_url = _html_m.escape(_art["url"])
                                            title_html = f'<a class="news-title-link" href="{_escaped_url}" target="_blank">{_escaped_title}</a>'
                                        else:
                                            title_html = f'<span class="news-title-text">{_escaped_title}</span>'
                                            
                                        news_items_html.append(f"""
                                        <div class="news-item">
                                          <div class="news-title-row">
                                            <span class="news-sent-icon">{_art_icon}</span>
                                            {title_html}
                                          </div>
                                          <div class="news-meta">
                                            <span>{_escaped_source}</span>
                                            <span>{_pub_date}</span>
                                          </div>
                                        </div>
                                        """)
                                else:
                                    news_items_html.append('<div class="news-item" style="color: #64748B; font-style: italic; padding-left: 4px;">No recent headlines found</div>')
                                    
                                theme_news_section = "\n".join(news_items_html)
                                
                                theme_cards_html.append(f"""
                                <div class="theme-card">
                                  <div class="theme-top">
                                    <div class="theme-header">
                                      <span class="theme-icon">{t_icon}</span>
                                      <span class="theme-title">{theme_name}</span>
                                    </div>
                                    <div class="theme-badges">
                                      {conv_badge}
                                      {count_badge}
                                      {bias_badge}
                                    </div>
                                    <div class="theme-transmission">
                                      {t_transmission}
                                    </div>
                                  </div>
                                  <div class="theme-news">
                                    <div class="news-header">Latest Headlines</div>
                                    {theme_news_section}
                                  </div>
                                </div>
                                """)
                            
                            theme_cards_html.append('</div>')
                            st.html("\n".join(theme_cards_html))
                        else:
                            st.info("Select at least one macro theme to visualize the network.")
                    except Exception as _exc_net:
                        st.error(f"Error rendering network: {_exc_net}")

    # ── RIGHT: ETF News ────────────────────────────────────────────────────────
    with news_col2:
        st.subheader("🏷️ ETF-Impact News")
        _etf_cat_n = st.selectbox("ETF Category", _ETF_CATS_N, key="etf_news_cat_n")
        _etf_from = st.date_input(
            "From date", value=pd.Timestamp.now() - pd.Timedelta(days=7),
            key="etf_from_date",
        )
        _etf_max_n = st.slider("Articles per topic", 2, 8, 4, key="etf_news_max_n")
        _refresh_etf = st.button(
            "🔄 Refresh ETF news (live → DB)", key="refresh_etf", type="primary",
        )

        if _refresh_etf:
            import time as _time_e
            _t0_e = _time_e.time()
            _cats_label = _etf_cat_n if _etf_cat_n != "All" else "all 10 categories"
            with st.spinner(f"Fetching {_cats_label} in parallel…"):
                try:
                    from src.tools.etf_news_scanner import (
                        scan_etf_news, save_etf_news_to_db,
                    )
                    from src.importer.clickhouse import ClickHouseImporter
                    _e_cats = None if _etf_cat_n == "All" else [_etf_cat_n]
                    _e_report = scan_etf_news(categories=_e_cats, max_per_topic=_etf_max_n)
                    _e_ch = ClickHouseImporter(
                        host=CH_HOST, port=CH_PORT,
                        database="market_data",
                        username=CH_USER, password=CH_PASS,
                    )
                    _e_ch.ensure_schema()
                    _e_saved = save_etf_news_to_db(_e_report, _e_ch)
                    _e_ch.close()
                    _elapsed_e = _time_e.time() - _t0_e
                    st.success(
                        f"✓ {len(_e_report.items)} articles · "
                        f"{len(_e_report.categories_scanned)} categories · "
                        f"{_e_saved} rows saved · {_elapsed_e:.1f}s"
                    )
                    st.rerun()
                except Exception as _exc_eref:
                    st.error(f"Refresh error: {_exc_eref}")

    st.divider()

    # ── Macro results from DB ──────────────────────────────────────────────────
    try:
        _macro_from_str = str(_macro_from)
        _macro_db_df = _query_df(
            f"SELECT category, etfs_impacted, sentiment, impact_tier, title, source, "
            f"published_at, url, fetched_at "
            f"FROM market_data.news_articles "
            f"WHERE source_type = 'macro_event' AND fetched_at >= '{_macro_from_str}' "
            f"ORDER BY fetched_at DESC"
        )
    except Exception:
        _macro_db_df = pd.DataFrame()

    st.subheader(f"🌍 Macro Events in DB ({len(_macro_db_df)} rows)")
    if _macro_db_df.empty:
        st.info(
            "No macro events in the database for the selected date range.  \n"
            "Run `mosaic macro --save` or click **Refresh** above to populate."
        )
    else:
        # Sentiment summary across all themes
        _m_pos = int((_macro_db_df["sentiment"] == "POSITIVE").sum())
        _m_neg = int((_macro_db_df["sentiment"] == "NEGATIVE").sum())
        _m_neu = int((_macro_db_df["sentiment"] == "NEUTRAL").sum())
        _mm1, _mm2, _mm3, _mm4 = st.columns(4)
        _mm1.metric("Total Events", len(_macro_db_df))
        _mm2.metric("🟢 Positive", _m_pos)
        _mm3.metric("🔴 Negative", _m_neg)
        _mm4.metric("⚪ Neutral",  _m_neu)

        from collections import defaultdict as _dd_m
        _macro_by_theme: dict = _dd_m(list)
        for _, _row in _macro_db_df.iterrows():
            _macro_by_theme[_row["category"]].append(_row)

        for _theme, _rows in _macro_by_theme.items():
            _first_imp = _rows[0].get("impact_tier", "LOW") if hasattr(_rows[0], "get") else "LOW"
            _t_pos = sum(1 for _r in _rows if str(_r.get("sentiment","")) == "POSITIVE")
            _t_neg = sum(1 for _r in _rows if str(_r.get("sentiment","")) == "NEGATIVE")
            with st.expander(
                f"{_CONV_COLOR_N.get(_first_imp, '⚪')} **{_theme}** "
                f"({len(_rows)} events · 🟢{_t_pos} 🔴{_t_neg})",
                expanded=(_first_imp == "HIGH"),
            ):
                for _r in _rows[:10]:
                    _si = _SENT_ICON_N.get(str(_r["sentiment"]), "⚪")
                    _etfs = str(_r.get("etfs_impacted", ""))
                    st.markdown(
                        f"{_si} **{_r['title']}**  \n"
                        f"*{_r['source']} · {str(_r['published_at'])[:16]}*  \n"
                        f"ETFs: `{_etfs}`"
                    )
        st.caption(f"Showing macro events since {_macro_from_str} · from ClickHouse")

    st.divider()

    # ── ETF News results from DB ───────────────────────────────────────────────
    try:
        _etf_from_str = str(_etf_from)
        _cat_filter = (
            "" if _etf_cat_n == "All"
            else f"AND category = '{_etf_cat_n}'"
        )
        _etf_db_df = _query_df(
            f"SELECT category, etfs_impacted, sentiment, impact_tier, title, source, "
            f"published_at, url, fetched_at "
            f"FROM market_data.news_articles "
            f"WHERE source_type = 'etf_news' AND fetched_at >= '{_etf_from_str}' "
            f"{_cat_filter} "
            f"ORDER BY fetched_at DESC"
        )
    except Exception:
        _etf_db_df = pd.DataFrame()

    st.subheader(f"🏷️ ETF News in DB ({len(_etf_db_df)} rows)")
    if _etf_db_df.empty:
        st.info(
            "No ETF news in the database for the selected date range / category.  \n"
            "Run `mosaic etf-news --save` or click **Refresh** above to populate."
        )
    else:
        # Sentiment headline metrics
        _pos_n = int((_etf_db_df["sentiment"] == "POSITIVE").sum())
        _neg_n = int((_etf_db_df["sentiment"] == "NEGATIVE").sum())
        _neu_n = int((_etf_db_df["sentiment"] == "NEUTRAL").sum())
        _em1, _em2, _em3, _em4 = st.columns(4)
        _em1.metric("Total Articles", len(_etf_db_df))
        _em2.metric("🟢 Positive", _pos_n)
        _em3.metric("🔴 Negative", _neg_n)
        _em4.metric("⚪ Neutral",  _neu_n)

        # Sentiment breakdown chart per category
        from collections import defaultdict as _dd_e
        _etf_by_cat: dict = _dd_e(list)
        for _, _row in _etf_db_df.iterrows():
            _etf_by_cat[_row["category"]].append(_row)

        _sent_chart_rows = []
        for _cat_k, _cat_items in _etf_by_cat.items():
            _sent_chart_rows.append({
                "Category": _cat_k[:12],
                "🟢 Pos":  sum(1 for i in _cat_items if str(i.get("sentiment","")) == "POSITIVE"),
                "🔴 Neg":  sum(1 for i in _cat_items if str(i.get("sentiment","")) == "NEGATIVE"),
                "⚪ Neu":  sum(1 for i in _cat_items if str(i.get("sentiment","")) == "NEUTRAL"),
            })
        if _sent_chart_rows:
            _sdf = pd.DataFrame(_sent_chart_rows).set_index("Category")
            with st.expander("📊 Sentiment breakdown by category", expanded=False):
                st.bar_chart(_sdf[["🟢 Pos", "🔴 Neg"]], height=200)

        for _cat, _items in _etf_by_cat.items():
            _etfs_str = str(_items[0].get("etfs_impacted", "")) if hasattr(_items[0], "get") else ""
            _imp = str(_items[0].get("impact_tier", "LOW")) if hasattr(_items[0], "get") else "LOW"
            _c_pos = sum(1 for i in _items if str(i.get("sentiment","")) == "POSITIVE")
            _c_neg = sum(1 for i in _items if str(i.get("sentiment","")) == "NEGATIVE")
            with st.expander(
                f"{_IMP_C_N.get(_imp, '⚪')} **{_cat}** — `{_etfs_str}` "
                f"({len(_items)} articles · 🟢{_c_pos} 🔴{_c_neg})",
                expanded=(_imp == "HIGH"),
            ):
                for _it in _items[:8]:
                    _si = _SENT_ICON_N.get(str(_it["sentiment"]), "⚪")
                    st.markdown(
                        f"{_si} {_it['title']}  \n"
                        f"*{_it['source']} · {str(_it['published_at'])[:16]}*"
                    )
        st.caption(f"Showing ETF news since {_etf_from_str} · from ClickHouse")

# ══════════════════════════════════════════════════════════════════════════════
# TAB 9 — SIGNAL DASHBOARD
# ══════════════════════════════════════════════════════════════════════════════

with tab_signals:
    st.header("🎛️ Signal Dashboard")
    st.caption(
        "Composite 0–100 score per ETF from 6 signal sources: "
        "macro, news sentiment, valuation (NAV Z-score), FII/DII flows, ML prediction, anomaly regime."
    )

    _sig_run = st.button(
        "🔄 Run Signal Aggregation + Save to DB",
        key="run_signals", type="primary",
    )

    if _sig_run:
        with st.spinner("Aggregating signals across 6 sources…"):
            try:
                from src.agents.signal_aggregator import run_signal_aggregation
                _sig_report = run_signal_aggregation(save=True, verbose=False)
                st.success(
                    f"✓ Scored {len(_sig_report.signals)} ETFs · "
                    f"Regime: **{_sig_report.regime}**"
                )
                st.rerun()
            except Exception as _exc_sig_run:
                st.error(f"Signal aggregation error: {_exc_sig_run}")

    # Read from DB
    try:
        _sig_df = _query_df(
            "SELECT as_of, etf_symbol, macro_score, sentiment_score, "
            "valuation_score, flow_score, ml_score, anomaly_flag, "
            "composite_score, action "
            "FROM market_data.signal_composite "
            "WHERE as_of = (SELECT max(as_of) FROM market_data.signal_composite) "
            "ORDER BY composite_score DESC"
        )
    except Exception:
        _sig_df = pd.DataFrame()

    if _sig_df.empty:
        st.info(
            "No signal data. Run `mosaic signals --save` or click the button above."
        )
    else:
        _sig_date = str(_sig_df["as_of"].iloc[0])
        st.subheader(f"Composite Scores — {_sig_date}")

        # ── Summary KPIs ──────────────────────────────────────────────────────
        _buys  = int((_sig_df["action"].isin(["BUY", "ACCUMULATE"])).sum())
        _holds = int((_sig_df["action"] == "HOLD").sum())
        _sells = int((_sig_df["action"].isin(["TRIM", "AVOID"])).sum())
        _avg   = float(_sig_df["composite_score"].mean())
        _sc1, _sc2, _sc3, _sc4, _sc5 = st.columns(5)
        _sc1.metric("ETFs Scored",        len(_sig_df))
        _sc2.metric("🟢 Buy/Accumulate",  _buys)
        _sc3.metric("🟡 Hold",            _holds)
        _sc4.metric("🔴 Trim/Avoid",      _sells)
        _sc5.metric("Avg Score",          f"{_avg:.0f}/100")

        # ── Altair horizontal bar chart — color-coded by action ───────────────
        try:
            import altair as alt  # noqa: F811

            _ACTION_HEX = {
                "BUY":        "#1b5e20",
                "ACCUMULATE": "#2e7d32",
                "HOLD":       "#f9a825",
                "TRIM":       "#c62828",
                "AVOID":      "#b71c1c",
            }
            _chart_src = _sig_df[["etf_symbol", "composite_score", "action"]].copy()
            _chart_src["color"] = _chart_src["action"].map(_ACTION_HEX).fillna("#888")

            _sig_bar = (
                alt.Chart(_chart_src)
                .mark_bar(cornerRadiusTopRight=3, cornerRadiusBottomRight=3)
                .encode(
                    y=alt.Y(
                        "etf_symbol:N",
                        sort=alt.EncodingSortField("composite_score", order="descending"),
                        title=None,
                        axis=alt.Axis(labelFontSize=12),
                    ),
                    x=alt.X(
                        "composite_score:Q",
                        title="Composite Score (0–100)",
                        scale=alt.Scale(domain=[0, 100]),
                        axis=alt.Axis(grid=True, gridOpacity=0.2),
                    ),
                    color=alt.Color(
                        "action:N",
                        scale=alt.Scale(
                            domain=["BUY", "ACCUMULATE", "HOLD", "TRIM", "AVOID"],
                            range=["#1b5e20", "#2e7d32", "#f9a825", "#c62828", "#b71c1c"],
                        ),
                        legend=alt.Legend(title="Action", orient="bottom", columns=5),
                    ),
                    tooltip=[
                        alt.Tooltip("etf_symbol:N",      title="ETF"),
                        alt.Tooltip("composite_score:Q", title="Score",   format=".0f"),
                        alt.Tooltip("action:N",          title="Action"),
                    ],
                )
                .properties(height=max(240, len(_sig_df) * 22))
            )
            # 50-point reference line (neutral threshold)
            _mid_rule = (
                alt.Chart(pd.DataFrame({"x": [50]}))
                .mark_rule(color="#555555", strokeWidth=1, strokeDash=[4, 3])
                .encode(x="x:Q")
            )
            st.altair_chart(
                (_sig_bar + _mid_rule)
                .configure_view(strokeWidth=0)
                .configure_title(anchor="start"),
                width="stretch",
            )
        except Exception as _e_sig_chart:
            # Fallback to plain bar chart
            st.bar_chart(
                _sig_df[["etf_symbol", "composite_score"]].set_index("etf_symbol"),
                color="#4CAF50",
            )

        # ── Styled breakdown table ────────────────────────────────────────────
        _ACTION_CSS = {
            "BUY":        "background-color: #1b5e20; color: white",
            "ACCUMULATE": "background-color: #2e7d32; color: white",
            "HOLD":       "background-color: #f9a825; color: black",
            "TRIM":       "background-color: #c62828; color: white",
            "AVOID":      "background-color: #b71c1c; color: white",
        }

        _display_cols = [
            "etf_symbol", "composite_score", "action",
            "macro_score", "sentiment_score", "valuation_score",
            "flow_score", "ml_score", "anomaly_flag",
        ]
        _styled = (
            _sig_df[_display_cols]
            .rename(columns={
                "etf_symbol":       "ETF",
                "composite_score":  "Score",
                "action":           "Action",
                "macro_score":      "Macro",
                "sentiment_score":  "Sentiment",
                "valuation_score":  "Valuation",
                "flow_score":       "Flow",
                "ml_score":         "ML",
                "anomaly_flag":     "Anomaly",
            })
            .style
            .map(lambda v: _ACTION_CSS.get(v, ""), subset=["Action"])
            .format({c: "{:.0f}" for c in ["Score","Macro","Sentiment","Valuation","Flow","ML"]})
        )
        st.dataframe(_styled, width="stretch")

        # ── Top picks & avoid panels ──────────────────────────────────────────
        _col_buy, _col_sell = st.columns(2)

        with _col_buy:
            _top = _sig_df[_sig_df["action"].isin(["BUY", "ACCUMULATE"])].head(5)
            if not _top.empty:
                st.subheader("🟢 Top Picks")
                for _, _r in _top.iterrows():
                    st.success(
                        f"**{_r['etf_symbol']}** — {_r['composite_score']:.0f}/100 → {_r['action']}  \n"
                        f"Macro: {_r['macro_score']:.0f} · Sent: {_r['sentiment_score']:.0f} · "
                        f"Flow: {_r['flow_score']:.0f} · ML: {_r['ml_score']:.0f}"
                    )

        with _col_sell:
            _bottom = _sig_df[_sig_df["action"].isin(["TRIM", "AVOID"])].head(5)
            if not _bottom.empty:
                st.subheader("🔴 Avoid / Trim")
                for _, _r in _bottom.iterrows():
                    st.error(
                        f"**{_r['etf_symbol']}** — {_r['composite_score']:.0f}/100 → {_r['action']}  \n"
                        f"Macro: {_r['macro_score']:.0f} · Sent: {_r['sentiment_score']:.0f} · "
                        f"Flow: {_r['flow_score']:.0f} · ML: {_r['ml_score']:.0f}"
                    )

        st.caption(f"Signal composite as of {_sig_date} · from ClickHouse · 6 sources: macro, sentiment, valuation, FII/DII flow, ML, anomaly")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 10 — KITE DASHBOARD
# ══════════════════════════════════════════════════════════════════════════════

with tab_kite:
    st.header("🪁 Kite Dashboard (Personal Portfolio)")

    # ── Sync timestamp ────────────────────────────────────────────────────────
    try:
        sync_df = _query_df("SELECT MAX(imported_at) as last_sync FROM market_data.user_holdings")
        if not sync_df.empty and sync_df.iloc[0]["last_sync"]:
            st.caption(f"Last synchronized with Kite: **{sync_df.iloc[0]['last_sync']}**")
        else:
            st.caption("No sync timestamp found.")
    except Exception:
        st.caption("Sync timestamp unavailable.")

    # ── Sub-tabs ──────────────────────────────────────────────────────────────
    ktab_overview, ktab_holdings, ktab_positions, ktab_margins = st.tabs([
        "📊 Overview", "📦 Holdings", "📈 Positions & Orders", "💰 Margins"
    ])

    # ══════════════════════════════════════════════════════════════════════════
    # SUB-TAB 1 — OVERVIEW
    # ══════════════════════════════════════════════════════════════════════════
    with ktab_overview:

        # ── KPI Cards ─────────────────────────────────────────────────────────
        try:
            summary_df = _query_df("""
                SELECT
                    SUM(quantity * average_price)                        AS total_invested,
                    SUM(quantity * last_price)                           AS total_current_value,
                    SUM(pnl)                                             AS total_pnl,
                    (SUM(pnl) / SUM(quantity * average_price)) * 100     AS total_pnl_pct
                FROM market_data.user_holdings FINAL
            """)
            if not summary_df.empty:
                s = summary_df.iloc[0]
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("💰 Total Invested",  f"₹{s['total_invested']:,.0f}")
                c2.metric("📈 Current Value",   f"₹{s['total_current_value']:,.0f}",
                          delta=f"₹{s['total_pnl']:+,.0f}")
                c3.metric("💹 Total P&L",       f"₹{s['total_pnl']:,.0f}")
                c4.metric("📊 Return %",        f"{s['total_pnl_pct']:.2f}%",
                          delta=f"{s['total_pnl_pct']:.2f}%")
            else:
                st.info("No holdings data to summarize.")
        except Exception as e:
            st.error(f"Error calculating summary: {e}")

        st.divider()

        # ── Charts Row: Wealth Trend + Allocation Donut ───────────────────────
        col_chart, col_donut = st.columns([3, 2])

        with col_chart:
            st.subheader("📈 Wealth Trend")
            try:
                import plotly.graph_objects as go  # noqa: F811
                wealth_df = _query_df("""
                    SELECT record_date, total_value, total_invested
                    FROM market_data.wealth_history
                    ORDER BY record_date
                """)
                if len(wealth_df) >= 2:
                    fig_wealth = go.Figure()
                    fig_wealth.add_trace(go.Scatter(
                        x=wealth_df["record_date"], y=wealth_df["total_value"],
                        name="Current Value", fill="tozeroy",
                        line=dict(color="#3b82f6", width=2),
                        fillcolor="rgba(59,130,246,0.15)",
                        hovertemplate="₹%{y:,.0f}<extra>Current Value</extra>",
                    ))
                    fig_wealth.add_trace(go.Scatter(
                        x=wealth_df["record_date"], y=wealth_df["total_invested"],
                        name="Invested", line=dict(color="#9ca3af", width=1.5, dash="dash"),
                        hovertemplate="₹%{y:,.0f}<extra>Invested</extra>",
                    ))
                    fig_wealth.update_layout(
                        template="plotly_dark",
                        yaxis=dict(tickprefix="₹", tickformat=",.0f"),
                        legend=dict(orientation="h", yanchor="bottom", y=1.02,
                                    xanchor="right", x=1),
                        margin=dict(l=10, r=10, t=30, b=10),
                        height=300,
                        hovermode="x unified",
                    )
                    st.plotly_chart(fig_wealth, use_container_width=True)
                elif len(wealth_df) == 1:
                    st.info("Wealth tracking started today. Chart will appear after the next sync.")
                else:
                    st.info("No wealth history yet. Run `src/scripts/portfolio/track_wealth_history.py` daily.")
            except Exception as e:
                st.warning(f"Wealth chart unavailable: {e}")

        with col_donut:
            st.subheader("🥧 Allocation")
            try:
                import plotly.express as px  # noqa: F811
                alloc_df = _query_df("""
                    SELECT tradingsymbol, exchange,
                           quantity * last_price AS market_value
                    FROM market_data.user_holdings FINAL
                    WHERE quantity > 0
                """)
                if not alloc_df.empty:
                    def _classify_type(row):
                        if row["exchange"] == "MF":
                            return "MF"
                        etf_kw = {"BEES", "ETF", "GOLD", "LIQUID", "NIFTY", "BANKEX", "JR"}
                        if any(k in str(row["tradingsymbol"]).upper() for k in etf_kw):
                            return "ETF"
                        return "Equity"

                    alloc_df["type"] = alloc_df.apply(_classify_type, axis=1)
                    type_summary = alloc_df.groupby("type")["market_value"].sum().reset_index()
                    type_summary.columns = ["Type", "Value"]
                    color_map = {"Equity": "#3b82f6", "ETF": "#f59e0b", "MF": "#10b981"}
                    fig_donut = px.pie(
                        type_summary, values="Value", names="Type",
                        hole=0.5, color="Type", color_discrete_map=color_map,
                    )
                    fig_donut.update_traces(
                        textinfo="percent+label",
                        hovertemplate="<b>%{label}</b><br>₹%{value:,.0f}<br>%{percent}<extra></extra>",
                    )
                    fig_donut.update_layout(
                        template="plotly_dark",
                        showlegend=True,
                        legend=dict(orientation="h", yanchor="bottom", y=-0.2),
                        margin=dict(l=10, r=10, t=10, b=10),
                        height=300,
                    )
                    st.plotly_chart(fig_donut, use_container_width=True)
                else:
                    st.info("No holdings data for allocation chart.")
            except Exception as e:
                st.warning(f"Allocation chart unavailable: {e}")

        st.divider()

        # ── Account Details (collapsible) ──────────────────────────────────────
        with st.expander("👤 Account Details"):
            try:
                profile_df = _query_df(
                    "SELECT * FROM market_data.user_profile ORDER BY imported_at DESC LIMIT 1"
                )
                if not profile_df.empty:
                    p = profile_df.iloc[0]
                    p1, p2, p3, p4 = st.columns(4)
                    p1.metric("Name",   p["user_name"])
                    p2.metric("ID",     p["user_id"])
                    p3.metric("Broker", p["broker"])
                    p4.metric("Type",   p["user_type"])
                else:
                    st.info("No profile data found.")
            except Exception as e:
                st.error(f"Error loading profile: {e}")

    # ══════════════════════════════════════════════════════════════════════════
    # SUB-TAB 2 — HOLDINGS
    # ══════════════════════════════════════════════════════════════════════════
    with ktab_holdings:

        # ── Fetch all holdings for charts ──────────────────────────────────────
        try:
            all_hld_df = _query_df("""
                SELECT tradingsymbol, exchange, quantity, average_price, last_price, pnl,
                       day_change, day_change_percentage,
                       quantity * last_price                                  AS market_value,
                       (pnl / (quantity * average_price)) * 100              AS pnl_pct
                FROM market_data.user_holdings FINAL
                WHERE quantity > 0
                ORDER BY pnl DESC
            """)
        except Exception:
            all_hld_df = pd.DataFrame()

        if not all_hld_df.empty:
            import plotly.graph_objects as go  # noqa: F811
            total_val = all_hld_df["market_value"].sum()
            all_hld_df["weight_pct"] = (all_hld_df["market_value"] / total_val * 100).round(2)

            chart_col1, chart_col2 = st.columns(2)

            # ── Chart: P&L per holding ─────────────────────────────────────────
            with chart_col1:
                st.subheader("💹 P&L by Holding")
                sorted_pnl = all_hld_df.sort_values("pnl")
                colors_pnl = ["#10b981" if v >= 0 else "#ef4444" for v in sorted_pnl["pnl"]]
                fig_pnl = go.Figure(go.Bar(
                    x=sorted_pnl["pnl"],
                    y=sorted_pnl["tradingsymbol"],
                    orientation="h",
                    marker_color=colors_pnl,
                    text=[f"₹{v:+,.0f}" for v in sorted_pnl["pnl"]],
                    textposition="outside",
                    hovertemplate="<b>%{y}</b><br>P&L: ₹%{x:+,.0f}<extra></extra>",
                ))
                fig_pnl.update_layout(
                    template="plotly_dark",
                    xaxis=dict(tickprefix="₹", tickformat=",.0f"),
                    margin=dict(l=10, r=70, t=20, b=10),
                    height=max(250, len(sorted_pnl) * 34),
                    showlegend=False,
                )
                st.plotly_chart(fig_pnl, use_container_width=True)

            # ── Chart: Portfolio weights ───────────────────────────────────────
            with chart_col2:
                st.subheader("⚖️ Portfolio Weights")
                sorted_w = all_hld_df.sort_values("weight_pct", ascending=True)
                fig_wt = go.Figure(go.Bar(
                    x=sorted_w["weight_pct"],
                    y=sorted_w["tradingsymbol"],
                    orientation="h",
                    marker_color="#4f87c4",
                    text=[f"{v:.1f}%" for v in sorted_w["weight_pct"]],
                    textposition="outside",
                    hovertemplate="<b>%{y}</b><br>Weight: %{x:.1f}%<extra></extra>",
                ))
                fig_wt.update_layout(
                    template="plotly_dark",
                    xaxis=dict(ticksuffix="%"),
                    margin=dict(l=10, r=50, t=20, b=10),
                    height=max(250, len(sorted_w) * 34),
                    showlegend=False,
                )
                st.plotly_chart(fig_wt, use_container_width=True)
        else:
            st.info("No holdings data for charts. Sync your Kite account first.")

        st.divider()

        # ── Equity & ETF Holdings table ────────────────────────────────────────
        st.subheader("📦 Equity & ETF Holdings")
        try:
            equity_df = _query_df(
                "SELECT * FROM market_data.user_holdings FINAL WHERE exchange != 'MF' ORDER BY pnl DESC"
            )
            if not equity_df.empty:
                st.dataframe(
                    equity_df.style.format({
                        "average_price":        "₹{:.2f}",
                        "last_price":           "₹{:.2f}",
                        "pnl":                  "₹{:.2f}",
                        "day_change":           "₹{:.2f}",
                        "day_change_percentage": "{:.2f}%",
                    }).map(
                        lambda v: "color: #10b981" if isinstance(v, (int, float)) and v > 0
                                  else ("color: #ef4444" if isinstance(v, (int, float)) and v < 0 else ""),
                        subset=["pnl", "day_change_percentage"],
                    ),
                    use_container_width=True,
                )
            else:
                st.info("No equity holdings found.")
        except Exception as e:
            st.error(f"Error loading equity holdings: {e}")

        # ── Mutual Fund Holdings table ─────────────────────────────────────────
        st.subheader("🏦 Mutual Fund Holdings")
        try:
            mf_df = _query_df(
                "SELECT * FROM market_data.user_holdings FINAL WHERE exchange = 'MF' ORDER BY pnl DESC"
            )
            if not mf_df.empty:
                st.dataframe(
                    mf_df.style.format({
                        "quantity":             "{:.3f}",
                        "average_price":        "₹{:.4f}",
                        "last_price":           "₹{:.4f}",
                        "pnl":                  "₹{:.2f}",
                        "day_change":           "₹{:.2f}",
                        "day_change_percentage": "{:.2f}%",
                    }).map(
                        lambda v: "color: #10b981" if isinstance(v, (int, float)) and v > 0
                                  else ("color: #ef4444" if isinstance(v, (int, float)) and v < 0 else ""),
                        subset=["pnl", "day_change_percentage"],
                    ),
                    use_container_width=True,
                )
            else:
                st.info("No mutual fund holdings found.")
        except Exception as e:
            st.error(f"Error loading mutual fund holdings: {e}")

    # ══════════════════════════════════════════════════════════════════════════
    # SUB-TAB 3 — POSITIONS & ORDERS
    # ══════════════════════════════════════════════════════════════════════════
    with ktab_positions:
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("📈 Open Positions")
            try:
                pos_df = _query_df("SELECT * FROM market_data.user_positions FINAL")
                if not pos_df.empty:
                    st.dataframe(pos_df, use_container_width=True)
                else:
                    st.info("No open positions.")
            except Exception as e:
                st.error(f"Error loading positions: {e}")
        with col2:
            st.subheader("📝 Recent Orders")
            try:
                ord_df = _query_df(
                    "SELECT * FROM market_data.user_orders FINAL ORDER BY order_timestamp DESC"
                )
                if not ord_df.empty:
                    st.dataframe(ord_df, use_container_width=True)
                else:
                    st.info("No recent orders.")
            except Exception as e:
                st.error(f"Error loading orders: {e}")

    # ══════════════════════════════════════════════════════════════════════════
    # SUB-TAB 4 — MARGINS
    # ══════════════════════════════════════════════════════════════════════════
    with ktab_margins:
        st.subheader("💰 Account Margins")
        try:
            margins_df = _query_df("SELECT * FROM market_data.user_margins FINAL ORDER BY segment")
            if not margins_df.empty:
                st.dataframe(margins_df, use_container_width=True)
            else:
                st.info("No margins data found.")
        except Exception as e:
            st.error(f"Error loading margins: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# TAB — DEEP DIVE
# Runs the deepdive pipeline for a US ticker and displays the report +
# all ClickHouse-persisted data (financials, valuation, segments, headcount,
# exec comp, jobs) in a structured view.
# ══════════════════════════════════════════════════════════════════════════════

with tab_deepdive:
    st.header("🏢 Company Deep Dive")
    st.caption(
        "Fetches SEC filings, XBRL financials, market data, 10-K sections, "
        "exec comp, and job postings for a US-listed company. "
        "Results are persisted to ClickHouse — repeat runs load from cache."
    )

    # ── Controls ──────────────────────────────────────────────────────────────
    col_ctrl, col_main = st.columns([1, 3])

    with col_ctrl:
        dd_ticker = st.text_input(
            "Ticker",
            value="ADSK",
            max_chars=10,
            help="US exchange ticker, e.g. ADSK, PCOR, CRM",
        ).upper().strip()

        # Validate before any SQL / subprocess use — allowlist: letters, digits,
        # hyphens, dots only (covers BRK.B, BF-B etc.)
        import re as _re_ticker
        if dd_ticker and not _re_ticker.fullmatch(r"[A-Z0-9.\-]{1,10}", dd_ticker):
            st.error(f"Invalid ticker '{dd_ticker}' — only letters, digits, hyphens and dots allowed.")
            dd_ticker = ""

        dd_skip_fetch = st.toggle(
            "Use cached data",
            value=True,
            help="--skip-fetch: reads from local cache and ClickHouse, no live API calls",
        )

        dd_run = st.button("▶  Run Analysis", type="primary", disabled=not dd_ticker)

    # ── Run pipeline ──────────────────────────────────────────────────────────
    with col_ctrl:
        log_placeholder = st.empty()

    if dd_run and dd_ticker:
        import io as _io
        import subprocess
        import sys as _sys

        log_placeholder.info("Running deepdive pipeline…")
        buf = _io.StringIO()

        cmd = [_sys.executable, "src/main.py", "deepdive", dd_ticker]
        if dd_skip_fetch:
            cmd.append("--skip-fetch")

        with st.spinner(f"Analysing {dd_ticker}…"):
            try:
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    cwd=os.path.join(os.path.dirname(__file__), "..", ".."),
                )
                output = result.stdout + ("\n" + result.stderr if result.stderr else "")
                # Strip ANSI colour codes
                import re as _re
                output = _re.sub(r"\x1b\[[0-9;]*m", "", output)
                log_placeholder.code(output[:6000], language="")
                if result.returncode == 0:
                    st.success(f"✓ {dd_ticker} analysis complete")
                else:
                    st.warning("Pipeline exited with errors — partial data may be shown below")
            except Exception as exc:
                log_placeholder.error(f"Failed to run pipeline: {exc}")

    st.divider()

    # ── Display data from ClickHouse ──────────────────────────────────────────
    if dd_ticker and ok:
        from datetime import date as _date

        # Find dates that have a substantive full report (> 5 000 chars).
        # Stub/partial runs (cache-only re-runs with no section generation)
        # produce a ~1 200-char placeholder — exclude them from the picker.
        @st.cache_data(ttl=30)
        def _dd_run_dates(ticker: str) -> list[str]:
            try:
                with _get_pool().acquire() as _c:
                    r = _c.query(
                        "SELECT DISTINCT toString(report_date) "
                        "FROM market_data.deepdive_reports FINAL "
                        "WHERE ticker = {t:String} "
                        "  AND section_key = '__full__' "
                        "  AND length(content_md) > 5000 "
                        "ORDER BY report_date DESC",
                        parameters={"t": ticker},
                    )
                    dates = [row[0] for row in r.result_rows]
                    if not dates:
                        r2 = _c.query(
                            "SELECT DISTINCT toString(period) FROM market_data.deepdive_watermarks "
                            "WHERE ticker = {t:String} ORDER BY period DESC",
                            parameters={"t": ticker},
                        )
                        dates = [row[0] for row in r2.result_rows]
                return dates
            except Exception:
                return []

        run_dates = _dd_run_dates(dd_ticker)

        if not run_dates:
            st.info(f"No deep dive data found for **{dd_ticker}** in ClickHouse. Click **▶ Run Analysis** above.")
        else:
            selected_date = st.selectbox(
                "Report date",
                options=run_dates,
                index=0,
                key="dd_date",
            )

            dd_tabs = st.tabs(["📋 Report", "📈 Financials", "🏷️ Valuation", "📊 Segments", "👥 Headcount & Comp", "💼 Jobs"])

            # ── Report tab — source of truth: ClickHouse ──────────────────────
            with dd_tabs[0]:
                import re as _re  # noqa: PLC0415

                def _clean_report(md: str) -> str:
                    """Strip [src: ...] inline citations and tidy spacing.

                    Handles nested brackets like [src: segments[0].revenue_usd_m]
                    by allowing one level of inner [...] within the src annotation.
                    """
                    # Pass 1 — remove [src: field] and [src: table[0].field]
                    md = _re.sub(
                        r"\s*\[src:[^\[\]]*(?:\[[^\]]*\][^\[\]]*)*\]",
                        "",
                        md,
                    )
                    # Pass 2 — catch any leftover .field_name] artifacts
                    md = _re.sub(r"\.[a-zA-Z_0-9\[\].]+\]", "", md)
                    # Collapse 3+ blank lines → 2
                    md = _re.sub(r"\n{3,}", "\n\n", md)
                    return md.strip()

                @st.cache_data(ttl=30)
                def _load_report_sections(ticker: str, rdate: str) -> dict[str, str]:
                    try:
                        with _get_pool().acquire() as _c:
                            r = _c.query(
                                "SELECT section_key, content_md "
                                "FROM market_data.deepdive_reports FINAL "
                                "WHERE ticker = {t:String} AND report_date = {d:Date}",
                                parameters={"t": ticker, "d": rdate},
                            )
                        return {row[0]: row[1] for row in r.result_rows}
                    except Exception:
                        return {}

                report_sections = _load_report_sections(dd_ticker, selected_date)

                if "__full__" in report_sections:
                    # Render in a centred, readable column (max ~800px)
                    _rpt_left, _rpt_mid, _rpt_right = st.columns([1, 6, 1])
                    with _rpt_mid:
                        st.markdown(_clean_report(report_sections["__full__"]))
                    if "__sources__" in report_sections:
                        with st.expander("📎 Data sources", expanded=False):
                            st.markdown(_clean_report(report_sections["__sources__"]))
                elif report_sections:
                    from src.deepdive.report import SECTION_ORDER as _SECTION_ORDER  # noqa: PLC0415
                    _rpt_left, _rpt_mid, _rpt_right = st.columns([1, 6, 1])
                    with _rpt_mid:
                        for key, _heading in _SECTION_ORDER:
                            if key in report_sections:
                                st.markdown(_clean_report(report_sections[key]))
                                st.divider()
                else:
                    st.info(
                        f"No report found in ClickHouse for **{dd_ticker} {selected_date}**. "
                        "Click **▶ Run Analysis** above to generate it."
                    )

            # ── Financials tab ────────────────────────────────────────────────
            with dd_tabs[1]:
                st.subheader(f"{dd_ticker} — Annual Financials (USD millions)")
                try:
                    import altair as alt  # noqa: F811
                    fin_df = _get_pool().query_df(
                        "SELECT fiscal_year, revenue_usd_m, gross_profit_usd_m, "
                        "operating_income_usd_m, net_income_usd_m, free_cash_flow_usd_m, "
                        "rd_expense_usd_m, gross_margin_pct, operating_margin_pct "
                        "FROM market_data.deepdive_financials FINAL "
                        "WHERE ticker = {t:String} AND report_date = {d:Date} "
                        "ORDER BY fiscal_year",
                        parameters={"t": dd_ticker, "d": selected_date},
                    )
                    if not fin_df.empty:
                        fin_df.columns = [
                            "FY", "Revenue", "Gross Profit", "Op. Income",
                            "Net Income", "FCF", "R&D", "GM%", "Op. Margin%",
                        ]

                        # KPI row — latest year
                        _lf = fin_df.iloc[-1]
                        _ff = fin_df.iloc[0]
                        _rev_cagr = (((_lf["Revenue"] / _ff["Revenue"]) ** (1 / max(len(fin_df)-1, 1))) - 1) * 100 if _ff["Revenue"] else 0
                        k1, k2, k3, k4, k5 = st.columns(5)
                        k1.metric("Revenue",     f"${_lf['Revenue']:,.0f}M",  f"{_rev_cagr:+.1f}% CAGR")
                        k2.metric("Gross Margin", f"{_lf['GM%']:.1f}%")
                        k3.metric("Op. Margin",   f"{_lf['Op. Margin%']:.1f}%")
                        k4.metric("FCF",          f"${_lf['FCF']:,.0f}M")
                        k5.metric("R&D",          f"${_lf['R&D']:,.0f}M")

                        st.divider()

                        # Styled table
                        _money_cols = ["Revenue", "Gross Profit", "Op. Income", "Net Income", "FCF", "R&D"]
                        _pct_cols   = ["GM%", "Op. Margin%"]
                        _fin_styled = (
                            fin_df.style
                            .format({c: "${:,.0f}M" for c in _money_cols})
                            .format({c: "{:.1f}%" for c in _pct_cols})
                            .bar(subset=["GM%", "Op. Margin%"], color=["#c62828", "#2e7d32"], vmin=0, vmax=100)
                        )
                        st.dataframe(_fin_styled, width="stretch", hide_index=True)

                        st.divider()

                        # Revenue / GP / FCF trend — Altair multi-line
                        _fin_long = fin_df[["FY", "Revenue", "Gross Profit", "FCF"]].melt(
                            id_vars="FY", var_name="Metric", value_name="USD_M"
                        )
                        _fin_chart = (
                            alt.Chart(_fin_long)
                            .mark_line(point=True, strokeWidth=2)
                            .encode(
                                x=alt.X("FY:O", title="Fiscal Year"),
                                y=alt.Y("USD_M:Q", title="USD millions", axis=alt.Axis(format="$,.0f")),
                                color=alt.Color("Metric:N", legend=alt.Legend(orient="bottom", title=None)),
                                tooltip=[
                                    alt.Tooltip("FY:O",     title="FY"),
                                    alt.Tooltip("Metric:N", title="Metric"),
                                    alt.Tooltip("USD_M:Q",  title="USD M", format="$,.0f"),
                                ],
                            )
                            .properties(title="Revenue · Gross Profit · FCF trend", height=280)
                            .interactive()
                        )
                        st.altair_chart(_fin_chart, width="stretch")

                        # Margin trend
                        _mar_long = fin_df[["FY", "GM%", "Op. Margin%"]].melt(
                            id_vars="FY", var_name="Margin", value_name="Pct"
                        )
                        _mar_chart = (
                            alt.Chart(_mar_long)
                            .mark_line(point=True, strokeWidth=2)
                            .encode(
                                x=alt.X("FY:O", title="Fiscal Year"),
                                y=alt.Y("Pct:Q", title="%", axis=alt.Axis(format=".0f")),
                                color=alt.Color("Margin:N", legend=alt.Legend(orient="bottom", title=None)),
                                tooltip=[
                                    alt.Tooltip("FY:O",    title="FY"),
                                    alt.Tooltip("Margin:N"),
                                    alt.Tooltip("Pct:Q",   format=".1f"),
                                ],
                            )
                            .properties(title="Gross Margin % · Operating Margin %", height=200)
                            .interactive()
                        )
                        st.altair_chart(_mar_chart, width="stretch")
                    else:
                        st.info("No financials data found.")
                except Exception as exc:
                    st.error(f"Error: {exc}")

            # ── Valuation tab ─────────────────────────────────────────────────
            with dd_tabs[2]:
                st.subheader(f"{dd_ticker} — Valuation Snapshot")
                try:
                    import altair as alt  # noqa: F811
                    val_df = _get_pool().query_df(
                        "SELECT as_of_date, market_cap_usd_b, pe_trailing, pe_forward, "
                        "ev_revenue, ev_ebitda, fcf_yield_pct, "
                        "peer_pe_median, peer_ev_ebitda_median, peer_ev_revenue_median "
                        "FROM market_data.deepdive_valuation FINAL "
                        "WHERE ticker = {t:String} AND report_date = {d:Date}",
                        parameters={"t": dd_ticker, "d": selected_date},
                    )
                    if not val_df.empty:
                        row = val_df.iloc[0]
                        c1, c2, c3, c4, c5 = st.columns(5)
                        c1.metric("Market Cap",    f"${row['market_cap_usd_b']:.1f}B")
                        c2.metric("P/E (trailing)", f"{row['pe_trailing']:.1f}×"   if row['pe_trailing']   else "—")
                        c3.metric("P/E (forward)",  f"{row['pe_forward']:.1f}×"    if row['pe_forward']    else "—")
                        c4.metric("EV/Revenue",     f"{row['ev_revenue']:.1f}×"    if row['ev_revenue']    else "—")
                        c5.metric("FCF Yield",      f"{row['fcf_yield_pct']:.1f}%" if row['fcf_yield_pct'] else "—")

                        # vs-peers comparison bar chart
                        _peers = []
                        for label, ticker_val, peer_val in [
                            ("P/E",         row["pe_trailing"],  row["peer_pe_median"]),
                            ("EV/EBITDA",   row["ev_ebitda"],    row["peer_ev_ebitda_median"]),
                            ("EV/Revenue",  row["ev_revenue"],   row["peer_ev_revenue_median"]),
                        ]:
                            if ticker_val and peer_val:
                                _peers.append({"Metric": label, "Entity": dd_ticker, "Value": ticker_val})
                                _peers.append({"Metric": label, "Entity": "Peer Median", "Value": peer_val})
                        if _peers:
                            _peers_df = pd.DataFrame(_peers)
                            _peers_chart = (
                                alt.Chart(_peers_df)
                                .mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
                                .encode(
                                    x=alt.X("Entity:N", title=None, axis=alt.Axis(labelAngle=0)),
                                    y=alt.Y("Value:Q",  title="Multiple (×)"),
                                    color=alt.Color(
                                        "Entity:N",
                                        scale=alt.Scale(
                                            domain=[dd_ticker, "Peer Median"],
                                            range=["#3498DB", "#888888"],
                                        ),
                                        legend=alt.Legend(orient="bottom", title=None),
                                    ),
                                    column=alt.Column("Metric:N", title=None),
                                    tooltip=[
                                        alt.Tooltip("Metric:N"),
                                        alt.Tooltip("Entity:N"),
                                        alt.Tooltip("Value:Q", format=".1f"),
                                    ],
                                )
                                .properties(title=f"{dd_ticker} vs Peers", width=160, height=220)
                                .configure_view(strokeWidth=0)
                                .configure_title(anchor="start")
                            )
                            st.altair_chart(_peers_chart)
                    else:
                        st.info("No valuation data found.")
                except Exception as exc:
                    st.error(f"Error: {exc}")

                # ── Price history chart ────────────────────────────────────────
                st.divider()
                st.subheader(f"{dd_ticker} — 2-Year Daily Price History")
                try:
                    import altair as alt  # noqa: F811
                    price_df = _get_pool().query_df(
                        "SELECT trade_date, open, high, low, close, volume "
                        "FROM market_data.deepdive_prices FINAL "
                        "WHERE ticker = {t:String} "
                        "  AND trade_date >= today() - INTERVAL 2 YEAR "
                        "ORDER BY trade_date",
                        parameters={"t": dd_ticker},
                    )
                    if not price_df.empty:
                        price_df["trade_date"] = pd.to_datetime(price_df["trade_date"])

                        # KPI row
                        sc1, sc2, sc3, sc4 = st.columns(4)
                        sc1.metric("Latest Close", f"${price_df['close'].iloc[-1]:.2f}")
                        sc2.metric("52W High",     f"${price_df['high'].tail(252).max():.2f}")
                        sc3.metric("52W Low",      f"${price_df['low'].tail(252).min():.2f}")
                        _chg = (price_df['close'].iloc[-1] / price_df['close'].iloc[0] - 1) * 100
                        sc4.metric("2Y Return",   f"{_chg:+.1f}%", delta_color="normal" if _chg >= 0 else "inverse")

                        # Close price line
                        _price_chart = (
                            alt.Chart(price_df)
                            .mark_line(strokeWidth=1.5, color="#3498DB")
                            .encode(
                                x=alt.X("trade_date:T", title=None, axis=alt.Axis(format="%b %y", labelAngle=-30, grid=False)),
                                y=alt.Y("close:Q",      title="Close ($)", axis=alt.Axis(format="$,.2f")),
                                tooltip=[
                                    alt.Tooltip("trade_date:T", title="Date",  format="%d %b %Y"),
                                    alt.Tooltip("close:Q",      title="Close", format="$,.2f"),
                                    alt.Tooltip("volume:Q",     title="Volume", format=","),
                                ],
                            )
                            .properties(height=280)
                        )
                        # 52W high/low band
                        _52w = price_df.tail(252)
                        _band_df = pd.DataFrame({
                            "trade_date": [_52w["trade_date"].min(), _52w["trade_date"].max()],
                            "hi": [_52w["high"].max(),  _52w["high"].max()],
                            "lo": [_52w["low"].min(),   _52w["low"].min()],
                        })
                        _hi_rule = (
                            alt.Chart(_band_df).mark_rule(color="#2ecc71", strokeDash=[4,3], strokeWidth=1, opacity=0.6)
                            .encode(y="hi:Q")
                        )
                        _lo_rule = (
                            alt.Chart(_band_df).mark_rule(color="#e74c3c", strokeDash=[4,3], strokeWidth=1, opacity=0.6)
                            .encode(y="lo:Q")
                        )
                        st.altair_chart(
                            alt.layer(_lo_rule, _hi_rule, _price_chart)
                            .properties(title=f"{dd_ticker} Close Price  (green = 52W high · red = 52W low)")
                            .interactive(),
                            width="stretch",
                        )

                        # Volume bars
                        _vol_chart = (
                            alt.Chart(price_df)
                            .mark_bar(opacity=0.6, color="#888888")
                            .encode(
                                x=alt.X("trade_date:T", title=None, axis=alt.Axis(format="%b %y", labelAngle=-30, grid=False)),
                                y=alt.Y("volume:Q", title="Volume", axis=alt.Axis(format=".2s")),
                                tooltip=[
                                    alt.Tooltip("trade_date:T", title="Date",   format="%d %b %Y"),
                                    alt.Tooltip("volume:Q",     title="Volume", format=","),
                                ],
                            )
                            .properties(height=100)
                        )
                        st.altair_chart(_vol_chart, width="stretch")
                    else:
                        st.info("No price data in ClickHouse yet. Run the pipeline to fetch it.")
                except Exception as exc:
                    st.error(f"Price chart error: {exc}")

            # ── Segments tab ──────────────────────────────────────────────────
            with dd_tabs[3]:
                st.subheader(f"{dd_ticker} — Segment Revenue")
                try:
                    import altair as alt  # noqa: F811
                    seg_df = _get_pool().query_df(
                        "SELECT segment_name, revenue_usd_m, yoy_growth_pct "
                        "FROM market_data.deepdive_segments FINAL "
                        "WHERE ticker = {t:String} AND report_date = {d:Date} "
                        "ORDER BY revenue_usd_m DESC",
                        parameters={"t": dd_ticker, "d": selected_date},
                    )
                    if not seg_df.empty:
                        seg_df.columns = ["Segment", "Revenue ($M)", "YoY%"]

                        _col_s1, _col_s2 = st.columns([2, 3])
                        with _col_s1:
                            st.dataframe(
                                seg_df.style
                                .format({"Revenue ($M)": "${:,.0f}M", "YoY%": "{:+.1f}%"})
                                .bar(subset=["Revenue ($M)"], color="#3498DB"),
                                width="stretch",
                                hide_index=True,
                            )
                        with _col_s2:
                            # Encode YoY growth as a separate colour column to avoid
                            # alt.condition which conflicts with bar rendering in Altair 6.
                            seg_df["_color"] = seg_df["YoY%"].apply(
                                lambda v: "#2e7d32" if v >= 0 else "#c62828"
                            )
                            _seg_bar = (
                                alt.Chart(seg_df)
                                .mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
                                .encode(
                                    y=alt.Y("Segment:N",
                                            sort=alt.EncodingSortField("Revenue ($M)", order="descending"),
                                            title=None),
                                    x=alt.X("Revenue ($M):Q", title="Revenue (USD millions)",
                                            axis=alt.Axis(format="$,.0f")),
                                    color=alt.Color("_color:N",
                                                    scale=None,
                                                    legend=None),
                                    tooltip=[
                                        alt.Tooltip("Segment:N"),
                                        alt.Tooltip("Revenue ($M):Q", format="$,.0f"),
                                        alt.Tooltip("YoY%:Q",         format="+.1f"),
                                    ],
                                )
                                .properties(height=max(160, len(seg_df) * 40))
                            )
                            st.altair_chart(_seg_bar, width="stretch")
                    else:
                        st.info("No segment data found.")
                except Exception as exc:
                    st.error(f"Error: {exc}")

            # ── Headcount & Exec Comp tab ─────────────────────────────────────
            with dd_tabs[4]:
                col_hc, col_ec = st.columns(2)

                with col_hc:
                    st.subheader("👥 Headcount")
                    try:
                        hc_df = _get_pool().query_df(
                            "SELECT fiscal_period, total_headcount, notes "
                            "FROM market_data.deepdive_headcount FINAL "
                            "WHERE ticker = {t:String} "
                            "  AND report_date = ("
                            "    SELECT max(report_date) FROM market_data.deepdive_headcount "
                            "    WHERE ticker = {t:String} AND report_date <= {d:Date}"
                            "  )",
                            parameters={"t": dd_ticker, "d": selected_date},
                        )
                        if not hc_df.empty:
                            hc_df.columns = ["Period", "Headcount", "Notes"]
                            st.dataframe(
                                hc_df.style.format({"Headcount": "{:,.0f}"}),
                                width="stretch",
                                hide_index=True,
                            )
                        else:
                            st.info("No headcount data.")
                    except Exception as exc:
                        st.error(f"Error: {exc}")

                with col_ec:
                    st.subheader("💰 Executive Compensation (NEOs)")
                    try:
                        ec_df = _get_pool().query_df(
                            "SELECT exec_name, position, fiscal_year, "
                            "round(total_usd / 1e6, 2) AS total_usd_m, "
                            "round(stock_awards_usd / 1e6, 2) AS stock_m, "
                            "round(stock_pct, 1) AS stock_pct "
                            "FROM market_data.deepdive_exec_comp FINAL "
                            "WHERE ticker = {t:String} "
                            "  AND report_date = ("
                            "    SELECT max(report_date) FROM market_data.deepdive_exec_comp "
                            "    WHERE ticker = {t:String} AND report_date <= {d:Date}"
                            "  ) "
                            "ORDER BY total_usd DESC",
                            parameters={"t": dd_ticker, "d": selected_date},
                        )
                        if not ec_df.empty:
                            ec_df.columns = ["Name", "Position", "FY", "Total ($M)", "Stock ($M)", "Stock%"]
                            st.dataframe(
                                ec_df.style
                                .format({"Total ($M)": "${:.2f}M", "Stock ($M)": "${:.2f}M", "Stock%": "{:.1f}%"})
                                .bar(subset=["Total ($M)"], color="#3498DB"),
                                width="stretch",
                                hide_index=True,
                            )
                        else:
                            st.info("No exec comp data.")
                    except Exception as exc:
                        st.error(f"Error: {exc}")

            # ── Jobs tab ──────────────────────────────────────────────────────
            with dd_tabs[5]:
                st.subheader(f"{dd_ticker} — Open Job Postings")
                try:
                    import altair as alt  # noqa: F811
                    import json as _json  # noqa: F811

                    jobs_df = _get_pool().query_df(
                        "SELECT function_bucket, location, job_count "
                        "FROM market_data.deepdive_jobs FINAL "
                        "WHERE ticker = {t:String} "
                        "  AND report_date = ("
                        "    SELECT max(report_date) FROM market_data.deepdive_jobs "
                        "    WHERE ticker = {t:String} AND report_date <= {d:Date}"
                        "  ) "
                        "ORDER BY job_count DESC",
                        parameters={"t": dd_ticker, "d": selected_date},
                    )
                    if not jobs_df.empty:
                        jobs_df.columns = ["Function", "Location", "Openings"]

                        # ── Try to load raw cache for region + AI analysis ─────
                        from pathlib import Path as _Path  # noqa: PLC0415
                        _cache_dirs = sorted(
                            _Path("output/deepdive/cache").glob(f"{dd_ticker}/*/workday_jobs_raw.json"),
                            key=lambda p: p.parent.name,
                            reverse=True,
                        )
                        _raw_jobs: list[dict] = []
                        if _cache_dirs:
                            try:
                                with open(_cache_dirs[0]) as _fj:
                                    _raw_jobs = _json.load(_fj)
                            except Exception:
                                pass

                        # ── Row 1: by-function chart + KPIs ───────────────────
                        by_func = (
                            jobs_df.groupby("Function")["Openings"]
                            .sum().sort_values(ascending=False).reset_index()
                        )
                        by_func.columns = ["Function", "Total Openings"]

                        _jk1, _jk2, _jk3, _jk4 = st.columns(4)
                        _jk1.metric("Total Openings (DB)", f"{jobs_df['Openings'].sum():,}")
                        _jk2.metric("Roles in Raw Cache",  f"{len(_raw_jobs):,}" if _raw_jobs else "—")
                        _jk3.metric("Functions",           len(by_func))
                        _jk4.metric("Locations",           jobs_df["Location"].nunique())

                        st.divider()

                        _col_j1, _col_j2 = st.columns([1, 2])
                        with _col_j1:
                            st.caption("**By function**")
                            st.dataframe(
                                by_func.style.format({"Total Openings": "{:,}"}),
                                width="stretch", hide_index=True,
                            )
                        with _col_j2:
                            _jobs_chart = (
                                alt.Chart(by_func)
                                .mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3, color="#3498DB")
                                .encode(
                                    y=alt.Y("Function:N",
                                            sort=alt.EncodingSortField("Total Openings", order="descending"),
                                            title=None),
                                    x=alt.X("Total Openings:Q", title="Open Roles"),
                                    tooltip=[alt.Tooltip("Function:N"),
                                             alt.Tooltip("Total Openings:Q", format=",")],
                                )
                                .properties(height=max(200, len(by_func) * 28))
                            )
                            st.altair_chart(_jobs_chart, width="stretch")

                        # ── Region breakdown (from raw cache) ─────────────────
                        if _raw_jobs:
                            st.divider()
                            st.subheader("🌍 Region Breakdown")

                            _REGION_MAP = {
                                "USA": "AMER", "CAN": "AMER", "BRA": "AMER",
                                "MEX": "AMER", "CRI": "AMER",
                                "IND": "APAC", "JPN": "APAC", "SGP": "APAC",
                                "AUS": "APAC", "CHN": "APAC", "KOR": "APAC",
                                "TWN": "APAC", "MYS": "APAC", "VNM": "APAC",
                                "PHL": "APAC", "IDN": "APAC", "THA": "APAC",
                                "HKG": "APAC", "NZL": "APAC",
                                "GBR": "EMEA", "DEU": "EMEA", "FRA": "EMEA",
                                "ESP": "EMEA", "IRL": "EMEA", "POL": "EMEA",
                                "NLD": "EMEA", "CHE": "EMEA", "ISR": "EMEA",
                                "ARE": "EMEA", "JOR": "EMEA", "SAU": "EMEA",
                                "RSD": "EMEA", "CZE": "EMEA", "DNK": "EMEA",
                                "AUT": "EMEA", "ITA": "EMEA", "HRV": "EMEA",
                                "TUR": "EMEA", "PRT": "EMEA", "NOR": "EMEA",
                                "SWE": "EMEA", "FIN": "EMEA", "BEL": "EMEA",
                            }

                            def _region(loc: str) -> str:
                                u = loc.upper()
                                if u.startswith("AMER"):   return "AMER"
                                if u.startswith("APAC"):   return "APAC"
                                if u.startswith("EMEA"):   return "EMEA"
                                for code, reg in _REGION_MAP.items():
                                    if code in u:          return reg
                                if "REMOTE" in u:          return "Remote"
                                return "Other"

                            _reg_counts: dict[str, int] = {}
                            for _j in _raw_jobs:
                                _r = _region(_j.get("locationsText", ""))
                                _reg_counts[_r] = _reg_counts.get(_r, 0) + 1

                            _reg_df = pd.DataFrame(
                                sorted(_reg_counts.items(), key=lambda x: -x[1]),
                                columns=["Region", "Openings"],
                            )
                            _reg_colors = {
                                "AMER": "#3498DB", "APAC": "#E67E22",
                                "EMEA": "#2ECC71", "Remote": "#9B59B6", "Other": "#95A5A6",
                            }
                            _reg_df["_color"] = _reg_df["Region"].map(_reg_colors).fillna("#888")

                            _rc1, _rc2 = st.columns([1, 2])
                            with _rc1:
                                st.dataframe(
                                    _reg_df[["Region", "Openings"]].style.format({"Openings": "{:,}"}),
                                    width="stretch", hide_index=True,
                                )
                            with _rc2:
                                _reg_chart = (
                                    alt.Chart(_reg_df)
                                    .mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
                                    .encode(
                                        y=alt.Y("Region:N",
                                                sort=alt.EncodingSortField("Openings", order="descending"),
                                                title=None),
                                        x=alt.X("Openings:Q", title="Job Openings"),
                                        color=alt.Color("_color:N", scale=None, legend=None),
                                        tooltip=[alt.Tooltip("Region:N"),
                                                 alt.Tooltip("Openings:Q", format=",")],
                                    )
                                    .properties(height=max(160, len(_reg_df) * 36))
                                )
                                st.altair_chart(_reg_chart, width="stretch")

                        # ── ML / Agentic AI openings ───────────────────────────
                        if _raw_jobs:
                            st.divider()
                            st.subheader("🤖 ML · AI · Agentic Openings")

                            _AI_KW = [
                                "machine learning", "ml ", " ai ", "artificial intelligence",
                                "agentic", "llm", "generative", "gen ai", "nlp",
                                "deep learning", "neural", "ai/ml", "foundation model",
                                "computer vision", "reinforcement learning",
                            ]
                            _ai_rows = []
                            for _j in _raw_jobs:
                                _t = (_j.get("title") or "").strip()
                                _tl = _t.lower()
                                if any(_kw in _tl for _kw in _AI_KW):
                                    _loc = _j.get("locationsText", "—")
                                    # Detect sub-type
                                    if "agentic" in _tl:            _tag = "🧠 Agentic AI"
                                    elif "llm" in _tl or "foundation" in _tl or "generative" in _tl or "gen ai" in _tl:
                                                                     _tag = "💬 Gen AI / LLM"
                                    elif "machine learning" in _tl or "ml " in _tl or "ai/ml" in _tl:
                                                                     _tag = "📊 ML Engineering"
                                    elif "nlp" in _tl:               _tag = "📝 NLP"
                                    elif "computer vision" in _tl:   _tag = "👁️ Computer Vision"
                                    else:                             _tag = "🤖 AI / Other"
                                    _ai_rows.append({"Title": _t.title(), "Tag": _tag, "Location": _loc})

                            if _ai_rows:
                                _ai_df = pd.DataFrame(_ai_rows).sort_values("Tag")
                                _aic1, _aic2 = st.columns([3, 1])
                                with _aic2:
                                    # Tag summary
                                    _tag_counts = _ai_df["Tag"].value_counts().reset_index()
                                    _tag_counts.columns = ["Category", "Count"]
                                    st.dataframe(_tag_counts, width="stretch", hide_index=True)
                                with _aic1:
                                    st.dataframe(
                                        _ai_df[["Tag", "Title", "Location"]],
                                        width="stretch", hide_index=True,
                                    )
                                st.caption(f"**{len(_ai_rows)}** AI/ML/Agentic roles out of {len(_raw_jobs)} total openings ({len(_ai_rows)/len(_raw_jobs)*100:.1f}%)")
                            else:
                                st.info("No ML/AI/Agentic roles detected in raw job titles.")

                        with st.expander("📋 Full function × location breakdown"):
                            st.dataframe(jobs_df, width="stretch", hide_index=True)
                    else:
                        st.info("No jobs data found.")
                except Exception as exc:
                    st.error(f"Error: {exc}")


# ══════════════════════════════════════════════════════════════════════════════
# TAB — INTERNATIONAL ETF PATTERNS
# ML-driven pattern analysis for the 6 RBI-capped international ETFs:
#   performance, scarcity premium, regime detection, correlation,
#   seasonality, LightGBM feature importance, drawdown timeline.
# ══════════════════════════════════════════════════════════════════════════════

with tab_intl_etf:
    st.header("🌍 International ETF Pattern Analysis")
    st.caption(
        "ML-powered 3-year analysis of MAFANG · HNGSNGBEES · MON100 · "
        "MASPTOP50 · MAHKTECH · MONQ50. "
        "Uses KMeans regime detection, Isolation Forest anomaly detection, "
        "and LightGBM feature importance. Results cached for 1 hour."
    )

    if not ok:
        st.error("ClickHouse unavailable — cannot run analysis.")
        st.stop()

    @st.cache_data(ttl=3600, show_spinner="Running ML analysis — ~30 seconds…")
    def _intl_etf_results():
        from src.ui.intl_etf_analysis import run_full_analysis
        return run_full_analysis(_get_pool())

    col_run, col_note = st.columns([1, 4])
    with col_run:
        if st.button("🔄 Refresh Analysis", help="Clear cache and recompute all ML models"):
            st.cache_data.clear()
            st.rerun()
    with col_note:
        st.caption("Analysis window: last 3 years · Risk-free rate: 6.5% (Indian T-bill proxy)")

    try:
        R = _intl_etf_results()
    except Exception as _exc:
        st.error(f"Analysis failed: {_exc}")
        st.stop()

    # ── Sub-tabs ──────────────────────────────────────────────────────────────
    (
        _ie_perf, _ie_prem, _ie_regime,
        _ie_corr, _ie_season, _ie_lgbm, _ie_dd,
    ) = st.tabs([
        "📊 Performance", "💰 Premium", "🎯 Regimes",
        "🔗 Correlation", "📅 Seasonality", "🤖 LightGBM", "📉 Drawdowns",
    ])

    # ── Performance ───────────────────────────────────────────────────────────
    with _ie_perf:
        st.subheader("3-Year Performance Summary")
        perf = R["perf_df"].copy()

        # KPI row: top Sharpe, best 3Y return, worst max DD
        if not perf.empty:
            best_sharpe = perf.loc[perf["Sharpe"].idxmax()]
            best_ret    = perf.loc[perf["3Y Ret %"].idxmax()]
            worst_dd    = perf.loc[perf["Max DD %"].idxmin()]
            k1, k2, k3 = st.columns(3)
            k1.metric("Best Sharpe",
                      best_sharpe["ETF"].split(" · ")[0],
                      f"{best_sharpe['Sharpe']:.2f}")
            k2.metric("Best 3Y Return",
                      best_ret["ETF"].split(" · ")[0],
                      f"+{best_ret['3Y Ret %']:.1f}%")
            k3.metric("Deepest Drawdown",
                      worst_dd["ETF"].split(" · ")[0],
                      f"{worst_dd['Max DD %']:.1f}%")
            st.divider()

        st.plotly_chart(R["perf_chart"], use_container_width=True)

        display_perf = perf.drop(columns=["_sym"], errors="ignore")
        _gradient_dataframe(
            display_perf,
            lambda s: s
                .background_gradient(subset=["3Y Ret %", "1Y Ret %", "6M Ret %"], cmap="RdYlGn")
                .background_gradient(subset=["Sharpe", "Calmar"], cmap="Blues")
                .background_gradient(subset=["Max DD %"], cmap="RdYlGn_r"),
            hide_index=True,
            use_container_width=True,
        )

    # ── Premium ───────────────────────────────────────────────────────────────
    with _ie_prem:
        st.subheader("Scarcity Premium Analysis")
        st.info(
            "**MASPTOP50 excluded** — its MF NAV scheme code uses a different unit base "
            "(₹19 NAV vs ₹77 market price), producing a spurious ~300% premium.",
            icon="ℹ️",
        )
        prem_stats = R["prem_stats"].copy().drop(columns=["_sym"], errors="ignore")
        if not prem_stats.empty:
            # KPI: highest premium, most anomalous
            hp = prem_stats.loc[prem_stats["Current %"].idxmax()]
            ha = prem_stats.loc[prem_stats["Anomaly Days"].idxmax()]
            p1, p2, p3 = st.columns(3)
            p1.metric("Highest Current Premium",
                      hp["ETF"].split(" · ")[0],
                      f"{hp['Current %']:.2f}%")
            p2.metric("Most Anomalous",
                      ha["ETF"].split(" · ")[0],
                      f"{ha['Anomaly Days']} days flagged")
            rising = prem_stats[prem_stats["Trend /mo"] > 0]
            p3.metric("ETFs with Rising Premium",
                      f"{len(rising)} / {len(prem_stats)}",
                      "structural widening" if len(rising) == len(prem_stats) else "")
            st.divider()

        st.plotly_chart(R["prem_chart"], use_container_width=True)
        _gradient_dataframe(
            prem_stats,
            lambda s: s
                .background_gradient(subset=["Current %", "Mean %", "Trend /mo"], cmap="YlOrRd")
                .background_gradient(subset=["Anomaly Days"], cmap="Reds"),
            hide_index=True,
            use_container_width=True,
        )

    # ── Regimes ───────────────────────────────────────────────────────────────
    with _ie_regime:
        st.subheader("Market Regime Detection (KMeans k=3)")
        st.caption("Features: 30D rolling return, volatility, momentum, scarcity premium.")
        reg_df = R["regime_df"].copy().drop(columns=["_sym"], errors="ignore")

        if not reg_df.empty:
            bears_now = reg_df[reg_df["Current"] == "Bear"]
            bulls_now = reg_df[reg_df["Current"] == "Bull"]
            r1, r2 = st.columns(2)
            r1.metric("Currently in Bull Regime", f"{len(bulls_now)} ETFs",
                      ", ".join(bulls_now["ETF"].str.split(" · ").str[0]))
            r2.metric("Currently in Bear Regime", f"{len(bears_now)} ETFs",
                      ", ".join(bears_now["ETF"].str.split(" · ").str[0]))
            st.divider()

        st.plotly_chart(R["regime_chart"], use_container_width=True)
        st.dataframe(
            reg_df.style
                .map(lambda v: "color: #e74c3c" if v == "Bear"
                     else "color: #2ecc71" if v == "Bull"
                     else "color: #f39c12", subset=["Current"]),
            hide_index=True,
            use_container_width=True,
        )

    # ── Correlation ───────────────────────────────────────────────────────────
    with _ie_corr:
        st.subheader("Return Correlations")
        st.plotly_chart(R["corr_chart"], use_container_width=True)

        if not R["usdinr_corr"].empty:
            st.subheader("USD/INR Correlation")
            st.caption("Low/negative correlation → INR depreciation does NOT reliably boost short-term Indian prices of these ETFs.")
            _gradient_dataframe(
                R["usdinr_corr"],
                lambda s: s.background_gradient(
                    subset=["Full-Period", "Last 6M"], cmap="RdBu", vmin=-0.5, vmax=0.5
                ),
                hide_index=True,
                use_container_width=True,
            )

        st.info(
            "**Two clusters detected:**  "
            "China (MAFANG ↔ MASPTOP50 ρ≈0.53, HNGSNGBEES ↔ MAHKTECH ρ≈0.63) "
            "and US (MON100 ↔ MONQ50 ρ≈0.47).  "
            "Cross-cluster correlation is weak (0.11–0.36) — "
            "MAFANG + MON100 is the best diversification pair.",
            icon="💡",
        )

    # ── Seasonality ───────────────────────────────────────────────────────────
    with _ie_season:
        st.subheader("Monthly Return Seasonality")
        st.plotly_chart(R["season_chart"], use_container_width=True)
        if not R["season_bw"].empty:
            st.subheader("Best / Worst Months & Half-Year Bias")
            _gradient_dataframe(
                R["season_bw"],
                lambda s: s
                    .background_gradient(subset=["Best Ret %"], cmap="Greens")
                    .background_gradient(subset=["Worst Ret %"], cmap="Reds_r"),
                hide_index=True,
                use_container_width=True,
            )
        st.info(
            "**July** is the strongest month for US/Nasdaq ETFs (Q2 earnings season).  "
            "**February** is the weakest across most names.  "
            "Apr–Sep outperforms Oct–Mar for all ETFs except HNGSNGBEES "
            "(which follows Chinese New Year / HK fiscal cycle).",
            icon="📅",
        )

    # ── LightGBM ──────────────────────────────────────────────────────────────
    with _ie_lgbm:
        st.subheader("LightGBM: 5-Day Return Direction Predictability")
        st.caption(
            "Time-series cross-validation (3 folds). Target: next-5D return positive/negative. "
            "50% = random baseline. Features include lagged returns, rolling volatility, "
            "premium level/z-score, and USD/INR."
        )
        if not R["lgbm_df"].empty:
            best_acc = R["lgbm_df"].loc[R["lgbm_df"]["CV Accuracy"].idxmax()]
            st.metric(
                "Most Predictable ETF",
                best_acc["ETF"].split(" · ")[0],
                f"{best_acc['CV Accuracy']:.1f}% accuracy (random = 50%)",
            )
            st.divider()
            _gradient_dataframe(
                R["lgbm_df"],
                lambda s: s.background_gradient(
                    subset=["CV Accuracy"], cmap="RdYlGn", vmin=45, vmax=60
                ),
                hide_index=True,
                use_container_width=True,
            )
        st.plotly_chart(R["lgbm_chart"], use_container_width=True)
        st.info(
            "**vol_30d dominates** across all ETFs — high-volatility regimes have more "
            "directional follow-through.  "
            "**Premium ranks #2–3** for MAFANG and HNGSNGBEES — an elevated premium "
            "(vs 60D mean) predicts negative next-5D returns (mean reversion).",
            icon="🤖",
        )

    # ── Drawdowns ─────────────────────────────────────────────────────────────
    with _ie_dd:
        st.subheader("Drawdown Timeline (episodes > 10%)")
        dd_df = R["dd_df"]
        if not dd_df.empty:
            unrecovered = dd_df[~dd_df["Recovered"]]
            if not unrecovered.empty:
                st.warning(
                    f"**{len(unrecovered)} unrecovered drawdown(s):** "
                    + " · ".join(
                        f"{r['ETF']} ({r['Max DD %']}% from {r['Peak Date']})"
                        for _, r in unrecovered.iterrows()
                    ),
                    icon="⚠️",
                )
            st.plotly_chart(R["dd_chart"], use_container_width=True)
            _gradient_dataframe(
                dd_df.drop(columns=["Recovered"]),
                lambda s: s
                    .background_gradient(subset=["Max DD %"], cmap="Reds_r")
                    .background_gradient(
                        subset=["Recovery Days"], cmap="YlOrRd",
                        gmap=pd.to_numeric(dd_df["Recovery Days"], errors="coerce"),
                    ),
                hide_index=True,
                use_container_width=True,
            )
        else:
            st.info("No drawdown episodes > 10% found in the 3-year window.")


# ══════════════════════════════════════════════════════════════════════════════
# TAB — REPORTS  (download generated PDFs, HTML, Markdown)
# ══════════════════════════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════════════════════════
# TAB 14 — WORKFLOWS
# ══════════════════════════════════════════════════════════════════════════════

with tab_workflows:
    st.header("🤖 Workflows")
    st.caption(
        "LangGraph **StateGraph** pipelines — deterministic structure, guaranteed section "
        "completeness, and **80–90% fewer tokens** than equivalent ReAct sub-agents. "
        "Data fetch runs in parallel Python threads; the LLM is called only for "
        "adversarial verification and final synthesis (1–2 calls per workflow)."
    )

    _wf_col_left, _wf_col_right = st.columns([2, 1])
    with _wf_col_right:
        st.info(
            "**Token estimates**\n\n"
            "| Workflow | Tokens |\n"
            "|---|---|\n"
            "| Autonomous Research | ~8,800 |\n"
            "| Indian Equity | ~7,000 |\n"
            "| MF Fund Consensus | ~4,000 |\n"
            "| Portfolio Analysis | ~9,800 |\n\n"
            "vs 15,000–42,000 for ReAct equivalent",
            icon="💡",
        )

    wf_research_tab, wf_equity_tab, wf_consensus_tab, wf_portfolio_tab = st.tabs([
        "🔭 Autonomous Research",
        "📈 Indian Equity",
        "🏦 MF Consensus",
        "💼 Portfolio Analysis",
    ])

    # ── Workflow 1: Autonomous Research ───────────────────────────────────────
    with wf_research_tab:
        st.subheader("🔭 Autonomous Research")
        st.caption(
            "5 phases · 2 LLM calls · ~8,800 tokens  \n"
            "`resolve → fetch_all (6 parallel groups) → correlate → verify (adversarial) → synthesise`"
        )
        _q_research = st.text_input(
            "Question",
            placeholder="comprehensive research on ADANIENT",
            key="wf_research_q",
        )
        _run_research = st.button("▶ Run", key="wf_research_run", type="primary", use_container_width=False)
        if _run_research:
            if not _q_research.strip():
                st.warning("Enter a question first.")
            else:
                _phases = st.empty()
                _phases.info("Phase 1/5 · Resolving symbol…")
                try:
                    with st.spinner("Running autonomous research workflow…"):
                        from src.workflows.autonomous_research import run as _run_ar
                        _result = _run_ar(_q_research.strip())
                    _phases.empty()
                    st.markdown(_result)
                    st.success("Workflow complete.", icon="✅")
                except Exception as _e:
                    _phases.empty()
                    st.error(f"Workflow failed: {_e}")

    # ── Workflow 2: Indian Equity ──────────────────────────────────────────────
    with wf_equity_tab:
        st.subheader("📈 Indian Equity Research")
        st.caption(
            "3 phases · 1 LLM call · ~7,000 tokens  \n"
            "`resolve → fetch_all (12 tools, guaranteed) → synthesise`  \n"
            "All 12 Round-2 tools run in parallel — no silent section skips."
        )
        _q_equity = st.text_input(
            "Company or question",
            placeholder="RELIANCE quarterly results and MF holdings",
            key="wf_equity_q",
        )
        _run_equity = st.button("▶ Run", key="wf_equity_run", type="primary", use_container_width=False)
        if _run_equity:
            if not _q_equity.strip():
                st.warning("Enter a company name or question first.")
            else:
                try:
                    with st.spinner("Fetching 12 data sources in parallel…"):
                        from src.workflows.india_equity import run as _run_ie
                        _result = _run_ie(_q_equity.strip())
                    st.markdown(_result)
                    st.success("Workflow complete.", icon="✅")
                except Exception as _e:
                    st.error(f"Workflow failed: {_e}")

    # ── Workflow 3: MF Fund Consensus ──────────────────────────────────────────
    with wf_consensus_tab:
        st.subheader("🏦 Multi-Fund Consensus")
        st.caption(
            "3 phases · 1 LLM call · ~4,000 tokens  \n"
            "`fetch_all_funds (7 parallel) → fetch_consensus → synthesise`  \n"
            "Covers: Nippon, DSP, Bajaj, Quant, ICICI multi-asset funds."
        )
        _period = st.radio(
            "Period",
            options=["mom", "yoy"],
            format_func=lambda x: "Month-over-Month" if x == "mom" else "Year-over-Year",
            horizontal=True,
            key="wf_consensus_period",
        )
        _run_consensus = st.button("▶ Run", key="wf_consensus_run", type="primary", use_container_width=False)
        if _run_consensus:
            try:
                with st.spinner(f"Fetching 7 funds in parallel ({_period.upper()})…"):
                    from src.workflows.multi_fund_consensus import run as _run_mfc
                    _result = _run_mfc(_period)
                st.markdown(_result)
                st.success("Workflow complete.", icon="✅")
            except Exception as _e:
                st.error(f"Workflow failed: {_e}")

    # ── Workflow 4: Portfolio Analysis ─────────────────────────────────────────
    with wf_portfolio_tab:
        st.subheader("💼 Portfolio Analysis")
        st.caption(
            "6 phases · N+K+1 LLM calls · ~9,800 tokens (10 holdings, 3 HIGH-conviction)  \n"
            "`discover → enrich_all → score_all → verify_high (adversarial) → fetch_macro → synthesise`  \n"
            "Reads from `market_data.user_holdings FINAL`. HIGH-conviction scores are "
            "adversarially challenged before the final report."
        )
        st.info(
            "Ensure holdings are synced: run **Import Data → Kite Holdings** first, "
            "or `python src/main.py analyze` to backfill `user_holdings`.",
            icon="ℹ️",
        )
        _run_portfolio = st.button("▶ Run Portfolio Analysis", key="wf_portfolio_run", type="primary")
        if _run_portfolio:
            try:
                with st.spinner("Running portfolio workflow (discover → enrich → score → verify → macro → synthesise)…"):
                    from src.workflows.portfolio_analysis import run as _run_pa
                    _result = _run_pa()
                st.markdown(_result)
                st.success("Workflow complete.", icon="✅")
            except Exception as _e:
                st.error(f"Workflow failed: {_e}")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 15 — REPORTS
# ══════════════════════════════════════════════════════════════════════════════

with tab_reports:
    import os
    from pathlib import Path
    from datetime import datetime as _dt

    st.header("📁 Generated Reports")
    st.caption(
        "All reports produced by `docker compose run mosaic` are listed here. "
        "Click **Download** to save a file locally. "
        "Reports are also browsable at **http://localhost:8502** (file server)."
    )

    reports_dir = Path(os.environ.get("OUTPUT_DIR", "/app/output")) / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    _EXT_ICON = {".pdf": "📄", ".html": "🌐", ".md": "📝", ".png": "🖼️"}
    _EXT_MIME = {
        ".pdf": "application/pdf",
        ".html": "text/html",
        ".md": "text/markdown",
        ".png": "image/png",
    }

    all_files = sorted(
        [f for f in reports_dir.iterdir() if f.is_file() and f.suffix in _EXT_MIME],
        key=lambda f: f.stat().st_mtime,
        reverse=True,
    )

    if not all_files:
        st.info("No reports found yet. Run `docker compose run mosaic` to generate one.", icon="ℹ️")
    else:
        # Filter controls
        col_filter, col_refresh = st.columns([3, 1])
        with col_filter:
            ext_filter = st.multiselect(
                "File type",
                options=[".pdf", ".html", ".md", ".png"],
                default=[".pdf", ".html", ".md"],
                label_visibility="collapsed",
            )
        with col_refresh:
            if st.button("🔄 Refresh", use_container_width=True):
                st.rerun()

        filtered = [f for f in all_files if f.suffix in ext_filter] if ext_filter else all_files

        if not filtered:
            st.info("No files match the selected filter.")
        else:
            st.caption(f"{len(filtered)} file(s) — sorted newest first")
            for fpath in filtered:
                stat = fpath.stat()
                size_kb = stat.st_size // 1024
                mtime = _dt.fromtimestamp(stat.st_mtime).strftime("%d %b %Y  %H:%M")
                icon = _EXT_ICON.get(fpath.suffix, "📎")
                mime = _EXT_MIME.get(fpath.suffix, "application/octet-stream")

                col_name, col_meta, col_btn = st.columns([5, 3, 2])
                with col_name:
                    st.markdown(f"{icon} **{fpath.name}**")
                with col_meta:
                    st.caption(f"{mtime} · {size_kb} KB")
                with col_btn:
                    st.download_button(
                        label="⬇ Download",
                        data=fpath.read_bytes(),
                        file_name=fpath.name,
                        mime=mime,
                        key=f"dl_{fpath.name}",
                        use_container_width=True,
                    )
                st.divider()
