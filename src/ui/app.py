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

# Ensure project root is importable when running as `streamlit run src/ui/app.py`
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

# ── ClickHouse connection params (from env / defaults) ────────────────────────
CH_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CH_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
CH_DB   = os.environ.get("CLICKHOUSE_DATABASE", "market_data")
CH_USER = os.environ.get("CLICKHOUSE_USER", "default")
CH_PASS = os.environ.get("CLICKHOUSE_PASSWORD", "")


# ── ClickHouse helpers ────────────────────────────────────────────────────────

@st.cache_resource
def _get_client():
    import clickhouse_connect
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT,
        username=CH_USER, password=CH_PASS,
        connect_timeout=8,
    )


def _query_df(sql: str) -> pd.DataFrame:
    result = _get_client().query(sql)
    return pd.DataFrame(result.result_rows, columns=result.column_names)


@st.cache_data(ttl=30)
def _ch_ok() -> bool:
    try:
        _get_client().command("SELECT 1")
        return True
    except Exception:
        return False


@st.cache_resource
def _ensure_schema() -> None:
    """Create all market_data tables if they don't exist (idempotent DDL)."""
    try:
        from src.importer.clickhouse import ClickHouseImporter
        ch = ClickHouseImporter(
            host=CH_HOST, port=CH_PORT,
            database="market_data",
            username=CH_USER, password=CH_PASS,
        )
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

tab_import, tab_query, tab_explorer, tab_anomaly, tab_wis, tab_holdings, tab_etf_scan = st.tabs(
    ["📥 Import Data", "🔍 SQL Query", "📊 Explorer", "🔬 Anomaly Detection", "🕵️ Who Is Selling?", "📦 MF Holdings", "🏦 ETF Scanner"]
)


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
            "indices":      "Nifty50, Sensex, S&P500, etc. (Yahoo Finance)",
            "mf":           "ETF NAV history from MFAPI.in (AMFI official)",
            "inav":         "Live iNAV snapshot from NSE API",
            "cot":          "CFTC COT Gold — hedge fund & commercial positioning (weekly)",
            "cb_reserves":  "Central bank gold reserves — 9 countries via IMF IFS (monthly)",
            "etf_aum":      "Gold ETF AUM — GLD, IAU, SGOL, PHYS + implied tonnes (daily)",
            "fx_rates":     "USD FX rates — INR, CNY, AED, SAR, KWD daily OHLC (Yahoo Finance)",
            "mf_holdings":  "📦 Monthly portfolio holdings — DSP/Quant/ICICI Multi Asset (Morningstar)",
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
    round(n.nav,   4)                                                       AS amfi_nav,
    if(n.nav > 0, round((p.close - n.nav) / n.nav * 100, 3), NULL)        AS premium_disc_pct
FROM (
    SELECT trade_date, close
    FROM market_data.daily_prices FINAL
    WHERE symbol = 'GOLDBEES' AND category = 'etfs'
) p
LEFT JOIN (
    SELECT nav_date AS trade_date, nav
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
    expected_return_pct,
    confidence_low,
    confidence_high,
    regime_signal,
    round(cv_r2_mean, 4) AS cv_r2_mean,
    n_training_rows,
    goldbees_close
FROM market_data.ml_predictions FINAL
ORDER BY as_of DESC, horizon_days""",

    "ML Predictions \u2014 accuracy check (predicted vs actual)": """\
SELECT
    m.as_of,
    m.horizon_days,
    m.expected_return_pct,
    m.regime_signal,
    m.goldbees_close                                            AS close_at_pred,
    argMax(p.close, p.trade_date)                               AS close_at_expiry,
    round((argMax(p.close, p.trade_date) / m.goldbees_close - 1) * 100, 3) AS actual_return_pct,
    round(((argMax(p.close, p.trade_date) / m.goldbees_close - 1) * 100)
          - m.expected_return_pct, 3)                           AS error_pct
FROM market_data.ml_predictions FINAL m
LEFT JOIN market_data.daily_prices FINAL p
    ON p.symbol = 'GOLDBEES'
   AND p.category = 'etfs'
   AND p.trade_date > m.as_of
   AND p.trade_date <= m.as_of + toIntervalDay(m.horizon_days + 3)
GROUP BY m.as_of, m.horizon_days, m.expected_return_pct,
         m.regime_signal, m.goldbees_close
HAVING close_at_expiry > 0
ORDER BY m.as_of DESC""",

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
                round(n.nav,   4)  AS amfi_nav,
                if(n.nav > 0, round((p.close - n.nav) / n.nav * 100, 3), NULL) AS premium_disc_pct
            FROM (
                SELECT trade_date, close
                FROM market_data.daily_prices FINAL
                WHERE symbol = 'GOLDBEES' AND category = 'etfs'
            ) p
            LEFT JOIN (
                SELECT nav_date AS trade_date, nav
                FROM market_data.mf_nav FINAL
                WHERE symbol = 'GOLDBEES'
            ) n USING (trade_date)
            WHERE n.nav > 0
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
                    nullIf(round(n.nav, 4), 0)    AS amfi_nav
                FROM (
                    SELECT trade_date, argMax(close, imported_at) AS market_close
                    FROM market_data.daily_prices
                    WHERE symbol = 'GOLDBEES' AND category = 'etfs'
                      AND trade_date >= toDate('{_gb_cutoff}')
                    GROUP BY trade_date
                ) p
                LEFT JOIN (
                    SELECT nav_date AS trade_date, argMax(nav, imported_at) AS nav
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
                SELECT
                    p.trade_date,
                    round(p.close, 4)                              AS price,
                    nullIf(round(n.nav, 4), 0)                    AS nav,
                    if(n.nav > 0, round((p.close - n.nav) / n.nav * 100, 3), NULL) AS premium_disc_pct
                FROM (
                    SELECT trade_date, close
                    FROM market_data.daily_prices FINAL
                    WHERE symbol = 'GOLDBEES' AND category = 'etfs'
                ) p
                LEFT JOIN (
                    SELECT nav_date AS trade_date, nav
                    FROM market_data.mf_nav FINAL
                    WHERE symbol = 'GOLDBEES'
                ) n USING (trade_date)
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
        st.subheader("📋 CFTC COT — Managed Money Net Positioning (Gold)")
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

                c1, c2, c3, c4 = st.columns(4)
                latest = cot_df.iloc[-1]
                c1.metric("MM Net (last week)",     f"{int(latest['mm_net']):,}")
                c2.metric("MM Net % OI",            f"{latest['mm_net_pct_oi']:+.1f}%")
                c3.metric("Open Interest",          f"{int(latest['open_interest']):,}")
                c4.metric("Commercial Net",         f"{int(latest['comm_net']):,}")

                st.caption("**MM Net % OI > +25%** = crowded long (crash risk)  ·  **< −5%** = extreme short (squeeze fuel)")
                st.line_chart(
                    cot_df[["mm_net", "comm_net"]],
                    height=240,
                    color=["#2196F3", "#FF5722"],
                )

                with st.expander("MM Net % of Open Interest"):
                    st.bar_chart(cot_df["mm_net_pct_oi"], height=200, color="#9C27B0")

                n_weeks = st.slider("Show last N weeks", 26, 260, 104, 26, key="cot_weeks")
                st.dataframe(
                    cot_df[["mm_long", "mm_short", "mm_net", "mm_net_pct_oi",
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
            "Daily FII and DII provisional cash-market net flows (₹ Crore) from NSE India. "
            "Positive = net buying, Negative = net selling."
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
                fii_df = fii_df.sort_values("trade_date")

                # ── KPI metrics ───────────────────────────────────────────────
                latest_row     = fii_df.iloc[-1]
                fii_5d         = fii_df["fii_net_cr"].tail(5).sum()
                dii_5d         = fii_df["dii_net_cr"].tail(5).sum()
                k1, k2, k3, k4 = st.columns(4)
                k1.metric("Latest FII Net",    f"₹{latest_row['fii_net_cr']:+,.0f} Cr")
                k2.metric("Latest DII Net",    f"₹{latest_row['dii_net_cr']:+,.0f} Cr")
                k3.metric("FII 5-Day Cumul.",  f"₹{fii_5d:+,.0f} Cr")
                k4.metric("DII 5-Day Cumul.",  f"₹{dii_5d:+,.0f} Cr")

                # ── Reshape to long form for grouped bars ─────────────────────
                fii_long = fii_df[["trade_date", "fii_net_cr"]].rename(
                    columns={"fii_net_cr": "net_cr"}
                ).assign(investor="FII")
                dii_long = fii_df[["trade_date", "dii_net_cr"]].rename(
                    columns={"dii_net_cr": "net_cr"}
                ).assign(investor="DII")
                long_df = pd.concat([fii_long, dii_long], ignore_index=True)

                # ── Altair grouped bar chart ──────────────────────────────────
                bars = (
                    alt.Chart(long_df)
                    .mark_bar(opacity=0.80)
                    .encode(
                        x=alt.X(
                            "trade_date:T",
                            title="Date",
                            axis=alt.Axis(format="%d %b"),
                        ),
                        y=alt.Y(
                            "net_cr:Q",
                            title="Net Flow (₹ Crore)",
                            scale=alt.Scale(zero=True),
                        ),
                        xOffset=alt.XOffset("investor:N"),
                        color=alt.Color(
                            "investor:N",
                            scale=alt.Scale(
                                domain=["FII", "DII"],
                                range=["#E74C3C", "#3498DB"],
                            ),
                            legend=alt.Legend(title="Investor"),
                        ),
                        tooltip=[
                            alt.Tooltip("trade_date:T", title="Date"),
                            alt.Tooltip("investor:N",   title="Investor"),
                            alt.Tooltip("net_cr:Q",     title="Net (₹ Cr)", format="+,.0f"),
                        ],
                    )
                    .properties(height=300)
                    .interactive()
                )

                zero_line = (
                    alt.Chart(pd.DataFrame({"y": [0]}))
                    .mark_rule(color="#888888", strokeDash=[4, 4], strokeWidth=1)
                    .encode(y="y:Q")
                )

                st.altair_chart(
                    (zero_line + bars).properties(height=300),
                    use_container_width=True,
                )

                with st.expander("📋 Raw data", expanded=False):
                    show_df = fii_df.copy()
                    show_df["trade_date"] = show_df["trade_date"].dt.date
                    show_df = show_df.sort_values("trade_date", ascending=False)
                    show_df.columns = ["Date", "FII Net (₹ Cr)", "DII Net (₹ Cr)"]
                    st.dataframe(show_df, use_container_width=True, hide_index=True)

        except ImportError as exc:
            st.error(
                f"Missing dependency: {exc}  \n"
                "Run: `.venv/bin/pip install altair`  then restart Streamlit."
            )
        except Exception as exc:
            # ClickHouse code 60 = UNKNOWN_TABLE — table not created yet
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
        "**Step 2** Random Forest Residual Z-Score  ·  "
        "**Step 3** Isolation Forest Confidence Multiplier  \n"
        "**Final Z** = Z_robust × (1 + IF_confidence)"
    )

    if not ok:
        st.warning("ClickHouse not connected.")
    else:
        col_cfg, col_res = st.columns([1, 3])

        with col_cfg:
            st.subheader("Settings")

            try:
                _sym_opts = _query_df("""
                    SELECT DISTINCT symbol, category
                    FROM market_data.daily_prices FINAL
                    WHERE category IN ('commodities', 'etfs')
                    ORDER BY category, symbol
                """)
                _sym_labels = [
                    f"{r['symbol']} ({r['category']})"
                    for _, r in _sym_opts.iterrows()
                ]
            except Exception:
                _sym_labels = ["GOLD (commodities)"]

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

            rf_lags = st.slider(
                "RF lag features (days)", min_value=3, max_value=10, value=5,
                help="Number of lagged close prices fed to the Random Forest.",
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

**Step 2 — Random Forest Residual Z-Score**
Train an RF to predict close from lagged prices + MA7 + MA30.
The residual (actual − predicted) isolates the *unexpected* component.
`Z_resid` is the MAD Z-score of those residuals.

| Z_robust | Z_resid | Regime |
|---|---|---|
| High | Low | 📈 Strong trend — HODL |
| Low | High | ⚡ Flash crash / Black Swan — EXIT |
| High | High | 🔥 Volatile breakout |
| Low | Low | ✅ Normal |

**Step 3 — Isolation Forest Confidence Multiplier**
IF `score_samples` is normalised to [0 → 1] (1 = most anomalous).
`Final_Z = Z_robust × (1 + IF_confidence)`
This *boosts* only days suspicious to **both** algorithms.
                """)

            if run_btn:
                try:
                    import altair as alt
                    from src.ml.anomaly import run_composite_anomaly

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

                    with st.spinner(
                        f"Running composite anomaly detection on {iso_sym}  "
                        "(Robust Z → RF residuals → Isolation Forest)…"
                    ):
                        df_if, flagged, r2_train = run_composite_anomaly(
                            raw,
                            rf_lags=rf_lags,
                            contamination=contamination,
                            z_threshold=z_threshold,
                            z_window=z_window,
                        )

                    # ── Summary metrics ───────────────────────────────────────
                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("Rows analysed",             f"{len(df_if):,}")
                    c2.metric(f"|Final Z| > {z_threshold}", len(flagged))
                    c3.metric("Max Final Z",               f"{df_if['final_z_abs'].max():.2f}")
                    c4.metric("RF R² (train 80%)",         f"{r2_train:.3f}")

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
                        "🔵 z_robust (MAD)  ·  🟠 z_resid (RF residual)  ·  "
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

                    # ── Chart 4: RF actual vs predicted ───────────────────────
                    st.subheader("Random Forest — Actual vs Predicted Close")
                    rf_melt = df_if[["trade_date", "close", "rf_pred"]].melt(
                        id_vars="trade_date", var_name="series", value_name="price"
                    )
                    rf_chart = alt.Chart(rf_melt).mark_line(opacity=0.75).encode(
                        x=alt.X("trade_date:T", title="Date"),
                        y=alt.Y("price:Q",      title="Price"),
                        color=alt.Color("series:N", scale=alt.Scale(
                            domain=["close", "rf_pred"],
                            range=["#4C78A8", "#72B7B2"],
                        )),
                        tooltip=["trade_date:T", "series:N",
                                 alt.Tooltip("price:Q", format=".2f")],
                    )
                    st.altair_chart(
                        rf_chart.interactive().properties(height=200),
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
        "Learns **soft thresholds** from all 4 signals jointly via walk-forward "
        "cross-validation. Unlike the expert system (hard IF/THEN rules), the model "
        "discovers that e.g. 15% COT crowding is dangerous *when* the GOLDBEES "
        "discount is also widening simultaneously.  \n"
        "**Target:** `(price[t+5] / price[t] − 1) × 100`  ·  "
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

                # Key metrics
                mc1, mc2, mc3, mc4, mc5 = st.columns(5)
                mc1.metric(
                    f"Expected {ml['horizon_days']}-Day Return",
                    f"{ml['expected_return_pct']:+.2f}%",
                )
                mc2.metric(
                    "Confidence Band",
                    f"[{ml['confidence_low']:+.1f}%, {ml['confidence_high']:+.1f}%]",
                )
                mc3.metric("CV R² Mean", f"{ml['cv_r2_mean']:.4f}",
                           help=">0.05 = useful predictive power. Negative = worse than mean baseline.")
                mc4.metric("Hit Ratio",
                           f"{ml.get('cv_hit_ratio_mean', 0)*100:.1f}%",
                           help="Directional accuracy. >52% = statistical edge, >55% = strong edge.")
                mc5.metric("Training Rows", f"{ml['n_training_rows']:,}")

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

                # Walk-forward CV R² per fold
                with st.expander("Walk-Forward CV R² per fold"):
                    folds_df = pd.DataFrame({
                        "fold": [f"Fold {i+1}" for i in range(len(ml["cv_r2_scores"]))],
                        "r2":   ml["cv_r2_scores"],
                    }).set_index("fold")
                    st.bar_chart(folds_df["r2"], height=160, color="#9C27B0")
                    st.caption(
                        "Each fold trains on earlier data only and tests on later data. "
                        "R² > 0 = model has out-of-sample predictive power. "
                        "Negative R² = that fold was noisier than the mean baseline."
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
        "QUANT_MULTI_ASSET": "Quant Multi Asset",
        "ICICI_MULTI_ASSET": "ICICI Pru Multi Asset",
    }
    _ASSET_COLORS = {
        "equity": "#1976D2",
        "gold":   "#FFA726",
        "bond":   "#43A047",
        "cash":   "#90A4AE",
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

    # ── Available months ───────────────────────────────────────────────────
    _months_df = _query_df(
        "SELECT DISTINCT as_of_month FROM market_data.mf_holdings FINAL ORDER BY as_of_month DESC"
    )
    _available_months = list(_months_df.iloc[:, 0]) if not _months_df.empty else []
    if not _available_months:
        st.warning("Holdings table exists but has no rows.")
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
          AND as_of_month = '{selected_month}'
        ORDER BY fund_name, pct_of_nav DESC
        """
    )
    if _hold_df.empty:
        st.warning(f"No holdings for selected funds in {selected_month}.")
        st.stop()

    _hold_df.columns = ["scheme_code", "fund_name", "isin", "security_name",
                        "asset_type", "market_value_cr", "pct_of_nav"]
    _hold_df["fund_label"] = _hold_df["fund_name"].map(_FUND_LABELS).fillna(_hold_df["fund_name"])

    # ══ 1. Asset allocation pie per fund ══════════════════════════════════
    st.subheader("Asset Allocation")
    pie_cols = st.columns(len(selected_funds))
    for i, fund_key in enumerate(selected_funds):
        with pie_cols[i]:
            _fd = _hold_df[_hold_df["fund_name"] == fund_key]
            _alloc = _fd.groupby("asset_type")["pct_of_nav"].sum().reset_index()
            _alloc.columns = ["asset_type", "weight"]
            if not _alloc.empty:
                import plotly.express as px  # type: ignore[import]
                fig_pie = px.pie(
                    _alloc,
                    values="weight",
                    names="asset_type",
                    title=_FUND_LABELS[fund_key],
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
    _disp_df = _hold_df[["fund_label", "security_name", "asset_type", "pct_of_nav", "isin"]].rename(columns={
        "fund_label":    "Fund",
        "security_name": "Security",
        "asset_type":    "Type",
        "pct_of_nav":    "Weight (%)",
        "isin":          "ISIN",
    })
    st.dataframe(
        _disp_df.style.format({"Weight (%)": "{:.2f}"}),
        width="stretch",
        height=420,
    )

    # ══ 3. Month-over-month drift ══════════════════════════════════════════
    if len(_available_months) >= 2:
        st.subheader("Month-over-Month Drift")
        _prev_month = _available_months[1] if _available_months[0] == selected_month else None
        if _prev_month:
            _drift_df = _query_df(
                f"""
                WITH cur AS (
                    SELECT fund_name, isin, security_name, asset_type, pct_of_nav
                    FROM market_data.mf_holdings FINAL
                    WHERE fund_name IN ({_fund_filter}) AND as_of_month = '{selected_month}'
                ),
                prev AS (
                    SELECT fund_name, isin, security_name, pct_of_nav
                    FROM market_data.mf_holdings FINAL
                    WHERE fund_name IN ({_fund_filter}) AND as_of_month = '{_prev_month}'
                )
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
                        WHEN abs(cur.pct_of_nav - prev.pct_of_nav) >= 2  THEN 'CHANGED'
                        ELSE 'UNCHANGED'
                    END AS event
                FROM cur
                FULL OUTER JOIN prev
                    ON cur.fund_name = prev.fund_name AND cur.isin = prev.isin
                WHERE event != 'UNCHANGED'
                ORDER BY fund_name, event, abs(drift) DESC
                """
            )
            if _drift_df.empty:
                st.success("No significant changes vs prior month.")
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
        else:
            st.info("Select the latest available month to compare with the prior month.")

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
            fig_trend = px.line(
                _trend_df,
                x="month",
                y="weight",
                color="asset_type",
                facet_col="fund_label",
                facet_col_wrap=len(selected_funds),
                color_discrete_map=_ASSET_COLORS,
                labels={"month": "", "weight": "Weight (%)", "asset_type": "Type"},
                height=320,
            )
            fig_trend.update_layout(margin=dict(t=30, b=0))
            st.plotly_chart(fig_trend, width="stretch")


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
        lookback_days = st.slider("Lookback window (days)", 7, 90, 30, key="etfscan_lookback")
        z_threshold   = st.slider("Z-score threshold", 0.5, 3.0, 1.5, step=0.25, key="etfscan_z")
        custom_syms   = st.text_input(
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
                results = scan_domestic_etfs(
                    ch_client=_get_client(),
                    symbols=sym_list,
                    lookback_days=lookback_days,
                    z_high=z_threshold,
                    z_low=-z_threshold,
                    z_mild_high=z_threshold - 0.5,
                    z_mild_low=-(z_threshold - 0.5),
                    min_snapshots=5,
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
                import clickhouse_connect as _cc_pa
                from src.tools.premium_alerts import check_premium_alerts, INTL_ETF_SYMBOLS

                _pa_client = _cc_pa.get_client(
                    host=CH_HOST, port=CH_PORT,
                    username=CH_USER, password=CH_PASS,
                    connect_timeout=10,
                )
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

                _sc1, _sc2, _sc3 = st.columns(3)
                _sc1.metric("🟢 SCREAMING BUY", len(_pa_buy))
                _sc2.metric("🟡 GOOD ENTRY",    len(_pa_entry))
                _sc3.metric("🔴 NO ACTION",      len(_pa_noact))

                # ── Z-score bar chart ─────────────────────────────────────────
                import plotly.graph_objects as _go_pa

                _valid = [r for r in _pa_results if r["z_score"] is not None]
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

                # ── Premium level chart (latest vs mean) ──────────────────────
                if _valid:
                    _fig_prem = _go_pa.Figure()
                    _fig_prem.add_trace(_go_pa.Bar(
                        name=f"{_pa_lookback}d Avg Premium",
                        x=[r["symbol"]       for r in _valid],
                        y=[r["mean_premium"] for r in _valid],
                        marker_color="#90A4AE",
                        opacity=0.6,
                    ))
                    _fig_prem.add_trace(_go_pa.Scatter(
                        name="Latest Premium",
                        x=[r["symbol"]         for r in _valid],
                        y=[r["latest_premium"] for r in _valid],
                        mode="markers",
                        marker=dict(size=14, color=_bar_colors, symbol="diamond"),
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
