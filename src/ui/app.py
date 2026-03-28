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

tab_import, tab_query, tab_explorer, tab_anomaly, tab_wis = st.tabs(
    ["📥 Import Data", "🔍 SQL Query", "📊 Explorer", "🔬 Anomaly Detection", "🕵️ Who Is Selling?"]
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
                    "cot", "cb_reserves", "etf_aum", "fx_rates"]
        CATEGORY_HELP = {
            "stocks":      "50 NSE large/mid-caps (Yahoo Finance)",
            "etfs":        "15 NSE ETFs — OHLCV (Yahoo Finance)",
            "commodities": "Gold, Silver, Oil futures (Yahoo Finance)",
            "indices":     "Nifty50, Sensex, S&P500, etc. (Yahoo Finance)",
            "mf":          "ETF NAV history from MFAPI.in (AMFI official)",
            "inav":        "Live iNAV snapshot from NSE API",
            "cot":         "CFTC COT Gold — hedge fund & commercial positioning (weekly)",
            "cb_reserves": "Central bank gold reserves — 9 countries via IMF IFS (monthly)",
            "etf_aum":     "Gold ETF AUM — GLD, IAU, SGOL, PHYS + implied tonnes (daily)",
            "fx_rates":    "USD FX rates — INR, CNY, AED, SAR, KWD daily OHLC (Yahoo Finance)",
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

        st.divider()
        run_btn = st.button(
            "▶  Start Import",
            type="primary",
            disabled=not ok or len(selected_cats) == 0,
            use_container_width=True,
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
    round(p.close, 4)                              AS market_close,
    round(n.nav,   4)                              AS amfi_nav,
    round((p.close - n.nav) / n.nav * 100, 3)     AS premium_disc_pct
FROM (
    SELECT trade_date, close
    FROM market_data.daily_prices FINAL
    WHERE symbol = 'GOLDBEES' AND category = 'etfs'
) p
JOIN (
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
            st.dataframe(df, use_container_width=True, height=400)

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

    # ── 2. GOLDBEES market price vs NAV ──────────────────────────────────────
    with st.container():
        st.subheader("📊 GOLDBEES — Market Close vs AMFI NAV (₹)")
        try:
            gbdf = _query_df("""
                SELECT
                    p.trade_date,
                    round(p.close, 4) AS market_close,
                    round(n.nav,   4) AS amfi_nav
                FROM (
                    SELECT trade_date, close
                    FROM market_data.daily_prices FINAL
                    WHERE symbol = 'GOLDBEES' AND category = 'etfs'
                ) p
                JOIN (
                    SELECT nav_date AS trade_date, nav
                    FROM market_data.mf_nav FINAL
                    WHERE symbol = 'GOLDBEES'
                ) n USING (trade_date)
                ORDER BY trade_date ASC
            """)
            if gbdf.empty:
                st.info("No GOLDBEES data. Run **Import → etfs + mf**.")
            else:
                gbdf["trade_date"] = pd.to_datetime(gbdf["trade_date"])
                gbdf = gbdf.set_index("trade_date")
                st.line_chart(gbdf[["market_close", "amfi_nav"]], height=280)
                st.caption(
                    "**market_close** = NSE last traded price (Yahoo Finance)  "
                    "·  **amfi_nav** = AMFI official NAV (MFAPI.in)"
                )
        except Exception as exc:
            st.error(f"GOLDBEES chart: {exc}")

    st.divider()

    # ── 3. Premium / Discount ─────────────────────────────────────────────────
    with st.container():
        st.subheader("↕ GOLDBEES — Premium / Discount to NAV (%)")
        try:
            pddf = _query_df("""
                SELECT
                    p.trade_date,
                    round((p.close - n.nav) / n.nav * 100, 3) AS premium_disc_pct
                FROM (
                    SELECT trade_date, close
                    FROM market_data.daily_prices FINAL
                    WHERE symbol = 'GOLDBEES' AND category = 'etfs'
                ) p
                JOIN (
                    SELECT nav_date AS trade_date, nav
                    FROM market_data.mf_nav FINAL
                    WHERE symbol = 'GOLDBEES'
                ) n USING (trade_date)
                ORDER BY trade_date ASC
            """)
            if not pddf.empty:
                pddf["trade_date"] = pd.to_datetime(pddf["trade_date"])
                pddf = pddf.set_index("trade_date")
                st.bar_chart(pddf["premium_disc_pct"], height=240, color="#00B4D8")
                st.caption(
                    "**+** = ETF trading at premium (market > NAV)  "
                    "·  **−** = ETF trading at discount (market < NAV)  "
                    "·  Bands: ±0.25% = FAIR VALUE"
                )
                avg = pddf["premium_disc_pct"].mean()
                mx  = pddf["premium_disc_pct"].max()
                mn  = pddf["premium_disc_pct"].min()
                d1, d2, d3, d4 = st.columns(4)
                d1.metric("Avg",         f"{avg:+.3f}%")
                d2.metric("Max Premium", f"{mx:+.3f}%")
                d3.metric("Max Discount", f"{mn:+.3f}%")
                days_disc = int((pddf["premium_disc_pct"] < -0.25).sum())
                d4.metric("Days at Discount", days_disc)
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
                    use_container_width=True,
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
                    use_container_width=True,
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
                        use_container_width=True,
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
                        use_container_width=True,
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
                                use_container_width=True, hide_index=True,
                            )
        except Exception as exc:
            st.error(f"FX chart: {exc}")


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
                "▶ Run Analysis", type="primary", use_container_width=True
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
                        use_container_width=True,
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
                        use_container_width=True,
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
                        use_container_width=True,
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
                        use_container_width=True,
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
                    st.dataframe(top_tbl, use_container_width=True)

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
        run_wis = st.button("🔍 Analyse Now", type="primary", use_container_width=True)
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
                           use_container_width=True, key="ml_btn")
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
                mc1, mc2, mc3, mc4 = st.columns(4)
                mc1.metric(
                    f"Expected {ml['horizon_days']}-Day Return",
                    f"{ml['expected_return_pct']:+.2f}%",
                )
                mc2.metric(
                    "Confidence Band",
                    f"[{ml['confidence_low']:+.1f}%, {ml['confidence_high']:+.1f}%]",
                )
                mc3.metric("CV R² Mean", f"{ml['cv_r2_mean']:.4f}")
                mc4.metric("Training Rows", f"{ml['n_training_rows']:,}")

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
