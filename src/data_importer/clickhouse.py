"""
src/importer/clickhouse.py
──────────────────────────
ClickHouse client wrapper for the historical data importer.

Tables (all in `market_data` database):
  daily_prices      — OHLCV for stocks, ETFs, commodities, indices
  mf_nav            — Daily NAV for mutual funds / ETFs from MFAPI.in
  import_watermarks — Last successfully imported date per (source, symbol)

Design:
  • ReplacingMergeTree — idempotent inserts; re-importing the same date
    is safe and simply replaces the existing row.
  • Watermark-based delta sync — only fetch data after last watermark date,
    with a configurable overlap window (default 3 days) to catch late-arriving
    corrections.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta
from typing import Any

import clickhouse_connect

logger = logging.getLogger(__name__)

# ── DDL ───────────────────────────────────────────────────────────────────────

_DDL_DATABASE = "CREATE DATABASE IF NOT EXISTS market_data"

_DDL_DAILY_PRICES = """
CREATE TABLE IF NOT EXISTS market_data.daily_prices (
    symbol       String,
    category     String,       -- stocks | etfs | commodities | indices
    trade_date   Date,
    open         Float64,
    high         Float64,
    low          Float64,
    close        Float64,
    volume       Float64,
    imported_at  DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(imported_at)
PARTITION BY toYYYYMM(trade_date)
ORDER BY (symbol, trade_date)
"""

_DDL_MF_NAV = """
CREATE TABLE IF NOT EXISTS market_data.mf_nav (
    symbol       String,
    scheme_code  String,
    nav_date     Date,
    nav          Float64,
    imported_at  DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(imported_at)
PARTITION BY toYYYYMM(nav_date)
ORDER BY (symbol, nav_date)
"""

_DDL_WATERMARKS = """
CREATE TABLE IF NOT EXISTS market_data.import_watermarks (
    source       String,       -- yfinance | mfapi
    symbol       String,
    dataset      String DEFAULT 'prices',  -- prices | earnings | insider | valuation | nav | holdings
    last_date    Date,
    updated_at   DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (source, symbol, dataset)
"""

_DDL_IMPORT_FAILURES = """
CREATE TABLE IF NOT EXISTS market_data.import_failures (
    failed_at    DateTime DEFAULT now(),
    source       LowCardinality(String),
    dataset      LowCardinality(String),
    symbol       String,
    from_date    Date,
    to_date      Date,
    error_class  LowCardinality(String),
    error_msg    String,
    retry_count  UInt8 DEFAULT 0,
    _ver         DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(_ver)
ORDER BY (source, dataset, symbol, failed_at)
"""

_DDL_AGENT_TRACES = """
CREATE TABLE IF NOT EXISTS market_data.agent_traces (
    created_at   DateTime DEFAULT now(),
    run_id       String,
    agent        LowCardinality(String),
    step_idx     UInt16 DEFAULT 0,
    tool_name    LowCardinality(String) DEFAULT '',
    args_json    String DEFAULT '{}',
    result_json  String DEFAULT '',
    latency_ms   UInt32 DEFAULT 0,
    tokens_in    UInt32 DEFAULT 0,
    tokens_out   UInt32 DEFAULT 0,
    status       LowCardinality(String) DEFAULT 'ok',
    error_class  LowCardinality(String) DEFAULT '',
    error_msg    String DEFAULT '',
    parent_run_id String DEFAULT '',
    _ver         DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(_ver)
PARTITION BY toYYYYMM(created_at)
ORDER BY (agent, run_id, step_idx)
"""

_DDL_AGENT_PREFERENCES = """
CREATE TABLE IF NOT EXISTS market_data.agent_preferences (
    preference_key String,
    value          String,
    updated_at     DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY preference_key
"""

# ── Idempotent column migrations (run on every startup) ───────────────────────
# These ALTER statements are safe to run repeatedly — IF NOT EXISTS / IF EXISTS
# guards prevent errors when the column already exists.

_ALTER_LOWCARDINALITY = [
    # daily_prices
    "ALTER TABLE market_data.daily_prices MODIFY COLUMN IF EXISTS category LowCardinality(String)",
    # fx_rates / etf_aum / cot_gold / cb_gold_reserves
    "ALTER TABLE market_data.fx_rates MODIFY COLUMN IF EXISTS source LowCardinality(String)",
    "ALTER TABLE market_data.etf_aum MODIFY COLUMN IF EXISTS source LowCardinality(String)",
    "ALTER TABLE market_data.cot_gold MODIFY COLUMN IF EXISTS source LowCardinality(String)",
    "ALTER TABLE market_data.cb_gold_reserves MODIFY COLUMN IF EXISTS source LowCardinality(String)",
    "ALTER TABLE market_data.macro_indicators MODIFY COLUMN IF EXISTS source LowCardinality(String)",
    # inav_snapshots
    "ALTER TABLE market_data.inav_snapshots MODIFY COLUMN IF EXISTS source LowCardinality(String)",
    # ml_predictions
    "ALTER TABLE market_data.ml_predictions MODIFY COLUMN IF EXISTS regime_signal LowCardinality(String)",
    # mf_holdings
    "ALTER TABLE market_data.mf_holdings MODIFY COLUMN IF EXISTS asset_type LowCardinality(String)",
    # news_articles
    "ALTER TABLE market_data.news_articles MODIFY COLUMN IF EXISTS source_type LowCardinality(String)",
    "ALTER TABLE market_data.news_articles MODIFY COLUMN IF EXISTS fetch_source LowCardinality(String)",
    "ALTER TABLE market_data.news_articles MODIFY COLUMN IF EXISTS category LowCardinality(String)",
    "ALTER TABLE market_data.news_articles MODIFY COLUMN IF EXISTS sentiment LowCardinality(String)",
    "ALTER TABLE market_data.news_articles MODIFY COLUMN IF EXISTS impact_tier LowCardinality(String)",
    "ALTER TABLE market_data.news_articles MODIFY COLUMN IF EXISTS source LowCardinality(String)",
    # weight_checkpoints
    "ALTER TABLE market_data.weight_checkpoints MODIFY COLUMN IF EXISTS method LowCardinality(String)",
    "ALTER TABLE market_data.weight_checkpoints MODIFY COLUMN IF EXISTS regime LowCardinality(String)",
    # signal_composite
    "ALTER TABLE market_data.signal_composite MODIFY COLUMN IF EXISTS anomaly_flag LowCardinality(String)",
    "ALTER TABLE market_data.signal_composite MODIFY COLUMN IF EXISTS action LowCardinality(String)",
    # import_watermarks
    "ALTER TABLE market_data.import_watermarks MODIFY COLUMN IF EXISTS source LowCardinality(String)",
    # add dataset column to import_watermarks for fine-grained watermark tracking
    "ALTER TABLE market_data.import_watermarks ADD COLUMN IF NOT EXISTS dataset LowCardinality(String) DEFAULT 'prices'",
]

_DDL_COT_GOLD = """
CREATE TABLE IF NOT EXISTS market_data.cot_gold (
    report_date     Date,
    mm_long         Int64,
    mm_short        Int64,
    mm_spread       Int64,
    mm_net          Int64,
    comm_long       Int64,
    comm_short      Int64,
    comm_net        Int64,
    open_interest   Int64,
    source          String DEFAULT 'cftc_disaggregated',
    _ver            DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(_ver)
ORDER BY (report_date)
"""

_DDL_CB_GOLD_RESERVES = """
CREATE TABLE IF NOT EXISTS market_data.cb_gold_reserves (
    ref_period      Date,
    country_code    String,
    country_name    String,
    reserves_tonnes Float64,
    source          String DEFAULT 'imf_ifs',
    _ver            DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(_ver)
ORDER BY (ref_period, country_code)
"""

_DDL_ETF_AUM = """
CREATE TABLE IF NOT EXISTS market_data.etf_aum (
    trade_date      Date,
    symbol          String,
    aum_usd         Float64,
    price           Float64,
    implied_tonnes  Float64,
    source          String DEFAULT 'yfinance',
    _ver            DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(_ver)
ORDER BY (trade_date, symbol)
"""

_DDL_FX_RATES = """
CREATE TABLE IF NOT EXISTS market_data.fx_rates (
    trade_date   Date,
    symbol       String,       -- e.g. USDINR, USDCNY, USDAED, USDSAR, USDKWD
    open         Float64,
    high         Float64,
    low          Float64,
    close        Float64,
    source       String DEFAULT 'yfinance',
    imported_at  DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(imported_at)
PARTITION BY toYYYYMM(trade_date)
ORDER BY (symbol, trade_date)
"""

_DDL_NSE_DELIVERY = """
CREATE TABLE IF NOT EXISTS market_data.nse_delivery (
    trade_date    Date,
    symbol        String,
    series        LowCardinality(String) DEFAULT 'EQ',
    prev_close    Float64,
    open_price    Float64,
    high_price    Float64,
    low_price     Float64,
    last_price    Float64,
    close_price   Float64,
    avg_price     Float64,
    ttl_trd_qty   UInt64,
    turnover_lacs Float64,
    no_of_trades  UInt64,
    deliv_qty     Nullable(UInt64),   -- NULL where NSE doesn't publish delivery (e.g. some debt series)
    deliv_per     Nullable(Float64),  -- NULL, not 0 — distinguishes "not published" from "0% delivered"
    source        LowCardinality(String) DEFAULT 'nse',
    imported_at   DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(imported_at)
PARTITION BY toYYYYMM(trade_date)
ORDER BY (symbol, trade_date, series)
"""

_DDL_INAV_SNAPSHOTS = """
CREATE TABLE IF NOT EXISTS market_data.inav_snapshots (
    symbol                String,
    snapshot_at           DateTime,    -- UTC timestamp of the fetch
    inav                  Float64,     -- Indicative NAV from NSE (₹)
    market_price          Float64,     -- Last traded price from NSE (₹)
    premium_discount_pct  Float64,     -- (market_price - inav) / inav * 100
    source                String       -- NSE | Yahoo
)
ENGINE = ReplacingMergeTree(snapshot_at)
PARTITION BY toYYYYMM(snapshot_at)
ORDER BY (symbol, snapshot_at)
"""

_DDL_ML_PREDICTIONS = """
CREATE TABLE IF NOT EXISTS market_data.ml_predictions (
    as_of                Date,
    horizon_days         UInt8,
    expected_return_pct  Float64,
    confidence_low       Float64,
    confidence_high      Float64,
    regime_signal        String,
    cv_r2_mean           Float64,
    n_training_rows      UInt32,
    goldbees_close       Float64,
    created_at           DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY (as_of, horizon_days)
"""

_DDL_MF_HOLDINGS = """
CREATE TABLE IF NOT EXISTS market_data.mf_holdings (
    scheme_code    String,
    fund_name      String,
    as_of_month    Date,
    isin           String,
    security_name  String,
    asset_type     String,       -- equity | gold | bond | cash | other
    market_value_cr Float64,
    pct_of_nav     Float64,
    imported_at    DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(imported_at)
PARTITION BY toYYYYMM(as_of_month)
ORDER BY (scheme_code, as_of_month, isin)
"""

_DDL_MF_HOLDING_SUMMARIES = """
CREATE TABLE IF NOT EXISTS market_data.mf_holding_summaries (
    isin               String,
    security_name      String,
    as_of_month        Date,
    active_funds_count UInt16,
    total_market_val_cr Float64,
    updated_at         DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
PRIMARY KEY (isin, as_of_month)
ORDER BY (isin, as_of_month)
"""

_DDL_MV_MF_HOLDING_SUMMARIES = """
CREATE MATERIALIZED VIEW IF NOT EXISTS market_data.mv_mf_holding_summaries
TO market_data.mf_holding_summaries AS
SELECT 
    isin,
    any(security_name) as security_name,
    as_of_month,
    count(DISTINCT fund_name) as active_funds_count,
    sum(market_value_cr) as total_market_val_cr,
    max(imported_at) as updated_at
FROM market_data.mf_holdings
GROUP BY isin, as_of_month
"""

_DDL_FII_DII_FLOWS = """
CREATE TABLE IF NOT EXISTS market_data.fii_dii_flows (
    trade_date         Date,
    fii_gross_buy_cr   Float64,   -- FII gross purchases (Rs Crore, cash segment)
    fii_gross_sell_cr  Float64,   -- FII gross sales (Rs Crore, cash segment)
    fii_net_cr         Float64,   -- FII net flow = buy -- sell (Rs Crore)
    dii_gross_buy_cr   Float64,   -- DII gross purchases (Rs Crore, cash segment)
    dii_gross_sell_cr  Float64,   -- DII gross sales (Rs Crore, cash segment)
    dii_net_cr         Float64,   -- DII net flow = buy -- sell (Rs Crore)
    imported_at        DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(imported_at)
ORDER BY (trade_date)
"""

_DDL_AMFI_CATEGORY_FLOWS = """
CREATE TABLE IF NOT EXISTS market_data.amfi_category_flows (
    report_month        Date,                        -- First day of month (YYYY-MM-01)
    category_name       String,                      -- Verbatim from AMFI: "Large Cap Fund", etc.
    subcategory_group   LowCardinality(String),      -- Normalised: Equity | Debt | Hybrid | Passive | FoF | Solution | Other
    gross_purchase_cr   Float64,                     -- Gross purchases (₹ Crore)
    gross_redemption_cr Float64,                     -- Gross redemptions (₹ Crore)
    net_flow_cr         Float64,                     -- gross_purchase - gross_redemption
    closing_aum_cr      Float64,                     -- Closing AUM (₹ Crore)
    flow_pct_of_aum     Float64,                     -- net_flow_cr / closing_aum_cr * 100
    imported_at         DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(imported_at)
ORDER BY (report_month, category_name)
"""

_DDL_FII_DII_MONTHLY = """
CREATE TABLE IF NOT EXISTS market_data.fii_dii_monthly (
    month_date         Date,        -- First day of month (YYYY-MM-01)
    fii_buy_cr         Float64,     -- FII gross purchases that month (Rs Crore)
    fii_sell_cr        Float64,     -- FII gross sales that month (Rs Crore)
    fii_net_cr         Float64,     -- FII net = buy - sell (Rs Crore)
    dii_buy_cr         Float64,     -- DII gross purchases that month (Rs Crore)
    dii_sell_cr        Float64,     -- DII gross sales that month (Rs Crore)
    dii_net_cr         Float64,     -- DII net = buy - sell (Rs Crore)
    nifty_close        Float64,     -- Nifty 50 month-end close
    nifty_change_pct   Float64,     -- Nifty % change that month
    imported_at        DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(imported_at)
ORDER BY (month_date)
"""

_DDL_FII_DII_FNO_DAILY = """
CREATE TABLE IF NOT EXISTS market_data.fii_dii_fno_daily (
    trade_date                    Date,
    -- Index futures (quantity-wise net OI change)
    fii_fut_net_oi                Float64,  -- FII index futures net OI change
    fii_fut_outstanding_oi        Float64,  -- FII index futures outstanding OI
    fii_fut_nifty_net_oi          Float64,  -- FII Nifty futures net OI change
    fii_fut_banknifty_net_oi      Float64,  -- FII BankNifty futures net OI change
    dii_fut_net_oi                Float64,
    dii_fut_outstanding_oi        Float64,
    pro_fut_net_oi                Float64,
    pro_fut_outstanding_oi        Float64,
    client_fut_net_oi             Float64,
    client_fut_outstanding_oi     Float64,
    -- Stock futures
    fii_fut_stock_net_oi          Float64,
    dii_fut_stock_net_oi          Float64,
    pro_fut_stock_net_oi          Float64,
    client_fut_stock_net_oi       Float64,
    -- Options (overall net OI)
    fii_opt_overall_net_oi        Float64,  -- FII overall options net OI
    fii_opt_overall_net_oi_change Float64,  -- FII options OI change (day)
    fii_opt_call_net_oi           Float64,
    fii_opt_put_net_oi            Float64,
    dii_opt_overall_net_oi        Float64,
    dii_opt_overall_net_oi_change Float64,
    pro_opt_overall_net_oi        Float64,
    pro_opt_overall_net_oi_change Float64,
    client_opt_overall_net_oi     Float64,
    client_opt_overall_net_oi_change Float64,
    -- Index context
    nifty_close                   Float64,
    banknifty_close               Float64,
    nifty_change_pct              Float64,
    banknifty_change_pct          Float64,
    imported_at                   DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(imported_at)
ORDER BY (trade_date)
"""

_DDL_SIGNAL_COMPOSITE = """
CREATE TABLE IF NOT EXISTS market_data.signal_composite (
    as_of               Date,
    etf_symbol          String,
    macro_score         Float32,
    sentiment_score     Float32,
    valuation_score     Float32,
    flow_score          Float32,
    ml_score            Float32,
    anomaly_flag        String,
    composite_score     Float32,
    action              String,
    rationale           String,
    imported_at         DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(imported_at)
ORDER BY (as_of, etf_symbol)
"""

_DDL_NEWS_ARTICLES = """
CREATE TABLE IF NOT EXISTS market_data.news_articles (
    fetched_at      DateTime,          -- when we scraped it
    published_at    String,            -- raw publish string from source
    source_type     String,            -- 'etf_news' | 'macro_event'
    fetch_source    String DEFAULT '', -- 'gnews' | 'yfinance' | 'newsapi'
    category        String,            -- e.g. 'Gold ETFs', 'Geopolitical / War'
    etfs_impacted   String,            -- comma-separated ETF symbols
    sentiment       String,            -- POSITIVE | NEGATIVE | NEUTRAL
    impact_tier     String,            -- HIGH | MEDIUM | LOW (etf_news) or conviction (macro)
    title           String,
    source          String,            -- publisher name
    url             String,
    imported_at     DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(imported_at)
ORDER BY (fetched_at, source_type, category, title)
"""

_DDL_NEWS_ARTICLES_MIGRATE = """
ALTER TABLE market_data.news_articles
    ADD COLUMN IF NOT EXISTS fetch_source String DEFAULT ''
"""

_DDL_LIVE_ALERTS_MIGRATE = """
ALTER TABLE market_data.live_alerts
    ADD COLUMN IF NOT EXISTS delivered_to_whatsapp UInt8 DEFAULT 0
"""

_DDL_WEIGHT_CHECKPOINTS = """
CREATE TABLE IF NOT EXISTS market_data.weight_checkpoints (
    as_of                Date,
    symbol               String,
    method               String,             -- 'rg' | 'kelly' | 'blended_50' | 'blended_30'
    recommended_weight   Float32,
    expected_return_pct  Nullable(Float32),  -- NULL for pure RG method
    expected_vol_pct     Nullable(Float32),  -- annualised implied vol, NULL for pure RG
    garch_vol_pct        Nullable(Float32),
    regime               String,
    composite_score      Nullable(Float32),
    price_below_ema50    UInt8,              -- 1 = below EMA50 (trend filter active)
    cv_r2                Nullable(Float32),  -- LightGBM walk-forward CV R²
    horizon_days         UInt8 DEFAULT 5,
    rationale            String,
    created_at           DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY (as_of, symbol, method)
"""

_DDL_PIPELINE_MANIFEST = """
CREATE TABLE IF NOT EXISTS market_data.pipeline_manifest (
    stage            LowCardinality(String),
    symbol           String,
    input_fingerprint String,
    code_version     String,
    computed_at      DateTime DEFAULT now(),
    input_details    String,
    duration_ms      UInt32 DEFAULT 0,
    status           LowCardinality(String) DEFAULT 'success'
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (stage, symbol)
TTL computed_at + INTERVAL 90 DAY
"""

_DDL_STOCK_EARNINGS = """
CREATE TABLE IF NOT EXISTS market_data.stock_earnings (
    symbol          String,
    earnings_date   Date,
    eps_estimate    Float64,
    eps_actual      Float64,
    surprise_pct    Float64,
    imported_at     DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(imported_at)
ORDER BY (symbol, earnings_date)
"""

_DDL_STOCK_INSIDER = """
CREATE TABLE IF NOT EXISTS market_data.stock_insider_trades (
    symbol              String,
    insider_name        String,
    relation            String,
    transaction_date    Date,
    transaction_type    String,
    shares              Float64,
    value               Float64,
    imported_at         DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(imported_at)
ORDER BY (symbol, transaction_date, insider_name)
"""

_DDL_STOCK_VALUATION = """
CREATE TABLE IF NOT EXISTS market_data.stock_valuation (
    symbol              String,
    snapshot_date       Date,
    market_cap          Float64,
    trailing_pe         Float64,
    forward_pe          Float64,
    peg_ratio           Float64,
    price_to_book       Float64,
    price_to_sales      Float64,
    debt_to_equity      Float64,
    return_on_equity    Float64,
    profit_margin       Float64,
    operating_margin    Float64,
    gross_margin        Float64,
    revenue_growth      Float64,
    earnings_growth     Float64,
    free_cashflow       Float64,
    beta                Float64,
    target_price        Float64,
    recommendation      String,
    analyst_count       UInt32,
    imported_at         DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(imported_at)
ORDER BY (symbol, snapshot_date)
"""

_DDL_USER_HOLDINGS = """
CREATE TABLE IF NOT EXISTS market_data.user_holdings (
    tradingsymbol         String,
    exchange              String,
    isin                  String,
    quantity              Float64,
    average_price         Float64,
    last_price            Float64,
    pnl                   Float64,
    day_change            Float64,
    day_change_percentage Float64,
    imported_at           DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(imported_at)
ORDER BY (tradingsymbol, imported_at)
"""

_DDL_USER_PROFILE = """
CREATE TABLE IF NOT EXISTS market_data.user_profile (
    user_id          String,
    user_name        String,
    email            String,
    broker           String,
    user_type        String,
    exchanges        Array(String),
    imported_at      DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(imported_at)
ORDER BY (user_id, imported_at)
"""

_DDL_USER_MARGINS = """
CREATE TABLE IF NOT EXISTS market_data.user_margins (
    segment          String,       -- equity | commodity
    cash             Float64,
    available_balance Float64,
    utilised_debits  Float64,
    utilised_m2m     Float64,
    utilised_holding_sales Float64,
    imported_at      DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(imported_at)
ORDER BY (segment, imported_at)
"""

_DDL_USER_POSITIONS = """
CREATE TABLE IF NOT EXISTS market_data.user_positions (
    tradingsymbol    String,
    exchange         String,
    instrument_token UInt32,
    product          String,
    quantity         Int64,
    average_price    Float64,
    last_price       Float64,
    pnl              Float64,
    imported_at      DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(imported_at)
ORDER BY (tradingsymbol, imported_at)
"""

_DDL_USER_ORDERS = """
CREATE TABLE IF NOT EXISTS market_data.user_orders (
    order_id         String,
    parent_order_id  String,
    status           String,
    tradingsymbol    String,
    exchange         String,
    transaction_type String,
    order_type       String,
    quantity         Int64,
    filled_quantity  Int64,
    pending_quantity Int64,
    price            Float64,
    average_price    Float64,
    order_timestamp  DateTime,
    imported_at      DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(imported_at)
ORDER BY (order_id, imported_at)
"""

_DDL_CORPORATE_ACTIONS = """
CREATE TABLE IF NOT EXISTS market_data.corporate_actions (
    symbol       String,
    ex_date      Date,
    action_type  LowCardinality(String),  -- split | bonus | demerger | rights | face_value_split | dividend | buyback | other
    ratio        String DEFAULT '',       -- "1:1", "Rs 0.25", etc.
    purpose      String DEFAULT '',       -- raw NSE purpose string
    series       String DEFAULT 'EQ',
    face_val     Float64 DEFAULT 0.0,
    record_date  Date,
    source       LowCardinality(String) DEFAULT 'nse',
    imported_at  DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(imported_at)
ORDER BY (symbol, ex_date, action_type)
"""

_DDL_MACRO_INDICATORS = """
CREATE TABLE IF NOT EXISTS market_data.macro_indicators (
    ref_year        UInt16,       -- e.g. 2024 (annual cadence for both WB and IMF)
    country_code    String,       -- ISO2: IN, US, CN, etc.
    indicator_code  String,       -- e.g. 'NY.GDP.MKTP.KD.ZG' or 'NGDP_RPCH'
    indicator_name  String,       -- human-readable label
    value           Float64,
    source          String,       -- 'world_bank' | 'imf_weo'
    is_forecast     UInt8,        -- 1 = IMF WEO projection (future year)
    imported_at     DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(imported_at)
ORDER BY (ref_year, country_code, indicator_code, source)
"""

_DDL_INDIAN_MACRO_INDICATORS = """
CREATE TABLE IF NOT EXISTS market_data.indian_macro_indicators (
    as_of_date      Date,
    indicator_code  String,
    indicator_name  String,
    parent_code     String,
    value           Float64,
    unit            String,
    imported_at     DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(imported_at)
ORDER BY (as_of_date, indicator_code)
"""


_DDL_LIVE_QUOTES = """
CREATE TABLE IF NOT EXISTS market_data.live_quotes (
    symbol                String,
    exchange              LowCardinality(String),
    company_name          String,
    isin                  String,
    last_trade_time       DateTime,
    last_price            Float64,
    prev_close            Float64,
    open                  Float64,
    high                  Float64,
    low                   Float64,
    avg_price             Float64,
    volume                UInt64,
    last_traded_qty       UInt32,
    upper_circuit         Float64,
    lower_circuit         Float64,
    week52_high           Float64,
    week52_low            Float64,
    issued_capital        Float64,
    total_buy_qty         UInt64,
    total_sell_qty        UInt64,
    bid_prices            Array(Float64),
    bid_quantities        Array(UInt64),
    bid_orders            Array(UInt32),
    ask_prices            Array(Float64),
    ask_quantities        Array(UInt64),
    ask_orders            Array(UInt32),
    imported_at           DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(imported_at)
PARTITION BY toYYYYMM(last_trade_time)
ORDER BY (symbol, last_trade_time)
"""


_DDL_LIVE_ALERTS = """
CREATE TABLE IF NOT EXISTS market_data.live_alerts (
    symbol                LowCardinality(String),
    alert_timestamp       DateTime,
    alert_type            LowCardinality(String),
    zscore                Float64,
    price                 Float64,
    volume                Float64,
    baseline_avg_volume   Float64,
    correlated_headline   String,
    correlated_source     String,
    delivered_to_slack    UInt8 DEFAULT 0,
    delivered_to_whatsapp UInt8 DEFAULT 0,
    imported_at           DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(imported_at)
PARTITION BY toYYYYMM(alert_timestamp)
ORDER BY (symbol, alert_timestamp)
"""


class ClickHouseImporter:
    """
    Thin wrapper around clickhouse_connect.Client for bulk data imports.

    Parameters
    ----------
    host, port, database, username, password : from Settings
    client : optional pre-created clickhouse_connect.Client (e.g. from CHPool).
        When provided the importer will NOT open a new TCP connection and will
        NOT close the client on close() — the pool owns the lifecycle.
    """

    def __init__(
        self,
        host: str = None,       # deprecated — ignored when pool is used
        port: int = None,       # deprecated
        database: str = None,   # deprecated
        username: str = None,   # deprecated
        password: str = None,   # deprecated
        *,
        client=None,
    ) -> None:
        from config.settings import settings
        self._host = host or settings.clickhouse_host
        self._port = port or settings.clickhouse_port
        self._database = database or settings.clickhouse_database
        self._username = username or settings.clickhouse_user
        self._password = password if password is not None else settings.clickhouse_password

        self._injected = client is not None
        if client is not None:
            # Caller-provided client (e.g. test injection)
            self._client = client
        elif any(p is not None for p in (host, port, database, username, password)):
            # Legacy callers passing explicit params — keep working but warn once
            self._client = clickhouse_connect.get_client(
                host=self._host,
                port=self._port,
                username=self._username,
                password=self._password,
                connect_timeout=15,
                compress="lz4",
            )
        else:
            # Default: use the pool singleton — no connection params needed
            from src.db.pool import get_client as _pool_get_client
            self._client = _pool_get_client()

    # ── Schema ────────────────────────────────────────────────────────────────

    def ensure_schema(self) -> None:
        """Create database and tables if they don't already exist."""
        for ddl in (
            _DDL_DATABASE, _DDL_DAILY_PRICES, _DDL_MF_NAV, _DDL_WATERMARKS,
            _DDL_INAV_SNAPSHOTS, _DDL_COT_GOLD, _DDL_CB_GOLD_RESERVES, _DDL_ETF_AUM,
            _DDL_FX_RATES, _DDL_NSE_DELIVERY, _DDL_ML_PREDICTIONS, _DDL_MF_HOLDINGS, _DDL_FII_DII_FLOWS,
            _DDL_FII_DII_MONTHLY, _DDL_FII_DII_FNO_DAILY, _DDL_NEWS_ARTICLES,
            _DDL_SIGNAL_COMPOSITE, _DDL_WEIGHT_CHECKPOINTS, _DDL_PIPELINE_MANIFEST,
            _DDL_STOCK_EARNINGS, _DDL_STOCK_INSIDER, _DDL_STOCK_VALUATION,
            _DDL_USER_HOLDINGS, _DDL_USER_PROFILE,
            _DDL_USER_MARGINS, _DDL_USER_POSITIONS, _DDL_USER_ORDERS,
            _DDL_MACRO_INDICATORS, _DDL_INDIAN_MACRO_INDICATORS, _DDL_IMPORT_FAILURES,
            _DDL_AGENT_TRACES, _DDL_AGENT_PREFERENCES, _DDL_CORPORATE_ACTIONS,
            _DDL_LIVE_QUOTES, _DDL_LIVE_ALERTS, _DDL_AMFI_CATEGORY_FLOWS,
            _DDL_MF_HOLDING_SUMMARIES, _DDL_MV_MF_HOLDING_SUMMARIES,
        ):
            self._client.command(ddl)
        # Column migrations: ADD COLUMN IF NOT EXISTS / MODIFY COLUMN (all idempotent)
        self._client.command(_DDL_NEWS_ARTICLES_MIGRATE)
        self._client.command(_DDL_LIVE_ALERTS_MIGRATE)
        for alter in _ALTER_LOWCARDINALITY:
            try:
                self._client.command(alter)
            except Exception as exc:  # non-fatal — table may not exist yet
                logger.debug("Schema migration skipped (%s): %s", alter[:60], exc)
        logger.debug("ClickHouse schema verified.")

    # ── Bulk insert: user_holdings ────────────────────────────────────────────

    def insert_user_holdings(self, rows: list[dict[str, Any]]) -> int:
        """
        Insert user portfolio holdings into market_data.user_holdings.

        Each row dict must have keys:
            tradingsymbol, exchange, isin, quantity, average_price,
            last_price, pnl, day_change, day_change_percentage
        """
        if not rows:
            return 0
        data = [
            [
                r["tradingsymbol"],
                r["exchange"],
                r["isin"],
                r["quantity"],
                r["average_price"],
                r["last_price"],
                r["pnl"],
                r["day_change"],
                r["day_change_percentage"],
            ]
            for r in rows
        ]
        self._client.insert(
            "market_data.user_holdings",
            data,
            column_names=[
                "tradingsymbol", "exchange", "isin", "quantity", "average_price",
                "last_price", "pnl", "day_change", "day_change_percentage",
            ],
        )
        logger.info("Inserted %d user holdings into market_data.user_holdings", len(rows))
        return len(rows)

    def insert_user_profile(self, r: dict[str, Any]) -> int:
        """Insert user profile into market_data.user_profile."""
        data = [[
            r["user_id"], r["user_name"], r["email"],
            r["broker"], r["user_type"], r.get("exchanges", []),
        ]]
        self._client.insert(
            "market_data.user_profile",
            data,
            column_names=["user_id", "user_name", "email", "broker", "user_type", "exchanges"],
        )
        return 1

    def insert_user_margins(self, rows: list[dict[str, Any]]) -> int:
        """Insert user margins into market_data.user_margins."""
        if not rows: return 0
        data = [[
            r["segment"], r["cash"], r["available_balance"],
            r["utilised_debits"], r["utilised_m2m"], r["utilised_holding_sales"]
        ] for r in rows]
        self._client.insert(
            "market_data.user_margins",
            data,
            column_names=[
                "segment", "cash", "available_balance",
                "utilised_debits", "utilised_m2m", "utilised_holding_sales"
            ],
        )
        return len(rows)

    def insert_user_positions(self, rows: list[dict[str, Any]]) -> int:
        """Insert user positions into market_data.user_positions."""
        if not rows: return 0
        data = [[
            r["tradingsymbol"], r["exchange"], r["instrument_token"],
            r["product"], r["quantity"], r["average_price"],
            r["last_price"], r["pnl"]
        ] for r in rows]
        self._client.insert(
            "market_data.user_positions",
            data,
            column_names=[
                "tradingsymbol", "exchange", "instrument_token",
                "product", "quantity", "average_price",
                "last_price", "pnl"
            ],
        )
        return len(rows)

    def insert_user_orders(self, rows: list[dict[str, Any]]) -> int:
        """Insert user orders into market_data.user_orders."""
        if not rows: return 0
        data = [[
            r["order_id"], r.get("parent_order_id", ""), r["status"],
            r["tradingsymbol"], r["exchange"], r["transaction_type"],
            r["order_type"], r["quantity"], r["filled_quantity"],
            r["pending_quantity"], r["price"], r["average_price"],
            r["order_timestamp"]
        ] for r in rows]
        self._client.insert(
            "market_data.user_orders",
            data,
            column_names=[
                "order_id", "parent_order_id", "status",
                "tradingsymbol", "exchange", "transaction_type",
                "order_type", "quantity", "filled_quantity",
                "pending_quantity", "price", "average_price",
                "order_timestamp"
            ],
        )
        return len(rows)

    # ── Bulk insert: news_articles ────────────────────────────────────────────

    def insert_news_articles(self, rows: list[dict]) -> int:
        """
        Insert news articles into market_data.news_articles.

        Each dict must have keys:
            fetched_at, published_at, source_type, fetch_source, category,
            etfs_impacted, sentiment, impact_tier, title, source, url

        Returns the number of rows inserted.
        """
        if not rows:
            return 0
        data = [
            [
                r["fetched_at"],
                r.get("published_at", ""),
                r["source_type"],
                r.get("fetch_source", ""),
                r["category"],
                r.get("etfs_impacted", ""),
                r.get("sentiment", "NEUTRAL"),
                r.get("impact_tier", ""),
                r["title"],
                r.get("source", ""),
                r.get("url", ""),
            ]
            for r in rows
        ]
        self._client.insert(
            "market_data.news_articles",
            data,
            column_names=[
                "fetched_at", "published_at", "source_type", "fetch_source", "category",
                "etfs_impacted", "sentiment", "impact_tier", "title", "source", "url",
            ],
        )
        logger.info("Inserted %d news articles into market_data.news_articles", len(rows))
        return len(rows)

    # ── Bulk insert: signal_composite ────────────────────────────────────────

    def insert_signal_composite(self, rows: list[dict]) -> int:
        """
        Insert signal composite scores into market_data.signal_composite.

        Each dict must have keys:
            as_of, etf_symbol, macro_score, sentiment_score, valuation_score,
            flow_score, ml_score, anomaly_flag, composite_score, action, rationale

        Returns the number of rows inserted.
        """
        if not rows:
            return 0
        data = [
            [
                r["as_of"], r["etf_symbol"],
                r.get("macro_score", 0.0), r.get("sentiment_score", 0.0),
                r.get("valuation_score", 0.0), r.get("flow_score", 0.0),
                r.get("ml_score", 0.0), r.get("anomaly_flag", "Normal"),
                r["composite_score"], r["action"], r.get("rationale", ""),
            ]
            for r in rows
        ]
        self._client.insert(
            "market_data.signal_composite",
            data,
            column_names=[
                "as_of", "etf_symbol", "macro_score", "sentiment_score",
                "valuation_score", "flow_score", "ml_score", "anomaly_flag",
                "composite_score", "action", "rationale",
            ],
        )
        logger.info("Inserted %d signal composite rows", len(rows))
        return len(rows)

    # ── Bulk insert: weight_checkpoints ──────────────────────────────────────

    def insert_weight_checkpoints(self, rows: list[dict]) -> int:
        """
        Insert position-sizing decisions into market_data.weight_checkpoints.

        Each dict must have keys:
            as_of, symbol, method, recommended_weight, regime, rationale
        Optional keys (NULL when absent):
            expected_return_pct, expected_vol_pct, garch_vol_pct,
            composite_score, cv_r2, price_below_ema50, horizon_days

        Returns the number of rows inserted.
        """
        if not rows:
            return 0
        data = [
            [
                r["as_of"],
                r["symbol"],
                r["method"],
                float(r["recommended_weight"]),
                r.get("expected_return_pct"),
                r.get("expected_vol_pct"),
                r.get("garch_vol_pct"),
                r.get("regime", ""),
                r.get("composite_score"),
                int(r.get("price_below_ema50", 0)),
                r.get("cv_r2"),
                int(r.get("horizon_days", 5)),
                r.get("rationale", ""),
            ]
            for r in rows
        ]
        self._client.insert(
            "market_data.weight_checkpoints",
            data,
            column_names=[
                "as_of", "symbol", "method", "recommended_weight",
                "expected_return_pct", "expected_vol_pct", "garch_vol_pct",
                "regime", "composite_score", "price_below_ema50",
                "cv_r2", "horizon_days", "rationale",
            ],
        )
        logger.info("Inserted %d weight checkpoint rows", len(rows))
        return len(rows)

    # ── Pipeline Manifest ───────────────────────────────────────────────────

    def insert_pipeline_manifest(self, row: dict) -> None:
        """
        Record a pipeline stage execution in market_data.pipeline_manifest.

        row must contain: stage, symbol, input_fingerprint, code_version,
                          input_details (JSON string or dict), duration_ms, status.
        """
        input_details_str = (
            json.dumps(row["input_details"])
            if isinstance(row.get("input_details"), dict)
            else str(row.get("input_details", "{}"))
        )
        computed_at = row.get("computed_at", datetime.now())
        data = [[
            row["stage"],
            row["symbol"],
            row["input_fingerprint"],
            row["code_version"],
            computed_at,
            input_details_str,
            int(row.get("duration_ms", 0)),
            row.get("status", "success"),
        ]]
        self._client.insert(
            "market_data.pipeline_manifest",
            data,
            column_names=[
                "stage", "symbol", "input_fingerprint", "code_version",
                "computed_at", "input_details", "duration_ms", "status",
            ],
        )

    def get_pipeline_manifest(self, stage: str, symbol: str) -> dict | None:
        """Get the latest manifest row for a given stage and symbol."""
        result = self._client.query(
            "SELECT stage, symbol, input_fingerprint, code_version, computed_at, input_details, duration_ms, status "
            "FROM market_data.pipeline_manifest FINAL "
            "WHERE stage = {stage:String} AND symbol = {symbol:String} LIMIT 1",
            parameters={"stage": stage, "symbol": symbol},
        )
        if result.result_rows:
            r = result.result_rows[0]
            return {
                "stage": r[0],
                "symbol": r[1],
                "input_fingerprint": r[2],
                "code_version": r[3],
                "computed_at": r[4],
                "input_details": r[5],
                "duration_ms": r[6],
                "status": r[7],
            }
        return None

    # ── Watermarks ────────────────────────────────────────────────────────────

    def get_watermark(self, source: str, symbol: str, dataset: str = "prices") -> date | None:
        """
        Return the last successfully imported date for (source, symbol, dataset),
        or None if this symbol has never been imported.

        dataset is one of: prices | earnings | insider | valuation | nav | holdings
        (defaults to 'prices' for back-compatibility with all existing callers)
        """
        result = self._client.query(
            "SELECT last_date FROM market_data.import_watermarks FINAL "
            "WHERE source = {source:String} AND symbol = {symbol:String} "
            "AND dataset = {dataset:String} "
            "LIMIT 1",
            parameters={"source": source, "symbol": symbol, "dataset": dataset},
        )
        rows = result.result_rows
        if rows:
            return rows[0][0]  # clickhouse_connect returns date objects
        return None

    def set_watermark(self, source: str, symbol: str, last_date: date, dataset: str = "prices") -> None:
        """Upsert the watermark for (source, symbol, dataset)."""
        self._client.insert(
            "market_data.import_watermarks",
            [[source, symbol, dataset, last_date]],
            column_names=["source", "symbol", "dataset", "last_date"],
        )

    # ── Bulk insert: daily_prices ────────────────────────────────────────────

    def insert_prices(
        self,
        rows: list[dict[str, Any]],
        *,
        dry_run: bool = False,
    ) -> int:
        """
        Bulk-insert OHLCV rows into daily_prices.

        Each row dict must have keys:
            symbol, category, trade_date (date), open, high, low, close, volume

        Returns the number of rows inserted (or that would have been inserted).
        """
        if not rows:
            return 0
        if dry_run:
            logger.info("[dry-run] Would insert %d price rows.", len(rows))
            return len(rows)

        import pandas as pd

        df = pd.DataFrame(rows, columns=[
            "symbol", "category", "trade_date", "open", "high", "low", "close", "volume"
        ])[["symbol", "category", "trade_date", "open", "high", "low", "close", "volume"]]

        # Chunk at 50k rows to stay within ClickHouse default block limits
        chunk_size = 50_000
        total = 0
        for start in range(0, len(df), chunk_size):
            chunk = df.iloc[start : start + chunk_size]
            self._client.insert_df(
                "market_data.daily_prices",
                chunk,
                settings={"max_partitions_per_insert_block": 1500},
            )
            total += len(chunk)
        from src.db.market_vector import vectorize_prices
        vectorize_prices(rows)
        return total

    # ── Bulk insert: mf_nav ───────────────────────────────────────────────────

    def insert_nav(
        self,
        rows: list[dict[str, Any]],
        *,
        dry_run: bool = False,
    ) -> int:
        """
        Bulk-insert NAV rows into mf_nav.

        Each row dict must have keys:
            symbol, scheme_code, nav_date (date), nav

        Returns the number of rows inserted (or that would have been inserted).
        """
        if not rows:
            return 0
        if dry_run:
            logger.info("[dry-run] Would insert %d NAV rows.", len(rows))
            return len(rows)

        data = [
            [r["symbol"], r["scheme_code"], r["nav_date"], r["nav"]]
            for r in rows
        ]
        self._client.insert(
            "market_data.mf_nav",
            data,
            column_names=["symbol", "scheme_code", "nav_date", "nav"],
            settings={"max_partitions_per_insert_block": 300},
        )
        from src.db.market_vector import vectorize_nav
        vectorize_nav(rows)
        return len(rows)

    # ── Bulk insert: mf_holdings ──────────────────────────────────────────────

    def insert_mf_holdings(
        self,
        rows: list[dict[str, Any]],
        *,
        dry_run: bool = False,
    ) -> int:
        """
        Bulk-insert MF portfolio holdings into mf_holdings.

        Each row dict must have keys:
            scheme_code, fund_name, as_of_month (date), isin, security_name,
            asset_type, market_value_cr, pct_of_nav

        Returns the number of rows inserted (or that would have been inserted).
        """
        if not rows:
            return 0
        if dry_run:
            logger.info("[dry-run] Would insert %d holdings rows.", len(rows))
            return len(rows)

        data = [
            [
                r["scheme_code"],
                r["fund_name"],
                r["as_of_month"],
                r["isin"],
                r["security_name"],
                r["asset_type"],
                r["market_value_cr"],
                r["pct_of_nav"],
            ]
            for r in rows
        ]
        self._client.insert(
            "market_data.mf_holdings",
            data,
            column_names=[
                "scheme_code", "fund_name", "as_of_month", "isin",
                "security_name", "asset_type", "market_value_cr", "pct_of_nav",
            ],
        )
        from src.db.mf_vector import vectorize_holdings
        vectorize_holdings(rows)
        return len(rows)

    # ── Bulk insert: inav_snapshots ───────────────────────────────────────────

    def insert_inav_snapshots(
        self,
        rows: list[dict[str, Any]],
        *,
        dry_run: bool = False,
    ) -> int:
        """
        Insert iNAV snapshot rows into inav_snapshots.

        Each row dict must have keys:
            symbol, snapshot_at (datetime), inav, market_price,
            premium_discount_pct, source

        Returns the number of rows inserted (or that would be inserted).
        """
        if not rows:
            return 0
        if dry_run:
            logger.info("[dry-run] Would insert %d iNAV snapshot rows.", len(rows))
            return len(rows)

        data = [
            [
                r["symbol"],
                r["snapshot_at"],
                r["inav"],
                r["market_price"],
                r["premium_discount_pct"],
                r["source"],
            ]
            for r in rows
        ]
        self._client.insert(
            "market_data.inav_snapshots",
            data,
            column_names=["symbol", "snapshot_at", "inav", "market_price",
                          "premium_discount_pct", "source"],
        )
        return len(rows)

    # ── Bulk insert: cot_gold ────────────────────────────────────────────────

    def insert_cot_gold(
        self,
        rows: list[dict[str, Any]],
        *,
        dry_run: bool = False,
    ) -> int:
        """Insert CFTC COT rows into cot_gold. Returns row count."""
        if not rows:
            return 0
        if dry_run:
            return len(rows)
        self._client.insert(
            "market_data.cot_gold",
            [[r["report_date"], r["mm_long"], r["mm_short"], r["mm_spread"],
              r["mm_net"], r["comm_long"], r["comm_short"], r["comm_net"],
              r["open_interest"], r["source"]]
             for r in rows],
            column_names=["report_date", "mm_long", "mm_short", "mm_spread",
                          "mm_net", "comm_long", "comm_short", "comm_net",
                          "open_interest", "source"],
            settings={"max_partitions_per_insert_block": 300},
        )
        from src.db.market_vector import vectorize_cot
        vectorize_cot(rows)
        return len(rows)

    # ── Bulk insert: cb_gold_reserves ────────────────────────────────────────

    def insert_cb_reserves(
        self,
        rows: list[dict[str, Any]],
        *,
        dry_run: bool = False,
    ) -> int:
        """Insert IMF central bank gold reserve rows. Returns row count."""
        if not rows:
            return 0
        if dry_run:
            return len(rows)
        self._client.insert(
            "market_data.cb_gold_reserves",
            [[r["ref_period"], r["country_code"], r["country_name"],
              r["reserves_tonnes"], r["source"]]
             for r in rows],
            column_names=["ref_period", "country_code", "country_name",
                          "reserves_tonnes", "source"],
            settings={"max_partitions_per_insert_block": 300},
        )
        return len(rows)

    # ── Bulk insert: macro_indicators ─────────────────────────────────────────

    def insert_macro_indicators(self, rows: list[dict[str, Any]]) -> int:
        """
        Insert macro indicator rows into market_data.macro_indicators.

        Each dict must have keys:
            ref_year (int), country_code, indicator_code, indicator_name,
            value (float), source, is_forecast (int 0|1)

        Returns the number of rows inserted.
        """
        if not rows:
            return 0
        self._client.insert(
            "market_data.macro_indicators",
            [[
                int(r["ref_year"]),
                r["country_code"],
                r["indicator_code"],
                r["indicator_name"],
                float(r["value"]),
                r["source"],
                int(r.get("is_forecast", 0)),
            ] for r in rows],
            column_names=[
                "ref_year", "country_code", "indicator_code",
                "indicator_name", "value", "source", "is_forecast",
            ],
        )
        logger.info("Inserted %d macro indicator rows", len(rows))
        from src.db.market_vector import vectorize_macro
        vectorize_macro(rows)
        return len(rows)

    # ── Bulk insert: indian_macro_indicators ──────────────────────────────────

    def insert_indian_macro_indicators(self, rows: list[dict[str, Any]]) -> int:
        """
        Insert Indian macro indicator rows into market_data.indian_macro_indicators.

        Each dict must have keys:
            as_of_date (date or str), indicator_code, indicator_name, parent_code,
            value (float), unit

        Returns the number of rows inserted.
        """
        if not rows:
            return 0
        
        parsed_rows = []
        for r in rows:
            as_of_date = r["as_of_date"]
            if isinstance(as_of_date, str):
                as_of_date = datetime.strptime(as_of_date, "%Y-%m-%d").date()
                
            parsed_rows.append([
                as_of_date,
                r["indicator_code"],
                r["indicator_name"],
                r["parent_code"],
                float(r["value"]) if r["value"] is not None else None,
                r["unit"],
            ])
            
        self._client.insert(
            "market_data.indian_macro_indicators",
            parsed_rows,
            column_names=[
                "as_of_date", "indicator_code", "indicator_name", "parent_code",
                "value", "unit"
            ],
            settings={"max_partitions_per_insert_block": 300},
        )
        logger.info("Inserted %d Indian macro indicator rows", len(rows))
        return len(rows)

    # ── Bulk insert: etf_aum ─────────────────────────────────────────────────

    def insert_etf_aum(
        self,
        rows: list[dict[str, Any]],
        *,
        dry_run: bool = False,
    ) -> int:
        """Insert ETF AUM snapshot rows. Returns row count."""
        if not rows:
            return 0
        if dry_run:
            return len(rows)
        self._client.insert(
            "market_data.etf_aum",
            [[r["trade_date"], r["symbol"], r["aum_usd"],
              r["price"], r["implied_tonnes"], r["source"]]
             for r in rows],
            column_names=["trade_date", "symbol", "aum_usd",
                          "price", "implied_tonnes", "source"],
            settings={"max_partitions_per_insert_block": 300},
        )
        return len(rows)

    # ── Bulk insert: fx_rates ─────────────────────────────────────────────────

    def insert_fx_rates(
        self,
        rows: list[dict[str, Any]],
        *,
        dry_run: bool = False,
    ) -> int:
        """Insert daily FX rate rows into fx_rates. Returns row count."""
        if not rows:
            return 0
        if dry_run:
            return len(rows)
        self._client.insert(
            "market_data.fx_rates",
            [[r["trade_date"], r["symbol"], r["open"], r["high"],
              r["low"], r["close"], r["source"]]
             for r in rows],
            column_names=["trade_date", "symbol", "open", "high", "low", "close", "source"],
            settings={"max_partitions_per_insert_block": 300},
        )
        from src.db.market_vector import vectorize_fx_rates
        vectorize_fx_rates(rows)
        return len(rows)

    # ── Bulk insert: nse_delivery ──────────────────────────────────────────

    def insert_nse_delivery(
        self,
        rows: list[dict[str, Any]],
        *,
        dry_run: bool = False,
    ) -> int:
        """Insert NSE daily delivery-position rows. Returns row count."""
        if not rows:
            return 0
        if dry_run:
            return len(rows)
        cols = [
            "trade_date", "symbol", "series", "prev_close", "open_price",
            "high_price", "low_price", "last_price", "close_price", "avg_price",
            "ttl_trd_qty", "turnover_lacs", "no_of_trades", "deliv_qty",
            "deliv_per", "source",
        ]
        self._client.insert(
            "market_data.nse_delivery",
            [[r.get(c) for c in cols] for r in rows],
            column_names=cols,
            settings={"max_partitions_per_insert_block": 300},
        )
        return len(rows)

    # ── Bulk insert: fii_dii_flows ────────────────────────────────────────────

    def insert_fii_dii_flows(
        self,
        rows: list[dict[str, Any]],
        *,
        dry_run: bool = False,
    ) -> int:
        """
        Bulk-insert FII/DII institutional flow rows into fii_dii_flows.

        Each row dict must have keys:
            trade_date (date), fii_gross_buy_cr, fii_gross_sell_cr, fii_net_cr,
            dii_gross_buy_cr, dii_gross_sell_cr, dii_net_cr

        Returns the number of rows inserted (or that would have been inserted).
        """
        if not rows:
            return 0
        if dry_run:
            logger.info("[dry-run] Would insert %d FII/DII flow rows.", len(rows))
            return len(rows)
        self._client.insert(
            "market_data.fii_dii_flows",
            [
                [
                    r["trade_date"],
                    r["fii_gross_buy_cr"],
                    r["fii_gross_sell_cr"],
                    r["fii_net_cr"],
                    r["dii_gross_buy_cr"],
                    r["dii_gross_sell_cr"],
                    r["dii_net_cr"],
                ]
                for r in rows
            ],
            column_names=[
                "trade_date",
                "fii_gross_buy_cr",
                "fii_gross_sell_cr",
                "fii_net_cr",
                "dii_gross_buy_cr",
                "dii_gross_sell_cr",
                "dii_net_cr",
            ],
            settings={"max_partitions_per_insert_block": 300},
        )
        return len(rows)

    def insert_fii_dii_monthly(
        self,
        rows: list[dict],
        *,
        dry_run: bool = False,
    ) -> int:
        """
        Bulk-insert monthly FII/DII cash-market aggregate rows.

        Each row dict must have keys:
            month_date (date), fii_buy_cr, fii_sell_cr, fii_net_cr,
            dii_buy_cr, dii_sell_cr, dii_net_cr, nifty_close, nifty_change_pct
        """
        if not rows:
            return 0
        if dry_run:
            logger.info("[dry-run] Would insert %d FII/DII monthly rows.", len(rows))
            return len(rows)
        self._client.insert(
            "market_data.fii_dii_monthly",
            [
                [
                    r["month_date"], r["fii_buy_cr"], r["fii_sell_cr"], r["fii_net_cr"],
                    r["dii_buy_cr"], r["dii_sell_cr"], r["dii_net_cr"],
                    r["nifty_close"], r["nifty_change_pct"],
                ]
                for r in rows
            ],
            column_names=[
                "month_date", "fii_buy_cr", "fii_sell_cr", "fii_net_cr",
                "dii_buy_cr", "dii_sell_cr", "dii_net_cr",
                "nifty_close", "nifty_change_pct",
            ],
        )
        return len(rows)

    def insert_amfi_category_flows(
        self,
        rows: list[dict[str, Any]],
        *,
        dry_run: bool = False,
    ) -> int:
        """
        Bulk-insert AMFI category-wise monthly flows into amfi_category_flows.

        Each row dict must have keys:
            report_month (date), category_name, subcategory_group,
            gross_purchase_cr, gross_redemption_cr, net_flow_cr,
            closing_aum_cr, flow_pct_of_aum

        Returns the number of rows inserted.
        """
        if not rows:
            return 0
        if dry_run:
            logger.info("[dry-run] Would insert %d AMFI category flow rows.", len(rows))
            return len(rows)
        self._client.insert(
            "market_data.amfi_category_flows",
            [
                [
                    r["report_month"],
                    r["category_name"],
                    r["subcategory_group"],
                    r["gross_purchase_cr"],
                    r["gross_redemption_cr"],
                    r["net_flow_cr"],
                    r["closing_aum_cr"],
                    r["flow_pct_of_aum"],
                ]
                for r in rows
            ],
            column_names=[
                "report_month", "category_name", "subcategory_group",
                "gross_purchase_cr", "gross_redemption_cr", "net_flow_cr",
                "closing_aum_cr", "flow_pct_of_aum",
            ],
        )
        return len(rows)

    def insert_fii_dii_fno_daily(
        self,
        rows: list[dict],
        *,
        dry_run: bool = False,
    ) -> int:
        """
        Bulk-insert daily F&O participant OI rows.

        Each row must have the keys matching fii_dii_fno_daily columns
        (see _DDL_FII_DII_FNO_DAILY for full list).
        """
        if not rows:
            return 0
        if dry_run:
            logger.info("[dry-run] Would insert %d FII/DII F&O daily rows.", len(rows))
            return len(rows)
        cols = [
            "trade_date",
            "fii_fut_net_oi", "fii_fut_outstanding_oi",
            "fii_fut_nifty_net_oi", "fii_fut_banknifty_net_oi",
            "dii_fut_net_oi", "dii_fut_outstanding_oi",
            "pro_fut_net_oi", "pro_fut_outstanding_oi",
            "client_fut_net_oi", "client_fut_outstanding_oi",
            "fii_fut_stock_net_oi", "dii_fut_stock_net_oi",
            "pro_fut_stock_net_oi", "client_fut_stock_net_oi",
            "fii_opt_overall_net_oi", "fii_opt_overall_net_oi_change",
            "fii_opt_call_net_oi", "fii_opt_put_net_oi",
            "dii_opt_overall_net_oi", "dii_opt_overall_net_oi_change",
            "pro_opt_overall_net_oi", "pro_opt_overall_net_oi_change",
            "client_opt_overall_net_oi", "client_opt_overall_net_oi_change",
            "nifty_close", "banknifty_close",
            "nifty_change_pct", "banknifty_change_pct",
        ]
        self._client.insert(
            "market_data.fii_dii_fno_daily",
            [[r.get(c, 0.0) for c in cols] for r in rows],
            column_names=cols,
            settings={"max_partitions_per_insert_block": 300},
        )
        return len(rows)

    def insert_stock_earnings(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        data = [[
            r["symbol"], r["earnings_date"],
            r.get("eps_estimate", 0.0), r.get("eps_actual", 0.0), r.get("surprise_pct", 0.0),
        ] for r in rows]
        self._client.insert(
            "market_data.stock_earnings", data,
            column_names=["symbol", "earnings_date", "eps_estimate", "eps_actual", "surprise_pct"],
        )
        return len(rows)

    def insert_stock_insider(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        data = [[
            r["symbol"], r["insider_name"], r.get("relation", ""),
            r["transaction_date"], r.get("transaction_type", ""),
            r.get("shares", 0.0), r.get("value", 0.0),
        ] for r in rows]
        self._client.insert(
            "market_data.stock_insider_trades", data,
            column_names=["symbol", "insider_name", "relation", "transaction_date",
                          "transaction_type", "shares", "value"],
        )
        return len(rows)

    def insert_stock_valuation(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        cols = [
            "symbol", "snapshot_date", "market_cap", "trailing_pe", "forward_pe",
            "peg_ratio", "price_to_book", "price_to_sales", "debt_to_equity",
            "return_on_equity", "profit_margin", "operating_margin", "gross_margin",
            "revenue_growth", "earnings_growth", "free_cashflow", "beta",
            "target_price", "recommendation", "analyst_count",
        ]
        data = [[
            r["symbol"], r["snapshot_date"],
            r.get("market_cap", 0.0), r.get("trailing_pe", 0.0), r.get("forward_pe", 0.0),
            r.get("peg_ratio", 0.0), r.get("price_to_book", 0.0), r.get("price_to_sales", 0.0),
            r.get("debt_to_equity", 0.0), r.get("return_on_equity", 0.0),
            r.get("profit_margin", 0.0), r.get("operating_margin", 0.0), r.get("gross_margin", 0.0),
            r.get("revenue_growth", 0.0), r.get("earnings_growth", 0.0), r.get("free_cashflow", 0.0),
            r.get("beta", 0.0), r.get("target_price", 0.0),
            r.get("recommendation", ""), r.get("analyst_count", 0),
        ] for r in rows]
        self._client.insert("market_data.stock_valuation", data, column_names=cols)
        return len(rows)

    def insert_live_quotes(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        cols = [
            "symbol", "exchange", "company_name", "isin", "last_trade_time",
            "last_price", "prev_close", "open", "high", "low", "avg_price",
            "volume", "last_traded_qty", "upper_circuit", "lower_circuit",
            "week52_high", "week52_low", "issued_capital", "total_buy_qty",
            "total_sell_qty", "bid_prices", "bid_quantities", "bid_orders",
            "ask_prices", "ask_quantities", "ask_orders",
        ]
        data = [[
            r["symbol"], r.get("exchange", ""), r.get("company_name", ""), r.get("isin", ""),
            r["last_trade_time"], r.get("last_price", 0.0), r.get("prev_close", 0.0),
            r.get("open", 0.0), r.get("high", 0.0), r.get("low", 0.0), r.get("avg_price", 0.0),
            r.get("volume", 0), r.get("last_traded_qty", 0), r.get("upper_circuit", 0.0),
            r.get("lower_circuit", 0.0), r.get("week52_high", 0.0), r.get("week52_low", 0.0),
            r.get("issued_capital", 0.0), r.get("total_buy_qty", 0), r.get("total_sell_qty", 0),
            r.get("bid_prices", []), r.get("bid_quantities", []), r.get("bid_orders", []),
            r.get("ask_prices", []), r.get("ask_quantities", []), r.get("ask_orders", []),
        ] for r in rows]
        self._client.insert("market_data.live_quotes", data, column_names=cols)
        return len(rows)

    def insert_live_alerts(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        cols = [
            "symbol", "alert_timestamp", "alert_type", "zscore", "price", "volume",
            "baseline_avg_volume", "correlated_headline", "correlated_source",
            "delivered_to_slack", "delivered_to_whatsapp",
        ]
        data = [[
            r["symbol"], r["alert_timestamp"], r.get("alert_type", ""),
            r.get("zscore", 0.0), r.get("price", 0.0), r.get("volume", 0.0),
            r.get("baseline_avg_volume", 0.0), r.get("correlated_headline", ""),
            r.get("correlated_source", ""), int(r.get("delivered_to_slack", 0)),
            int(r.get("delivered_to_whatsapp", 0)),
        ] for r in rows]
        self._client.insert("market_data.live_alerts", data, column_names=cols)
        return len(rows)

    def close(self) -> None:
        # Do not close a pool-managed client — the pool handles recycling.
        if not self._injected:
            self._client.close()

    def snapshot(self, lookback_days: int) -> list[dict[str, Any]]:
        """
        Snapshot current state for the given lookback window.
        Actually, let's just snapshot the watermarks as that's easier to undo.
        In a real scenario, we might want to snapshot data too, but replacing 
        rows is safe with ReplacingMergeTree.
        """
        # For simplicity, we just snapshot watermarks
        result = self._client.query("SELECT * FROM market_data.import_watermarks FINAL")
        return [dict(zip(result.column_names, row)) for row in result.result_rows]

    def restore(self, snapshot: list[dict[str, Any]]) -> None:
        """Restore watermarks from a snapshot."""
        if not snapshot:
            return
        # We could TRUNCATE first, but ReplacingMergeTree will just take the latest
        self._client.insert(
            "market_data.import_watermarks",
            [[r["source"], r["symbol"], r["last_date"]] for r in snapshot],
            column_names=["source", "symbol", "last_date"],
        )

    def run(
        self, 
        categories: list[str], 
        lookback_days: int = 3650, 
        full_reimport: bool = False, 
        dry_run: bool = False,
        data_source: str = "",
        target_month: str = "",
        freshness_months: int = 0,
    ) -> dict[str, Any]:
        """
        Execute the import logic (refactored from cli.py).
        Returns a summary dict.
        """
        from src.data_importer.cli import run_import
        # We'll use the existing run_import for now, but in a real refactor
        # we'd move the logic here.
        run_import(
            categories=categories,
            lookback_days=lookback_days,
            full_reimport=full_reimport,
            dry_run=dry_run,
            data_source=data_source,
            clickhouse_host=self._host,
            clickhouse_port=self._port,
            clickhouse_database=self._database,
            clickhouse_user=self._username,
            clickhouse_password=self._password,
            target_month=target_month,
            freshness_months=freshness_months,
        )
        return {"status": "success", "categories": categories}

    def __enter__(self) -> "ClickHouseImporter":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()
