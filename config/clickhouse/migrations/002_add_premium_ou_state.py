#!/usr/bin/env python3
"""
config/clickhouse/migrations/002_add_premium_ou_state.py
────────────────────────────────────────────────────────
Creates `market_data.premium_ou_state` for persisting nightly OU fits
and `market_data.premium_pair_state` for cross-ETF cointegration pairs.

Run once:
    python config/clickhouse/migrations/002_add_premium_ou_state.py
"""
from src.db.pool import execute, query_df


_OU_STATE_DDL = """
CREATE TABLE IF NOT EXISTS market_data.premium_ou_state
(
    symbol         String,
    fit_date       Date,
    theta          Float64     COMMENT 'Mean-reversion speed (per day)',
    mu             Float64     COMMENT 'Long-term equilibrium premium (%)',
    sigma          Float64     COMMENT 'OU diffusion volatility',
    half_life_days Float64     COMMENT 'ln(2)/theta — days to halve gap',
    n_obs          UInt32      COMMENT 'Observations used for fit',
    fit_r2         Float64     COMMENT 'AR(1) regression R²',
    fitted_at      DateTime    DEFAULT now()
)
ENGINE = ReplacingMergeTree(fitted_at)
PARTITION BY toYYYYMM(fit_date)
ORDER BY (symbol, fit_date)
COMMENT 'Nightly OU parameter fits on ETF iNAV premium series'
"""

_PAIR_STATE_DDL = """
CREATE TABLE IF NOT EXISTS market_data.premium_pair_state
(
    symbol_a       String,
    symbol_b       String,
    fit_date       Date,
    coint_pvalue   Float64     COMMENT 'Engle-Granger cointegration p-value',
    hedge_ratio    Float64     COMMENT 'OLS beta: prem_A = alpha + beta*prem_B',
    alpha          Float64     COMMENT 'OLS intercept',
    theta          Float64     COMMENT 'OU speed on spread residual',
    mu             Float64     COMMENT 'Long-term spread equilibrium',
    sigma          Float64     COMMENT 'Spread OU volatility',
    half_life_days Float64     COMMENT 'Spread half-life',
    fitted_at      DateTime    DEFAULT now()
)
ENGINE = ReplacingMergeTree(fitted_at)
PARTITION BY toYYYYMM(fit_date)
ORDER BY (symbol_a, symbol_b, fit_date)
COMMENT 'Nightly cross-ETF premium cointegration pair fits'
"""

_SIGNAL_LOG_DDL = """
CREATE TABLE IF NOT EXISTS market_data.premium_signal_log
(
    as_of                    Date,
    symbol                   String,
    current_prem             Float64     COMMENT 'Premium at time of signal',
    ou_mu                    Float64     COMMENT 'OU equilibrium at time of signal',
    half_life_days           Float64     COMMENT 'OU half-life at time of signal',
    expected_reversion_pct   Float64     COMMENT 'OU-predicted reversion over horizon',
    net_pnl_stcg_pct         Float64     COMMENT 'Net P&L after STCG + costs',
    action                   String      COMMENT 'Signal label emitted',
    ou_available             UInt8       COMMENT '1 if OU state was used, 0 if naive fallback',
    is_profitable_after_costs UInt8      COMMENT '1 if net_pnl_stcg > 0',
    signal_source            String      COMMENT 'domestic_scanner or premium_alerts',
    logged_at                DateTime    DEFAULT now()
)
ENGINE = ReplacingMergeTree(logged_at)
PARTITION BY toYYYYMM(as_of)
ORDER BY (symbol, as_of)
COMMENT 'Daily signal log for premium strategy paper trading'
"""


def migrate():
    # ── premium_ou_state ────────────────────────────────────────────────────
    df = query_df(
        "SELECT name FROM system.tables "
        "WHERE database = 'market_data' AND name = 'premium_ou_state'"
    )
    if not df.empty:
        print("✅ Table market_data.premium_ou_state already exists")
    else:
        execute(_OU_STATE_DDL)
        print("✅ Created market_data.premium_ou_state")

    # ── premium_pair_state ──────────────────────────────────────────────────
    df = query_df(
        "SELECT name FROM system.tables "
        "WHERE database = 'market_data' AND name = 'premium_pair_state'"
    )
    if not df.empty:
        print("✅ Table market_data.premium_pair_state already exists")
    else:
        execute(_PAIR_STATE_DDL)
        print("✅ Created market_data.premium_pair_state")

    # ── premium_signal_log ──────────────────────────────────────────────────
    df = query_df(
        "SELECT name FROM system.tables "
        "WHERE database = 'market_data' AND name = 'premium_signal_log'"
    )
    if not df.empty:
        print("✅ Table market_data.premium_signal_log already exists")
    else:
        execute(_SIGNAL_LOG_DDL)
        print("✅ Created market_data.premium_signal_log")


if __name__ == "__main__":
    migrate()
