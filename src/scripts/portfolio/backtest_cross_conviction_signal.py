"""
src/scripts/portfolio/backtest_cross_conviction_signal.py
───────────────────────────────────────────────────────────
Backtest: does the smallcap cross-conviction persistence signal
(MFCapPatternAnalyzer.fetch_cross_conviction's multi_amc_streak_months)
predict forward SMALLCAP ETF returns?

For each historical month M, replays the exact point-in-time cross-conviction
computation (same streak-scoring code as production, no lookahead — only
mf_holdings data up to and including M is used), aggregates a "persistent-
conviction breadth" score (count of stocks with a >=3-month multi-AMC streak
that month), then measures the SMALLCAP ETF's forward 1/3/6-month return
from M.

Reuses MFCapPatternAnalyzer._score_cross_conviction_persistence /
._AMC_CASE_SQL directly (rather than re-deriving the streak logic here) so
this backtest always tests the *actual shipped* signal — if that scoring
logic changes, this backtest changes with it instead of silently drifting.

Forward returns are measured against the SMALLCAP ETF ticker (not the
NIFTYSC250 index) because SMALLCAP's daily_prices history is clean and
continuous (Mar 2024 - Jul 2026); NIFTYSC250 has a decade of history but is
full of multi-month gaps (most months in 2018-2023 have zero rows), making
it useless for a reliable monthly-return series. This caps the backtest at
~20 usable test months — a real data constraint, not a design choice; the
script prints the sample size so results aren't over-trusted.

Usage:
    python src/scripts/portfolio/backtest_cross_conviction_signal.py
    python src/scripts/portfolio/backtest_cross_conviction_signal.py --lookback-months 6
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(_ROOT / "src"))
sys.path.insert(0, str(_ROOT))

from src.db.pool import get_pool
from src.scripts.portfolio.smallcap_pattern_analyzer import MFCapPatternAnalyzer, CATEGORY_CONFIG

PRICE_SYMBOL = "SMALLCAP"
PERSISTENT_STREAK_THRESHOLD = 3  # matches multi_asset_consensus.py's min_streak_months default


def _all_smallcap_months(pool) -> list:
    membership = CATEGORY_CONFIG["small"]["membership_sql"]
    df = pool.query_df(f"""
        SELECT DISTINCT as_of_month FROM market_data.mf_holdings FINAL
        WHERE {membership}
        ORDER BY as_of_month
    """)
    return sorted(pd.to_datetime(df["as_of_month"]).tolist())


def _point_in_time_conviction_breadth(analyzer: MFCapPatternAnalyzer, as_of_month, all_months: list, lookback_months: int):
    """Replay fetch_cross_conviction's exact logic as of a historical month
    (no lookahead: only uses months <= as_of_month)."""
    prior_months = [m for m in all_months if m <= as_of_month]
    window_months = prior_months[-lookback_months:]
    if len(window_months) < lookback_months:
        return None  # not enough trailing history yet

    membership = CATEGORY_CONFIG["small"]["membership_sql"]
    latest_month = window_months[-1].strftime("%Y-%m-%d")
    month_list_sql = ", ".join(f"'{m.strftime('%Y-%m-%d')}'" for m in window_months)

    history = analyzer.pool.query_df(f"""
        SELECT
            as_of_month,
            any(security_name) as security_name,
            isin,
            count(DISTINCT {analyzer._AMC_CASE_SQL}) as amc_count,
            count(DISTINCT fund_name) as total_fund_count,
            round(sum(market_value_cr), 1) as total_market_value_cr
        FROM market_data.mf_holdings FINAL
        WHERE as_of_month IN ({month_list_sql})
          AND isin IN (
              SELECT DISTINCT isin FROM market_data.mf_holdings FINAL
              WHERE as_of_month = '{latest_month}'
                AND {membership}
                AND isin NOT LIKE 'PH_%' AND isin != ''
          )
          AND isin NOT LIKE 'PH_%' AND isin != ''
        GROUP BY as_of_month, isin
    """)
    if history.empty:
        return None

    scored = analyzer._score_cross_conviction_persistence(history, window_months, limit=100_000)
    if scored.empty:
        return None

    breadth = int((scored["multi_amc_streak_months"] >= PERSISTENT_STREAK_THRESHOLD).sum())
    return {
        "as_of_month": as_of_month,
        "n_stocks_scored": len(scored),
        "breadth_persistent": breadth,
        "breadth_pct": round(100 * breadth / len(scored), 2),
        "avg_streak": round(float(scored["multi_amc_streak_months"].mean()), 2),
    }


def _forward_return(price_df: pd.DataFrame, as_of_month, months_fwd: int):
    target = as_of_month + pd.DateOffset(months=months_fwd)
    base_rows = price_df[price_df["trade_date"] <= as_of_month]
    fwd_rows = price_df[price_df["trade_date"] >= target]
    if base_rows.empty or fwd_rows.empty:
        return None
    base_price = base_rows.iloc[-1]["close"]
    fwd_price = fwd_rows.iloc[0]["close"]
    return round((fwd_price / base_price - 1) * 100, 2)


def run_backtest(lookback_months: int = 12) -> pd.DataFrame:
    pool = get_pool()
    analyzer = MFCapPatternAnalyzer(category="small")

    all_months = _all_smallcap_months(pool)

    price_df = pool.query_df(f"""
        SELECT trade_date, close FROM market_data.daily_prices FINAL
        WHERE symbol = '{PRICE_SYMBOL}' ORDER BY trade_date
    """)
    price_df["trade_date"] = pd.to_datetime(price_df["trade_date"])
    price_min, price_max = price_df["trade_date"].min(), price_df["trade_date"].max()
    fwd_cutoff = price_max - pd.DateOffset(months=6)

    rows = []
    for m in all_months:
        if m < price_min or m > fwd_cutoff:
            continue  # outside the window where we have both a signal and 6mo of forward price data
        signal = _point_in_time_conviction_breadth(analyzer, m, all_months, lookback_months)
        if signal is None:
            continue
        signal["fwd_ret_1m"] = _forward_return(price_df, m, 1)
        signal["fwd_ret_3m"] = _forward_return(price_df, m, 3)
        signal["fwd_ret_6m"] = _forward_return(price_df, m, 6)
        rows.append(signal)

    return pd.DataFrame(rows)


def summarize(results: pd.DataFrame) -> None:
    if results.empty:
        print("No usable test months — insufficient overlap between holdings history and price history.")
        return

    n = len(results)
    print(f"Usable test months: {n} ({results['as_of_month'].min().date()} to {results['as_of_month'].max().date()})")
    print("(Capped by SMALLCAP ETF's clean price history — see script docstring for why.)\n")

    for horizon, col in [("1mo", "fwd_ret_1m"), ("3mo", "fwd_ret_3m"), ("6mo", "fwd_ret_6m")]:
        sub = results.dropna(subset=[col])
        if len(sub) < 4:
            print(f"{horizon}: insufficient forward data (n={len(sub)})")
            continue
        corr = sub["breadth_persistent"].corr(sub[col], method="spearman")
        median_breadth = sub["breadth_persistent"].median()
        high = sub[sub["breadth_persistent"] >= median_breadth][col]
        low  = sub[sub["breadth_persistent"] <  median_breadth][col]
        print(f"--- Forward {horizon} SMALLCAP return ---")
        print(f"  n={len(sub)}  Spearman corr(breadth, fwd_ret) = {corr:+.2f}")
        print(f"  High-breadth months (>= median {median_breadth:.0f}): n={len(high)}, mean fwd ret = {high.mean():+.2f}%, median = {high.median():+.2f}%")
        print(f"  Low-breadth months  (<  median {median_breadth:.0f}): n={len(low)}, mean fwd ret = {low.mean():+.2f}%, median = {low.median():+.2f}%")
        print()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Backtest smallcap cross-conviction persistence signal vs forward SMALLCAP ETF returns")
    parser.add_argument("--lookback-months", type=int, default=12)
    args = parser.parse_args()
    results = run_backtest(lookback_months=args.lookback_months)
    summarize(results)
    print(results.to_string())
