"""
tests/_backtest_risk_governor.py
───────────────────────────────
Backtest the Risk Governor's volatility-targeting strategy for GOLDBEES.

Compares:
  1. Buy & Hold  (100% weight every day)
  2. Risk Governor  (Vol-targeted weight: min(1, 15% / garch_vol) × regime_mult)

Metrics:
  Total Return, Annualised Return, Annualised Volatility, Sharpe (rf=0), Max Drawdown

NOTE — In-sample GARCH limitation:
  GARCH(1,1) is fit once on the full price history.  This means the ω/α/β parameters
  are estimated with knowledge of the entire period.  True walk-forward GARCH would
  refit each day, but the conditional vol time series is dominated by local variance
  clustering (not parameter sensitivity), so in-sample vol is a reasonable proxy.
  Treat results as indicative, not as forward-looking performance estimates.
"""
import argparse
import os
import sys
import warnings

import clickhouse_connect
import numpy as np
import pandas as pd

sys.path.insert(0, os.getcwd())   # project root → src.* and config.* both importable

from src.ml.anomaly import run_composite_anomaly
from src.tools.risk_governor import _REGIME_MULT, _VOL_TARGET_PCT, _W_MAX
from config.settings import settings

warnings.filterwarnings("ignore")


def _vectorised_weights(
    garch_vol: pd.Series,
    regime: pd.Series,
    vol_target_pct: float = _VOL_TARGET_PCT,
    w_max: float = _W_MAX,
) -> np.ndarray:
    """
    Vectorised equivalent of compute_position_weight() with composite_score=50
    (neutral — no score gate fired).  ~100× faster than iterrows() for 2500 rows.

    NaN garch_vol rows (GARCH warmup period) are treated as full weight (1.0) so
    the warmup period doesn't distort the strategy return with 0-weight gaps.
    """
    vol_arr      = garch_vol.fillna(vol_target_pct).values.astype(float)
    regime_mult  = regime.map(_REGIME_MULT).fillna(1.0).values.astype(float)

    vol_scaled   = np.minimum(w_max, vol_target_pct / np.maximum(vol_arr, 0.1))
    weights      = np.clip(vol_scaled * regime_mult, 0.0, w_max)
    return weights


def _metrics(returns: pd.Series, name: str) -> dict:
    r = returns.dropna()
    ann_ret = r.mean() * 252
    ann_vol = r.std()  * np.sqrt(252)
    sharpe  = ann_ret / ann_vol if ann_vol > 0 else 0.0

    cum     = np.exp(r.cumsum())
    peak    = cum.cummax()
    max_dd  = ((cum - peak) / peak).min()

    return {
        "Name":         name,
        "Total Return": f"{(cum.iloc[-1] - 1) * 100:.1f}%",
        "Ann. Return":  f"{ann_ret * 100:.1f}%",
        "Ann. Vol":     f"{ann_vol * 100:.1f}%",
        "Sharpe":       f"{sharpe:.2f}",
        "Max DD":       f"{max_dd * 100:.1f}%",
    }


def run_backtest(symbol: str = "GOLDBEES", target_vol: float = 0.15):
    print(f"Risk Governor Backtest — {symbol}  (target vol: {target_vol*100:.0f}%)")

    # ── 1. Fetch OHLCV ────────────────────────────────────────────────────────
    client = clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        database=settings.clickhouse_database,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
    )
    df = client.query_df(f"""
        SELECT
            trade_date,
            toFloat64(argMax(open,   imported_at)) AS open,
            toFloat64(argMax(high,   imported_at)) AS high,
            toFloat64(argMax(low,    imported_at)) AS low,
            toFloat64(argMax(close,  imported_at)) AS close,
            toFloat64(argMax(volume, imported_at)) AS volume
        FROM market_data.daily_prices
        WHERE symbol = '{symbol}' AND category = 'etfs'
        GROUP BY trade_date
        ORDER BY trade_date ASC
    """)
    client.close()

    if df.empty:
        print(f"Error: No data found for {symbol} in category 'etfs'.")
        return

    df["trade_date"] = pd.to_datetime(df["trade_date"])  # date → datetime64 for GARCH

    # ── 2. GARCH anomaly pipeline ─────────────────────────────────────────────
    print("Computing GARCH volatility and regimes…")
    df_res, _, _ = run_composite_anomaly(df)

    # ── 3. Vectorised weights (O(n) numpy, not O(n) Python loop) ─────────────
    print("Applying Risk Governor logic…")
    df_res["weight"] = _vectorised_weights(
        df_res["garch_vol"], df_res["regime"], vol_target_pct=target_vol * 100
    )

    # Lag by 1 day: today's vol estimate → tomorrow's weight
    # fillna(1.0): warmup rows before first valid weight → treat as full exposure
    df_res["applied_weight"] = df_res["weight"].shift(1).fillna(1.0)

    # ── 4. Returns ────────────────────────────────────────────────────────────
    df_res["log_ret"]   = np.log(df_res["close"] / df_res["close"].shift(1))
    df_res["strat_ret"] = df_res["applied_weight"] * df_res["log_ret"]

    # ── 5. Metrics ────────────────────────────────────────────────────────────
    bh_m    = _metrics(df_res["log_ret"],   "Buy & Hold")
    strat_m = _metrics(df_res["strat_ret"], "Risk Governor")

    print("\n" + "=" * 56)
    print(f"{'Metric':<20} | {'Buy & Hold':>12} | {'Risk Governor':>12}")
    print("-" * 56)
    for key in ("Total Return", "Ann. Return", "Ann. Vol", "Sharpe", "Max DD"):
        print(f"{key:<20} | {bh_m[key]:>12} | {strat_m[key]:>12}")
    print("=" * 56)

    # Last valid weight (skip NaN warmup tail)
    last_valid = df_res["weight"].dropna()
    last_w = last_valid.iloc[-1] if not last_valid.empty else float("nan")
    print(f"\nBacktest period : {df_res['trade_date'].min().date()} → {df_res['trade_date'].max().date()}")
    print(f"Trading days    : {len(df_res):,}")
    print(f"Current weight  : {last_w * 100:.1f}%")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Risk Governor backtest")
    parser.add_argument("--symbol",     default="GOLDBEES")
    parser.add_argument("--target-vol", default=0.15, type=float,
                        help="Annualised vol target as decimal (default 0.15 = 15%%)")
    args = parser.parse_args()
    run_backtest(symbol=args.symbol, target_vol=args.target_vol)
