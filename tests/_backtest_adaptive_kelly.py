"""
tests/_backtest_adaptive_kelly.py
───────────────────────────────────
Walk-forward backtest comparing four position-sizing strategies for GOLDBEES.

Strategies
──────────
  1. Buy & Hold        — 100% every day
  2. RG                — inverse-vol + regime + trend filter (rule-based)
  3. Kelly             — half-Kelly from ML expected return + quantile vol
  4. Blended 50/50     — 50% RG + 50% Kelly

Data sources
────────────
  - daily_prices (ClickHouse) — OHLCV for GOLDBEES
  - ml_predictions (ClickHouse) — LightGBM expected_return_pct + confidence bounds

Walk-forward design
───────────────────
  For each trading day that has a matching ml_predictions row:
    1. Compute RG weight using GARCH vol and regime from run_composite_anomaly
    2. Compute Kelly weight from that day's ML prediction
    3. Compute 50/50 blend
    4. Apply that weight to the realised next-horizon_days log return

  GARCH is fit once on full history (same limitation noted in _backtest_risk_governor.py).
  ML predictions used are the historical ones stored in ml_predictions — not refit each day.
  This means if you've only run the predictor on recent dates, older days will have no match.

Usage
─────
  .venv/bin/python3 tests/_backtest_adaptive_kelly.py
  .venv/bin/python3 tests/_backtest_adaptive_kelly.py --months 6
  .venv/bin/python3 tests/_backtest_adaptive_kelly.py --months 12 --save
"""
from __future__ import annotations

import argparse
import os
import sys
import warnings
from datetime import date

import clickhouse_connect
import numpy as np
import pandas as pd

sys.path.insert(0, os.getcwd())
warnings.filterwarnings("ignore")

from src.ml.anomaly import run_composite_anomaly
from src.tools.risk_governor import _REGIME_MULT, _W_MAX, vol_target_for
from src.tools.adaptive_kelly import compute_kelly_weight, compute_blended_weight, _CV_R2_MIN, _CV_R2_HAIRCUT
from config.settings import settings

SYMBOL   = "GOLDBEES"
HORIZON  = 5          # days — must match ml_predictions.horizon_days
FRACTION = 0.5        # half-Kelly
BLEND    = 0.5        # 50/50 blend


def _load_prices(client) -> pd.DataFrame:
    df = client.query_df(f"""
        SELECT
            trade_date,
            toFloat64(argMax(open,   imported_at)) AS open,
            toFloat64(argMax(high,   imported_at)) AS high,
            toFloat64(argMax(low,    imported_at)) AS low,
            toFloat64(argMax(close,  imported_at)) AS close,
            toFloat64(argMax(volume, imported_at)) AS volume
        FROM market_data.daily_prices
        WHERE symbol = '{SYMBOL}' AND category = 'etfs'
        GROUP BY trade_date ORDER BY trade_date ASC
    """)
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    return df


def _load_predictions(client) -> pd.DataFrame:
    df = client.query_df("""
        SELECT
            as_of,
            expected_return_pct,
            confidence_low,
            confidence_high,
            cv_r2_mean,
            horizon_days
        FROM market_data.ml_predictions FINAL
        ORDER BY as_of ASC
    """)
    df["as_of"] = pd.to_datetime(df["as_of"])
    return df


def _vectorised_rg_weights(
    garch_vol: pd.Series,
    regime: pd.Series,
    close: pd.Series,
    vol_target: float,
) -> np.ndarray:
    """RG weights: inverse-vol × regime × trend filter (EMA50)."""
    vol_arr     = garch_vol.fillna(vol_target).values.astype(float)
    regime_mult = regime.map(_REGIME_MULT).fillna(1.0).values.astype(float)
    ema50       = close.ewm(span=50, adjust=False).mean()
    trend_mult  = np.where(close.values < ema50.values, 0.75, 1.0)

    vol_scaled  = np.minimum(_W_MAX, vol_target / np.maximum(vol_arr, 0.1))
    return np.clip(vol_scaled * regime_mult * trend_mult, 0.0, _W_MAX)


def _kelly_weights_series(
    pred_df: pd.DataFrame,
    price_df_res: pd.DataFrame,
) -> pd.Series:
    """
    Compute Kelly weights for each trading day that has a matching ML prediction.
    Returns a Series aligned to price_df_res index (NaN for days without a prediction).

    Uses the GARCH vol from price_df_res for the matching day so Kelly's σ is
    asset-return vol rather than the model's prediction-interval span.
    """
    # Build lookup: date → annualised GARCH vol (%) on that day
    garch_map = (
        price_df_res.set_index("trade_date")["garch_vol"].to_dict()
        if "garch_vol" in price_df_res.columns else {}
    )

    kelly_map: dict = {}
    for _, r in pred_df.iterrows():
        garch_v = garch_map.get(r["as_of"])
        dec = compute_kelly_weight(
            expected_return_pct  = float(r["expected_return_pct"]),
            confidence_low_pct   = float(r["confidence_low"]),
            confidence_high_pct  = float(r["confidence_high"]),
            horizon_days         = int(r["horizon_days"]),
            cv_r2                = float(r["cv_r2_mean"]),
            fraction             = FRACTION,
            garch_annual_vol_pct = float(garch_v) if garch_v is not None and pd.notna(garch_v) else None,
        )
        kelly_map[r["as_of"]] = dec.final_weight

    return price_df_res["trade_date"].map(kelly_map)


def _metrics(returns: pd.Series, name: str) -> dict:
    r = returns.dropna()
    if r.empty:
        return {k: "N/A" for k in ("Name","Total Return","Ann. Return","Ann. Vol","Sharpe","Max DD","N")}
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
        "N":            str(len(r)),
    }


def run_backtest(eval_months: int = 0, save: bool = False):
    client = clickhouse_connect.get_client(
        host=settings.clickhouse_host, port=settings.clickhouse_port,
        database=settings.clickhouse_database,
        username=settings.clickhouse_user, password=settings.clickhouse_password,
    )

    print("Loading price data…", end=" ", flush=True)
    price_df = _load_prices(client)
    print(f"{len(price_df)} rows")

    print("Loading ML predictions…", end=" ", flush=True)
    pred_df = _load_predictions(client)
    print(f"{len(pred_df)} rows")
    client.close()

    if price_df.empty:
        print(f"No price data for {SYMBOL}"); return
    if pred_df.empty:
        print("No ML predictions found. Run the trend predictor first."); return

    print("Computing GARCH volatility…", end=" ", flush=True)
    df_res, _, _ = run_composite_anomaly(price_df)
    print("done")

    vol_target = vol_target_for(SYMBOL)
    price_dates = df_res["trade_date"]

    # Build weight series (on full history, then slice eval window)
    rg_w = _vectorised_rg_weights(
        df_res["garch_vol"], df_res["regime"], df_res["close"], vol_target
    )
    df_res["w_bh"]    = 1.0
    df_res["w_rg"]    = rg_w

    print("Computing Kelly weights…", end=" ", flush=True)
    kelly_w = _kelly_weights_series(pred_df, df_res)
    df_res["w_kelly"]   = kelly_w.values
    df_res["w_blended"] = np.where(
        df_res["w_kelly"].isna(),
        df_res["w_rg"],
        df_res.apply(
            lambda r: compute_blended_weight(r["w_rg"], r["w_kelly"], BLEND),
            axis=1,
        )
    )
    print("done")

    # Lag by 1 day + log returns. RG/blended fall back to B&H (=1) on day 0;
    # Kelly stays NaN on days without an ML prediction so it's excluded from
    # Kelly-only metrics (no forward-filling stale signals).
    df_res["log_ret"]   = np.log(df_res["close"] / df_res["close"].shift(1))
    df_res["w_rg"]      = df_res["w_rg"].shift(1).fillna(1.0)
    df_res["w_blended"] = df_res["w_blended"].shift(1).fillna(1.0)
    df_res["w_kelly"]   = df_res["w_kelly"].shift(1)  # NaN preserved

    # Slice eval window
    if eval_months > 0:
        cutoff = df_res["trade_date"].max() - pd.DateOffset(months=eval_months)
        ev = df_res[df_res["trade_date"] > cutoff].copy()
    else:
        ev = df_res.copy()

    # Restrict to days that had a Kelly prediction (meaningful comparison)
    kelly_available = ev["w_kelly"].notna()
    ev_kelly = ev[kelly_available].copy()

    r = ev["log_ret"]
    strategies_all = [
        ("Buy & Hold", ev["w_bh"]    * r),
        ("RG",         ev["w_rg"]    * r),
    ]
    strategies_kelly = [
        ("Buy & Hold", ev_kelly["w_bh"]    * ev_kelly["log_ret"]),
        ("RG",         ev_kelly["w_rg"]    * ev_kelly["log_ret"]),
        ("Kelly",      ev_kelly["w_kelly"] * ev_kelly["log_ret"]),
        ("Blended 50", ev_kelly["w_blended"] * ev_kelly["log_ret"]),
    ]

    label = f"last {eval_months}m" if eval_months else "full history"
    print(f"\n{'='*70}")
    print(f"  {SYMBOL} — Adaptive Kelly Backtest  ({label})")
    print(f"  Eval: {ev['trade_date'].min().date()} → {ev['trade_date'].max().date()}")
    print(f"  Days with ML predictions: {kelly_available.sum()} / {len(ev)}")
    print(f"{'='*70}")

    keys = ("Total Return", "Ann. Return", "Ann. Vol", "Sharpe", "Max DD", "N")
    header = f"\n  {'Strategy':<14}" + "".join(f" {k:>13}" for k in keys)
    print("\n[Full period — RG vs B&H on all days]")
    print(header); print("  " + "-" * (14 + 13*len(keys)))
    for name, ret_s in strategies_all:
        m = _metrics(ret_s, name)
        print(f"  {name:<14}" + "".join(f" {m[k]:>13}" for k in keys))

    print("\n[ML prediction days only — all 4 methods]")
    print(header); print("  " + "-" * (14 + 13*len(keys)))
    for name, ret_s in strategies_kelly:
        m = _metrics(ret_s, name)
        print(f"  {name:<14}" + "".join(f" {m[k]:>13}" for k in keys))

    print(f"\n  vol target: {vol_target:.0f}%  |  fraction: {FRACTION:.0%}  |  blend: {BLEND:.0%}")

    # ── Save simulation decisions to weight_checkpoints ───────────────────────
    if save and not ev_kelly.empty:
        from src.tools.weight_checkpoint import save_checkpoints
        rows = []
        for _, r_ in ev_kelly.iterrows():
            d = r_["trade_date"]
            as_of = d.date() if hasattr(d, "date") else d
            for method, w in [("rg", r_["w_rg"]), ("kelly", r_["w_kelly"]),
                               ("blended_50", r_["w_blended"])]:
                rows.append({
                    "as_of": as_of, "symbol": SYMBOL, "method": method,
                    "recommended_weight": float(w),
                    "horizon_days": HORIZON,
                    "regime": str(r_.get("regime", "")),
                    "rationale": "backtest_replay",
                })
        n = save_checkpoints(rows)
        print(f"\n  ✓ Saved {n} backtest checkpoint rows to ClickHouse")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Adaptive Kelly backtest")
    parser.add_argument("--months", default=6, type=int,
                        help="Evaluate over last N months (0 = full history)")
    parser.add_argument("--save", action="store_true",
                        help="Persist simulation decisions to weight_checkpoints table")
    args = parser.parse_args()
    run_backtest(eval_months=args.months, save=args.save)
