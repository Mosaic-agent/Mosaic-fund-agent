"""
tests/_backtest_risk_governor_optimised.py
──────────────────────────────────────────
Side-by-side comparison of four weighting strategies over the last N months.

Strategies
──────────
  1. Buy & Hold          — 100% weight every day
  2. Risk Governor       — existing: inverse-vol + regime (no extras)
  3. RG + Trend Filter   — add 0.75× multiplier when close < EMA(50)
  4. RG + DD Brake       — add 0.70× multiplier when 20d rolling return < -10%
  5. RG + Both           — trend filter AND drawdown brake combined

Failure modes being fixed
─────────────────────────
  • "Slow bleed" (NIFTYBEES, MID150BEES): price drifts down on low vol →
    GARCH never spikes → RG stays near 100% → no protection.
    Fix: trend filter catches gradual declines via EMA.

  • "Over-sizing in volatile declines" (SILVERBEES): vol is high but fell
    fast enough that GARCH lags. DD brake reduces exposure after a -10% 20d loss.

Per-asset vol targets tested
─────────────────────────────
  Gold/Silver: 15%  (calibrated on GARCH p50 2016-2026)
  Equity ETFs: 20%  (structurally higher vol → less aggressive de-risking)
"""
from __future__ import annotations

import argparse
import os
import sys
import warnings

import clickhouse_connect
import numpy as np
import pandas as pd

sys.path.insert(0, os.getcwd())

from src.ml.anomaly import run_composite_anomaly
from src.tools.risk_governor import _REGIME_MULT, _W_MAX
from config.settings import settings

warnings.filterwarnings("ignore")

# Per-asset-class vol targets
_EQUITY_ETFS = {"NIFTYBEES", "BANKBEES", "ITBEES", "PSUBNKBEES", "MID150BEES",
                "SMALL250", "JUNIORBEES", "MONIFTY500", "AUTOBEES", "PHARMABEES",
                "CPSEETF", "ICICIB22"}
_INTL_ETFS   = {"MON100", "MAFANG", "HNGSNGBEES", "MAHKTECH", "MASPTOP50"}

_VOL_TARGETS = {
    "gold":   15.0,
    "equity": 20.0,
    "intl":   18.0,
}

def _vol_target_for(symbol: str) -> float:
    if symbol in _EQUITY_ETFS:
        return _VOL_TARGETS["equity"]
    if symbol in _INTL_ETFS:
        return _VOL_TARGETS["intl"]
    return _VOL_TARGETS["gold"]


# ── Weight builders ────────────────────────────────────────────────────────────

def _base_weights(garch_vol: pd.Series, regime: pd.Series,
                  vol_target: float) -> np.ndarray:
    vol_arr     = garch_vol.fillna(vol_target).values.astype(float)
    regime_mult = regime.map(_REGIME_MULT).fillna(1.0).values.astype(float)
    vol_scaled  = np.minimum(_W_MAX, vol_target / np.maximum(vol_arr, 0.1))
    return np.clip(vol_scaled * regime_mult, 0.0, _W_MAX)


def _trend_mult(close: pd.Series, window: int = 50) -> np.ndarray:
    """0.75 when close < EMA(50), else 1.0 — catches slow bleeds."""
    ema = close.ewm(span=window, adjust=False).mean()
    return np.where(close.values < ema.values, 0.75, 1.0)


def _drawdown_mult(log_ret: pd.Series, lookback: int = 20,
                   threshold: float = -0.10) -> np.ndarray:
    """0.70 when 20-day rolling log return < -10% — catches medium-speed declines."""
    rolling_ret = log_ret.rolling(lookback).sum()
    return np.where(rolling_ret.values < threshold, 0.70, 1.0)


# ── Metrics ────────────────────────────────────────────────────────────────────

def _metrics(returns: pd.Series, name: str) -> dict:
    r = returns.dropna()
    if r.empty:
        return {"Name": name, "Total Return": "N/A", "Ann. Return": "N/A",
                "Ann. Vol": "N/A", "Sharpe": "N/A", "Max DD": "N/A"}
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


# ── Main ───────────────────────────────────────────────────────────────────────

def run_comparison(symbol: str = "GOLDBEES", eval_months: int = 3):
    vol_target = _vol_target_for(symbol)
    print(f"\n{'='*66}")
    print(f"  {symbol}  |  eval: last {eval_months}m  |  vol target: {vol_target:.0f}%")
    print(f"{'='*66}")

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
        print(f"  No data for {symbol}"); return

    df["trade_date"] = pd.to_datetime(df["trade_date"])
    print("  Computing GARCH…", end=" ", flush=True)
    df_res, _, _ = run_composite_anomaly(df)
    print("done")

    df_res["log_ret"] = np.log(df_res["close"] / df_res["close"].shift(1))

    # Build all weight variants on full history, then slice eval window
    base   = _base_weights(df_res["garch_vol"], df_res["regime"], vol_target)
    t_mult = _trend_mult(df_res["close"])
    d_mult = _drawdown_mult(df_res["log_ret"])

    df_res["w_bh"]    = 1.0
    df_res["w_rg"]    = base
    df_res["w_trend"] = np.clip(base * t_mult, 0.0, _W_MAX)
    df_res["w_dd"]    = np.clip(base * d_mult, 0.0, _W_MAX)
    df_res["w_both"]  = np.clip(base * t_mult * d_mult, 0.0, _W_MAX)

    # Lag by 1 day: today's signal → tomorrow's weight
    for col in ["w_rg", "w_trend", "w_dd", "w_both"]:
        df_res[col] = df_res[col].shift(1).fillna(1.0)

    # Slice to eval window
    if eval_months > 0:
        cutoff = df_res["trade_date"].max() - pd.DateOffset(months=eval_months)
        ev = df_res[df_res["trade_date"] > cutoff].copy()
    else:
        ev = df_res.copy()

    r = ev["log_ret"]
    strategies = [
        ("Buy & Hold",       ev["w_bh"]    * r),
        ("RG (current)",     ev["w_rg"]    * r),
        ("RG + Trend",       ev["w_trend"] * r),
        ("RG + DD Brake",    ev["w_dd"]    * r),
        ("RG + Both",        ev["w_both"]  * r),
    ]

    print(f"  Period: {ev['trade_date'].min().date()} → {ev['trade_date'].max().date()}  "
          f"({len(ev)} trading days)\n")

    keys = ("Total Return", "Ann. Vol", "Sharpe", "Max DD")
    header = f"  {'Strategy':<18}" + "".join(f" {k:>13}" for k in keys)
    print(header)
    print("  " + "-" * (18 + 13 * len(keys)))

    for name, ret_series in strategies:
        m = _metrics(ret_series, name)
        row = f"  {name:<18}" + "".join(f" {m[k]:>13}" for k in keys)
        print(row)

    # Show average weights in eval window
    print(f"\n  Avg weight in eval window:")
    for col, label in [("w_rg","RG"), ("w_trend","RG+Trend"),
                       ("w_dd","RG+DD"), ("w_both","RG+Both")]:
        print(f"    {label:<12} {ev[col].mean()*100:.1f}%")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Risk Governor optimisation comparison")
    parser.add_argument("--symbols", default="GOLDBEES,NIFTYBEES,SILVERBEES,ITBEES,MID150BEES,MAHKTECH")
    parser.add_argument("--months",  default=3, type=int)
    args = parser.parse_args()

    for sym in [s.strip() for s in args.symbols.split(",") if s.strip()]:
        run_comparison(sym, args.months)
