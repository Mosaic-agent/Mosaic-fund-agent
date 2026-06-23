#!/usr/bin/env python3
"""
src/scripts/portfolio/optimize_risk_governor.py
───────────────────────────────────────────────
Grid search optimizer for the Risk Governor parameters on GOLDBEES.
Sweeps volatility target, trend filter windows/multipliers, and drawdown brakes.
"""
import os
import sys
import warnings
import numpy as np
import pandas as pd

sys.path.insert(0, os.getcwd())
warnings.filterwarnings("ignore")

from src.ml.anomaly import run_composite_anomaly
from src.tools.risk_governor import _REGIME_MULT, _W_MAX
from config.settings import settings

def load_data(symbol="GOLDBEES"):
    from src.db.pool import get_client
    client = get_client()
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
        raise ValueError(f"No price data found for {symbol} in database.")
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    return df

def calculate_metrics(returns: pd.Series) -> tuple[float, float, float, float, float]:
    r = returns.dropna()
    if r.empty or r.std() == 0:
        return 0.0, 0.0, 0.0, 0.0, 0.0
    ann_ret = r.mean() * 252
    ann_vol = r.std() * np.sqrt(252)
    sharpe = ann_ret / ann_vol
    cum = np.exp(r.cumsum())
    peak = cum.cummax()
    max_dd = ((cum - peak) / peak).min()
    total_ret = (cum.iloc[-1] - 1)
    return total_ret, ann_ret, ann_vol, sharpe, max_dd

def main():
    print("Loading GOLDBEES data and computing GARCH/regimes...")
    df = load_data("GOLDBEES")
    df_res, _, _ = run_composite_anomaly(df)
    
    # Calculate returns
    df_res["log_ret"] = np.log(df_res["close"] / df_res["close"].shift(1))
    
    # Pre-calculate base series
    close_vals = df_res["close"].values
    log_ret_series = df_res["log_ret"]
    r = log_ret_series.values
    regime_mult = df_res["regime"].map(_REGIME_MULT).fillna(1.0).values.astype(float)
    garch_vol_vals = df_res["garch_vol"].fillna(15.0).values.astype(float)

    # Grids to sweep
    vol_targets = [12.0, 15.0, 18.0]
    trend_windows = [30, 50, 70, 100]
    trend_mults = [0.70, 0.75, 0.80, 0.90]
    dd_lookbacks = [10, 20, 30]
    dd_thresholds = [-0.05, -0.08, -0.10]
    dd_mults = [0.60, 0.70, 0.80]

    # Pre-calculate EMAs and Drawdowns to optimize loops
    emas = {}
    for tw in trend_windows:
        emas[tw] = df_res["close"].ewm(span=tw, adjust=False).mean().values

    dd_rolling = {}
    for dl in dd_lookbacks:
        dd_rolling[dl] = log_ret_series.rolling(dl).sum().values

    results = []

    print("Running grid search sweep...")
    for vt in vol_targets:
        # Base scaling
        vol_scaled = np.minimum(_W_MAX, vt / np.maximum(garch_vol_vals, 0.1))
        base_w = np.clip(vol_scaled * regime_mult, 0.0, _W_MAX)
        
        for tw in trend_windows:
            ema = emas[tw]
            t_under = close_vals < ema
            
            for tm_val in trend_mults:
                t_weight_mult = np.where(t_under, tm_val, 1.0)
                
                for dl in dd_lookbacks:
                    rolling_ret = dd_rolling[dl]
                    
                    for dt_val in dd_thresholds:
                        dd_under = rolling_ret < dt_val
                        
                        for dm_val in dd_mults:
                            d_weight_mult = np.where(dd_under, dm_val, 1.0)
                            
                            # Combine weights
                            w = base_w * t_weight_mult * d_weight_mult
                            w = np.clip(w, 0.0, _W_MAX)
                            
                            # Shift weight by 1 day (trade tomorrow using today's signal)
                            w_shifted = np.roll(w, 1)
                            w_shifted[0] = 1.0 # default warmup
                            
                            # Calculate strategy returns
                            strat_r = w_shifted * r
                            
                            total_ret, ann_ret, ann_vol, sharpe, max_dd = calculate_metrics(pd.Series(strat_r))
                            
                            results.append({
                                "vol_target": vt,
                                "trend_window": tw,
                                "trend_mult": tm_val,
                                "dd_lookback": dl,
                                "dd_threshold": dt_val,
                                "dd_mult": dm_val,
                                "total_ret": total_ret,
                                "ann_ret": ann_ret,
                                "ann_vol": ann_vol,
                                "sharpe": sharpe,
                                "max_dd": max_dd
                            })

    df_results = pd.DataFrame(results)
    
    # Sort by Sharpe ratio descending
    df_sorted = df_results.sort_values(by="sharpe", ascending=False)
    
    print("\n" + "="*80)
    print("  RISK GOVERNOR PARAMETER OPTIMISATION RESULTS (GOLDBEES)")
    print("="*80)
    print(f"  Total combinations tested: {len(df_results)}")
    
    # Benchmark metrics (Buy & Hold)
    bh_r = 1.0 * r
    bh_tot, bh_ann, bh_vol, bh_sharpe, bh_dd = calculate_metrics(pd.Series(bh_r))
    print(f"  Buy & Hold Benchmark  : Return={bh_tot*100:.1f}% | Vol={bh_vol*100:.1f}% | Sharpe={bh_sharpe:.2f} | MaxDD={bh_dd*100:.1f}%")
    
    # Show the top 10 configurations
    print("\nTop 10 configurations (sorted by Sharpe ratio):")
    print(f"{'Rank':<5} {'VolT':<5} {'TrW':<5} {'TrM':<5} {'DDB_L':<6} {'DDB_T':<6} {'DDB_M':<6} {'Return':<8} {'Vol':<8} {'Sharpe':<7} {'MaxDD':<7}")
    print("-" * 80)
    
    for idx, (_, row) in enumerate(df_sorted.head(10).iterrows(), 1):
        print(f"{idx:<5} {row['vol_target']:<5.0f} {row['trend_window']:<5.0f} {row['trend_mult']:<5.2f} {row['dd_lookback']:<6.0f} {row['dd_threshold']:<6.2f} {row['dd_mult']:<6.2f} {row['total_ret']*100:<7.1f}% {row['ann_vol']*100:<7.1f}% {row['sharpe']:<7.2f} {row['max_dd']*100:<6.1f}%")
        
if __name__ == "__main__":
    main()
