import argparse
import os
import sys
import warnings
import numpy as np
import pandas as pd
import clickhouse_connect

sys.path.insert(0, os.getcwd())
warnings.filterwarnings("ignore")

from src.ml.anomaly import run_composite_anomaly
from src.tools.risk_governor import _REGIME_MULT, _W_MAX
from src.tools.adaptive_kelly import compute_kelly_weight
from config.settings import settings

HORIZON  = 5
FRACTION = 0.5

def _load_prices(client, symbol: str) -> pd.DataFrame:
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
        GROUP BY trade_date ORDER BY trade_date ASC
    """)
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    return df

def _load_predictions(client) -> pd.DataFrame:
    # ml_predictions table is only seeded for GOLDBEES
    try:
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
    except Exception:
        return pd.DataFrame()

def _vectorised_rg_weights(garch_vol, regime, close, vol_target):
    vol_arr     = garch_vol.fillna(vol_target).values.astype(float)
    regime_mult = regime.map(_REGIME_MULT).fillna(1.0).values.astype(float)
    ema50       = close.ewm(span=50, adjust=False).mean()
    trend_mult  = np.where(close.values < ema50.values, 0.75, 1.0)
    vol_scaled  = np.minimum(_W_MAX, vol_target / np.maximum(vol_arr, 0.1))
    return np.clip(vol_scaled * regime_mult * trend_mult, 0.0, _W_MAX)

def _kelly_weights_series(pred_df, price_df_res):
    if pred_df.empty:
        return pd.Series(np.nan, index=price_df_res.index)
    garch_map = price_df_res.set_index("trade_date")["garch_vol"].to_dict() if "garch_vol" in price_df_res.columns else {}
    kelly_map = {}
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

def _metrics(returns):
    r = returns.dropna()
    if r.empty:
        return {"total": 0.0, "ann_ret": 0.0, "ann_vol": 0.0, "sharpe": 0.0, "max_dd": 0.0, "n": 0}
    ann_ret = r.mean() * 252
    ann_vol = r.std()  * np.sqrt(252)
    sharpe  = ann_ret / ann_vol if ann_vol > 0 else 0.0
    cum     = np.exp(r.cumsum())
    peak    = cum.cummax()
    max_dd  = ((cum - peak) / peak).min()
    return {
        "total": (cum.iloc[-1] - 1) * 100,
        "ann_ret": ann_ret * 100,
        "ann_vol": ann_vol * 100,
        "sharpe": sharpe,
        "max_dd": max_dd * 100,
        "n": len(r)
    }

def main():
    parser = argparse.ArgumentParser(description="Sweep GARCH vol targets and Kelly blend weights")
    parser.add_argument("--symbol", default="GOLDBEES", help="Symbol to test")
    parser.add_argument("--months", default=96, type=int)
    args = parser.parse_args()

    client = clickhouse_connect.get_client(
        host=settings.clickhouse_host, port=settings.clickhouse_port,
        database=settings.clickhouse_database,
        username=settings.clickhouse_user, password=settings.clickhouse_password,
    )
    price_df = _load_prices(client, args.symbol)
    pred_df = _load_predictions(client) if args.symbol == "GOLDBEES" else pd.DataFrame()
    client.close()

    if price_df.empty:
        print(f"Error: No daily price data found for {args.symbol} in ClickHouse.")
        sys.exit(1)

    df_res, _, _ = run_composite_anomaly(price_df)

    # Pre-compute Kelly series on full history
    kelly_w = _kelly_weights_series(pred_df, df_res)
    df_res["w_kelly"] = kelly_w.values
    df_res["log_ret"] = np.log(df_res["close"] / df_res["close"].shift(1))
    df_res["w_kelly_lag"] = df_res["w_kelly"].shift(1)

    if args.months > 0:
        cutoff = df_res["trade_date"].max() - pd.DateOffset(months=args.months)
        ev = df_res[df_res["trade_date"] > cutoff].copy()
    else:
        ev = df_res.copy()

    kelly_available = ev["w_kelly_lag"].notna()
    ev_kelly = ev[kelly_available].copy()

    vol_targets = [10.0, 12.5, 15.0, 17.5, 20.0]
    blends      = [0.0, 0.25, 0.50, 0.75, 1.00]

    # --- 1. FULL HISTORY GARCH VOL TARGET SWEEP ---
    print("\n" + "="*95)
    print(f"  GARCH VOL TARGET SWEEP — FULL PERIOD ({args.symbol}, Last {args.months}m)")
    print(f"  Evaluation Window: {ev['trade_date'].min().date()} to {ev['trade_date'].max().date()}")
    print(f"  Total Trading Days: {ev.shape[0]}")
    print("="*95)
    print(f"{'Vol Target':<10} | {'Total Return':<12} | {'Ann. Return':<12} | {'Ann. Vol':<12} | {'Sharpe':<8} | {'Max DD':<10}")
    print("-" * 95)
    
    # Buy & Hold Benchmark
    m_bh = _metrics(ev["log_ret"])
    print(f"{'Buy & Hold':<10} | {m_bh['total']:>11.1f}% | {m_bh['ann_ret']:>11.1f}% | {m_bh['ann_vol']:>11.1f}% | {m_bh['sharpe']:>7.2f} | {m_bh['max_dd']:>9.1f}%")
    print("-" * 95)
    
    for vt in vol_targets:
        rg_w = _vectorised_rg_weights(df_res["garch_vol"], df_res["regime"], df_res["close"], vt)
        df_res["w_rg_temp"] = rg_w
        df_res["w_rg_temp_lag"] = df_res["w_rg_temp"].shift(1).fillna(1.0)
        w_rg_lag = df_res.loc[ev.index, "w_rg_temp_lag"]
        
        m = _metrics(w_rg_lag * ev["log_ret"])
        print(f"vt {vt:>5.1f}% | {m['total']:>11.1f}% | {m['ann_ret']:>11.1f}% | {m['ann_vol']:>11.1f}% | {m['sharpe']:>7.2f} | {m['max_dd']:>9.1f}%")
    print("="*95)

    # --- 2. 2D GRID SEARCH (PREDICTION DAYS ONLY) ---
    if not ev_kelly.empty:
        print("\n" + "="*95)
        print(f"  2D GRID SEARCH: GARCH VOL TARGET vs. KELLY BLEND WEIGHT ({args.symbol}, Last {args.months}m)")
        print(f"  Prediction Days: {ev_kelly.shape[0]}")
        print("="*95)
        print(f"{'Vol Target':<10} | {'Blend % (Kelly)':<15} | {'Total Return':<12} | {'Ann. Return':<12} | {'Ann. Vol':<12} | {'Sharpe':<8} | {'Max DD':<10}")
        print("-" * 95)

        for vt in vol_targets:
            rg_w = _vectorised_rg_weights(df_res["garch_vol"], df_res["regime"], df_res["close"], vt)
            df_res["w_rg_temp"] = rg_w
            df_res["w_rg_temp_lag"] = df_res["w_rg_temp"].shift(1).fillna(1.0)
            w_rg_lag = df_res.loc[ev_kelly.index, "w_rg_temp_lag"]
            
            for b in blends:
                w_blended = w_rg_lag * (1 - b) + ev_kelly["w_kelly_lag"] * b
                m = _metrics(w_blended * ev_kelly["log_ret"])
                print(f"{vt:>9.1f}% | {b*100:>13.0f}% | {m['total']:>11.1f}% | {m['ann_ret']:>11.1f}% | {m['ann_vol']:>11.1f}% | {m['sharpe']:>7.2f} | {m['max_dd']:>9.1f}%")
            print("-" * 95)
        print("="*95)
    else:
        print(f"\n[Note] Kelly blend grid search skipped for {args.symbol} (no stored ML predictions available).")

if __name__ == "__main__":
    main()
