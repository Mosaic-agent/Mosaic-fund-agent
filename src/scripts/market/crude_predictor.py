"""
src/scripts/market/crude_predictor.py
──────────────────────────────────────
LightGBM Walk-Forward Multi-Horizon Return & Volatility Predictor for CRUDE OIL.
Uses exact quantitative modeling: GARCH(1,1) conditional vol + Quantile Regression.
"""

from __future__ import annotations
import sys
import os
from pathlib import Path
import numpy as np
import pandas as pd
import lightgbm as lgb
from sklearn.model_selection import TimeSeriesSplit
from arch import arch_model
import plotext as plt

# Add project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))
from src.db.pool import get_client

def load_data():
    client = get_client()
    
    # 1. Crude oil daily prices
    df_crude = client.query_df("""
        SELECT trade_date,
               toFloat64(argMax(open, imported_at)) as open,
               toFloat64(argMax(high, imported_at)) as high,
               toFloat64(argMax(low, imported_at)) as low,
               toFloat64(argMax(close, imported_at)) as close,
               toFloat64(argMax(volume, imported_at)) as volume
        FROM market_data.daily_prices FINAL
        WHERE symbol = 'CRUDEOIL' AND category = 'commodities'
        GROUP BY trade_date
        ORDER BY trade_date ASC
    """)
    df_crude["trade_date"] = pd.to_datetime(df_crude["trade_date"])
    df_crude = df_crude.sort_values("trade_date").reset_index(drop=True)
    
    # 2. USDINR
    df_fx = client.query_df("""
        SELECT trade_date, toFloat64(close) as usdinr
        FROM market_data.fx_rates FINAL
        WHERE symbol = 'USDINR'
        ORDER BY trade_date ASC
    """)
    df_fx["trade_date"] = pd.to_datetime(df_fx["trade_date"])
    
    # 3. DXY
    df_dxy = client.query_df("""
        SELECT trade_date, toFloat64(argMax(close, imported_at)) as dxy
        FROM market_data.daily_prices FINAL
        WHERE symbol = 'DXY'
        GROUP BY trade_date
        ORDER BY trade_date ASC
    """)
    df_dxy["trade_date"] = pd.to_datetime(df_dxy["trade_date"])
    
    # Merge
    df = pd.merge(df_crude, df_fx, on="trade_date", how="left")
    df = pd.merge(df, df_dxy, on="trade_date", how="left")
    df["usdinr"] = df["usdinr"].ffill()
    df["dxy"] = df["dxy"].ffill()
    
    return df

def build_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    c = df["close"]
    
    # Log returns
    df["f_logret1"] = np.log(c / c.shift(1))
    df["f_logret5"] = np.log(c / c.shift(5))
    df["f_logret20"] = np.log(c / c.shift(20))
    
    # Moving average & EMA crossovers
    ema3 = c.ewm(span=3, adjust=False).mean()
    ema9 = c.ewm(span=9, adjust=False).mean()
    ema20 = c.ewm(span=20, adjust=False).mean()
    sma20 = c.rolling(20).mean()
    sma50 = c.rolling(50).mean()
    sma200 = c.rolling(200).mean()
    
    df["f_ema_cross39"] = (ema3 / ema9) - 1.0
    df["f_ema_cross920"] = (ema9 / ema20) - 1.0
    df["f_ma20_ratio"] = (c / sma20) - 1.0
    df["f_ma50_ratio"] = (c / sma50) - 1.0
    df["f_ma200_ratio"] = (c / sma200) - 1.0
    
    # Volatility & ATR
    tr1 = df["high"] - df["low"]
    tr2 = (df["high"] - df["close"].shift(1)).abs()
    tr3 = (df["low"] - df["close"].shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    df["f_atr14_pct"] = (tr.rolling(14).mean() / c) * 100.0
    df["f_hvol10"] = df["f_logret1"].rolling(10).std() * np.sqrt(252) * 100.0
    df["f_hvol20"] = df["f_logret1"].rolling(20).std() * np.sqrt(252) * 100.0
    
    # RSI 14
    delta = c.diff()
    gain = (delta.where(delta > 0, 0)).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    rs = gain / loss.replace(0, np.nan)
    df["f_rsi14"] = 100 - (100 / (1 + rs))
    
    # Cross-assets
    if "usdinr" in df.columns:
        df["f_usdinr_ret5"] = np.log(df["usdinr"] / df["usdinr"].shift(5))
    else:
        df["f_usdinr_ret5"] = 0.0
        
    if "dxy" in df.columns:
        df["f_dxy_ret5"] = np.log(df["dxy"] / df["dxy"].shift(5))
    else:
        df["f_dxy_ret5"] = 0.0
        
    # Seasonality
    dt = df["trade_date"].dt
    df["f_month_sin"] = np.sin(2 * np.pi * dt.month / 12)
    df["f_month_cos"] = np.cos(2 * np.pi * dt.month / 12)
    df["f_dow_sin"] = np.sin(2 * np.pi * dt.dayofweek / 5)
    df["f_dow_cos"] = np.cos(2 * np.pi * dt.dayofweek / 5)
    
    # Forward targets
    df["target_5d"] = (c.shift(-5) / c) - 1.0
    df["target_20d"] = (c.shift(-20) / c) - 1.0
    df["target_5d_up"] = (df["target_5d"] > 0).astype(int)
    
    return df

def run_prediction():
    df_raw = load_data()
    df = build_features(df_raw)
    
    feature_cols = [c for c in df.columns if c.startswith("f_")]
    
    # Training set (drop rows with NaN targets for evaluation)
    clean_df = df.dropna(subset=feature_cols + ["target_5d"]).reset_index(drop=True)
    
    X = clean_df[feature_cols]
    y_reg = clean_df["target_5d"]
    y_clf = clean_df["target_5d_up"]
    
    # 1. Walk-forward cross validation
    tscv = TimeSeriesSplit(n_splits=5, gap=10)
    hit_rates = []
    r2_scores = []
    
    for train_idx, test_idx in tscv.split(X):
        X_tr, X_te = X.iloc[train_idx], X.iloc[test_idx]
        y_tr_reg, y_te_reg = y_reg.iloc[train_idx], y_reg.iloc[test_idx]
        y_tr_clf, y_te_clf = y_clf.iloc[train_idx], y_clf.iloc[test_idx]
        
        clf = lgb.LGBMClassifier(n_estimators=100, learning_rate=0.03, max_depth=3, random_state=42, verbose=-1)
        clf.fit(X_tr, y_tr_clf)
        preds_clf = clf.predict(X_te)
        hit_rates.append((preds_clf == y_te_clf).mean())
        
        reg = lgb.LGBMRegressor(n_estimators=100, learning_rate=0.03, max_depth=3, random_state=42, verbose=-1)
        reg.fit(X_tr, y_tr_reg)
        preds_reg = reg.predict(X_te)
        ss_tot = np.sum((y_te_reg - y_te_reg.mean())**2)
        ss_res = np.sum((y_te_reg - preds_reg)**2)
        r2 = 1 - (ss_res / ss_tot) if ss_tot != 0 else 0
        r2_scores.append(r2)
        
    avg_hit_rate = np.mean(hit_rates) * 100.0
    avg_r2 = np.mean(r2_scores)
    
    # 2. Fit Full Models for current inference
    latest_row = df.iloc[-1]
    latest_X = pd.DataFrame([latest_row[feature_cols].to_dict()])
    
    # Probability Up
    full_clf = lgb.LGBMClassifier(n_estimators=120, learning_rate=0.03, max_depth=3, random_state=42, verbose=-1)
    full_clf.fit(X, y_clf)
    prob_up = full_clf.predict_proba(latest_X)[0][1]
    
    # Quantile Models: 10th percentile, 50th (median), 90th percentile
    q10 = lgb.LGBMRegressor(objective="quantile", alpha=0.10, n_estimators=120, learning_rate=0.03, max_depth=3, random_state=42, verbose=-1)
    q10.fit(X, y_reg)
    pred_q10 = q10.predict(latest_X)[0] * 100.0
    
    q50 = lgb.LGBMRegressor(objective="quantile", alpha=0.50, n_estimators=120, learning_rate=0.03, max_depth=3, random_state=42, verbose=-1)
    q50.fit(X, y_reg)
    pred_q50 = q50.predict(latest_X)[0] * 100.0
    
    q90 = lgb.LGBMRegressor(objective="quantile", alpha=0.90, n_estimators=120, learning_rate=0.03, max_depth=3, random_state=42, verbose=-1)
    q90.fit(X, y_reg)
    pred_q90 = q90.predict(latest_X)[0] * 100.0
    
    # 20-day model
    clean_20d = df.dropna(subset=feature_cols + ["target_20d"]).reset_index(drop=True)
    q50_20d = lgb.LGBMRegressor(objective="quantile", alpha=0.50, n_estimators=120, learning_rate=0.03, max_depth=3, random_state=42, verbose=-1)
    q50_20d.fit(clean_20d[feature_cols], clean_20d["target_20d"])
    pred_20d = q50_20d.predict(latest_X)[0] * 100.0
    
    # 3. GARCH(1,1) Volatility Modeling
    returns = clean_df["f_logret1"].dropna() * 100.0
    am = arch_model(returns, vol="Garch", p=1, q=1, dist="Normal")
    res = am.fit(disp="off")
    garch_forecast = res.forecast(horizon=5)
    garch_vol_5d = np.sqrt(garch_forecast.variance.iloc[-1].mean() * 252)
    current_garch_vol = np.sqrt(res.conditional_volatility.iloc[-1]**2 * 252)
    
    # Current price stats
    cur_price = latest_row["close"]
    p_q10 = cur_price * (1 + pred_q10 / 100.0)
    p_q50 = cur_price * (1 + pred_q50 / 100.0)
    p_q90 = cur_price * (1 + pred_q90 / 100.0)
    p_20d = cur_price * (1 + pred_20d / 100.0)
    
    # Key levels
    recent_high = df["high"].iloc[-60:].max()
    recent_low = df["low"].iloc[-60:].min()
    fib_382 = recent_high - 0.382 * (recent_high - recent_low)
    fib_500 = recent_high - 0.500 * (recent_high - recent_low)
    fib_618 = recent_high - 0.618 * (recent_high - recent_low)
    
    print("=== CRUDE OIL PREDICTION REPORT ===")
    print(f"Current Date: {latest_row['trade_date'].strftime('%Y-%m-%d')}")
    print(f"Current Close: ${cur_price:.2f}")
    print(f"5-Day Probability Up: {prob_up:.4f} ({prob_up*100:.1f}%)")
    print(f"5-Day Expected Return (Median): {pred_q50:+.2f}% -> Target: ${p_q50:.2f}")
    print(f"5-Day 80% Confidence Interval: [{pred_q10:+.2f}%, {pred_q90:+.2f}%] -> [${p_q10:.2f}, ${p_q90:.2f}]")
    print(f"20-Day Expected Return (1-Month): {pred_20d:+.2f}% -> Target: ${p_20d:.2f}")
    print(f"Current GARCH(1,1) Volatility: {current_garch_vol:.2f}%")
    print(f"5-Day Forward GARCH Volatility: {garch_vol_5d:.2f}%")
    print(f"CV Hit Rate (Directional Accuracy): {avg_hit_rate:.1f}%")
    print(f"Recent 60D High: ${recent_high:.2f} | 60D Low: ${recent_low:.2f}")
    print(f"Fibonacci 38.2% Retracement: ${fib_382:.2f}")
    print(f"Fibonacci 50.0% Retracement: ${fib_500:.2f}")
    print(f"Fibonacci 61.8% Retracement: ${fib_618:.2f}")
    print(f"RSI(14): {latest_row['f_rsi14']:.2f}")
    print(f"20-Day SMA: ${cur_price / (1 + latest_row['f_ma20_ratio']):.2f}")
    print(f"50-Day SMA: ${cur_price / (1 + latest_row['f_ma50_ratio']):.2f}")
    
    # Build a visual forecast band chart using plotext
    hist_prices = df["close"].iloc[-30:].tolist()
    hist_x = list(range(len(hist_prices)))
    
    fwd_x = [len(hist_prices) - 1, len(hist_prices) + 4]
    fwd_med = [cur_price, p_q50]
    fwd_hi = [cur_price, p_q90]
    fwd_lo = [cur_price, p_q10]
    
    plt.clf()
    plt.plotsize(80, 16)
    plt.plot(hist_x, hist_prices, label="Historical Close ($)")
    plt.plot(fwd_x, fwd_med, label="5D Median Forecast")
    plt.plot(fwd_x, fwd_hi, label="5D 90th Pct (Upside)")
    plt.plot(fwd_x, fwd_lo, label="5D 10th Pct (Downside)")
    plt.title("CRUDE OIL: 30-Day Historical + 5-Day Forward ML Forecast Band")
    plt.theme("clear")
    plt.grid(True, True)
    print("=== FORECAST BAND CHART ===")
    print(plt.build())

if __name__ == "__main__":
    run_prediction()
