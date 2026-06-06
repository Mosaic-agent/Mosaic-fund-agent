import sys
import os
import pandas as pd
import numpy as np

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from src.db.pool import query_df
from src.ml.anomaly import robust_zscore, build_features, fit_garch_residuals, fit_isolation_forest, fit_change_points, run_composite_anomaly

def main():
    # 1. Fetch MSUMI daily prices
    symbol = "MSUMI"
    print(f"Fetching 1 year data for {symbol} from ClickHouse...")
    df = query_df(
        """
        SELECT trade_date,
               toFloat64(argMax(open,   imported_at)) AS open,
               toFloat64(argMax(high,   imported_at)) AS high,
               toFloat64(argMax(low,    imported_at)) AS low,
               toFloat64(argMax(close,  imported_at)) AS close,
               toFloat64(argMax(volume, imported_at)) AS volume
        FROM market_data.daily_prices FINAL
        WHERE symbol = {sym:String}
        GROUP BY trade_date ORDER BY trade_date ASC
        """,
        parameters={"sym": symbol}
    )
    
    if df.empty:
        print("No data found for MSUMI in ClickHouse!")
        return

    print(f"Successfully loaded {len(df)} rows. Date range: {df['trade_date'].min()} to {df['trade_date'].max()}")
    
    # Preprocess
    df = build_features(df)
    
    # 2. Run Robust Z-Score
    df["z_return"] = robust_zscore(df["daily_return"].fillna(0), window=30)
    df["z_range"]  = robust_zscore(df["range_pct"], window=30)
    df["z_robust"] = (df["z_return"].abs() + df["z_range"]) / 2.0
    df["z_volume"] = robust_zscore(df["volume"].fillna(0), window=30)
    
    robust_flagged = df[df["z_return"].abs() > 2.5]
    print(f"\n[Robust Z-Score (daily_return) > 2.5] Flagged {len(robust_flagged)} days:")
    for _, row in robust_flagged.iterrows():
        print(f"  - {row['trade_date']}: Close={row['close']:.2f}, Return={row['daily_return']:.2f}%, Z_return={row['z_return']:.2f}")

    # 3. Run GARCH(1,1) Volatility Normalized residuals
    df_garch, loglik = fit_garch_residuals(df)
    garch_flagged = df_garch[df_garch["z_resid"].abs() > 2.5]
    print(f"\n[GARCH(1,1) Residual Z > 2.5] Log-Likelihood: {loglik:.2f}. Flagged {len(garch_flagged)} days:")
    for _, row in garch_flagged.iterrows():
        print(f"  - {row['trade_date']}: Close={row['close']:.2f}, Return={row['daily_return']:.2f}%, GARCH Vol={row['garch_vol']:.2f}%, Residual Z={row['z_resid']:.2f}")

    # 4. Run Isolation Forest
    df_if = fit_isolation_forest(df, contamination=0.05)
    if_flagged = df_if[df_if["if_label"] == -1]
    print(f"\n[Isolation Forest (contamination=0.05)] Flagged {len(if_flagged)} days:")
    for _, row in if_flagged.iterrows():
        print(f"  - {row['trade_date']}: Close={row['close']:.2f}, Return={row['daily_return']:.2f}%, IF Confidence={row['if_confidence']:.2f}")

    # 5. Run PELT Change-Point Detection
    df_pelt = fit_change_points(df)
    pelt_breaks = df_pelt[df_pelt["is_changepoint"] == True]
    print(f"\n[PELT Change-Point Detection (variance/regime shifts)] Found {len(pelt_breaks)} breaks:")
    for _, row in pelt_breaks.iterrows():
        # Get local stats before/after break to show the shift
        idx = df.index[df['trade_date'] == row['trade_date']][0]
        pre_window = df.iloc[max(0, idx-10):idx]['daily_return']
        post_window = df.iloc[idx:min(len(df), idx+10)]['daily_return']
        print(f"  - {row['trade_date']}: Volatility Shift (10d Pre-std: {pre_window.std():.2f}%, 10d Post-std: {post_window.std():.2f}%)")

    # 6. Run Composite Pipeline (Robust Z * (1 + IF_Confidence) + PELT)
    df_comp, df_comp_flagged, _ = run_composite_anomaly(df)
    print(f"\n[Composite Pipeline (Z_threshold=2.5)] Flagged {len(df_comp_flagged)} days:")
    for _, row in df_comp_flagged.iterrows():
        print(f"  - {row['trade_date']}: Close={row['close']:.2f}, Return={row['daily_return']:.2f}%, Regime={row['regime']}, Final Z={row['final_z']:.2f}")

if __name__ == "__main__":
    main()
