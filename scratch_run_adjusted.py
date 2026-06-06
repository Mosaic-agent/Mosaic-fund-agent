import sys
import os
import pandas as pd

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from src.db.pool import query_df
from src.ml.anomaly import build_features, run_composite_anomaly

def main():
    symbol = "MSUMI"
    print("Loading MSUMI data...")
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
        print("No data found!")
        return

    df = build_features(df)
    
    # Run composite pipeline with adjusted parameters
    z_thresh = 3.0
    contamination = 0.03
    print(f"\nRunning composite anomaly detection with z_threshold={z_thresh} and contamination={contamination}...")
    df_result, df_flagged, _ = run_composite_anomaly(
        df,
        contamination=contamination,
        z_threshold=z_thresh
    )
    
    print(f"\nFlagged {len(df_flagged)} days (down from 17 days):")
    df_flagged = df_flagged.sort_values("trade_date", ascending=False)
    for _, row in df_flagged.iterrows():
        print(f"  - {row['trade_date'].strftime('%Y-%m-%d')}: Close={row['close']:.2f}, Return={row['daily_return']:+.2f}%, Regime={row['regime']}, Final Z={row['final_z']:+.2f}")

if __name__ == "__main__":
    main()
