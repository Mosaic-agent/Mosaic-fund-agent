import sys
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from src.db.pool import query_df
from src.ml.anomaly import build_features, run_composite_anomaly

def main():
    # 1. Fetch MSUMI data
    symbol = "MSUMI"
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
        print("Error: No data found!")
        return

    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df = build_features(df)
    
    # 2. Run both pipelines
    # Run default (z=2.5, contam=0.05)
    df_def, df_def_flagged, _ = run_composite_anomaly(
        df.copy(), contamination=0.05, z_threshold=2.5
    )
    
    # Run adjusted (z=3.0, contam=0.03)
    df_adj, df_adj_flagged, _ = run_composite_anomaly(
        df.copy(), contamination=0.03, z_threshold=3.0
    )
    
    # 3. Create the plot
    plt.style.use('seaborn-v0_8-whitegrid' if 'seaborn-v0_8-whitegrid' in plt.style.available else 'default')
    fig, ax = plt.subplots(figsize=(12, 6.5), dpi=300)
    
    # Plot close price line
    ax.plot(df['trade_date'], df['close'], label='MSUMI Close Price (₹)', color='#1f77b4', linewidth=2, alpha=0.85)
    
    # Plot default anomalies (light red, slightly transparent)
    ax.scatter(df_def_flagged['trade_date'], df_def_flagged['close'], 
               color='#ff7f0e', s=90, label='Default Anomalies (z > 2.5)', alpha=0.6, edgecolors='none', zorder=4)
               
    # Plot adjusted anomalies (bright crimson red, solid, smaller overlay)
    ax.scatter(df_adj_flagged['trade_date'], df_adj_flagged['close'], 
               color='#d62728', s=100, label='Adjusted Anomalies (z > 3.0)', alpha=1.0, edgecolors='black', linewidths=1.2, zorder=5)

    # 4. Annotate major anomaly events
    annotations = {
        '2025-08-19': 'Auto Sector Rally (+6.2%)',
        '2025-12-19': 'Block Deal Shock (+6.1%)',
        '2026-04-28': 'FY26 Earnings (-4.5%)',
        '2025-07-30': '1:2 Bonus Issue Allotment (-3.1%)' # default only
    }
    
    for date_str, label in annotations.items():
        dt = pd.to_datetime(date_str)
        # Find matching close price
        match = df[df['trade_date'] == dt]
        if not match.empty:
            close_val = float(match['close'].iloc[0])
            # Default only vs Adjusted
            is_adj = dt in pd.to_datetime(df_adj_flagged['trade_date']).values
            color = '#d62728' if is_adj else '#ff7f0e'
            
            # Draw pointer and label
            ax.annotate(
                label,
                xy=(dt, close_val),
                xytext=(0, 25 if close_val < 45 else -30),
                textcoords='offset points',
                arrowprops=dict(arrowstyle="->", color=color, lw=1.2),
                fontsize=8.5,
                fontweight='bold',
                color='#333333',
                bbox=dict(boxstyle="round,pad=0.3", fc="yellow", alpha=0.2, ec="gray", lw=0.5),
                ha='center'
            )

    # Title & Labels
    ax.set_title('Motherson Sumi Wiring (MSUMI) Anomaly Analysis\nSensitivity Comparison (z=2.5 vs z=3.0)', 
                 fontsize=14, fontweight='bold', pad=15)
    ax.set_xlabel('Trade Date', fontsize=11, labelpad=10)
    ax.set_ylabel('Close Price (₹)', fontsize=11, labelpad=10)
    
    # Formatting X Axis Dates
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
    fig.autofmt_xdate()
    
    # Legend
    ax.legend(loc='upper left', frameon=True, facecolor='white', edgecolor='gray', framealpha=0.9, fontsize=9.5)
    
    # Layout adjustments
    plt.tight_layout()
    
    # Save path
    output_dir = "/home/dt/.gemini/antigravity-cli/brain/df289c3f-4472-4242-bc74-d0b395cef144"
    os.makedirs(output_dir, exist_ok=True)
    out_file = os.path.join(output_dir, "msumi_anomalies.png")
    
    plt.savefig(out_file, dpi=300)
    print(f"Chart saved successfully to: {out_file}")
    plt.close()

if __name__ == "__main__":
    main()
