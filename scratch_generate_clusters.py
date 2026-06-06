import sys
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

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
    
    # 2. Run adjusted pipeline (z_threshold=3.0, contamination=0.03)
    df_res, _, _ = run_composite_anomaly(
        df, contamination=0.03, z_threshold=3.0
    )
    
    # 3. Create the multi-panel scatter plot
    plt.style.use('seaborn-v0_8-whitegrid' if 'seaborn-v0_8-whitegrid' in plt.style.available else 'default')
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6.5), dpi=300)
    
    # Define colors and marker sizes for each regime
    regime_styles = {
        '✅ Normal': {'color': '#bdc3c7', 'size': 30, 'label': 'Normal', 'alpha': 0.4, 'zorder': 1},
        '🔥 Volatile Breakout': {'color': '#e74c3c', 'size': 120, 'label': 'Volatile Breakout', 'alpha': 0.9, 'zorder': 5},
        '⚡ Flash Crash / Black Swan (EXIT)': {'color': '#9b59b6', 'size': 120, 'label': 'Flash Crash / Black Swan', 'alpha': 0.9, 'zorder': 4},
        '📈 Strong Trend (HODL)': {'color': '#2ecc71', 'size': 100, 'label': 'Strong Trend (HODL)', 'alpha': 0.9, 'zorder': 3},
        '🧨 Blow-off Top (Weak)': {'color': '#f39c12', 'size': 140, 'label': 'Blow-off Top', 'alpha': 0.9, 'zorder': 6}
    }
    
    # Check what regimes exist in the output dataset
    unique_regimes = df_res['regime'].unique()
    print("Regimes present in dataset:", unique_regimes)
    
    # Panel 1: Daily Return vs Intraday Range (Volatility clusters)
    ax1.set_title('Regime Clusters: Return vs. Range', fontsize=12, fontweight='bold')
    ax1.set_xlabel('Daily Return (%)', fontsize=10)
    ax1.set_ylabel('Intraday Range (%)', fontsize=10)
    
    # Panel 2: Daily Return vs Volume Z-Score (Liquidity clusters)
    ax2.set_title('Regime Clusters: Return vs. Volume Z-Score', fontsize=12, fontweight='bold')
    ax2.set_xlabel('Daily Return (%)', fontsize=10)
    ax2.set_ylabel('Volume Z-Score', fontsize=10)
    
    # Plot each regime separately to apply styles and build a clean legend
    for regime_name, style in regime_styles.items():
        sub_df = df_res[df_res['regime'] == regime_name]
        if sub_df.empty:
            continue
            
        # Panel 1 Scatter
        ax1.scatter(
            sub_df['daily_return'], 
            sub_df['range_pct'], 
            c=style['color'], 
            s=style['size'], 
            label=style['label'], 
            alpha=style['alpha'], 
            edgecolors='black' if regime_name != '✅ Normal' else 'none',
            linewidths=0.8,
            zorder=style['zorder']
        )
        
        # Panel 2 Scatter
        ax2.scatter(
            sub_df['daily_return'], 
            sub_df['z_volume'], 
            c=style['color'], 
            s=style['size'], 
            label=style['label'], 
            alpha=style['alpha'], 
            edgecolors='black' if regime_name != '✅ Normal' else 'none',
            linewidths=0.8,
            zorder=style['zorder']
        )
        
    # Annotate specific outlier dates on Panel 1
    outliers = {
        '2025-12-19': 'Block Deal',
        '2026-04-28': 'Earnings',
        '2025-08-19': 'Auto Rally'
    }
    for date_str, label in outliers.items():
        dt = pd.to_datetime(date_str)
        match = df_res[df_res['trade_date'] == dt]
        if not match.empty:
            ret = float(match['daily_return'].iloc[0])
            rng = float(match['range_pct'].iloc[0])
            z_vol = float(match['z_volume'].iloc[0])
            
            ax1.annotate(
                f"{label}\n({date_str})",
                xy=(ret, rng),
                xytext=(ret + (2.5 if ret > 0 else -2.5), rng - 1),
                arrowprops=dict(arrowstyle="->", color='black', lw=0.8),
                fontsize=8,
                fontweight='bold',
                bbox=dict(boxstyle="round,pad=0.2", fc="yellow", alpha=0.3, ec="gray", lw=0.5),
                ha='center'
            )
            ax2.annotate(
                f"{label}\n({date_str})",
                xy=(ret, z_vol),
                xytext=(ret + (2.5 if ret > 0 else -2.5), z_vol - 0.5),
                arrowprops=dict(arrowstyle="->", color='black', lw=0.8),
                fontsize=8,
                fontweight='bold',
                bbox=dict(boxstyle="round,pad=0.2", fc="yellow", alpha=0.3, ec="gray", lw=0.5),
                ha='center'
            )

    # Gridlines and Legends
    ax1.axvline(0, color='grey', linestyle='--', linewidth=0.8, alpha=0.5)
    ax2.axvline(0, color='grey', linestyle='--', linewidth=0.8, alpha=0.5)
    ax2.axhline(0, color='grey', linestyle='--', linewidth=0.8, alpha=0.5)
    
    ax1.legend(loc='upper right', frameon=True, fontsize=8.5)
    ax2.legend(loc='upper left', frameon=True, fontsize=8.5)
    
    fig.suptitle(f'MSUMI 1-Year Anomaly Regime Clustering (z_threshold=3.0)\nFeature Space Visualization', 
                 fontsize=14, fontweight='bold', y=0.98)
    
    plt.tight_layout(rect=[0, 0, 1, 0.95])
    
    # Save path
    output_dir = "/home/dt/.gemini/antigravity-cli/brain/df289c3f-4472-4242-bc74-d0b395cef144"
    os.makedirs(output_dir, exist_ok=True)
    out_file = os.path.join(output_dir, "msumi_anomaly_clusters.png")
    
    plt.savefig(out_file, dpi=300)
    print(f"Clusters chart saved successfully to: {out_file}")
    plt.close()

if __name__ == "__main__":
    main()
