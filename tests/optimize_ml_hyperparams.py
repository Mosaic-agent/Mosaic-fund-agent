import argparse
import os
import sys
import warnings
import numpy as np
import pandas as pd
import clickhouse_connect

sys.path.insert(0, os.getcwd())
warnings.filterwarnings("ignore")

from src.ml.trend_predictor import build_master_table, engineer_features, label_forward_return, fit_walk_forward
from config.settings import settings

def main():
    parser = argparse.ArgumentParser(description="Grid search LightGBM hyperparameters")
    parser.add_argument("--splits", default=5, type=int, help="Number of CV splits")
    parser.add_argument("--since", default="2013-01-01", type=str, help="Start date for training data")
    args = parser.parse_args()

    print("Connecting to ClickHouse...")
    client = clickhouse_connect.get_client(
        host=settings.clickhouse_host, port=settings.clickhouse_port,
        database=settings.clickhouse_database,
        username=settings.clickhouse_user, password=settings.clickhouse_password,
    )
    
    print("Loading price and macro data...")
    df = build_master_table(client)
    client.close()
    
    print("Engineering alpha factors...")
    df = engineer_features(df)
    df = label_forward_return(df, horizon=5)
    
    # Filter by start date
    df = df[df["trade_date"] >= args.since].reset_index(drop=True)
    print(f"Data subset filtered since {args.since} ({len(df)} rows)")
    
    # Hyperparameter search space
    learning_rates     = [0.01, 0.03, 0.05]
    max_depths         = [3, 4, 5]
    min_child_samples  = [15, 25, 40]
    regularizations    = [0.15, 0.50, 1.00]
    
    grid = []
    for lr in learning_rates:
        for depth in max_depths:
            for mcs in min_child_samples:
                for reg in regularizations:
                    grid.append({
                        "learning_rate": lr,
                        "max_depth": depth,
                        "num_leaves": 2 ** depth - 1,
                        "min_child_samples": mcs,
                        "reg_alpha": reg,
                        "reg_lambda": reg
                    })
                    
    print(f"Starting Grid Search Sweep of {len(grid)} hyperparameter candidates...")
    print(f"{'lr':<5} | {'depth':<5} | {'leaves':<6} | {'min_child':<9} | {'reg':<5} | {'Mean CV AUC':<12} | {'Mean Hit %':<10}")
    print("-" * 75)
    
    results = []
    
    # Silence LightGBM inner training logs during sweep
    import logging
    logging.getLogger("lightgbm").setLevel(logging.ERROR)
    
    for i, params in enumerate(grid):
        try:
            out = fit_walk_forward(df, n_splits=args.splits, gap=10, hyperparams=params)
            aucs = out[6]
            hit_ratios = out[3]
            mean_auc = float(np.mean(aucs))
            mean_hit = float(np.mean(hit_ratios))
            
            results.append({
                "params": params,
                "mean_auc": mean_auc,
                "mean_hit": mean_hit
            })
            
            # Print update every few candidates or if AUC > 0.5
            if mean_auc > 0.50 or i % 10 == 0:
                print(f"{params['learning_rate']:<5.2f} | {params['max_depth']:<5d} | {params['num_leaves']:<6d} | {params['min_child_samples']:<9d} | {params['reg_alpha']:<5.2f} | {mean_auc:<12.4f} | {mean_hit:<10.1%}")
        except Exception as exc:
            pass
            
    print("=" * 75)
    
    if not results:
        print("No candidates completed successfully.")
        return
        
    df_results = pd.DataFrame(results)
    df_results = df_results.sort_values("mean_auc", ascending=False).reset_index(drop=True)
    
    print("\n🏆 TOP 5 PARAMETER CONFIGURATIONS (Sorted by Mean CV AUC):")
    for idx, row in df_results.head(5).iterrows():
        p = row["params"]
        print(f"Rank {idx+1}: AUC = {row['mean_auc']:.4f} | Hit Ratio = {row['mean_hit']:.1%}")
        print(f"        Parameters: lr={p['learning_rate']}, depth={p['max_depth']}, leaves={p['num_leaves']}, min_child={p['min_child_samples']}, reg={p['reg_alpha']}")
        print("-" * 65)

if __name__ == "__main__":
    main()
