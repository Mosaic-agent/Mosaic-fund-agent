#!/bin/bash
# scripts/cron_daily_sync.sh
# Daily Market Data Sync & Signal Generation (End-of-Day)
# Recommended: Run Mon-Fri at 16:30 IST

PROJECT_DIR="/home/dt/project/Mosaic-fund-agent"
cd "$PROJECT_DIR"

LOG_FILE="$PROJECT_DIR/output/daily_sync_$(date +\%Y\%m\%d).log"
exec > >(tee -a "$LOG_FILE") 2>&1

echo "Starting Daily Sync: $(date)"

# 1. Import EOD Market Data
.venv/bin/python3 src/main.py import --category stocks,etfs,commodities,indices,fii_dii,fx_rates

# 2. Update ML Forecast (GOLDBEES)
.venv/bin/python3 src/ml/trend_predictor.py --symbol GOLDBEES --save

# 3. Generate Composite Signals
.venv/bin/python3 src/main.py signals --save

# 4. Generate Risk Checkpoints (Kelly Sizing)
.venv/bin/python3 src/main.py risk --save --symbol GOLDBEES

echo "Daily Sync Complete: $(date)"
