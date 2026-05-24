#!/bin/bash
# scripts/cron_mf_holdings.sh
# Monthly MF holdings import (DSP, ICICI, Nippon)

PROJECT_DIR="/home/dt/project/Mosaic-fund-agent"
cd "$PROJECT_DIR"

# Log output to a file
LOG_FILE="$PROJECT_DIR/output/mf_import_$(date +\%Y\%m\%d).log"
exec > >(tee -a "$LOG_FILE") 2>&1

echo "Starting monthly MF holdings import: $(date)"

# 1. ICICI & Nippon (Factory Pattern)
.venv/bin/python3 src/scripts/fund_imports/run.py all

# 2. DSP (Specialized script)
.venv/bin/python3 src/scripts/dsp/import_all_dsp_equity.py

echo "Monthly MF holdings import complete: $(date)"
