# Skill: Daily-Brief-PDF

Generates a self-contained PDF morning brief combining macro events, ETF signals, FII/DII flows, ML prediction, and buy/sell recommendations.

## Trigger

Use this skill when the user asks:
- "Generate a daily PDF report"
- "Create a morning brief PDF"
- "Export today's analysis to PDF"
- "Give me a shareable daily report"

## How to Generate

Run both scanners first, then compile into PDF:

```bash
cd /Users/dhiraj.thakur/project/Mosaic-fund-agent
source .venv/bin/activate

# 1. Run macro scan and save output
python src/main.py macro --max 4 > /tmp/macro_output.txt 2>&1

# 2. Run ETF news scan and save output
python src/main.py etf-news --max 3 > /tmp/etf_news_output.txt 2>&1

# 3. Use the pdf skill to compile the brief
```

Then use `anthropic-skills:pdf` to compile the outputs into a formatted PDF with:
- Cover page: date, market summary (COMEX signals)
- Section 1: Active Macro Themes (from macro scan)
- Section 2: ETF Net Signal Scoreboard
- Section 3: Buy / Hold / Sell recommendations table
- Section 4: FII/DII flow trend (last 5 days)
- Section 5: ML prediction for GOLDBEES
- Footer: "Not financial advice — Mosaic Fund Agent research tool"

## PDF Naming Convention

```
output/daily_brief_YYYYMMDD.pdf
```

## Key Data to Pull for the Brief

```python
import clickhouse_connect
from config.settings import settings
client = clickhouse_connect.get_client(...)

# ETF momentum
# FII/DII last 5 days
# ML prediction from market_data.ml_predictions
# GOLDBEES NAV spread
```

## Dependencies

- `reportlab` (already in requirements.txt)
- `anthropic-skills:pdf` skill loaded

## Source Files

- `src/tools/macro_event_scanner.py`
- `src/tools/etf_news_scanner.py`
- `src/ml/trend_predictor.py`
