---
name: stock-quant-deepdive-agent
description: Run a comprehensive quantitative research note and valuation deep-dive for any Indian NSE stock (e.g. RELIANCE, KALYANKJIL, ADVENZYMES) without using local LLM resources. Includes price snapshots, quarterly earnings tables, free cash flow trends, promoter holdings check, GARCH/PELT anomaly detection, moving average crossover backtests, and ClickHouse peer comparison matrix. Use when the user asks "deep down analysis [stock]", "/deepdive", or "run quant analysis on [stock]".
tools:
  - run_command
  - view_file
  - search_web
  - read_url_content
  - grep_search
  - list_dir
model: inherit
temperature: 0.1
max_turns: 20
---

# Skill: Stock Quantitative Deep-Dive Workflow (CLI-only)

Run a multi-pillar valuation, cash flow, shareholding, moving average crossover, and anomaly detection analysis for any NSE listed stock. This skill runs entirely on Python and ClickHouse data structures, avoiding expensive local LLM synthesis calls, and presents the raw data for analysis.

## Trigger

Use this skill when the user asks:
- "deep down analysis Kalyan Jewellers"
- "run valuation and quant analysis on RELIANCE"
- "/deepdive ADVENZYMES"
- "invest or sell recommendation for [stock]"

## What it does

Runs the comprehensive equity deep-dive workflow:
1. Resolves company name to NSE symbol and auto-imports EOD prices if stale or missing.
2. Runs the 12-tool parallel fetch in threads to load prices, momentum, earnings, cash flows, promoter stakeholders, news, and historical change-point anomalies.
3. Backtests a 20d vs 50d SMA moving average crossover to verify trend regime and Sharpe ratio.
4. Queries ClickHouse `stock_valuation` to output peer multiples comparison.
5. Saves a combined Markdown research note report named `<symbol>_deep_down_analysis.md` in the user's active session artifacts folder.

## Usage

Run the deep-dive pipeline from the terminal:

```bash
ALLOW_LOCAL_RUN=1 PYTHONPATH=. .venv/bin/python src/scripts/portfolio/stock_quant_deepdive_cli.py "<STOCK_NAME_OR_SYMBOL>" --artifact-dir "<ARTIFACTS_DIR>"
```

### Arguments

- `query` (str): Stock name, partial name, or NSE symbol (e.g. `"Kalyan Jewellers"`, `"ADVENZYMES"`).
- `--artifact-dir` (str): Path to save the output markdown report (pass `/Users/dhiraj.thakur/.gemini/antigravity-cli/brain/b2da23a3-8a46-49d9-8d1c-580d5c35c591/`).
