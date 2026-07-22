---
name: ma-crossover-agent
description: Run a Moving Average Crossover strategy backtest (SMA/EMA) on ClickHouse EOD price history. Use when the user asks "run crossover backtest", "golden cross", "death cross", "moving average backtest", or invokes /ma-crossover.
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

# Skill: Moving Average Crossover Strategy Backtester

Run a Moving Average Crossover backtest for any stock or ETF in ClickHouse and output metrics and a chart.

## Trigger

Use this skill when the user asks:
- "Backtest a golden cross / death cross strategy"
- "Moving average crossover backtester"
- "crossover strategy results"
- "/ma-crossover"

## What it does

Runs a long-only cash-exchange backtest:
1. Golden Cross (Fast MA > Slow MA) -> Go Long (deploy all capital).
2. Death Cross (Fast MA <= Slow MA) -> Exit to Cash (0% returns).
3. Computes CAGR, Sharpe, Win Rate, and Max Drawdown.
4. Generates a dark-themed performance plot in `output/reports/<symbol>_crossover.png`.

## Usage

```bash
python src/main.py crossover --symbol GOLDBEES --fast 50 --slow 200 --type sma
```

## Options

- `--symbol` / `-s` (str): NSE ticker symbol to analyze (default: `GOLDBEES`).
- `--fast` / `-f` (int): Fast moving average period (default: `50`).
- `--slow` / `-l` (int): Slow moving average period (default: `200`).
- `--type` / `-t` (str): MA type: `sma` or `ema` (default: `sma`).
- `--no-plot` (bool): Disable saving the chart.
