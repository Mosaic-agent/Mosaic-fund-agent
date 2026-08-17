---
name: stock-anomaly-news-agent
description: Run a comprehensive price anomaly detection and qualitative news/event correlation report for any Indian stock or commodity.
tools:
  - run_command
  - view_file
  - search_web
  - read_url_content
  - grep_search
  - list_dir
model: inherit
temperature: 0.1
max_turns: 8
---

# Stock Anomaly & News Correlation Skill

This skill runs the 5-step GARCH-composite anomaly detection pipeline (GARCH conditional volatility + Isolation Forest + PELT change-point detection) on any Indian stock or commodity, filters out corporate actions (splits, bonuses, mergers), and performs parallel news and event attribution searches (including semantic RAG scoring via Ollama) to explain the causes of price shocks.

## Trigger Phrases

Recommend or use this skill when the user asks:
- "run anomaly analysis on [STOCK]"
- "explain the price shocks for [STOCK]"
- "find news on [STOCK] anomaly dates"
- "correlate [STOCK] price moves with macro events"
- "investigate what happened to [STOCK] in the last year"

## CLI Execution

Run the consolidated script inside Docker:
```bash
./mosaic.sh src/scripts/market/stock_anomaly_detector.py --symbol <SYMBOL> --days <LOOKBACK_DAYS>
```

### Options:
- `--symbol` (required): NSE/BSE trading symbol (e.g., `INFY`, `RELIANCE`, `TCS`).
- `--days` (optional): Lookback window in calendar days (default `180`).
- `--z-threshold` (optional): Raw GARCH residual Z-score cutoff (default `5.0`).
- `--contamination` (optional): Isolation Forest outlier contamination rate (default `0.01`).

## Output Files & ASCII Visualizations

The script generates a comprehensive markdown report in the output directory:
- Report Path: `output/<symbol>_anomaly_report.md`
- Visual timeline plots are saved to `output/reports/<SYMBOL>_correlation_timeline.png`
- Lead-lag grids are saved to `output/reports/<SYMBOL>_lead_lag_grid.png`

### Terminal ASCII Chart Integration
For interactive CLI responses or inline summary reports, call `plot_price_chart(symbol, days)` or `render_sparkline(prices)` from `src.tools.chart_tools` to render ASCII candlestick charts and volume spike flags directly in the terminal output.

## Framework for Interpreting Results

### 1. Statistical Correlation Strength (Cohen Convention)
* **Strong**: $|r| \ge 0.5$ — High co-movement, clear linear dependency.
* **Moderate**: $0.3 \le |r| < 0.5$ — Noticeable relationship, some shared variance.
* **Weak**: $|r| < 0.3$ — Minimal linear relationship (e.g. currency spot dependency).

### 2. Statistical Significance Boundaries (p-value)
* **Significant ($p < 0.05$)**: The relationship is statistically valid.
* **Not Significant ($p \ge 0.05$)**: The correlation cannot be distinguished from noise.

### 3. Event-Attribution Confidence Scores
* **HIGH Confidence (Score $\ge 70.0$)**: Immediate, direct causal transmission (e.g. major corporate filing or policy ex-date).
* **MODERATE Confidence (Score $40.0$ to $69.9$)**: Probable driver, backed by tight timing and news frequency.
* **LOW Confidence (Score $< 40.0$)**: Plausible correlation, but either lagged, indirect, or from a general macro announcement.
