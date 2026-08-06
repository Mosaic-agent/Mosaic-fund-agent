---
name: intraday
description: Run the real-time read-only intraday signal monitor for a specified ETF or stock. Includes bid/ask delta order flow, VWAP deviation bands, and technical indicators. Trigger when the user says "track intraday", "intraday signal for symbol", or invokes /intraday <symbol>.
---

# Skill: Intraday Signal Monitor

Track live tick-by-tick market data via Shoonya WebSockets and show a real-time signal dashboard for any ETF or stock.

## Trigger

Use this skill when the user asks:
- "Track intraday GOLDBEES"
- "Show real-time signal for HDFCBANK"
- "What is the live momentum for MAFANG?"
- "/intraday <symbol> [duration]"

## What it does

Executes the read-only intraday agent to connect to Shoonya's WebSocket, fetch ClickHouse baselines, capture live ticks, and calculate:
1. **LTP vs VWAP**: Evaluates live pricing relative to intraday volume-weighted average price.
2. **Spread Overlays (ETFs only)**: Evaluates iNAV premium/discount spreads using category-specific thresholds (Domestic 1.5%, Commodity 2.5%, International 5.0%).
3. **Momentum Indicators**: Tracks Cumulative Delta (order flow imbalance), VWAP Z-score bands (±2σ), Tick RSI (9), and Micro-Momentum EMA.

## Usage

### Run from CLI
```bash
ALLOW_LOCAL_RUN=1 python src/agents/intraday_agent.py --symbol <SYMBOL> --interval <INTERVAL_SECS> --duration <DURATION_SECS>
```

### Prompt-Driven Execution
When the user invokes `/intraday <SYMBOL>`, execute the command with a default `--interval 5 --duration 15` to capture multiple tick frames and print the resulting dashboard verbatim.

### Terminal ASCII Chart Output
For terminal visualization, stream 5-minute VWAP deviation bands and cumulative delta bars using `plotext` or `render_sparkline()` from `src.tools.chart_tools`.
