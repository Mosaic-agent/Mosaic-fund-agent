---
name: etf-setups-agent
description: Scan all 18 tracked ETFs for volume-volatility setups (breakouts, squeezes, exhaustion). Use when the user asks "etf setups", "volume volatility scan", "identify breakouts", or invokes /etf-setups.
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

# Skill: ETF Volume-Volatility Scanner

Scan all 18 tracked ETFs for trade setups based on volume-volatility patterns.

## Trigger

Use this skill when the user asks:
- "Scan ETFs for setups"
- "Show volume volatility anomalies"
- "Find squeezes or breakouts"
- "/etf-setups"

## What it does

Runs the EOD setup scanner across ClickHouse daily prices and volumes to identify:
1. **`🚀 Volatile Breakout`**: High daily return on high relative volume (potential momentum trades).
2. **`⚠️ Volume Exhaustion`**: High daily return on low/dry relative volume (potential counter-trend mean reversion trades).
3. **`📦 Volatility Squeeze`**: Flat returns + dry volume + Bollinger Bands pinching (pre-breakout consolidation).

## Usage

Run the scanner directly from CLI:
```bash
python src/main.py scan-setups
```

Or call the `scan_etf_setups` tool from your agent context. Print the resulting markdown table verbatim.
