# Skill: Macro-Scanner

Detects live macro and geopolitical events from free news sources and maps each event to ETF directional impact with transmission logic.

## Trigger

Use this skill when the user asks about:
- Macro events impacting the market (wars, Fed/RBI decisions, crude oil, trade war, INR moves)
- "What is driving gold / equities / banks today?"
- "How does US-Iran / Fed / OPEC affect my ETFs?"
- "What macro risks are active right now?"

## How to Run

```bash
cd /Users/dhiraj.thakur/project/Mosaic-fund-agent
source .venv/bin/activate

# Print to terminal
python src/main.py macro --max 4

# Scan + persist to ClickHouse (market_data.news_articles)
python src/main.py macro --save

# Fewer articles for a quick check
python src/main.py macro --max 2 --save
```

After saving, view in the Streamlit UI → **📰 Market News** tab (left column).
Filter by date range; grouped by theme with ETF tags.

## What It Does

Monitors 8 macro themes via Google News RSS + Yahoo Finance (no API key):

| Theme | Key ETF Impact |
|---|---|
| ⚔️ Geopolitical / War | GOLDBEES ↑, NIFTYBEES ↓, BANKBEES ↓ |
| 🏦 Fed / RBI Policy | GILT5YBEES ↑ (cut), GOLDBEES ↑ (cut), MON100 ↓ (hike) |
| 🛢️ Crude Oil Shock | GOLDBEES ↑ (inflation hedge), AUTOBEES ↓ |
| 💱 INR Move | ITBEES ↑ (weak INR), GOLDBEES ↑, NIFTYBEES ↓ |
| ⚖️ Trade War / Tariffs | GOLDBEES ↑, MON100 ↓, ITBEES ↓ |
| 🇮🇳 India Macro | NIFTYBEES ↑, CPSEETF ↑, GILT5YBEES ↓ |
| 🥇 Gold / Commodity | GOLDBEES ↑↓ directly |
| 📉 Global Risk-Off | GOLDBEES ↑, LIQUIDBEES ↑, all equity ETFs ↓ |

## Output

- Per-theme headlines with sentiment (POSITIVE / NEGATIVE / NEUTRAL)
- Transmission mechanism — why the event moves each ETF
- Aggregated net signal score per ETF across all active themes
- Higher score = more themes pointing the same direction

## Interpretation

- Net score ≥ +4 → Strong bullish confluence → accumulate
- Net score ≤ −4 → Strong bearish confluence → reduce / avoid
- Score near 0 → Mixed / wait for clarity

## Source File

`src/tools/macro_event_scanner.py`
