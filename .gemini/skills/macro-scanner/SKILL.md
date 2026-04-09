---
name: macro-scanner
description: Scan live macro & geopolitical events (war, Fed/RBI, crude, INR, trade war, gold, risk-off) and map each theme to ETF directional impact. Use when user asks about macro risks, what is driving gold/equities today, or wants to save macro signals to ClickHouse.
---

# Skill: Macro Scanner

Detects active macro and geopolitical themes from free news sources (Google News RSS + Yahoo Finance) and maps each event to ETF directional impact with transmission logic. No API key required.

## Trigger

Use this skill when the user asks:
- "What macro events are moving the market today?"
- "How does US-Iran / Fed / OPEC affect my ETFs?"
- "What is driving gold / banks / IT sector right now?"
- "Save macro signals to DB"

## Commands

```bash
# Print to terminal
python src/main.py macro --max 4

# Scan + persist to ClickHouse (market_data.news_articles)
python src/main.py macro --save

# Quick scan with save
python src/main.py macro --max 2 --save
```

## 8 Macro Themes

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

## Net Score Interpretation

- ≥ +4 → Strong bullish confluence → accumulate
- ≤ −4 → Strong bearish confluence → reduce / avoid
- Near 0 → Mixed signals → wait for clarity

## DB Storage

Results saved to `market_data.news_articles` with `source_type = 'macro_event'`.
View in Streamlit UI → **📰 Market News** tab → left column (Macro Events).

## Source File

`src/tools/macro_event_scanner.py`
