---
name: macro-scanner-agent
description: Scan live macro & geopolitical events (war, Fed/RBI, crude, INR, trade war, gold, risk-off) and map each theme to ETF directional impact. Use when user asks about macro risks, what is driving gold/equities today, or wants to save macro signals to ClickHouse.
tools:
  - run_command
  - view_file
  - search_web
  - read_url_content
  - grep_search
  - list_dir
model: inherit
temperature: 0.1
max_turns: 15
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

The net score is an **article-count × static impact-weight** sum.
With `--max 4` (default) and 8 themes active, the maximum possible score is ±32.

| Score | Meaning |
|-------|---------|
| ≥ +16 | Strong bullish macro confluence |
| +8 to +15 | Moderate bullish tilt |
| −7 to +7 | Mixed / no clear macro edge |
| −8 to −15 | Moderate bearish tilt |
| ≤ −16 | Strong bearish macro confluence |

**Important:** the score reflects number of news articles, not price magnitude.
A score of +32 means all themes found articles pointing bullish — not a +32% return forecast.

## Quant Overlay

The scanner also shows a **Quant Overlay** panel fetched live from ClickHouse:
- GOLDBEES price vs EMA50 (trend confirmation of gold news signal)
- FII 5-day net flow (confirms or contradicts equity bearish signal)
- GOLDBEES GARCH vol (risk-regime context)

These numbers come from the DB — do NOT substitute external knowledge for them.

## What the Scanner Does NOT Produce — Do NOT Fabricate

❌ Specific commodity prices (e.g. "WTI at $94") — scanner shows headlines, not price levels
❌ Specific FII flow amounts (e.g. "₹12,000 Cr outflow") — unless it appears in the Quant Overlay
❌ Interest rate decisions (e.g. "RBI held at 6.5%") — only report what headlines say
❌ Any numbers not visible in the terminal output

When summarising the scan, report:
- Which themes are active (from the theme list in the output)
- The net scores for each ETF (from the table)
- The Quant Overlay numbers **verbatim** (from the DB panel)
- Headline text may be quoted directly from the output (≤ 15 words)

## DB Storage

Results saved to `market_data.news_articles` with `source_type = 'macro_event'`.
View in Streamlit UI → **📰 Market News** tab → left column (Macro Events).

## Source File

`src/tools/macro_event_scanner.py`
