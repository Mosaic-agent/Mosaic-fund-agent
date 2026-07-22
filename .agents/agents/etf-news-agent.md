---
name: etf-news-agent
description: Fetch and tag news articles by Indian ETF category with sentiment scores. Covers 10 categories (Gold, Nifty, Bank, IT, PSU, Mid/Small, Pharma, International, Debt, Auto). Uses Google News RSS + Yahoo Finance — no API key required. Use when user asks about news affecting specific ETFs or wants to save ETF news to ClickHouse.
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

# Skill: ETF-Impact News

Fetches live news that can directly impact Indian ETFs, tagged by category, affected symbols, and sentiment. No API key required.

## Trigger

Use this skill when the user asks:
- "What news is affecting GOLDBEES / BANKBEES / ITBEES?"
- "Any ETF-relevant news today?"
- "Show me news for gold ETFs / bank ETFs / IT ETFs"
- "Save ETF news to DB"

## Commands

```bash
# Print all categories to terminal
python src/main.py etf-news --max 4

# Scan + persist to ClickHouse
python src/main.py etf-news --save

# Specific categories
python src/main.py etf-news --category "Gold ETFs" --max 5 --save
python src/main.py etf-news --category "Gold ETFs,Bank ETFs,IT ETFs" --save
```

## 10 ETF Categories

| Category | ETFs Covered |
|---|---|
| Gold ETFs | GOLDBEES, SILVERBEES |
| Nifty ETFs | NIFTYBEES, SETFNIF50, HDFCNIFTY, MONIFTY500 |
| Bank ETFs | BANKBEES, PSUBNKBEES |
| IT ETFs | ITBEES |
| PSU ETFs | CPSEETF, ICICIB22 |
| Mid/Small Cap ETFs | JUNIORBEES, MID150BEES, SMALL250 |
| Pharma ETFs | PHARMABEES |
| International ETFs | MON100, MAFANG, HNGSNGBEES, MAHKTECH, MASPTOP50 |
| Debt / Liquid ETFs | LIQUIDBEES, LIQUIDCASE, GILT5YBEES |
| Auto ETFs | AUTOBEES |

## Data Sources

- **Google News RSS** via `gnews` — no quota, no key, India edition
- **Yahoo Finance** via `yfinance` — no key, ticker-specific news
- **NewsAPI** (optional) — Indian financial press; requires `NEWSAPI_KEY` in `.env`; cached 1h

## What the Scanner Does NOT Produce — Do NOT Fabricate

The output is: headlines + sentiment tag (POSITIVE/NEGATIVE/NEUTRAL) + category.

❌ Do NOT add price levels (e.g. "gold at ₹7,200/g") — only report headlines shown
❌ Do NOT add FII flow amounts unless visible in output
❌ Do NOT assign buy/sell recommendations — the scanner only classifies sentiment

When summarising, report:
- Category names and how many articles were found
- The positive/negative/neutral headline counts from the header
- Specific headlines may be quoted directly (≤15 words)
- Sentiment counts shown in the header panel are ground truth

## DB Storage

Results saved to `market_data.news_articles` with `source_type = 'etf_news'`.
View in Streamlit UI → **📰 Market News** tab → right column (ETF-Impact News).
Filter by category and date range; sentiment counts shown at top.

## Source File

`src/tools/etf_news_scanner.py`
