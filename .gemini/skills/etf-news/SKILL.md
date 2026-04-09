---
name: etf-news
description: Fetch and tag news articles by Indian ETF category with sentiment scores. Covers 10 categories (Gold, Nifty, Bank, IT, PSU, Mid/Small, Pharma, International, Debt, Auto). Uses Google News RSS + Yahoo Finance — no API key required. Use when user asks about news affecting specific ETFs or wants to save ETF news to ClickHouse.
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

- **Google News RSS** via `gnews` — no quota, no key, India + US editions
- **Yahoo Finance** via `yfinance` — no key, ticker-specific news

## DB Storage

Results saved to `market_data.news_articles` with `source_type = 'etf_news'`.
View in Streamlit UI → **📰 Market News** tab → right column (ETF-Impact News).
Filter by category and date range; sentiment counts shown at top.

## Source File

`src/tools/etf_news_scanner.py`
