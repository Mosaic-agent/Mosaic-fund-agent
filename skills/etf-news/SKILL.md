# Skill: ETF-News

Fetches live news that can directly impact Indian ETFs, tagged by category, affected symbols, and sentiment. No API key required.

## Trigger

Use this skill when the user asks about:
- "What news is affecting GOLDBEES / BANKBEES / ITBEES?"
- "Any ETF-relevant news today?"
- "Show me news for gold ETFs / bank ETFs / IT ETFs"
- Current events mapped to specific ETF categories

## How to Run

```bash
cd /Users/dhiraj.thakur/project/Mosaic-fund-agent
source .venv/bin/activate

# All categories
python src/main.py etf-news --max 4

# Specific categories
python src/main.py etf-news --category "Gold ETFs" --max 5
python src/main.py etf-news --category "Gold ETFs,Bank ETFs,IT ETFs"
```

## Categories Available

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

## Output

- Articles grouped by ETF category
- Each article: sentiment icon (🟢 POSITIVE / 🔴 NEGATIVE / ⚪ NEUTRAL), title, source, date
- Summary: total articles, positive/negative/neutral counts

## Source File

`src/tools/etf_news_scanner.py`
