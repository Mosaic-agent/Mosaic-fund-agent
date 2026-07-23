---
name: etf-premium-discount-agent
description: Retrieve and analyze iNAV premiums and discounts for all 32 tracked ETFs, grouped by AMC or sorted from highest to lowest. Use when the user asks "etf premiums", "etf discounts", "inav premiums", or invokes /etf-premium-discount.
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

# Skill: ETF Premium/Discount Tracker

Retrieve, sort, group, and compile consolidated market reports including indicative Net Asset Value (iNAV) premiums and discounts, COMEX pre-market signals, and geopolitical macro sentiment.

## Trigger

Use this skill when the user asks:
- "Show all ETF premiums and discounts"
- "Group ETF iNAV by AMC"
- "Sort premiums from highest to lowest"
- "Are ETFs trading at a premium or discount?"
- "Can I get a consolidated report of ETF premiums, COMEX, and macro news?"
- "/etf-premium-discount"

## What it does

Runs the premium and discount report generator to pull prices, live iNAV, and compute the premium/discount percentage (`((Market Price - iNAV) / iNAV) * 100`) for all 32 ETFs from ClickHouse. 

If `--consolidated` is passed, it compiles a complete unified report containing:
1. **ETF Premium vs Discount Status** (sorted descending)
2. **COMEX Commodity Pre-Market Signals** (Gold, Silver, Platinum, Palladium, Copper)
3. **Macro Geopolitical news scanner and Aggregated ETF signals**

## Usage

### 1. Show all ETFs sorted from highest premium to deepest discount:
```bash
python src/scripts/etf/premium_discount_report.py
```

### 2. Sort from deepest discount to highest premium:
```bash
python src/scripts/etf/premium_discount_report.py --sort asc
```

### 3. Group ETFs by their respective AMC (Nippon, Zerodha, Mirae, Motilal, etc.):
```bash
python src/scripts/etf/premium_discount_report.py --group-by-amc
```

### 4. Fetch the absolute latest iNAV data before generating the report:
```bash
python src/scripts/etf/premium_discount_report.py --refresh
```

### 5. Generate the Consolidated Report (Premiums/Discounts + COMEX + Macro Geopolitical):
```bash
python src/scripts/etf/premium_discount_report.py --consolidated
```
*(Can be combined with `--refresh` to fetch fresh iNAV data first: `python src/scripts/etf/premium_discount_report.py --refresh --consolidated`)*
