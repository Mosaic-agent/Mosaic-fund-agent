---
name: db-freshness-agent
description: Audit database table dates and import/refresh market data (ETFs, Forex, DXY indices, stocks, commodities, Indian macro data, FII/DII institutional flows) to ensure the platform is up-to-date. Use when the user asks "make the db fresh", "refresh database", "check db freshness", "import latest market data", "import indian macro data", "import fii-dii", or invokes /db-freshness.
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

# Skill: Database Freshness and Market Data Refresh

Audits the dates of ClickHouse database tables and runs targeted import pipelines (such as ETFs, FX Rates/Forex, DXY indices, Indian macro data, and FII/DII institutional flows) to ensure that cross-asset quantitative indicators and machine learning models are running on fresh, up-to-date data for the current business day.

## Trigger

Use this skill when the user asks:
- "Make the db fresh" / "make the fb fresh with etf, forex, dxy"
- "Audit database freshness" / "check db freshness"
- "Import latest market data"
- "Update forex rates and DXY"
- "Refresh ETFs and indices"
- "Import Indian macro data" / "refresh Indian macro data"
- "Import FII/DII flows" / "refresh FII/DII data"
- "/db-freshness"

---

## Commands

### 1. Audit Database Freshness Scanner
Check the maximum trade date for all registered categories (`stocks`, `etfs`, `indices`, `commodities`, `us_stocks`, `fx_rates`, `inav`, `indian_macro`, and `fii_dii`) to see which tables are stale.
```bash
PYTHONPATH=. python src/scripts/db/audit_freshness.py
```

### 2. Refresh Targeted Categories (ETFs, Forex, DXY)
Import the latest data for ETFs, FX rates (Forex), and indices (including the DXY index ticker `DX-Y.NYB`). Subsequent runs perform a fast delta-sync from the last watermark to today.
```bash
PYTHONPATH=. python src/main.py import --category etfs,fx_rates,indices --source yfinance
```

### 3. Import Indian Macro Indicators
Scrape and import macro and industry indicators from Tijori Finance (e.g. auto sales, insurance premiums):
```bash
PYTHONPATH=. python src/main.py import --category indian_macro
```

### 4. Import FII / DII Institutional Flows
Import daily cash market flows, daily derivatives (F&O) OI participant flows, and monthly aggregate flows:
```bash
PYTHONPATH=. python src/main.py import --category fii_dii
```

### 5. Run General / Full Delta Sync (All Categories)
Delta-sync all registered categories (stocks, ETFs, mutual funds, FII/DII, COT, FX rates, etc.) to today:
```bash
PYTHONPATH=. python src/main.py import
```

### 6. Run Data Sanity Check
Run the data sanity check suite to validate imported data for price anomalies or daily outliers:
```bash
PYTHONPATH=. python src/scripts/db/run_data_sanity_check.py
```

---

## Validation / Troubleshooting Queries

### Check DXY (Dollar Index) Database Max Date
```sql
SELECT symbol, max(trade_date), count() 
FROM market_data.daily_prices 
WHERE symbol = 'DXY' 
GROUP BY symbol;
```

### Check Max Dates Grouped by Category in daily_prices
```sql
SELECT category, max(trade_date) 
FROM market_data.daily_prices 
GROUP BY category;
```

### Check Max Date in FX Rates (Forex)
```sql
SELECT max(trade_date) 
FROM market_data.fx_rates;
```

### Check Latest Scraped Indian Macro Data
```sql
SELECT as_of_date, indicator_name, value, unit 
FROM market_data.indian_macro_indicators FINAL 
ORDER BY as_of_date DESC 
LIMIT 10;
```

### Check Latest Daily FII/DII Cash Flows
```sql
SELECT trade_date, fii_net_cr, dii_net_cr 
FROM market_data.fii_dii_flows FINAL 
ORDER BY trade_date DESC 
LIMIT 5;
```
