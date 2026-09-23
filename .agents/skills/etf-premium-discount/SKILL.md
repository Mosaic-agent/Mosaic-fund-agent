---
name: etf-premium-discount
description: Retrieve and analyze iNAV premiums and discounts for all 32 tracked ETFs, grouped by AMC or sorted from highest to lowest. Use when the user asks "etf premiums", "etf discounts", "inav premiums", or invokes /etf-premium-discount.
---

# Skill: ETF Premium/Discount Tracker

Retrieve, sort, group, and compile consolidated market reports including indicative Net Asset Value (iNAV) premiums and discounts, COMEX pre-market signals, and geopolitical macro sentiment.

## Trigger

Use this skill when the user asks:
- "Show all ETF premiums and discounts"
- "Group ETF iNAV by AMC"
- "Sort premiums from highest to lowest"
- "Are ETFs trading at a premium or discount?"
- "Can you identify arbitrage entry or exit signals?"
- "Which ETFs to buy or sell based on premium?"
- "Can I get a consolidated report of ETF premiums, COMEX, and macro news?"
- "/etf-premium-discount"

## What it does

Runs the premium and discount report generator to pull prices, live iNAV, compute the premium/discount percentage (`((Market Price - iNAV) / iNAV) * 100`), compute the **Parity Gap / Downside Risk to iNAV**, and generate **Actionable Arbitrage Entry & Exit Directives**:

- **Arbitrage Entry Signals**:
  - `🟢 ARBITRAGE BUY (DEEP DISCOUNT)`: Discount < -1.5% (or Z ≤ -1.5) — High probability snap-back to iNAV.
  - `🟢 ARBITRAGE ENTRY (DISCOUNT)`: Discount -0.4% to -1.5% — Underpriced secondary market entry.
  - `🟡 GOOD ENTRY (ACCUMULATE)`: Z ≤ -1.0 — Dips below historical mean.
- **Arbitrage Exit Signals**:
  - `💥 BUBBLE (LIQUIDATE)`: Premium > +40.0% (or Z ≥ +2.5) — Asymmetric crash risk; immediate exit / do not buy (e.g. MONQ50, MASPTOP50).
  - `🚨 ARBITRAGE EXIT (SELL)`: Premium > +25.0% (or Z ≥ +1.8) — Severe scarcity markup; take profits / liquidate long.
  - `⚠️ CAUTION (OVERPRICED)`: Premium > +12.0% — Elevated premium; avoid fresh entry.
- **Hold / Parity**:
  - `⚪ FAIR VALUE (PARITY)`: Secondary price trading within ±0.5% of iNAV.

## Usage

### 1. Show all Actionable Arbitrage Entry & Exit Signals:
```bash
./mosaic.sh python src/scripts/etf/premium_discount_report.py --arbitrage
```

### 2. Show all ETFs sorted from highest premium to deepest discount:
```bash
./mosaic.sh python src/scripts/etf/premium_discount_report.py
```

### 3. Run Statistical Z-Score Arbitrage Alerts Engine:
```bash
./mosaic.sh python src/main.py premium-alerts
```

### 4. Sort from deepest discount to highest premium:
```bash
./mosaic.sh python src/scripts/etf/premium_discount_report.py --sort asc
```

### 5. Group ETFs by their respective AMC (Nippon, Zerodha, Mirae, Motilal, etc.):
```bash
./mosaic.sh python src/scripts/etf/premium_discount_report.py --group-by-amc
```

### 6. Fetch the absolute latest iNAV data before generating the report:
```bash
./mosaic.sh python src/scripts/etf/premium_discount_report.py --refresh
```

### 7. Generate the Consolidated Report (Premiums/Discounts + Arbitrage + COMEX + Macro):
```bash
./mosaic.sh python src/scripts/etf/premium_discount_report.py --consolidated
```
*(Can be combined with `--refresh` to fetch fresh iNAV data first: `./mosaic.sh python src/scripts/etf/premium_discount_report.py --refresh --consolidated`)*

### 8. Consolidated International ETF Suite (Facade & Presenter Pattern):
```bash
# Complete suite: Live scan + 2Y Backtest + Plotext + Signals + Matplotlib figures
./mosaic.sh intl-etf all

# Live scarcity & arbitrage scan:
./mosaic.sh intl-etf scan

# 2-Year statistical backtest with forward win rates & crash avoidance:
./mosaic.sh intl-etf backtest --years 2.0

# Chronological trigger history for specific ETF:
./mosaic.sh intl-etf signals --symbol MONQ50 --limit 20

# Generate publication-grade dark-theme Matplotlib figures:
./mosaic.sh intl-etf plot --symbol MONQ50
```

---

## Persistent Rule: AMC iNAV Authority Over NSE EOD Feed (Rule 18)
- **Never Use NSE EOD Feed for Premium vs Discount**: NEVER consider or use the NSE EOD feed (e.g. static declared NAV or lagged EOD prices from NSE `/api/etf` or `daily_prices`) to measure ETF premium vs discount when a live iNAV feed is available directly through the AMC (Nippon India, Zerodha, Mirae, Motilal, etc.).
- **Static vs Live Prohibition**: The static `nav` field in NSE feeds represents historical/prior-day declared NAV, not real-time intraday iNAV.
- **Synchronous Market Price & iNAV**: Always pair live real-time indicative NAV (iNAV) published directly by the AMC with the real-time live secondary market Last Traded Price (LTP). Never compare a live AMC iNAV against an EOD closing price or an NSE static NAV.
