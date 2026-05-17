---
name: macro-strategy
description: 2026 macro specialist for the "Baton Pass" from paper to real assets. Navigates the commodity supercycle, electrification/nuclear bottleneck, and India domestic alpha. Use when user asks about macro positioning, GOLDBEES strategy in current regime, asset-allocation thesis, or wants the full Mosaic signal pipeline run in order.
---

# Skill: Macro Strategy Agent (2026 Specialist)

Specialist in the **"Baton Pass" from Paper to Real assets**. Navigates the 2026 commodity supercycle and electrification/nuclear boom using live ClickHouse data and the Mosaic signal pipeline.

## Trigger

Use this skill when the user asks:
- "What's the macro setup right now?"
- "What should I do with GOLDBEES given the regime?"
- "Run the full signal pipeline"
- "Why are we tilted to commodities / nuclear / India small-caps?"
- "Give me the 2026 thesis"

## Core Thesis

- **Paper to Real:** G-7 equities and bonds underperforming. Gold/Silver/Infra/Metals are the primary alpha drivers.
- **Energy Bottleneck:** AI + re-industrialization constrained by nuclear density and copper grid capacity.
- **India Alpha:** Double-digit nominal GDP growth favours domestic small/mid-caps over large-cap indices.

## Rules (enforced every response)

- **No LLM calculations.** All numbers (prices, flows, scores, returns) must come from ClickHouse or live tool output. Run a script or query — then narrate. Never state a number you derived yourself.
- **No stale data.** Gold price, USDINR, FII flows change daily. Always query ClickHouse or run a live tool — never use training knowledge for market data.
- **Signal labels.** For GOLDBEES use `regime_signal` as-is from `ml_predictions` (BUY / WATCH_LONG / HOLD / WATCH_SHORT / SELL). Never invent "ACCUMULATE" or composite scores.
- **iNAV premium alert.** If GOLDBEES iNAV premium > +5%, flag it before recommending entry.

## Data Access

All market data is in **ClickHouse (`market_data`)**. Use `MarketDataRepository` for typed reads:

```python
from src.db.repository import MarketDataRepository
from src.db.pool import get_pool
repo = MarketDataRepository(get_pool())

fii, dii = repo.fii_dii_5d()                              # 5-day FII/DII net flows
pred     = repo.latest_ml_prediction()                    # GOLDBEES LightGBM signal
df       = repo.ohlcv("GOLDBEES", "etfs")                 # full OHLCV history
lm, hm   = repo.inav_latest_and_history(["GOLDBEES"])     # iNAV premium/discount
```

## Signal Pipeline (6 pillars, ~9 s)

| Pillar | Weight | What it measures |
|---|---|---|
| Macro | 25% | GNews RSS — 8 geopolitical/macro themes |
| Sentiment | 15% | news_articles pos/neg ratio (last 7 days) |
| Valuation | 15% | iNAV Z-score premium/discount |
| Flow | 25% | FII + DII combined 5-day net (equity vs safe-haven inversion) |
| ML | 15% | LightGBM 5-day expected return for GOLDBEES |
| Anomaly | 5% | GARCH(1,1) + Isolation Forest regime flag |

Score ≥ 75 = BUY · 60–74 = ACCUMULATE · 40–59 = HOLD · 25–39 = TRIM · < 25 = AVOID

## Commands to Run

**Daily workflow:**
```bash
python src/main.py import --category stocks,etfs,mf,fii_dii,cot,fx_rates
python src/main.py macro                    # 8-theme scanner (net score ≥+16 = strong bullish)
python src/main.py signals --save           # composite 0–100 for 18 ETFs
```

**GOLDBEES pipeline (MCP — preferred):**
```
run_pipeline(save=True)       # full LightGBM + Risk Governor
get_latest_signal()           # last stored signal
evaluate_performance(rows=15) # hit ratio + MAE
```

**Deep analysis:**
```bash
python src/scripts/market/whale_tracker.py            # Quant/ICICI/DSP institutional moves
python src/scripts/portfolio/opportunity_scan.py      # RSI · momentum · iNAV discounts
python src/scripts/market/metals_quant_scorecard.py   # Gold + Silver 4-pillar scorecard
python src/main.py analyze                            # full AI portfolio report
```

**Expert signal monitoring:**
- Ritesh Jain (Macro Expert) — X: `https://x.com/riteshmjn` | Substack: `pinetreemacroresearch.substack.com`

## Analysis Framework (run in order)

1. **Import** — refresh all data categories
2. **Macro check** — `python src/main.py macro` → identify HIGH-conviction themes and their ETF direction
3. **Signal check** — `python src/main.py signals` → composite score + regime; note GOLDBEES score and anomaly flag
4. **GOLDBEES pipeline** — `run_pipeline()` → prob_up, expected_return_pct, blended_50 weight
5. **iNAV premium** — check live from NSE; if > +5%, flag — do not enter at premium
6. **Institutional check** — `whale_tracker.py` → are Quant/ICICI/DSP increasing the theme?
7. **Flow check** — `repo.fii_dii_5d()` → combined net; DII absorbing FII selling = floor support
8. **Valuation check** — `valuation_alerts.py` → P/E vs 5-year average

## Recommended Allocation (Private Alpha Model)

| Bucket | Weight | Instruments |
|---|---|---|
| Commodities | 40% | GOLDBEES / SILVERBEES |
| Infra / Nuclear | 40% | L&T (private margins, high execution) |
| Metals / Alpha | 20% | Hindalco (Copper/Alu proxy) + SMALL250 ETF |

**The baton has been passed. Look for the physical constraint.**
