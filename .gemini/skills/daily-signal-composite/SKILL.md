---
name: daily-signal-composite
description: Run the Signal Aggregator to compute a unified 0–100 composite score per ETF by combining macro events, news sentiment, NAV Z-scores, FII/DII flows, ML predictions, and anomaly regimes. Use when user asks for ETF buy/sell recommendations, signal dashboard, or composite view.
---

# Skill: Daily Signal Composite

Aggregates 6 signal sources into a unified 0–100 composite score per ETF with BUY/ACCUMULATE/HOLD/TRIM/AVOID actions.

## Trigger

Use this skill when the user asks:
- "What should I buy/sell today?"
- "Which ETFs have the strongest signals?"
- "Run the signal aggregator"
- "Give me a composite view of all ETFs"
- "What does the signal dashboard show?"

## Commands

```bash
# Compute and display
python src/main.py signals

# Compute + persist to ClickHouse (market_data.signal_composite)
python src/main.py signals --save

# Verbose (show per-source debug info)
python src/main.py signals --save -v
```

## 6 Signal Sources (Weighted)

| Source | Weight | Data |
|--------|--------|------|
| Macro net signal | 25% | 8-theme geopolitical → ETF directional impact sum |
| ETF news sentiment | 15% | Positive/negative ratio from last 7 days in DB |
| NAV premium Z-score | 15% | Discount = buy opportunity, premium = overpriced |
| FII/DII flow momentum | 25% | 5-day institutional net flows |
| ML prediction | 15% | LightGBM 5-day forward return (GOLDBEES) |
| Anomaly regime | 5% | Flash Crash → contrarian boost; Blow-off → dampening |

## Action Thresholds

| Score Range | Action |
|-------------|--------|
| ≥ 75 | **BUY** |
| 60–74 | **ACCUMULATE** |
| 40–59 | **HOLD** |
| 25–39 | **TRIM** |
| < 25 | **AVOID** |

## 18 ETFs Covered

GOLDBEES, NIFTYBEES, BANKBEES, ITBEES, JUNIORBEES, SILVERBEES, CPSEETF,
LIQUIDBEES, LIQUIDCASE, GILT5YBEES, MON100, MAFANG, HNGSNGBEES, AUTOBEES,
PHARMABEES, PSUBNKBEES, MID150BEES, SMALL250

## DB Storage

Results saved to `market_data.signal_composite` table.
View in Streamlit UI → **🎛️ Signals** tab.

## Source File

`src/agents/signal_aggregator.py`
