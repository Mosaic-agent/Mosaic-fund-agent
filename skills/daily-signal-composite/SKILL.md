# Skill: Daily Signal Composite

Aggregates 6 signal sources into a unified 0–100 composite score per ETF with BUY/ACCUMULATE/HOLD/TRIM/AVOID actions.

## Trigger

Use this skill when the user asks about:
- "What should I buy/sell today?"
- "Which ETFs have the strongest signals?"
- "Run the signal aggregator"
- "Give me a composite view of all ETFs"

## How to Run

```bash
cd /Users/dhiraj.thakur/project/Mosaic-fund-agent
source .venv/bin/activate

# Compute and display
python src/main.py signals

# Compute + persist to ClickHouse
python src/main.py signals --save

# Verbose (show per-source debug info)
python src/main.py signals --save -v
```

After saving, view in the Streamlit UI → **🎛️ Signals** tab.

## 6 Signal Sources (Weighted)

| Source | Weight | Data |
|--------|--------|------|
| Macro net signal | 25% | 8-theme geopolitical → ETF directional impact sum |
| ETF news sentiment | 15% | Positive/negative ratio from last 7 days in DB |
| NAV premium Z-score | 15% | Discount = buy opportunity, premium = overpriced |
| FII/DII flow momentum | 25% | 5-day institutional net → equity bullish, gold inverse |
| ML prediction | 15% | LightGBM 5-day forward return (GOLDBEES; others get neutral) |
| Anomaly regime | 5% | Flash Crash → contrarian boost; Blow-off → dampening |

## Action Thresholds

| Score Range | Action |
|-------------|--------|
| ≥ 75 | **BUY** |
| 60–74 | **ACCUMULATE** |
| 40–59 | **HOLD** |
| 25–39 | **TRIM** |
| < 25 | **AVOID** |

## Output

- Per-ETF table: composite score + 6 pillar scores + action
- Top Picks panel (BUY/ACCUMULATE)
- Avoid/Trim panel
- Overall regime: RISK_ON / RISK_OFF / MIXED

## ClickHouse Table

`market_data.signal_composite` — daily per-ETF composite scores + actions.

## Source File

`src/agents/signal_aggregator.py`
