---
name: risk-governor
description: Compute volatility-targeted position sizing using GARCH conditional volatility. Applies continuous inverse-vol scaling (w = vol_target/σ_t) plus regime and quant score overrides. Use when user asks about position size, how much to hold, risk management, or when GARCH vol is elevated.
---

# Skill: Risk Governor

Computes recommended position weight using GARCH annualised vol + anomaly regime + composite quant score.

## Trigger

Use this skill when the user asks:
- "How much GOLDBEES should I hold?"
- "What position size given current vol?"
- "Is it safe to add at this volatility?"
- "Run the risk governor"
- "What does the risk model say?"

## Command

```bash
python -c "
import sys; sys.path.insert(0,'.')
from src.tools.risk_governor import compute_position_weight, explain_decision
import clickhouse_connect, warnings
warnings.filterwarnings('ignore')

# Get latest GARCH vol and regime from ClickHouse
try:
    c = clickhouse_connect.get_client(host='localhost', port=8123)
    row = c.query_df('''
        SELECT trade_date, toFloat64(open) AS open, toFloat64(high) AS high,
               toFloat64(low) AS low, toFloat64(close) AS close,
               toFloat64(volume) AS volume
        FROM market_data.daily_prices FINAL
        WHERE symbol='GOLD' AND category='commodities'
        ORDER BY trade_date DESC LIMIT 200
    ''')
    import pandas as pd; row['trade_date'] = pd.to_datetime(row['trade_date'])
    row = row.sort_values('trade_date').reset_index(drop=True)
    from src.ml.anomaly import run_composite_anomaly
    df_r, _, _ = run_composite_anomaly(row)
    garch_vol = float(df_r['garch_vol'].dropna().iloc[-1])
    regime    = str(df_r['regime'].iloc[-1])
    c.close()
except Exception as e:
    garch_vol = 34.5
    regime    = '✅ Normal'
    print(f'Warning: using defaults ({e})')

d = compute_position_weight(garch_vol, regime)
print(explain_decision(d))
"
```

## Formula

```
w(t) = min(1.0,  15% / σ_t)   × regime_mult × score_gate
```

| Vol | Weight | Tier |
|-----|--------|------|
| ≤15% | 100% | FULL |
| 22% | 68% | REDUCED |
| 34.5% | 43% | HALF |
| 38% + Flash Crash + bear score | 10% | MINIMAL |

## Regime Multipliers
- ⚡ Flash Crash → 0.50×
- 🔥 Volatile Breakout → 0.75×
- ⚠️ Crowded Long → 0.80×
- 🧨 Blow-off Top → 0.70×
- ✅ Normal / 📈 Strong Trend → 1.00×
