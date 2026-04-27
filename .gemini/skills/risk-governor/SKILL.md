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

## Recommended Command (use the MCP pipeline tool)

```
run_pipeline        ← calls Risk Governor + Kelly blend in one step
get_latest_signal   ← reads last stored RG weight from DB
```

## Manual Command (standalone, no Kelly blend)

```bash
python -c "
import sys; sys.path.insert(0,'.')
from src.tools.risk_governor import compute_position_weight, explain_decision, vol_target_for
import clickhouse_connect, pandas as pd, warnings
warnings.filterwarnings('ignore')

try:
    c = clickhouse_connect.get_client(host='localhost', port=8123)
    # Use GOLDBEES ETF prices — NOT COMEX GOLD (different asset class)
    price_df = c.query_df('''
        SELECT trade_date,
               toFloat64(argMax(open,   imported_at)) AS open,
               toFloat64(argMax(high,   imported_at)) AS high,
               toFloat64(argMax(low,    imported_at)) AS low,
               toFloat64(argMax(close,  imported_at)) AS close,
               toFloat64(argMax(volume, imported_at)) AS volume
        FROM market_data.daily_prices
        WHERE symbol='GOLDBEES' AND category='etfs'
        GROUP BY trade_date ORDER BY trade_date DESC LIMIT 300
    ''')
    c.close()
    price_df['trade_date'] = pd.to_datetime(price_df['trade_date'])
    price_df = price_df.sort_values('trade_date').reset_index(drop=True)
    from src.ml.anomaly import run_composite_anomaly
    df_r, _, _ = run_composite_anomaly(price_df)
    garch_vol = float(df_r['garch_vol'].dropna().iloc[-1])
    regime    = str(df_r['regime'].iloc[-1])
    latest    = float(df_r['close'].iloc[-1])
    ema50     = float(price_df['close'].ewm(span=50, adjust=False).mean().iloc[-1])
    below_ema = latest < ema50
except Exception as e:
    garch_vol = 16.5; regime = '✅ Normal'; below_ema = False
    print(f'Warning: using defaults ({e})')

vol_target = vol_target_for('GOLDBEES')
d = compute_position_weight(
    garch_annual_vol_pct=garch_vol,
    regime=regime,
    vol_target_pct=vol_target,
    price_below_ema50=below_ema,
)
print(explain_decision(d))
"
```

## Formula

```
w(t) = min(1.0,  vol_target / σ_t)  ×  regime_mult  ×  trend_mult
```

Vol target per asset class: gold/silver = 15%, equity = 18%, debt = 5%.

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
