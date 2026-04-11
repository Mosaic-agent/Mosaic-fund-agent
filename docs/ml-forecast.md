# ML Forecast — LightGBM 5-Day Return Predictor

`src/ml/trend_predictor.py` — a walk-forward LightGBM model that predicts GOLDBEES 5-day forward log return and emits a regime signal.

## Alpha Features (25 total)

| Feature | Source | Description |
|---|---|---|
| `f_logret1` | GOLDBEES | Daily log return — stationarity fix |
| `f_goldbees_logret5` | GOLDBEES | 5-day log momentum |
| `f_goldbees_logret20` | GOLDBEES | 20-day log momentum |
| `f_ema_cross39` | GOLDBEES | EMA(3)/EMA(9)−1 — short-term trend |
| `f_ema_cross920` | GOLDBEES | EMA(9)/EMA(20)−1 — medium trend |
| `f_ma_ratio` | GOLDBEES | close / 20-day SMA — mean reversion |
| `f_atr14_pct` | GOLDBEES | ATR(14) / close × 100 — vol regime |
| `f_hvol10` | GOLDBEES | 10-day historical vol (annualised) |
| `f_cot_pct_oi` | COT *(3-day lag)* | mm_net / open_interest × 100 |
| `f_spread_pct` | iNAV | GOLDBEES premium/discount to AMFI NAV |
| `f_spread_delta5` | iNAV | 5-day change in retail spread |
| `f_aum_mom_30d` | ETF AUM | 30-day log % Δ of GLD total assets |
| `f_usdinr_vol14` | FX | 14-day USDINR realised vol × 100 |
| `f_usdinr_60d` | FX | 60-day USDINR log % change |
| `f_dxy_logret5` | Yahoo Finance | 5-day DXY log return (real DXY) |
| `f_dxy_logret20` | Yahoo Finance | 20-day DXY log return |
| `f_gold_logret5` | COMEX GOLD | 5-day COMEX gold log return |
| `f_us10y_level` | Yahoo Finance (^TNX) | US 10Y yield level — rate regime |
| `f_us10y_delta5` | Yahoo Finance (^TNX) | 5-day yield shock |
| `f_real_yield` | Derived | US10Y − 2.5% inflation constant |
| `f_real_yield_delta5` | Derived | 5-day change in real yield |
| `f_gsr` | Yahoo Finance | Gold/Silver Ratio — risk-on/off |
| `f_gsr_zscore` | Derived | 60-day rolling z-score of GSR |
| `f_month_sin/cos` | Calendar | Cyclical month encoding |
| `f_dow_sin/cos` | Calendar | Cyclical 5-day trading week encoding |
| `f_fii_net_5d` | FII/DII flows | 5-day rolling FII net cash flows (₹ Cr) |
| `f_dii_net_5d` | FII/DII flows | 5-day rolling DII net cash flows (₹ Cr) |
| `f_inst_net_momentum` | FII/DII flows | 5-day combined FII+DII net flow |

> **COT look-ahead bias fix:** The COT subquery uses `WHERE addDays(report_date, 3) <= trade_date` to simulate the CFTC's Friday publication delay (data compiled Tuesday, published Friday).

> **DXY collinearity fix:** `f_dxy_proxy` (−USDINR return) is dropped when real DXY is available to prevent feature splitting on correlated signals.

## Confidence Intervals — Quantile Regression

Three concurrent LightGBM models are trained **per CV fold** with independent early stopping:

| Model | Objective | Output |
|---|---|---|
| `m_mean` | `regression` (MSE) | Expected return |
| `m_low` | `quantile α=0.10` | 10th percentile (downside bound) |
| `m_high` | `quantile α=0.90` | 90th percentile (upside bound) |

Each model's `n_estimators` for the final full-data fit is derived from its own average `best_iteration_` across folds — quantile trees converge at a different depth than MSE trees.

This produces **true 80% prediction intervals**, not heuristic ±σ multipliers. Example:
```
Expected return: +0.63%   [WATCH_LONG]
80% CI:          [−1.81%, +4.50%]
```
Asymmetry (positive mean, negative lower bound) signals a high-variance setup — the Risk Governor uses this to reduce allocation.

## Regime Signals

| Signal | Threshold | Meaning |
|---|---|---|
| `BUY` | pred ≥ +1.5% | All signals constructive |
| `WATCH_LONG` | pred ≥ +0.5% | Mild upside — hold |
| `HOLD` | −0.5% ≤ pred < +0.5% | No strong edge |
| `WATCH_SHORT` | pred ≥ −1.5% | Mild downside — reduce |
| `SELL` | pred < −1.5% | Strong negative signal |

## Walk-Forward Validation

Uses `sklearn.TimeSeriesSplit` with a gap of 10 rows (2× horizon) between train and test folds — no look-ahead leakage. Internal early-stopping validation uses the last 15% of each training window; the test fold is never touched.

## Persistence

Every prediction is written to:
- `market_data.ml_predictions` (ClickHouse) — queryable via the SQL Explorer preset
- `predictions_log.jsonl` (repo root) — git-trackable for accuracy backtesting

### Accuracy backtesting

Use the **"ML predictions accuracy"** preset in the SQL Explorer tab. Requires ≥ `horizon_days` of subsequent GOLDBEES price data before `actual_return_pct` is populated.

## Requirements

- ≥ 120 clean training rows (GOLDBEES + NAV + COT + FX)
- Use `--lookback 1000` on first import to build enough history
- macOS: `brew install libomp` (required by LightGBM)
