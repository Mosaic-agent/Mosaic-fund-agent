# ML Forecast — LightGBM 5-Day Return Predictor

`src/ml/trend_predictor.py` — a walk-forward LightGBM model that predicts GOLDBEES 5-day forward log return and emits a regime signal.

## Alpha Features

| Feature | Source | Description |
|---|---|---|
| `f_cot_leverage` | COT | MM Net / OI ratio — speculative crowding proxy |
| `f_goldbees_spread` | iNAV | GOLDBEES market price vs NAV spread |
| `f_gld_aum_mom` | ETF AUM | 5-day GLD AUM momentum |
| `f_usdinr_vol14` | FX | 14-day USDINR realised volatility |
| `f_usdinr_trend60` | FX | 60-day USDINR trend slope |
| `f_price_mom5` | GOLDBEES | 5-day price momentum |
| `f_price_mom20` | GOLDBEES | 20-day price momentum |
| `f_ma_ratio` | GOLDBEES | MA5 / MA20 cross |
| `f_spread_delta` | iNAV | Change in iNAV spread |
| `f_us10y_delta5` | Yahoo Finance | 5-day change in US 10Y yield |
| `f_month_sin/cos` | Calendar | Cyclical month encoding (seasonality) |
| `f_dow` | Calendar | Day-of-week (0=Mon → 1=Fri) |
| `f_fii_net_5d` | FII/DII flows | 5-day rolling sum of FII net cash flows (₹ Cr) |
| `f_dii_net_5d` | FII/DII flows | 5-day rolling sum of DII net cash flows (₹ Cr) |
| `f_inst_net_momentum` | FII/DII flows | 5-day combined FII + DII net flow |

## Regime Signals

| Signal | Meaning |
|---|---|
| `BUY` | Strong expected return, positive regime |
| `WATCH_LONG` | Moderate upside, proceed with caution |
| `HOLD` | Neutral |
| `WATCH_SHORT` | Moderate downside risk |
| `SELL` | Negative expected return, bearish regime |

## Walk-Forward Validation

Uses `sklearn.TimeSeriesSplit` — no look-ahead leakage. CV R² is typically negative with < 2 years of data and improves as history accumulates.

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
