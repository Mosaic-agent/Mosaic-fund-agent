# Anomaly Detection — How It Works

The **🔬 Anomaly Detection** tab runs a three-step composite pipeline on any symbol in ClickHouse.

## Step 1 — Robust Z-Score (MAD)

Standard Z inflates σ when prices trend, causing it to report near-zero on a real crash (the high prices leading up to the crash bloat σ).

Rolling MAD Z stays centred on the local median and resists outlier inflation:

$$Z_{robust} = 0.6745 \times \frac{x - \tilde{x}_{rolling}}{\text{MAD}_{rolling}}$$

Applied to `daily_return %` and `range %` (high−low / close), averaged for a combined `z_robust` score.

## Step 2 — Random Forest Residual Z-Score

Trains a Random Forest regressor on lagged close prices (`lag_1..lag_N`), `MA7`, `MA30`, and lagged volume. The **residual** (actual − predicted) isolates the unexpected component of each day's price move.

`Z_resid` is the rolling MAD Z-score of those residuals.

| `z_robust` | `z_resid` | Regime |
|---|---|---|
| High | Low | 📈 Strong Trend (HODL) — predictable uptrend |
| Low | High | ⚡ Flash Crash / Black Swan (EXIT) — unexpected shock |
| High | High | 🔥 Volatile Breakout — caution |
| Low | Low | ✅ Normal |

## Step 3 — Isolation Forest Confidence Multiplier

Isolation Forest is run on `[daily_return, range_pct, z_robust]`. Its `score_samples` output is normalised to [0 → 1] (1 = most anomalous).

$$Z_{final} = Z_{robust} \times (1 + IF_{confidence})$$

This **boosts** days suspicious to both algorithms while filtering out noise where only one signal fires.

## Configurable Parameters

| Parameter | Default | Range | Effect |
|---|---|---|---|
| IF Contamination | 5% | 1–20% | Expected anomaly fraction |
| Final-Z threshold | 2.5 | 1.0–5.0 | Flagging sensitivity |
| RF lag features | 5 | 3–10 | How much history RF sees |
| Z-score rolling window | 30 | 10–60 | Rolling MAD lookback |

## Requirements

- ≥ 60 rows per symbol in ClickHouse
- Run `python src/main.py import --category etfs` (or any category) first
