# Anomaly Detection — How It Works

The **🔬 Anomaly Detection** tab runs a three-step composite pipeline on any symbol in ClickHouse.

## Step 1 — Robust Z-Score (MAD)

Standard Z inflates σ when prices trend, causing it to report near-zero on a real crash (the high prices leading up to the crash bloat σ).

Rolling MAD Z stays centred on the local median and resists outlier inflation:

$$Z_{robust} = 0.6745 \times \frac{x - \tilde{x}_{rolling}}{\text{MAD}_{rolling}}$$

Applied to `daily_return %` and `range %` (high−low / close), averaged for a combined `z_robust` score.
An independent `z_volume` score is also computed on raw volume.

## Step 2 — GARCH(1,1) Standardised Residual

Replaces the previous Random Forest step. Daily log-returns are near a random walk (RF R²≈0.32, firing on 21% of days); GARCH models **conditional volatility** σ_t directly:

$$\sigma^2_t = \omega + \alpha \cdot \varepsilon^2_{t-1} + \beta \cdot \sigma^2_{t-1}$$

The **standardised residual** `e_t = r_t / σ_t` is the industry-standard financial anomaly score:
- During quiet periods: σ_t is small → moderate returns are correctly flagged
- During volatile periods: σ_t is large → only truly extreme moves flag
- Fire rate: **~8%** (vs RF's 21%)

A Student-t distribution is used (fat tails — more realistic for gold than Gaussian).

### Output columns

| Column | Description |
|---|---|
| `garch_vol` | Annualised conditional volatility % (e.g. 34.5%) |
| `garch_band_1s` / `_2s` | Price-space band width at ±1σ / ±2σ |
| `z_resid` | Standardised residual e_t = r_t / σ_t |

## Step 3 — Isolation Forest Confidence Multiplier

Isolation Forest is run on an enriched feature set:

| Feature | Source |
|---|---|
| `daily_return` | OHLCV |
| `range_pct` | OHLCV |
| `z_volume` | OHLCV |
| `usdinr_logret` | USDINR FX (if available) |
| `usdinr_vol14` | USDINR 14-day realised vol |
| `cot_pct_oi` | COT mm_net / open_interest (forward-filled weekly) |

`score_samples` normalised to [0 → 1] (1 = most anomalous).

$$Z_{final} = Z_{robust} \times (1 + IF_{confidence})$$

This **boosts** days suspicious to both algorithms while filtering noise where only one signal fires.

## Regime Classification

Thresholds are dynamic (80th percentile of the full window) to prevent threshold drift across different vol regimes.

| Regime | Condition | Action |
|---|---|---|
| ⚡ Flash Crash / Black Swan (EXIT) | Low z_robust + High z_resid | Unexpected shock — reduce exposure |
| 🔥 Volatile Breakout | High z_robust + High z_resid | Caution |
| ⚠️ Crowded Long (Squeeze Risk) | High z_robust + COT > 75th pct + Positive return | Positioning risk |
| 🧨 Blow-off Top (Weak) | High z_robust + Low volume + Positive return | Thin-volume rally |
| 📈 Strong Trend (HODL) | High z_robust + Low z_resid | Predictable uptrend |
| ✅ Normal | All other | No action |

## Risk Governor Integration

`garch_vol` feeds directly into the **Risk Governor** (`src/tools/risk_governor.py`):

$$w(t) = \min\left(1.0,\ \frac{15\%}{\sigma_t}\right) \times \text{regime\_mult} \times \text{score\_gate}$$

At current gold vol (34.5%): `w = min(1.0, 15/34.5) = 43%` → hold 43% of target position.

## Configurable Parameters

| Parameter | Default | Range | Effect |
|---|---|---|---|
| IF Contamination | 5% | 1–20% | Expected anomaly fraction |
| Final-Z threshold | 2.5 | 1.0–5.0 | Flagging sensitivity |
| Z-score rolling window | 30 | 10–60 | Rolling MAD lookback |

## Requirements

- ≥ 60 rows per symbol in ClickHouse
- Run `python src/main.py import --category etfs` (or any category) first
- Cross-asset enrichment (COT + USDINR) fetched automatically if available
