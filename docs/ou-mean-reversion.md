# OU Mean-Reversion — International ETF Premium Strategy

`src/ml/ou_estimator.py` (fit) + `src/scripts/chart_ou_premium.py` (shared chart builder) —
models the scarcity premium of NSE-listed international ETFs (MAFANG, HNGSNGBEES, MON100,
MASPTOP50, MAHKTECH, MONQ50) as an Ornstein-Uhlenbeck mean-reverting process and derives
buy/sell zones, a forward expected path, and reversion probabilities from the fit.

## Why premium, not price

These ETFs hold foreign securities. Creation/redemption isn't continuous (SEBI overseas
investment caps, AP friction), so market price persistently trades away from iNAV — not
randomly, but pulled toward a structural equilibrium premium. That's a mean-reverting
process, which is exactly what OU models. The strategy is a **relative-value trade on the
premium itself** (buy when the premium is compressed, expect it to re-expand toward its own
equilibrium) — it is not a directional bet on the underlying index.

## The model

```
dX_t = θ(μ − X_t)dt + σ dW_t
```

| Symbol | Meaning |
|---|---|
| `X_t` | premium/discount % at time t |
| `μ` | long-run equilibrium premium (not 0% — wherever this ETF structurally settles) |
| `θ` | speed of mean reversion (per day) — higher reverts faster |
| `σ` | volatility of the premium process |
| `half_life = ln(2)/θ` | days for half of any gap to μ to close |

## Estimation — exact discrete-time OLS on AR(1)

`fit_ou(premiums, dt=1.0)` in `src/ml/ou_estimator.py` regresses `X_{t+1} = a + b·X_t + ε`
and maps back to continuous OU parameters:

```
θ = −ln(b) / dt
μ = a / (1 − b)
σ = std(ε) × √(−2·ln(b) / (dt·(1 − b²)))
```

`b` must lie strictly in `(0, 1)` for the series to be mean-reverting. If `b ≤ 0` or `b ≥ 1`
(random walk or explosive), `fit_ou()` returns `None` — callers must handle this (e.g. a
90-day rolling window during a sustained one-directional premium drift can fail the fit;
`build_ou_chart()` skips those windows rather than plotting garbage).

Minimum 30 observations (`_MIN_OBS`) required.

## Stationary bands (σ∞)

The OU process has a known stationary distribution `X_∞ ~ Normal(μ, σ∞²)` where:

```
σ∞ = σ / √(2θ)
```

This — not a rolling stdev — is what the ±1σ/±2σ bands on the chart are built from. It's the
theoretical steady-state spread implied by the fitted θ and σ.

## Signal thresholds

```
Buy zone:  premium < μ − 1.5·σ∞
Sell zone: premium > μ + 1.5·σ∞
```

Backtested rule of thumb (1.5σ), not a statistically optimal cutoff — trades signal
frequency against reliability.

## Forward path & reversion probability

`expected_premium(current, θ, μ, h)` — exact conditional expectation, an exponential decay
toward μ (not linear, since reversion speed is proportional to distance from equilibrium):

```
E[X_h] = μ + (X_0 − μ)·e^(−θh)
```

`prob_revert(current, θ, μ, σ, threshold, h)` uses the OU's conditional normal distribution
`X_h | X_0 ~ N(E[X_h], Var[X_h])` with `Var[X_h] = σ²/(2θ)·(1 − e^(−2θh))` to compute the
probability of crossing back to a threshold (typically μ) within `h` days.

`expected_reversion(current, θ, μ, h)` returns the expected **change** — `(μ − current)·(1 − e^(−θh))`
— used for the "E[Δprem 10d]" annotation.

## Worked example — MAFANG (365d lookback, as of 2026-07-04)

| Stat | Value |
|---|---|
| Current premium | 18.45% (39th percentile of trailing 365d) |
| OU equilibrium μ | 18.88% |
| θ (speed) | 0.4552/day |
| Half-life | 1.5 days — a shock washes out in ~1-2 trading days |
| σ∞ | 3.42% |
| Buy threshold | < 13.76% |
| Sell threshold | > 24.01% |
| R² of AR(1) fit | 0.413 |
| Signal | ⚪ HOLD (−0.1σ from equilibrium) |

At θ=0.4552, half-life=1.5d, the reversion is fast — this is a short-holding-period signal,
not a multi-week thesis. Compare to HNGSNGBEES in the same window: θ=0.0388, half-life=17.9d —
a much slower-reverting regime, so the same 1.5σ buy/sell rule implies a much longer expected
holding period there.

## Chart panels (`build_ou_chart()`)

One function builds all 4 panels — both the CLI script and the agent tool call it, so they
never drift out of sync:

1. **Premium + OU equilibrium** — premium series, rolling 90d μ, μ/±1σ reference lines,
   shaded buy/sell zones, backtested signal markers, today's marker — plus a distribution
   side-panel (histogram of the full lookback window, sharing the y-axis) showing today's
   percentile rank.
2. **Price vs iNAV, rebased to 100** — what's actually driving the premium (price rallying
   vs iNAV catching up), since the premium alone doesn't show which side is moving.
3. **Rolling OU half-life (90d window)** — regime-speed context; a rising half-life means
   the mean-reversion assumption is weakening for that stretch (see Caveats).
4. **Forward expected path (60d)** — `E[X_h]` with ±1σ∞ band, plus a probability box
   (`P(→μ in 5d/10d/20d)`, `E[Δprem 10d]`).

## Entry points

| Where | How |
|---|---|
| CLI | `python src/scripts/chart_ou_premium.py --symbol MAFANG --lookback 365` |
| Agent tool | `plot_ou_premium_chart(symbol="MAFANG", lookback=365)` (`src/tools/chart_tools.py`) — saves to `output/reports/<SYMBOL>_ou_premium_strategy.png` and returns a text summary |
| Streamlit UI | **🌍 Intl ETFs → 📐 OU Mean-Reversion** tab at `localhost:8501` — symbol + lookback picker, live chart, KPI tiles, PNG download |

## Caveats

- **R²=0.413 (MAFANG)** — roughly 41% of day-to-day premium moves are explained by the AR(1)
  fit. Real signal, but noisy; don't treat θ/μ as fixed physical constants.
- **θ is regime-dependent** — the rolling half-life panel can show it blowing out 3-4x during
  sustained one-directional premium drift (a period where the "pull back to μ" assumption
  itself weakens). Check the rolling half-life panel before sizing a trade off the full-period
  fit alone.
- **Fit can fail entirely** on rolling windows where `b` falls outside `(0, 1)` — those
  windows are silently skipped (`NaN`) in the rolling half-life series, not treated as errors.
- Not a directional call on the underlying index — this only prices the premium/discount
  component. A correct BUY signal can still lose money if the underlying index itself falls
  hard enough to swamp the premium reversion.
