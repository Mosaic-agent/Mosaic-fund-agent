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

---

## ETF Premium Strategy Backtest (`ou_regime_backtest.py`)

A full walk-forward backtest that applies PELT regime detection *before* fitting the OU mean-reversion
model — the strategy only trades inside confirmed stationary segments.

### Architecture: two-layer pipeline

```
premiums[0..t]
  ── PELT change-point detection ──► latest stationary segment
  ── ADF stationarity gate ──────► pass/fail
  ── if pass: fit_ou → ZJL thresholds b*, s*
  ── trade decision (4 states)
```

Layer 1 — **PELT** (`ruptures`, `model="rbf"`) detects structural breaks in the premium
series. Only the most-recent segment is passed to the OU fit. This prevents a regime change
(e.g. RBI quota opening) from polluting the current fit with pre-break data.

- **Penalty Estimation (`pen_window`):** The penalty variance is computed over a rolling `pen_window` (default 250 observations $\approx$ 1 year) rather than the full history to date. This ensures the penalty tracks current volatility instead of an ever-growing, inflated blend of old and new regimes (which historically caused `MAFANG` to flatline from Dec 2024 onward at `pen_multiplier=3.0`).
- **Data-Driven Penalty (CROPS):** You can opt in to a data-driven penalty selector via `penalty_method="crops"`. This sweeps a log-spaced penalty grid and uses elbow detection on the `(number of breakpoints, cost)` curve (approximating Haynes, Eckley & Fearnhead 2017) to find the optimal trade-off point. *Note:* While it improves performance on `MAFANG`, it can overfit on other symbols (e.g., `MON100` Sharpe 2.80 $\to$ 1.24).

Layer 2 — **Stationarity gate** (`src/ml/premium_regime.py`): ADF + KPSS tests run on the
current PELT segment. The gate is **ADF-only** — only `adf_p < threshold` must pass to
enable trading. KPSS is advisory and only adjusts the confidence score.

> **Why ADF-only?** ETF premiums have ARCH-type heteroskedasticity. KPSS rejects too
> aggressively on short segments with clustered volatility. Making KPSS a hard gate produced
> near-zero trades despite significant mean-reversion in backtests.

### 4 market states

| State | Trigger | Action |
|---|---|---|
| `CHEAP` | stationary + confident + `prem ≤ b*` | Buy (go to full exposure) |
| `EXPENSIVE` | stationary + confident + `prem ≥ s*` | Sell (close position) |
| `FAIR` | stationary + confident + between b*/s* | Hold — no new trade |
| `STRUCTURAL_SHIFT` | PELT break detected AND segment age < `structural_shift_window` (10d) | Hold — wait for new regime to mature |
| `NON_STATIONARY` | ADF gate fails on current segment | Hold |
| `LOW_CONFIDENCE` | stationary but `confidence < confidence_threshold` | Hold |
| `BURNIN` | first N days of data | No trading |

### Confidence score (0–100)

Weighted composite over four signals:

| Component | Weight | What it measures |
|---|---|---|
| ADF p-value | 30% | `1 − min(adf_p / threshold, 1)` — lower p = higher confidence |
| KPSS p-value | 30% | `min(kpss_p / 0.05, 1)` — higher p (fail-to-reject) = more confident |
| Segment maturity | 20% | `min(segment_age / structural_shift_window, 1)` — avoids trading into a fresh break |
| R² of AR(1) fit | 20% | direct OU fit quality |

Set `confidence_threshold=50` to require at least 50/100 before trading. Range 50–70
is the practical sweet spot — below 50 the gate rarely binds, above 70 it over-filters.

### ZJL optimal double-stopping thresholds

`b*` (buy) and `s*` (sell) come from a dynamic-programming solution to the optimal
double-stopping problem for OU processes (Zervos-Johnson-Lai). Implemented in
`src/ml/ou_estimator.py`. These are optimal for the fitted θ, μ, σ, and a given
transaction cost and discount rate — more principled than fixed ±1.5σ∞ bands,
especially when θ is low (slow-reverting, so tighter thresholds are costly).

### Position sizing

Exposure is binary: 0 (out) or 1 (full). `floor_exposure` (default 0.20) is held
during `STRUCTURAL_SHIFT` / `TRANSITION` events rather than going to zero.

### Refit cadence (`--refit-every`)

PELT+ADF+KPSS+OU is refit only every N days (default 5). Between refits, the cached
regime state (`b*`, `s*`, status) is reused; today's live premium is still compared
against the cached thresholds. This bounds runtime to ~90 s for 850 days of history:

| `--refit-every` | Runtime (850 rows) | Use case |
|---|---|---|
| 1 | ~4 min | highest accuracy |
| 5 | ~90 s | **default** |
| 10 | ~50 s | quick exploration |

### Performance (GOLDBEES, 2023-01-02 → 2026-07-09)

| Metric | PELT-OU (base) | Naive ±1.5σ |
|---|---|---|
| P&L (pp) | **+19.39** | +13.32 |
| Sharpe | **+0.852** | +0.663 |
| Win rate | **97.1%** | — |
| Round trips | 35 | — |
| Max drawdown | 6.68 pp | 6.68 pp |

PELT-OU beats naive by **+46%** on P&L and **+28%** on Sharpe. The outperformance
comes from the regime gate — it suppresses trades when the premium series loses
stationarity (28% of days classified `NON_STATIONARY`).

### Entry points

| Where | How |
|---|---|
| CLI | `PYTHONPATH=. python src/scripts/market/ou_regime_backtest.py --symbol GOLDBEES --confidence-threshold 50` |
| Agent | `run_ou_regime_backtest("GOLDBEES", confidence_threshold=50)` — routes via `IntlETFSubAgent` |
| Streamlit UI | **🌍 Intl ETFs → 🔁 OU Regime Backtest** tab, or **⚙️ Workflows → OU Regime Backtest** |

### CLI reference

```
--symbol              ETF symbol (default: MON100)
--start / --end       Date range (default: 3 years to today)
--confidence-threshold  Only trade when confidence ≥ N (default: 0, recommended: 50–70)
--pen-multiplier      PELT penalty = multiplier × var(premiums) (default: 3.0)
--pen-window          Trailing obs window for PELT penalty variance (default: 250, 0=full history)
--c-buy / --c-sell    Transaction costs in bps (default: 10/10)
--burnin              Days before trading starts (default: 90)
--notional            ₹ notional for P&L display (default: 10,00,000)
--floor-exposure      Exposure during STRUCTURAL_SHIFT events (default: 0.20)
--refit-every         Refit cadence in days (default: 5)
--include-crops-sensitivity  Add a 5th sensitivity run using CROPS-style penalty selection
--log-level           Logging verbosity (default: WARNING)
--csv-path            Override ClickHouse with a local CSV [date, premium_pct, price, inav]
--event-flags-csv     CSV with [date, event_flag] for SEBI/RBI override dates
```

### Agent caching

`run_ou_regime_backtest` caches results in `output/.cache/ou_backtest_<SYMBOL>_<hash>.txt`.
The cache key includes all parameters **and** the latest `trade_date` in
`market_data.inav_snapshots` for that symbol — so the cache is automatically invalidated
when new iNAV data arrives (daily import). Repeated agent calls with the same params return
instantly from cache.

### Sensitivity Runs & Parallelization

The CLI automatically runs three sensitivity checks after the base run:
- `2x_costs` — double entry/exit costs (friction robustness)
- `pen×1.5` — PELT penalty multiplier 1.5× (more break-detection sensitivity)
- `pen×6.0` — PELT penalty multiplier 6.0× (fewer breaks, longer segments)
- `crops` — data-driven elbow penalty selection (if opted in via `--include-crops-sensitivity`)

The base run and sensitivity runs are dispatched in parallel to a `ProcessPoolExecutor` (since they are fully independent). This parallel execution yields a **~2.6x wall-clock speedup**.
Sensitivity runs are omitted from the agent tool output (trimmed to reduce context) but appear in the CLI report and chart Panel 4.

### Key source files

| File | Role |
|---|---|
| `src/ml/premium_regime.py` | PELT + ADF/KPSS gate + confidence score + `RegimeState` |
| `src/ml/ou_estimator.py` | OU AR(1) fit + ZJL dynamic-programming thresholds |
| `src/scripts/market/ou_regime_backtest.py` | Walk-forward engine + Plotly chart + CLI |
| `src/tools/intl_etf_tools.py` | `run_ou_regime_backtest` agent tool with caching |
| `src/agents/sub_agents/intl_etf.py` | Routing rules (RULE 1a backtest / RULE 1b OU signal) |
