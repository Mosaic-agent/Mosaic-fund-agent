# VLRT v3 — result summary

Rebuild of the VLRT tactical allocator as `src/vlrt/`. Run: `python src/scripts/portfolio/vlrt_v3.py`.

## Headline: the V/L/R/T composite does not add value

Evaluated 2018-11 → 2026-08 (1940 trading days, 108 monthly decisions, ~12.5 independent
bets at AR(1)=+0.79). Sleeves: equity (NIFTYBEES) / pm (GOLDBEES) / cash (liquid-fund NAV).
Hold-out is the untouched 2025-08 → 2026-08 window (~13 months), matching the approved plan.

**Acceptance gates: 0/3 passed**, evaluated on the hold-out period only:

| Gate | Result | Evidence |
|---|---|---|
| Beats static 55/20/25 (CI excludes zero) | FAIL | dSharpe -0.332, CI [-1.025, +0.267] |
| Beats block-shuffled-signal null (p<0.05) | FAIL | p=0.692 |
| Replication beats random-walk MAE (equity & pm) | FAIL | equity 13.6 vs 6.6, pm 11.9 vs 2.1 |

This is a negative result plus the mechanical fixes, reported as one — not a tuned model.

| Strategy | CAGR % | Vol % | Sharpe | MaxDD % |
|---|---|---|---|---|
| Equal weight 1/3 | 14.13 | 7.83 | **1.806** | -15.15 |
| Static 55/20/25 | 13.49 | 9.27 | 1.455 | -21.48 |
| B4 vol-only (no tilt) | 11.72 | 8.59 | 1.365 | -21.15 |
| **VLRT v3** | 11.60 | 8.84 | 1.312 | -23.78 |
| 100% NIFTYBEES | 12.95 | 15.46 | 0.838 | -36.34 |

Every paired block-bootstrap Sharpe difference has a 95% CI containing zero, in all three
windows (full / train / hold-out). Two results settle it:

- **vs B4 (identical model with the tilt switched off):** ΔSharpe −0.049 full, −0.071 train,
  +0.014 hold-out — all indistinguishable. Whatever the allocator achieves comes from
  volatility targeting, not from the pillars.
- **vs a block-shuffled-signal null:** VLRT sits at the **27th percentile** (p=0.734). A signal
  with the same autocorrelation but random alignment to returns does *better* on average.

Naive 1/N has the best Sharpe in every window. That is the standard result and it is the bar.

## Why v2 failed (measured, not inferred)

| Defect | v2 | v3 |
|---|---|---|
| V pillar dead (NaN fallback) | 18 of 43 months | 0 |
| R pinned at floor | 11 months | 0 |
| Output frozen | 44% of months | 0% (pillars), 6% (pm weight) |
| Top regime reachable | no (score peaked 73.3 < 75) | n/a — continuous map |
| Equity MAE vs random-walk (4.65) | 14.2 | 9.75 |
| Direction accuracy | 18.6% reported / 34.8% clean | 45.5% (p=0.830) |

v2's `momentum_phase` was computed *from* L and R and then raised the weight *on* L and R
while driving the valuation weight to 0.05 — suppressing the only pillar with measurable IC.

## Signal evidence

Spearman IC vs forward 3-month NIFTYBEES return, causal expanding-percentile-rank inputs:

| Signal | 2016–2026 IC | p | n |
|---|---|---|---|
| `v_px_vs_3y` | +0.291 | 0.001 | 122 |
| `v_cagr5y_rev` | +0.201 | 0.049 | 97 |
| all L / R inputs | \|IC\| ≤ 0.18 | ns | — |

Only `v_px_vs_3y` survives Bonferroni across 12 candidates (α=0.0042). `r_cot_gold` looked
significant on 2023–2026 (IC −0.652, p=0.005, n=17) but is p=0.355 at n=99 — a small-sample
false positive.

Volatility, by contrast, is forecastable: INDIAVIX_t → realised vol_t+1 Spearman **+0.618**
(p=3.5e-14, n=122). Hence the design: size risk, do not time returns.

## Data defects found and corrected

1. `LIQUIDBEES.close` is unusable for returns — constant-NAV daily-dividend fund, 898/907 days
   at exactly 1000.00 with stray values producing ±10% phantom moves. `mf_nav` is also flat.
   Cash now comes from a liquid-fund **growth** NAV (AMFI scheme 100851).
2. `NIFTYBEES.close` has an un-reversed 10:1 split (effective ~2017-08). `repair_price_glitches`
   already unwinds it correctly and produces a fully clean series back to 2011 — **cross-checked
   against NSE's own unadjusted series (nselib)**: NSE's last pre-split close (997.78 on
   2017-08-11) matches the repaired value (99.778) exactly, confirming the true ratio is 10:1.
   `CLEAN_START` moved from 2017-09-01 (overly conservative) to **2011-05-03**, GOLDBEES's own
   start date and the real binding constraint. Shoonya's broker API was checked first as an
   alternative cross-check and ruled out — its historical daily-bar depth only reaches back to
   2021-08-27, entirely inside the window already trusted.
3. `asset_type='gold'` is **precious metals** — gold *and silver*. Sleeve renamed `pm`.
4. `asset_type='bond'` is ~70% TREPS overnight repo. Sleeve renamed `cash`.
5. `asset_type='other'` holds a single-stock equity position (LIC, up to 9.06% of NAV),
   CDs, g-secs and short futures — reclassified at security level.
6. `mf_holdings` carries first-of-month rows from a second scraper (totals of exactly 100.00).
   43 distinct month-end rows, not 47.

Unusable over the window (verified): `fii_dii_flows` (starts 2025-10-01), `stock_valuation`
(no history), `amfi_category_flows` (`category_name` corrupted to roman numerals),
`nse_delivery` (2026-07+), `etf_aum` (2026-04+).

## Replication is now a panel, pooled across both multi-asset funds

`QUANT_MULTI_ASSET` (scheme 120821, 43 months) and `QUANT_DYNAMIC_ASSET_ALLOCATION`
(scheme 120833, 40 months) are pooled at the row level — 83 fund-months, not 43 — to lift
the effective sample size, as the plan specified. `QUANT_DYNAMIC_ASSET_ALLOCATION` carries
no precious-metals sleeve (its disclosed `pm` weight is 0 in every month), which is a mandate
fact, not a model error, and is reported per-fund so it stays visible rather than distorting
the pooled number silently.

Pooled: equity MAE 13.6 vs a 6.6 random-walk baseline, pm 11.9 vs 2.1, direction accuracy
47.0% on 66 scored fund-months (p=0.781 vs a block-shuffled null). The model loses to the
random-walk baseline on every sleeve.

## Extending history revealed a genuine, if narrow, positive result

The `CLEAN_START` extension mostly widens *warm-up* history (V needs a 36-month rolling mean
plus a 24-month rank warmup), so the composite's own valid window only moved from 2016-04 to
125 months total (up from 108) — a real but modest N_eff gain, not the large jump the raw date
range suggested. Independent bets: ~12.6 (barely changed, AR(1)=+0.816 still caps it).

But re-running the block-shuffled-signal null on the full extended sample surfaced something new:

| Test | Sharpe | Null median | p (2000 draws) |
|---|---|---|---|
| **Full composite** vs its own block-shuffled reorderings | 1.431 | 1.242 | **0.0020** |
| B4 (vol-targeting only, flat composite) vs the same null | 1.256 | 1.242 | 0.443 |

The real composite clears its null at p=0.002 (3/2000 draws matched or exceeded it, comfortably
above the null's 99th percentile of 1.380) while B4 does not — and it isn't a turnover-cost
artifact (null-draw turnover 13.42 vs the model's 11.48, too small a gap to explain the jump).
This is the first internally-consistent evidence in this exercise that the V/L/R/T ordering
carries genuine information beyond noise, over the long sample.

**This does not flip the acceptance gates, and here is exactly why not:**
- The same null test restricted to the hold-out window alone (2025-08+) is *not* significant
  (p=0.692) — whatever information the full-sample test detected is not reliably showing up in
  the most recent 13 months specifically.
- Every paired-bootstrap Sharpe comparison against concrete alternatives (static 55/20/25, equal
  weight, B4, the fund's own NAV) still has a 95% CI containing zero, full-sample and hold-out
  alike — "better than a random reordering of itself" is a real but much weaker claim than
  "beats a specific investable alternative with usable confidence."
- Replication against the fund's disclosed book is unchanged and still loses to random-walk.

Read together: the composite is not pure noise — but it has not been shown, at this sample size,
to translate into a portfolio that reliably beats simple alternatives, or into an ability to
predict what a real manager did with the same information. Both statements are true at once and
neither should be dropped to make the story simpler.

## The "proprietary signal" excuse does not hold

v2 attributed its largest divergences to internal signals invisible to market data. Quant's
other equity funds explain **R² = 0.012** of Multi Asset's equity weight (p=0.481, n=43)
and **R² = 0.044** of Dynamic Asset Allocation's (p=0.192, n=40). Neither fund's allocation
is predicted by its own house positioning either.

## Fund-NAV benchmark (reference only, not apples-to-apples)

The fund's own NAV (scheme 120821) is shown alongside the backtest for context. It is net of
TER, may hold silver the model does not, and discloses a short single-stock-futures overlay in
some months — so it is a reference point, not a claim the model was compared fairly against a
tradeable alternative. Full-sample Sharpe 1.75 vs VLRT v3's 1.31; the paired-bootstrap CI is
wide and contains zero either way.


---

## The gap, synthesized: three distinct problems, not one

Everything above collapses into three separate gaps. Conflating them produces the wrong fix —
each needed its own test, and each has its own (dis)confirmed remediation.

### Gap 1 — Statistical power (N_eff ~ 12-13)

NIFTY50's own 125 months of usable history, at AR(1)=0.82 persistence, gives roughly
12-13 independent decisions. That is a structural ceiling on what this dataset can prove,
not a data-quality defect.

**Tried and ruled out:**
- **Cross-sector replication** (19 Indian sector indices, same V-rank rule applied to each):
  17/19 positive IC, only 5/19 individually significant. A naive Stouffer's combination across
  all 19 gave p~0 — but that used the wrong variance formula. Correctly adjusted for the
  measured 0.637 average pairwise return correlation among sectors
  (`Var(sum z) = N + N(N-1)*rho`, not a Sharpe-style N_eff divisor), the combined result is
  **p = 0.108, not significant**. Implied N_eff from 19 "independent" sectors: **1.52**.
  Indian equity sectors share too much common-factor exposure to multiply statistical power.
- **Cross-market replication** (NIFTY50 + SP500 + NASDAQ, identical rule): SP500 (IC +0.241,
  p=0.007) and NASDAQ (IC +0.188, p=0.038) *also* show significant positive IC — genuine
  evidence the phenomenon isn't an India-specific artifact. But SP500 and NASDAQ correlate at
  **0.918** with each other (they're substantially the same market), so combining all three
  gives implied N_eff of only **1.28**, and the properly-correlation-adjusted combined
  **p = 0.0020 is actually worse than NIFTY50 alone (p = 0.0006)** — adding two markets that are
  themselves nearly redundant diluted the strongest single result rather than reinforcing it.
- **Extending history via NSE** (`nselib`, independently cross-validated against the existing
  `repair_price_glitches` output — NSE's real 2017-08-11 pre-split close of 997.78 matches the
  repaired value of 99.778 exactly): genuine but modest gain, 108 -> 125 months. Bounded by V's
  own 36-month rolling-mean + 24-month rank warmup, not by raw data availability.

**Not addressable without:** more calendar time, or data from economies with structurally low
correlation to global risk sentiment — untested here, and no guarantee it would work given how
synchronized global equities have been post-2011.

### Gap 2 — Signal-horizon mismatch

`v_px_vs_3y` — the one input that survives Bonferroni correction across 12 candidates — has IC
that triples with horizon: 3m = +0.30, 12m = +0.47, 24m = +0.59. That is the textbook CAPE-style
valuation-mean-reversion signature: real over years, unusable for monthly timing because
volatility during the multi-year "waiting period" swamps the edge. Confirmed by sweeping the
monthly allocator's tilt strength 0.5x-4x on the composite: no sweet spot, only monotonic
Sharpe decay on the train window — the constraint is horizon, not conviction.

**Tried:** an annual-rebalance strategic-tilt variant (`src/vlrt/strategic.py`,
`src/scripts/portfolio/vlrt_v3_strategic.py`) — same composite and inverse-vol machinery,
half the tilt curvature (kappa 0.7 vs 1.4), one decision per year (December), judged on
terminal wealth and rolling CAGR rather than monthly Sharpe.

| Strategy | Years | Terminal $1-> | CAGR % | MaxDD % | Sharpe | Roll5y median % | Roll5y min % |
|---|---|---|---|---|---|---|---|
| Static 55/20/25 | 9.5 | 3.23 | 13.15 | -21.48 | 1.511 | 13.14 | 10.15 |
| Annual tilt (full composite) | 9.5 | 3.13 | 12.77 | -23.40 | 1.453 | 13.00 | 10.04 |
| Annual tilt (V only) | 9.5 | 3.14 | 12.81 | -23.59 | 1.467 | 12.96 | 9.99 |

**Also negative.** The annual reframe underperforms the static anchor on every metric shown —
terminal wealth, CAGR, drawdown, Sharpe, and rolling-window median *and* minimum CAGR — whether
using the full composite or V alone. With only ~10 annual decisions (2016-2025) this is
descriptive, not a confidence-bearing test (no bootstrap or null is computed against it for
that reason — see the module docstring), but the direction is consistent and the hypothesis
does not clear even a low bar. A well-motivated fix, properly tested, that did not work.

### Gap 3 — Replication of the fund's actual decisions

Independent of Gaps 1 and 2: pooled MAE across both `QUANT_MULTI_ASSET` and
`QUANT_DYNAMIC_ASSET_ALLOCATION` loses to random-walk on every sleeve, and Quant's own other
equity funds explain only R^2 = 0.012-0.044 of either fund's equity weight. The manager's
decisions are not well explained by public market data or by the fund house's own other
positioning.

**Not closeable with public data.** This is evidence of discretionary, internally-informed
decisions rather than a modelling gap to iterate on. It stays what it already is in this
harness: a secondary diagnostic, not an optimization target.

## What actually moved, and what did not

| Attempted fix | Result |
|---|---|
| Rank normalisation (replacing v2's clipped bands) | Fixed — 0% frozen, all mechanical defects removed |
| Continuous box-simplex allocation | Fixed — no step function, no unreachable regimes |
| NSE-verified history extension | Genuine, modest gain (108 -> 125 months) |
| Cross-sector replication for N_eff | Ruled out — correlation collapses it to N_eff~1.5 |
| Cross-market replication for N_eff | Ruled out — same collapse (N_eff~1.3), slightly worse p than NIFTY alone |
| Annual strategic-tilt reframe for horizon mismatch | Tested, negative — underperforms static on every metric |
| Real fundamentals-based V (Screener/stock_earnings) | Tested, negative on available (2019+) window; deeper history needs new scraper engineering, not yet built |

Three honestly negative results and one honestly positive-but-narrow one (the full-sample
p=0.002 vs the shuffled-signal null) is the actual state of this research, not a bug list still
to clear.
