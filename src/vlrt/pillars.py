"""
VLRT v3 — pillar construction.

Two rules govern this module:

1. **Everything is normalised by causal expanding percentile rank**, never by fixed
   ``np.clip`` bands. v2's bands censored R to exactly 0.0 for eleven consecutive
   months; a percentile rank is uniform on [0, 1] by construction and cannot saturate.

2. **Pillar weights are fixed, not regime-adaptive.** v2 computed its ``momentum_phase``
   *from* L and R and then used it to raise the weight *on* L and R while driving the
   valuation weight to 0.05 — a feedback loop that suppressed the only pillar carrying
   measurable signal. The weights below are set from measured information coefficient,
   with V dominant, and do not vary with the state of the market.

Every signal is oriented so that **higher = more bullish for equity** (or, for the gold
block, more bullish for gold), so ranks compose by simple averaging.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

#: Fixed pillar weights, set from measured IC (V ~ +0.29 and the only survivor of a
#: Bonferroni correction across 12 candidates; L and R individually non-significant).
PILLAR_WEIGHTS: dict[str, float] = {"V": 0.60, "L": 0.15, "R": 0.15, "T": 0.10}

#: Minimum prior observations before a rank is emitted.
MIN_RANK_HISTORY = 24

#: CFTC publishes with a lag; shift COT by a month before it may influence a decision.
COT_LAG_MONTHS = 1


def expanding_rank(s: pd.Series, min_periods: int = MIN_RANK_HISTORY) -> pd.Series:
    """
    Fraction of *strictly prior* non-NaN observations below ``s[t]``.

    Strictly causal: the value at ``t`` depends only on observations before ``t``, so
    appending future rows can never change an earlier value. ``tests/test_vlrt.py``
    asserts exactly that.
    """
    v = s.to_numpy(dtype=float)
    out = np.full(len(v), np.nan)
    for i in range(len(v)):
        if np.isnan(v[i]):
            continue
        hist = v[:i]
        hist = hist[~np.isnan(hist)]
        if len(hist) >= min_periods:
            out[i] = float((hist < v[i]).mean())
    return pd.Series(out, index=s.index, name=s.name)


def _ret(s: pd.Series, n: int) -> pd.Series:
    return s.pct_change(n)


def build_signal_inputs(monthly: pd.DataFrame) -> pd.DataFrame:
    """
    Raw (un-normalised) pillar inputs at month-end.

    Orientation: higher = more bullish for equity, except the ``g_*`` block which is
    oriented higher = more bullish for gold.
    """
    m = monthly
    out = pd.DataFrame(index=m.index)

    # ── V — valuation mean reversion ─────────────────────────────────────────
    # Price relative to its own 3y mean, inverted so cheap = high.
    out["v_px_vs_3y"] = -(m["NIFTY50"] / m["NIFTY50"].rolling(36).mean() - 1.0)
    # 5y trailing CAGR, inverted: a stretched long-horizon return implies lower forward return.
    out["v_cagr5y_rev"] = -((m["NIFTY50"] / m["NIFTY50"].shift(60)) ** (1 / 5) - 1.0)

    # ── L — liquidity / financial conditions ─────────────────────────────────
    out["l_dxy_3m"] = -_ret(m["DXY"], 3)          # weaker dollar = easier global liquidity
    out["l_us10y_3m"] = -m["US10Y"].diff(3)        # falling yields = easing
    out["l_usdinr_3m"] = -_ret(m["USDINR"], 3)     # stronger INR = domestic inflows
    out["l_vix_lvl"] = -m["INDIAVIX"]              # low vol = ample risk-bearing capacity

    # ── R — risk appetite (behavioural, not volatility) ──────────────────────
    out["r_mid_vs_lg_3m"] = _ret(m["NIFTYMID"], 3) - _ret(m["NIFTY50"], 3)
    out["r_gold_vs_eq_3m"] = -(_ret(m["GOLDBEES"], 3) - _ret(m["NIFTYBEES"], 3))
    out["r_sp500_3m"] = _ret(m["SP500"], 3)
    out["r_vix_chg_3m"] = -m["INDIAVIX"].diff(3)
    # Crowded managed-money gold longs = risk-off. Lagged for CFTC publication delay.
    out["r_cot_gold"] = -m["cot_pct_oi"].shift(COT_LAG_MONTHS)

    # ── Gold block — drives the gold sleeve on its own signal, not as a residual ──
    out["g_real_yield"] = -m["US10Y"].diff(3)      # falling yields lift gold
    out["g_dxy"] = -_ret(m["DXY"], 3)              # weak dollar lifts gold
    out["g_mom"] = _ret(m["GOLDBEES"], 3)
    out["g_cot_contra"] = -m["cot_pct_oi"].shift(COT_LAG_MONTHS)  # crowded long = contrarian

    return out


def build_pillars(
    monthly: pd.DataFrame,
    min_periods: int = MIN_RANK_HISTORY,
    weights: dict[str, float] | None = None,
) -> pd.DataFrame:
    """
    Rank-normalise the inputs and assemble V / L / R / T, the equity composite and
    the independent gold signal. All columns are on [0, 1].
    """
    w = dict(PILLAR_WEIGHTS if weights is None else weights)
    raw = build_signal_inputs(monthly)
    ranks = raw.apply(lambda s: expanding_rank(s, min_periods=min_periods))

    def _blk(prefix: str) -> pd.Series:
        cols = [c for c in ranks.columns if c.startswith(prefix)]
        return ranks[cols].mean(axis=1, skipna=True)

    out = pd.DataFrame(index=monthly.index)
    out["V"] = _blk("v_")
    out["L"] = _blk("l_")
    out["R"] = _blk("r_")

    # T — synthesis, not a self-referential momentum term. Alignment shrinks the
    # V/L/R consensus toward neutral when the pillars disagree; it never feeds back
    # into the weights applied to its own inputs.
    tri = out[["V", "L", "R"]]
    alignment = (1.0 - 2.0 * tri.std(axis=1, ddof=0)).clip(0.0, 1.0)
    out["T"] = (0.5 + (tri.mean(axis=1) - 0.5) * alignment).clip(0.0, 1.0)
    out["alignment"] = alignment

    out["composite"] = sum(out[k] * w[k] for k in ("V", "L", "R", "T")) / sum(w.values())
    out["pm_signal"] = _blk("g_")

    return pd.concat([out, ranks.add_prefix("rank_")], axis=1)
