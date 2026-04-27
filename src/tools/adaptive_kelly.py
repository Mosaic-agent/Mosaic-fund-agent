"""
src/tools/adaptive_kelly.py
────────────────────────────
Adaptive Kelly position-sizing — blends LightGBM expected-return signal
with the existing inverse-vol Risk Governor weight.

Core formula
────────────
    kelly_raw = μ_annual / σ_annual²
    kelly_frac = kelly_raw × fraction          # half-Kelly (fraction=0.5)
    kelly_final = clip(kelly_frac × haircut, 0, w_max)

    blended = rg_weight × (1 - blend) + kelly_final × blend

Where:
  μ_annual  = expected_return_pct × (252 / horizon) / 100
  σ_annual  = implied_vol × sqrt(252 / horizon) / 100
  implied_vol = (confidence_high_pct - confidence_low_pct) / 2.563
               (10th/90th quantile span → 1σ for normal distribution)
  haircut   = 0.5 if cv_r2 < CV_R2_MIN_THRESHOLD else 1.0

Why half-Kelly
──────────────
Full Kelly is theoretically optimal but requires the predicted distribution
to be exactly correct. With CV R² of 0.05-0.15 on daily financial data,
full-Kelly overestimates certainty. Half-Kelly (fraction=0.5) + 50/50 blend
with the rule-based RG gives ~25% effective Kelly weight — conservative and
robust to model mis-specification (same approach used by AQR / Man Group).

Public API
──────────
    compute_kelly_weight(expected_return_pct, confidence_low_pct,
                         confidence_high_pct, horizon_days, cv_r2,
                         fraction, w_max) -> KellyDecision

    compute_blended_weight(rg_weight, kelly_weight, blend) -> float
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field

# ── Constants ──────────────────────────────────────────────────────────────────
_FRACTION_DEFAULT  = 0.5    # half-Kelly
_W_MAX             = 1.0    # no leverage
_BLEND_DEFAULT     = 0.5    # 50% RG + 50% Kelly
_CV_R2_MIN         = 0.05   # below this threshold → confidence haircut fires
_CV_R2_HAIRCUT     = 0.5    # multiplier when ML confidence is too low
_TRADING_DAYS      = 252    # annualisation constant
# Normal quantile for 10th/90th percentile pair (Φ⁻¹(0.90) - Φ⁻¹(0.10) = 2.563)
_Q10_Q90_SIGMA     = 2.563


@dataclass
class KellyDecision:
    """Full audit trail for an Adaptive Kelly sizing decision."""
    # Inputs
    expected_return_pct:  float
    confidence_low_pct:   float
    confidence_high_pct:  float
    horizon_days:         int
    cv_r2:                float | None
    fraction:             float

    # Derived intermediates
    implied_vol_pct:      float    # annualised σ used in Kelly (GARCH if provided, else implied)
    sigma_source:         str      # "garch" | "implied"
    annualised_mu:        float    # μ as decimal (e.g. 0.12 = 12%)
    annualised_sigma:     float    # σ as decimal
    raw_kelly:            float    # μ / σ² (uncapped)
    fractional_kelly:     float    # min(1, max(0, raw_kelly)) × fraction
    confidence_haircut:   float    # 1.0 or _CV_R2_HAIRCUT
    final_weight:         float    # clipped to [0, w_max]

    alerts: list[str] = field(default_factory=list)


def compute_kelly_weight(
    expected_return_pct: float,
    confidence_low_pct:  float,
    confidence_high_pct: float,
    horizon_days:        int   = 5,
    cv_r2:               float | None = None,
    fraction:            float = _FRACTION_DEFAULT,
    w_max:               float = _W_MAX,
    garch_annual_vol_pct: float | None = None,
) -> KellyDecision:
    """
    Compute a Kelly-optimal position weight from LightGBM forecast outputs.

    Parameters
    ----------
    expected_return_pct  : Predicted forward return over horizon_days (e.g. 1.5 = 1.5%)
    confidence_low_pct   : 10th-percentile bound from quantile regression
    confidence_high_pct  : 90th-percentile bound from quantile regression
    horizon_days         : Forecast horizon (default 5 trading days)
    cv_r2                : Walk-forward CV R² mean — low values trigger a haircut
    fraction             : Kelly fraction (default 0.5 = half-Kelly)
    w_max                : Maximum weight cap — 1.0 means no leverage

    Returns
    -------
    KellyDecision with all intermediate steps recorded.
    """
    alerts: list[str] = []

    # ── Step 0: Hard zero on non-predictive model ─────────────────────────────
    # CV R² ≤ 0 means the model is worse than predicting the mean — there is
    # no signal to size on, regardless of expected_return_pct's magnitude.
    if cv_r2 is not None and cv_r2 <= 0:
        alerts.append(
            f"CV R² {cv_r2:.3f} ≤ 0 — model has no predictive value, "
            "Kelly weight forced to 0"
        )
        return KellyDecision(
            expected_return_pct  = expected_return_pct,
            confidence_low_pct   = confidence_low_pct,
            confidence_high_pct  = confidence_high_pct,
            horizon_days         = horizon_days,
            cv_r2                = cv_r2,
            fraction             = fraction,
            implied_vol_pct      = 0.0,
            sigma_source         = "garch" if garch_annual_vol_pct is not None else "implied",
            annualised_mu        = 0.0,
            annualised_sigma     = 0.0,
            raw_kelly            = 0.0,
            fractional_kelly     = 0.0,
            confidence_haircut   = 0.0,
            final_weight         = 0.0,
            alerts               = alerts,
        )

    # ── Step 1: Vol — prefer GARCH return-vol over prediction-interval span ───
    # Bug 1 fix: Kelly's σ is the *asset return* volatility, not the model's
    # forecast uncertainty. When a GARCH estimate is available, use it.
    if garch_annual_vol_pct is not None and garch_annual_vol_pct > 0:
        sigma_annual_pct = float(garch_annual_vol_pct)
        sigma_source     = "garch"
    else:
        # Fallback: derive from quantile span (q90 - q10 ≈ 2.563σ for normal)
        span = abs(confidence_high_pct - confidence_low_pct)
        implied_vol_5d_pct = max(span / _Q10_Q90_SIGMA, 0.01)
        ann_factor = math.sqrt(_TRADING_DAYS / max(horizon_days, 1))
        sigma_annual_pct = implied_vol_5d_pct * ann_factor
        sigma_source     = "implied"

    # ── Step 2: Annualise expected return ─────────────────────────────────────
    ann_mu    = (expected_return_pct / 100) * (_TRADING_DAYS / max(horizon_days, 1))
    ann_sigma = sigma_annual_pct / 100

    # ── Step 3: Raw Kelly fraction ────────────────────────────────────────────
    sigma_sq = ann_sigma ** 2
    raw_kelly = ann_mu / sigma_sq if sigma_sq > 0 else 0.0
    # Bug 3 fix: cap raw Kelly at 1× *before* applying fraction — otherwise a
    # large μ saturates final_weight at w_max regardless of fraction/haircut,
    # collapsing Kelly to a constant 100% signal.
    capped_kelly = min(1.0, max(0.0, raw_kelly))
    frac_kelly   = capped_kelly * fraction

    # ── Step 4: Confidence haircut ────────────────────────────────────────────
    haircut = 1.0
    if cv_r2 is not None and cv_r2 < _CV_R2_MIN:
        haircut = _CV_R2_HAIRCUT
        alerts.append(
            f"CV R² {cv_r2:.3f} < {_CV_R2_MIN} — low ML confidence, "
            f"haircut {_CV_R2_HAIRCUT:.0%} applied"
        )

    # ── Step 5: Clip to [0, w_max] ────────────────────────────────────────────
    # Kelly goes negative when μ < 0 → we floor at 0 (no short selling)
    final = max(0.0, min(w_max, frac_kelly * haircut))

    if expected_return_pct < 0:
        alerts.append(
            f"Negative expected return ({expected_return_pct:.2f}%) "
            "— Kelly weight floored to 0 (no short selling)"
        )
    if raw_kelly > 2.0:
        alerts.append(
            f"Raw Kelly ({raw_kelly:.1f}×) is very high — "
            "fractional Kelly + clip protect against over-sizing"
        )

    return KellyDecision(
        expected_return_pct  = expected_return_pct,
        confidence_low_pct   = confidence_low_pct,
        confidence_high_pct  = confidence_high_pct,
        horizon_days         = horizon_days,
        cv_r2                = cv_r2,
        fraction             = fraction,
        implied_vol_pct      = round(sigma_annual_pct, 4),
        sigma_source         = sigma_source,
        annualised_mu        = round(ann_mu, 6),
        annualised_sigma     = round(ann_sigma, 6),
        raw_kelly            = round(raw_kelly, 4),
        fractional_kelly     = round(frac_kelly, 4),
        confidence_haircut   = haircut,
        final_weight         = round(final, 4),
        alerts               = alerts,
    )


def compute_blended_weight(
    rg_weight:    float,
    kelly_weight: float,
    blend:        float = _BLEND_DEFAULT,
) -> float:
    """
    Convex combination of the Rule-Based and Kelly weights.

        blended = rg_weight × (1 - blend) + kelly_weight × blend

    Parameters
    ----------
    rg_weight    : Weight from Risk Governor (inverse-vol + regime + trend)
    kelly_weight : Weight from compute_kelly_weight().final_weight
    blend        : Kelly proportion [0, 1]; default 0.5 (50/50)

    Returns
    -------
    Blended weight clipped to [0, 1].
    """
    blend = max(0.0, min(1.0, blend))
    return round(
        max(0.0, min(1.0, rg_weight * (1.0 - blend) + kelly_weight * blend)),
        4,
    )


def explain_kelly(decision: KellyDecision) -> str:
    """Plain-English explanation of a KellyDecision for CLI / LLM context."""
    lines = [
        "## Adaptive Kelly — Position Sizing",
        "",
        f"**Kelly weight: {decision.final_weight:.0%}**",
        "",
        "### Inputs",
        f"- Expected 5d return: **{decision.expected_return_pct:.2f}%**",
        f"- Confidence band: [{decision.confidence_low_pct:.2f}%, {decision.confidence_high_pct:.2f}%]",
        f"- Implied vol (annualised): **{decision.implied_vol_pct:.1f}%**",
        f"- CV R²: **{decision.cv_r2:.3f}**" if decision.cv_r2 is not None else "- CV R²: N/A",
        "",
        "### Calculation",
        f"1. μ_annual = {decision.expected_return_pct:.2f}% × (252/5) = **{decision.annualised_mu*100:.1f}%**",
        f"2. σ_annual = {decision.implied_vol_pct:.1f}%",
        f"3. Raw Kelly = μ/σ² = **{decision.raw_kelly:.2f}×** (σ from {decision.sigma_source})",
        f"4. Capped × fraction = min(1, {decision.raw_kelly:.2f}) × {decision.fraction:.0%} = **{decision.fractional_kelly:.2f}×**",
        f"5. Confidence haircut = **{decision.confidence_haircut:.0%}**",
        f"6. Final = clip({decision.fractional_kelly:.2f} × {decision.confidence_haircut:.0%}, 0, 1) = **{decision.final_weight:.0%}**",
    ]
    if decision.alerts:
        lines += ["", "### Alerts"]
        for a in decision.alerts:
            lines.append(f"⚠️  {a}")
    return "\n".join(lines)
