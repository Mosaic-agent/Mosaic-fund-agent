"""
src/tools/risk_governor.py
───────────────────────────
Volatility-targeting Risk Governor for GOLDBEES / SILVERBEES positions.

Core formula
────────────
    w(t) = min(w_max,  vol_target / σ_t)

where:
  vol_target  = desired annualised portfolio volatility exposure  (default 15%)
                chosen as the long-run GARCH median for gold (2016–2026 p50 ≈ 14.8%)
  σ_t         = current GARCH annualised conditional volatility %
  w_max       = maximum weight cap — 1.0 means no leverage allowed

Rationale for continuous scaling vs hard cap
──────────────────────────────────────────────
A hard cap at σ > 30% creates a cliff edge (29.9% → full size, 30.1% → capped),
producing whipsaw transaction costs at the threshold boundary.
Continuous inverse-vol scaling gives smooth, monotonically decreasing exposure
as vol rises — the same approach used by AQR, Winton and Renaissance internally.

At current gold vol (34.5%):  w = min(1.0, 15/34.5) = 0.43  → hold 43% of target
At COVID peak vol   (38.0%):  w = min(1.0, 15/38.0) = 0.39
At calm 2018 vol    (12.0%):  w = min(1.0, 15/12.0) = 1.00  (full, capped at 1.0)

Regime overrides
─────────────────
The GARCH weight is further modulated by the anomaly regime signal:
  ⚡ Flash Crash / Black Swan  → multiply by 0.5   (halve regardless of vol level)
  🔥 Volatile Breakout         → multiply by 0.75
  ⚠️ Crowded Long              → multiply by 0.80  (squeeze risk)
  🧨 Blow-off Top              → multiply by 0.70
  📈 Strong Trend              → multiply by 1.00  (no change)
  ✅ Normal                    → multiply by 1.00

Composite Quant Score gate
───────────────────────────
If composite_score < 35 (bearish territory), an additional 0.5 × multiplier
is applied on top of the vol-scaled and regime-adjusted weight.

Public API
──────────
    compute_position_weight(
        garch_annual_vol_pct,
        regime,
        composite_score,
        vol_target_pct,
        w_max,
    ) -> RiskDecision

    explain_decision(decision: RiskDecision) -> str   # plain-English rationale
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# ── Constants ──────────────────────────────────────────────────────────────────
_VOL_TARGET_PCT    = 15.0   # long-run gold vol target (2016-2026 GARCH p50 ≈ 14.8%)
_W_MAX             = 1.0    # no leverage
_SCORE_BEAR_GATE   = 35.0   # composite score below this triggers additional 0.5× cut
_SCORE_GATE_MULT   = 0.50

# Regime multipliers — applied on top of vol-scaled weight
_REGIME_MULT: dict[str, float] = {
    "⚡ Flash Crash / Black Swan (EXIT)": 0.50,
    "🔥 Volatile Breakout":               0.75,
    "⚠️ Crowded Long (Squeeze Risk)":     0.80,
    "🧨 Blow-off Top (Weak)":             0.70,
    "📈 Strong Trend (HODL)":             1.00,
    "✅ Normal":                          1.00,
}

# Readable tier labels for the recommended weight
_WEIGHT_TIERS = [
    (0.85, "FULL",    "Full position — vol within target range"),
    (0.65, "REDUCED", "Reduced position — vol moderately elevated"),
    (0.40, "HALF",    "Half position — vol significantly above target"),
    (0.20, "QUARTER", "Quarter position — vol severely elevated"),
    (0.00, "MINIMAL", "Minimal exposure — extreme vol / risk-off signal"),
]


@dataclass
class RiskDecision:
    """Output of compute_position_weight."""
    # Inputs (captured for audit trail)
    garch_annual_vol_pct: float
    regime:               str
    composite_score:      float | None

    # Intermediate steps
    vol_target_pct:    float
    w_max:             float
    vol_scaled_weight: float         # min(w_max, vol_target / σ_t)
    regime_mult:       float
    score_gate_mult:   float         # 1.0 unless score < _SCORE_BEAR_GATE
    final_weight:      float         # vol_scaled × regime_mult × score_gate_mult

    # Summary
    tier:        str    # FULL | REDUCED | HALF | QUARTER | MINIMAL
    tier_label:  str
    alerts:      list[str] = field(default_factory=list)


def compute_position_weight(
    garch_annual_vol_pct: float,
    regime: str = "✅ Normal",
    composite_score: float | None = None,
    vol_target_pct: float = _VOL_TARGET_PCT,
    w_max: float = _W_MAX,
) -> RiskDecision:
    """
    Compute the recommended position weight using continuous inverse-vol scaling
    plus regime and composite score overrides.

    Parameters
    ----------
    garch_annual_vol_pct : current GARCH annualised conditional vol (e.g. 34.5)
    regime               : anomaly regime label from classify_regime()
    composite_score      : quant scorecard 0-100 (None = not available)
    vol_target_pct       : desired annualised vol exposure (default 15%)
    w_max                : maximum weight, no leverage (default 1.0)

    Returns
    -------
    RiskDecision dataclass with all intermediate steps for transparency
    """
    alerts: list[str] = []

    # ── Step 1: Inverse-vol scaling ───────────────────────────────────────────
    σ = max(garch_annual_vol_pct, 0.1)   # guard against zero
    vol_scaled = min(w_max, vol_target_pct / σ)

    if garch_annual_vol_pct > 30:
        alerts.append(
            f"GARCH vol {garch_annual_vol_pct:.1f}% > 30% — "
            f"inverse-vol scaling reduces weight to {vol_scaled:.0%}"
        )

    # ── Step 2: Regime override ───────────────────────────────────────────────
    regime_mult = _REGIME_MULT.get(regime, 1.0)
    if regime_mult < 1.0:
        alerts.append(
            f"Regime '{regime}' applies {regime_mult:.0%} multiplier"
        )

    # ── Step 3: Composite score gate ──────────────────────────────────────────
    score_gate = 1.0
    if composite_score is not None and composite_score < _SCORE_BEAR_GATE:
        score_gate = _SCORE_GATE_MULT
        alerts.append(
            f"Composite score {composite_score:.0f} < {_SCORE_BEAR_GATE:.0f} "
            f"— bearish gate applies additional {_SCORE_GATE_MULT:.0%} cut"
        )

    # ── Step 4: Final weight ──────────────────────────────────────────────────
    final = round(vol_scaled * regime_mult * score_gate, 4)
    final = max(0.0, min(w_max, final))

    # Tier classification
    tier = "MINIMAL"
    tier_label = _WEIGHT_TIERS[-1][2]
    for threshold, t_name, t_label in _WEIGHT_TIERS:
        if final >= threshold:
            tier = t_name
            tier_label = t_label
            break

    return RiskDecision(
        garch_annual_vol_pct = garch_annual_vol_pct,
        regime               = regime,
        composite_score      = composite_score,
        vol_target_pct       = vol_target_pct,
        w_max                = w_max,
        vol_scaled_weight    = round(vol_scaled, 4),
        regime_mult          = regime_mult,
        score_gate_mult      = score_gate,
        final_weight         = final,
        tier                 = tier,
        tier_label           = tier_label,
        alerts               = alerts,
    )


def explain_decision(decision: RiskDecision) -> str:
    """
    Return a plain-English rationale for the position sizing decision.
    Suitable for display in the UI or as context for the LLM agent.
    """
    lines = [
        f"## Risk Governor — Position Sizing",
        f"",
        f"**Recommended weight: {decision.final_weight:.0%}  [{decision.tier}]**",
        f"_{decision.tier_label}_",
        f"",
        f"### Inputs",
        f"- GARCH annualised vol: **{decision.garch_annual_vol_pct:.1f}%**  "
        f"(target: {decision.vol_target_pct:.0f}%)",
        f"- Anomaly regime: **{decision.regime}**",
        f"- Composite quant score: **"
        + (f"{decision.composite_score:.0f}/100" if decision.composite_score is not None
           else "N/A") + "**",
        f"",
        f"### Calculation steps",
        f"1. Inverse-vol weight = min({decision.w_max:.0f},  "
        f"{decision.vol_target_pct:.0f}% / {decision.garch_annual_vol_pct:.1f}%)  "
        f"= **{decision.vol_scaled_weight:.0%}**",
        f"2. Regime multiplier = **{decision.regime_mult:.0%}**",
        f"3. Score gate multiplier = **{decision.score_gate_mult:.0%}**",
        f"4. Final = {decision.vol_scaled_weight:.0%} × "
        f"{decision.regime_mult:.0%} × "
        f"{decision.score_gate_mult:.0%} = **{decision.final_weight:.0%}**",
    ]

    if decision.alerts:
        lines += ["", "### Alerts"]
        for a in decision.alerts:
            lines.append(f"⚠️  {a}")

    return "\n".join(lines)


# ── Convenience function for direct use from the scorecard / UI ────────────────

def governor_from_scorecard(scorecard: dict[str, Any]) -> RiskDecision:
    """
    Build a RiskDecision directly from the output of compute_gold_scorecard()
    or compute_silver_scorecard(), combined with the latest GARCH run result.

    scorecard dict must have:
      - signals["garch_annual_vol_pct"]   float  (added to signals by the UI)
      - signals["latest_regime"]          str    (added to signals by the UI)
      - composite_score                   float | None

    Both keys are optional — degrades gracefully to defaults.
    """
    garch_vol = float(scorecard.get("signals", {}).get("garch_annual_vol_pct") or _VOL_TARGET_PCT)
    regime    = str(scorecard.get("signals", {}).get("latest_regime") or "✅ Normal")
    score     = scorecard.get("composite_score")

    return compute_position_weight(
        garch_annual_vol_pct=garch_vol,
        regime=regime,
        composite_score=float(score) if score is not None else None,
    )
