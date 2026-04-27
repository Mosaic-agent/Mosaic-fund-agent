"""
tests/test_adaptive_kelly.py
─────────────────────────────
Unit tests for src/tools/adaptive_kelly.py — no DB or API keys needed.
"""
import math
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.tools.adaptive_kelly import (
    compute_kelly_weight,
    compute_blended_weight,
    _CV_R2_MIN,
    _CV_R2_HAIRCUT,
    _Q10_Q90_SIGMA,
)


def test_positive_return_produces_positive_weight():
    d = compute_kelly_weight(1.5, -0.5, 3.5, cv_r2=0.10)
    assert d.final_weight > 0, "Positive expected return should give positive weight"


def test_negative_return_floors_to_zero():
    d = compute_kelly_weight(-2.0, -5.0, 1.0, cv_r2=0.10)
    assert d.final_weight == 0.0, "Negative expected return should produce weight=0"
    assert any("Negative" in a for a in d.alerts)


def test_weight_capped_at_one():
    # Very high expected return should not exceed w_max=1.0
    d = compute_kelly_weight(50.0, 49.0, 51.0, cv_r2=0.99)
    assert d.final_weight <= 1.0


def test_cv_r2_haircut_fires():
    # Use a wide confidence band (high vol) so Kelly doesn't clip to 1.0
    # and the haircut effect is visible in the final weight
    d_low  = compute_kelly_weight(0.3, -8.0, 8.0, cv_r2=_CV_R2_MIN - 0.001)
    d_high = compute_kelly_weight(0.3, -8.0, 8.0, cv_r2=_CV_R2_MIN + 0.001)
    assert d_low.confidence_haircut == _CV_R2_HAIRCUT
    assert d_high.confidence_haircut == 1.0
    assert d_low.final_weight < d_high.final_weight


def test_implied_vol_from_quantile_span():
    low, high = -1.0, 1.0
    d = compute_kelly_weight(1.0, low, high, cv_r2=0.10)
    expected_5d_vol = abs(high - low) / _Q10_Q90_SIGMA
    expected_ann_vol = expected_5d_vol * math.sqrt(252 / 5)
    assert abs(d.implied_vol_pct - expected_ann_vol) < 0.001


def test_blended_weight_bounds():
    for rg, kelly, blend in [(0.9, 0.3, 0.5), (1.0, 0.0, 0.3), (0.5, 0.5, 0.7)]:
        result = compute_blended_weight(rg, kelly, blend)
        assert 0.0 <= result <= 1.0, f"Blended {result} out of bounds"


def test_blended_weight_pure_rg():
    result = compute_blended_weight(0.8, 0.2, blend=0.0)
    assert abs(result - 0.8) < 1e-6, "blend=0 should return pure RG weight"


def test_blended_weight_pure_kelly():
    result = compute_blended_weight(0.8, 0.2, blend=1.0)
    assert abs(result - 0.2) < 1e-6, "blend=1 should return pure Kelly weight"


def test_blended_50_50():
    result = compute_blended_weight(0.8, 0.4, blend=0.5)
    assert abs(result - 0.6) < 1e-6


def test_zero_span_does_not_crash():
    # Degenerate case: identical quantile bounds → near-zero vol → Kelly could blow up
    d = compute_kelly_weight(1.0, 1.0, 1.0, cv_r2=0.10)
    assert 0.0 <= d.final_weight <= 1.0


def test_zero_expected_return():
    d = compute_kelly_weight(0.0, -1.0, 1.0, cv_r2=0.10)
    assert d.final_weight == 0.0
    assert d.raw_kelly == 0.0


def test_negative_cv_r2_zeros_weight():
    # CV R² ≤ 0 means the model has no predictive value → weight forced to 0
    d = compute_kelly_weight(2.0, -1.0, 5.0, cv_r2=-0.05)
    assert d.final_weight == 0.0
    assert any("no predictive value" in a for a in d.alerts)
    d0 = compute_kelly_weight(2.0, -1.0, 5.0, cv_r2=0.0)
    assert d0.final_weight == 0.0


def test_garch_vol_overrides_implied_vol():
    # With GARCH vol provided, σ should come from GARCH not the quantile span
    d = compute_kelly_weight(
        expected_return_pct=1.0, confidence_low_pct=-1.0, confidence_high_pct=1.0,
        cv_r2=0.10, garch_annual_vol_pct=18.0,
    )
    assert d.sigma_source == "garch"
    assert abs(d.implied_vol_pct - 18.0) < 1e-6
    d2 = compute_kelly_weight(
        expected_return_pct=1.0, confidence_low_pct=-1.0, confidence_high_pct=1.0,
        cv_r2=0.10,
    )
    assert d2.sigma_source == "implied"


def test_raw_kelly_capped_before_fraction():
    # Huge μ with small σ → raw_kelly >> 1; capped means final_weight ≤ fraction
    d = compute_kelly_weight(
        expected_return_pct=5.0, confidence_low_pct=-0.1, confidence_high_pct=0.1,
        cv_r2=0.10, fraction=0.5, garch_annual_vol_pct=15.0,
    )
    assert d.raw_kelly > 1.0  # uncapped raw is large
    assert d.fractional_kelly <= 0.5 + 1e-9  # capped × 0.5
    assert d.final_weight <= 0.5 + 1e-9


if __name__ == "__main__":
    tests = [
        test_positive_return_produces_positive_weight,
        test_negative_return_floors_to_zero,
        test_weight_capped_at_one,
        test_cv_r2_haircut_fires,
        test_implied_vol_from_quantile_span,
        test_blended_weight_bounds,
        test_blended_weight_pure_rg,
        test_blended_weight_pure_kelly,
        test_blended_50_50,
        test_zero_span_does_not_crash,
        test_zero_expected_return,
        test_negative_cv_r2_zeros_weight,
        test_garch_vol_overrides_implied_vol,
        test_raw_kelly_capped_before_fraction,
    ]
    passed = failed = 0
    for t in tests:
        try:
            t()
            print(f"  ✓  {t.__name__}")
            passed += 1
        except Exception as e:
            print(f"  ✗  {t.__name__}: {e}")
            failed += 1
    print(f"\n{passed}/{passed+failed} tests passed")
