"""
tests/test_ou_estimator.py
──────────────────────────
Validates OU parameter recovery on synthetic data and helper functions.

Run:
    python -m pytest tests/test_ou_estimator.py -v
    python tests/test_ou_estimator.py
"""
from __future__ import annotations

import math
import os
import sys

import numpy as np
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.ml.ou_estimator import (
    OUState,
    expected_premium,
    expected_reversion,
    fit_ou,
    prob_revert,
)

PASS = "\033[92m✓\033[0m"
FAIL = "\033[91m✗\033[0m"


def _simulate_ou(
    theta: float, mu: float, sigma: float, n: int, dt: float = 1.0, seed: int = 42
) -> np.ndarray:
    """Generate a synthetic OU path using exact discretisation."""
    rng = np.random.default_rng(seed)
    x = np.zeros(n)
    x[0] = mu + rng.normal(0, sigma / math.sqrt(2 * theta))

    exp_neg_theta_dt = math.exp(-theta * dt)
    var_dt = (sigma ** 2) / (2 * theta) * (1 - math.exp(-2 * theta * dt))
    std_dt = math.sqrt(var_dt)

    for i in range(1, n):
        x[i] = mu + (x[i - 1] - mu) * exp_neg_theta_dt + std_dt * rng.normal()
    return x


class TestOUFit:
    """Test OU parameter recovery from synthetic data."""

    def test_recovery_fast_reversion(self):
        """Fast mean-reversion (θ=0.3, half-life ~2.3d) — should recover within 25%.
        Fast-reverting OU has higher finite-sample estimation variance."""
        theta_true, mu_true, sigma_true = 0.3, 15.0, 2.0
        x = _simulate_ou(theta_true, mu_true, sigma_true, n=2000, seed=101)
        state = fit_ou(x, dt=1.0)

        assert state is not None, "OU fit should succeed on 2000 obs"
        assert abs(state.theta - theta_true) / theta_true < 0.25, f"θ recovery: {state.theta} vs {theta_true}"
        assert abs(state.mu - mu_true) / abs(mu_true) < 0.10, f"μ recovery: {state.mu} vs {mu_true}"
        assert state.half_life_days > 0

    def test_recovery_slow_reversion(self):
        """Slow mean-reversion (θ=0.05, half-life ~14d)."""
        theta_true, mu_true, sigma_true = 0.05, 20.0, 3.0
        x = _simulate_ou(theta_true, mu_true, sigma_true, n=1000, seed=202)
        state = fit_ou(x, dt=1.0)

        assert state is not None
        assert abs(state.theta - theta_true) / theta_true < 0.25
        assert abs(state.mu - mu_true) / abs(mu_true) < 0.10

    def test_half_life_formula(self):
        """Half-life should equal ln(2)/θ."""
        x = _simulate_ou(0.1, 10.0, 1.5, n=500, seed=303)
        state = fit_ou(x, dt=1.0)
        assert state is not None
        expected_hl = math.log(2) / state.theta
        assert abs(state.half_life_days - expected_hl) < 0.01

    def test_insufficient_data(self):
        """Should return None for < 30 observations."""
        x = _simulate_ou(0.1, 10.0, 1.5, n=20, seed=404)
        state = fit_ou(x, dt=1.0)
        assert state is None

    def test_non_stationary_rejection(self):
        """Random walk with drift (no mean reversion) should be rejected or show very slow reversion."""
        rng = np.random.default_rng(505)
        x = np.cumsum(rng.normal(0.01, 1, 500))  # random walk with drift
        state = fit_ou(x, dt=1.0)
        # Either rejected (b >= 1 → None) or fitted with very slow θ / long half-life
        if state is not None:
            assert state.half_life_days > 50, (
                f"Random walk fitted with suspiciously fast half-life: {state.half_life_days:.1f}d"
            )

    def test_constant_series_rejection(self):
        """Constant series should be rejected."""
        x = np.full(100, 15.0)
        state = fit_ou(x, dt=1.0)
        assert state is None

    def test_r2_is_reasonable(self):
        """R² should be positive and < 1 for a well-specified OU."""
        x = _simulate_ou(0.2, 12.0, 1.0, n=500, seed=606)
        state = fit_ou(x, dt=1.0)
        assert state is not None
        assert 0 < state.fit_r2 < 1.0


class TestExpectedPremium:
    """Test the E[X_h] calculation."""

    def test_converges_to_mu(self):
        """As horizon → ∞, expected premium → μ."""
        ep = expected_premium(current=20.0, theta=0.1, mu=15.0, horizon_days=1000)
        assert abs(ep - 15.0) < 0.01

    def test_at_horizon_zero(self):
        """At horizon 0, expected premium = current."""
        ep = expected_premium(current=20.0, theta=0.1, mu=15.0, horizon_days=0)
        assert abs(ep - 20.0) < 1e-10

    def test_monotone_convergence(self):
        """Expected premium should monotonically approach μ."""
        horizons = [1, 5, 10, 20, 50]
        eps = [expected_premium(20.0, 0.1, 15.0, h) for h in horizons]
        # Current > μ, so eps should be decreasing toward 15
        for i in range(len(eps) - 1):
            assert eps[i] > eps[i + 1], f"Not monotone at h={horizons[i+1]}"


class TestExpectedReversion:
    """Test the OU-adjusted reversion formula."""

    def test_direction(self):
        """When current > μ, reversion should be negative (premium dropping)."""
        rev = expected_reversion(current=20.0, theta=0.1, mu=15.0, horizon_days=10)
        assert rev < 0

    def test_at_mu(self):
        """When current = μ, expected reversion = 0."""
        rev = expected_reversion(current=15.0, theta=0.1, mu=15.0, horizon_days=10)
        assert abs(rev) < 1e-10

    def test_scales_with_horizon(self):
        """Longer horizon → larger absolute reversion."""
        r5 = abs(expected_reversion(20.0, 0.1, 15.0, 5))
        r20 = abs(expected_reversion(20.0, 0.1, 15.0, 20))
        assert r20 > r5


class TestProbRevert:
    """Test the reversion probability calculation."""

    def test_high_prob_near_mu(self):
        """Premium well above μ with long horizon — P(drop to 16%) should be high.
        We use threshold=16 (not μ=15) because at long horizons the OU
        distribution centers on μ, making P(≤μ) ≈ 50% by symmetry."""
        p = prob_revert(current=20.0, theta=0.2, mu=15.0, sigma=1.5,
                        threshold=16.0, horizon_days=30)
        assert p > 0.5, f"P(revert) should be > 0.5, got {p:.3f}"

    def test_low_prob_far_from_threshold(self):
        """Premium far below threshold with short horizon — should be low."""
        p = prob_revert(current=5.0, theta=0.1, mu=15.0, sigma=1.0,
                        threshold=25.0, horizon_days=2)
        assert p < 0.3

    def test_increases_with_horizon(self):
        """P(revert) should increase with horizon."""
        p5 = prob_revert(current=20.0, theta=0.1, mu=15.0, sigma=2.0,
                         threshold=15.0, horizon_days=5)
        p30 = prob_revert(current=20.0, theta=0.1, mu=15.0, sigma=2.0,
                          threshold=15.0, horizon_days=30)
        assert p30 > p5


# ── Standalone runner ────────────────────────────────────────────────────────

def _run_standalone():
    """Run tests without pytest."""
    tests = [
        ("OU recovery (fast)", TestOUFit().test_recovery_fast_reversion),
        ("OU recovery (slow)", TestOUFit().test_recovery_slow_reversion),
        ("Half-life formula", TestOUFit().test_half_life_formula),
        ("Insufficient data", TestOUFit().test_insufficient_data),
        ("Non-stationary rejection", TestOUFit().test_non_stationary_rejection),
        ("Constant series rejection", TestOUFit().test_constant_series_rejection),
        ("R² is reasonable", TestOUFit().test_r2_is_reasonable),
        ("E[X_h] → μ", TestExpectedPremium().test_converges_to_mu),
        ("E[X_0] = current", TestExpectedPremium().test_at_horizon_zero),
        ("Monotone convergence", TestExpectedPremium().test_monotone_convergence),
        ("Reversion direction", TestExpectedReversion().test_direction),
        ("Reversion at μ = 0", TestExpectedReversion().test_at_mu),
        ("Reversion scales", TestExpectedReversion().test_scales_with_horizon),
        ("P(revert) near μ", TestProbRevert().test_high_prob_near_mu),
        ("P(revert) far", TestProbRevert().test_low_prob_far_from_threshold),
        ("P(revert) increases", TestProbRevert().test_increases_with_horizon),
    ]

    passed = 0
    for name, fn in tests:
        try:
            fn()
            print(f"  {PASS} {name}")
            passed += 1
        except (AssertionError, Exception) as e:
            print(f"  {FAIL} {name}: {e}")

    print(f"\n{passed}/{len(tests)} passed")
    return passed == len(tests)


if __name__ == "__main__":
    print("=" * 60)
    print("OU Estimator Tests")
    print("=" * 60)
    ok = _run_standalone()
    sys.exit(0 if ok else 1)
