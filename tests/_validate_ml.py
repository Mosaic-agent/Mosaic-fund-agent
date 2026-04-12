"""
Validation script for ML prediction quality improvements.
Run: python tests/_validate_ml.py
"""
import logging
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

from src.ml.trend_predictor import (
    _fetch_macro_series,
    run_trend_prediction,
    _GAP,
)

PASS = "\033[92m✓\033[0m"
FAIL = "\033[91m✗\033[0m"

# ── 1. Config sanity ──────────────────────────────────────────────────────────
assert _GAP == 10, f"Expected _GAP=10, got {_GAP}"
print(f"{PASS} _GAP = {_GAP}  (2× horizon)")

# ── 2. Macro series fetch ─────────────────────────────────────────────────────
df_macro = _fetch_macro_series("2024-01-01", "2026-04-01")
print(f"{PASS} macro fetch: {len(df_macro)} rows  cols={list(df_macro.columns)}")
if not df_macro.empty:
    has_dxy  = "dxy_close"   in df_macro.columns and df_macro["dxy_close"].notna().sum() > 50
    has_tnx  = "us10y_close" in df_macro.columns and df_macro["us10y_close"].notna().sum() > 50
    print(f"  DXY data   : {PASS if has_dxy  else FAIL}  ({df_macro['dxy_close'].notna().sum()} rows)")
    print(f"  US10Y data : {PASS if has_tnx  else FAIL}  ({df_macro['us10y_close'].notna().sum()} rows)")
    print(df_macro.tail(3).to_string())

# ── 3. Full end-to-end prediction ─────────────────────────────────────────────
print("\nRunning full prediction pipeline ...")
result = run_trend_prediction(verbose=True)

print()
print(f"  regime          : {result['regime_signal']}")
print(f"  expected_return : {result['expected_return_pct']:+.3f}%")
print(f"  confidence      : [{result['confidence_low']:+.3f}%, {result['confidence_high']:+.3f}%]")
print(f"  CV R\u00b2 mean      : {result['cv_r2_mean']:.4f}")
print(f"  CV R\u00b2 per fold  : {result['cv_r2_scores']}")
print(f"  hit ratio mean  : {result['cv_hit_ratio_mean']:.3f}")
print(f"  hit per fold    : {result['cv_hit_ratios']}")
print(f"  training rows   : {result['n_training_rows']}")
print(f"  horizon         : {result['horizon_days']}d")

# ── 4. Verify new features are in the model ───────────────────────────────────
feat_names = result["feature_importances"]["feature"].tolist()
new_feats = [
    "f_dxy_logret5", "f_dxy_logret20",
    "f_us10y_level", "f_us10y_delta5",
    "f_month_sin", "f_month_cos",
    "f_dow_sin", "f_dow_cos",
]
print("\nNew feature presence check:")
all_ok = True
for f in new_feats:
    ok = f in feat_names
    all_ok = all_ok and ok
    print(f"  {PASS if ok else FAIL}  {f}")

print("\nTop 10 features by importance:")
print(result["feature_importances"].head(10).to_string(index=False))

if all_ok:
    print(f"\n{PASS} All validations passed.")
else:
    print(f"\n{FAIL} Some new features were missing — check engineer_features().")
    sys.exit(1)
