"""
src/ml/anomaly — composite anomaly detection pipeline.

Public API re-exported from sub-modules for backward compatibility.
All existing callers of `from src.ml.anomaly import X` continue to work.
"""
from __future__ import annotations

from ._features import robust_zscore, repair_decimal_glitches, build_features
from ._garch import fit_garch_residuals, _GARCH_CACHE
from ._isolation import fit_isolation_forest, _IF_CACHE
from ._changepoint import fit_change_points
from ._regime import classify_regime
from ._cross_asset import _inject_cross_asset
from ._qdrant import retrieve_similar_anomalies, _store_anomalies
from ._pipeline import (
    AnomalyDetectorStrategy,
    RobustZScoreStrategy,
    GarchResidualStrategy,
    IsolationForestStrategy,
    PeltChangePointStrategy,
    CompositeAnomalyPipeline,
    run_composite_anomaly,
)

__all__ = [
    "robust_zscore",
    "build_features",
    "fit_garch_residuals",
    "fit_isolation_forest",
    "fit_change_points",
    "classify_regime",
    "run_composite_anomaly",
    "retrieve_similar_anomalies",
    "AnomalyDetectorStrategy",
    "RobustZScoreStrategy",
    "GarchResidualStrategy",
    "IsolationForestStrategy",
    "PeltChangePointStrategy",
    "CompositeAnomalyPipeline",
]
