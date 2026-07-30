"""
src/pipeline/__init__.py
─────────────────────────
Pipeline manifest tracking for dependency-aware recomputation.
"""
from src.pipeline.manifest import (
    ALL_STAGES,
    ML_PREDICTIONS,
    SIGNAL_COMPOSITE,
    WEIGHT_CHECKPOINTS,
    ManifestTracker,
    StageDefinition,
    StageStatus,
)

__all__ = [
    "ManifestTracker",
    "StageStatus",
    "StageDefinition",
    "ML_PREDICTIONS",
    "SIGNAL_COMPOSITE",
    "WEIGHT_CHECKPOINTS",
    "ALL_STAGES",
]
