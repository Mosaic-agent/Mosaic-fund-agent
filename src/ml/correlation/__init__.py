"""
src/ml/correlation/__init__.py
───────────────────────────────
Correlation engine package — maps price/volume anomalies to corporate actions,
macro events, and news using pluggable strategies.

All public symbols are re-exported here for backward compatibility:
    from src.ml.correlation import CorrelationService, EventType, ...
"""

from .models import CandidateEvent, CorrelationFinding, EventType
from .strategies import (
    CorrelationStrategy,
    CrossAssetCoMovementStrategy,
    PostMacroShockStrategy,
    PreEventLeakStrategy,  # deprecated — kept for backward compat imports only
)
from .service import CorrelationService
from .event_registry import EventRegistry
from .filters import (
    FindingsPipeline,
    apply_precedent_weight,
    apply_quality_weights,
    cluster_episodes,
    deduplicate_by_date,
)

__all__ = [
    "CandidateEvent",
    "CorrelationFinding",
    "CorrelationService",
    "CorrelationStrategy",
    "CrossAssetCoMovementStrategy",
    "EventRegistry",
    "EventType",
    "FindingsPipeline",
    "PostMacroShockStrategy",
    "PreEventLeakStrategy",  # deprecated
    "apply_precedent_weight",
    "apply_quality_weights",
    "cluster_episodes",
    "deduplicate_by_date",
]
