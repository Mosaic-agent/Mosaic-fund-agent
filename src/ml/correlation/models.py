"""
src/ml/correlation/models.py
─────────────────────────────
Core data models for the correlation engine: event types, candidate events,
and correlation findings.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from enum import Enum
from typing import Optional


class EventType(str, Enum):
    COMPANY_FILING = "company_filing"
    NEWS_ANNOUNCEMENT = "news_announcement"
    MACRO_RATE_DECISION = "macro_rate_decision"
    MACRO_COMMODITY_SHOCK = "macro_commodity_shock"
    MACRO_GEOPOLITICAL = "macro_geopolitical"
    # PELT structural break in the symbol's own return distribution. Derived
    # from the price series itself, so it's CONTEXT ("this anomaly sits at a
    # volatility-regime transition"), not an external cause — it frames an
    # anomaly as part of a regime shift and prompts looking for the macro/news
    # that drove the transition.
    REGIME_SHIFT = "regime_shift"


@dataclass
class CandidateEvent:
    """A qualitative corporate filing or external macro event."""
    trade_date: date
    event_type: EventType
    label: str
    description: str
    metadata: dict = field(default_factory=dict)


@dataclass
class CorrelationFinding:
    """The mapping of an anomaly day to a candidate event trigger."""
    anomaly_date: date
    event: CandidateEvent
    strategy_name: str
    correlation_score: float  # 0 to 100
    lead_lag_days: int        # Negative = anomaly is BEFORE event (leak), Positive = AFTER (reaction)
    confidence: str           # "HIGH" | "MODERATE" | "LOW"
    explanation: str
    abnormal_return: Optional[float] = None
