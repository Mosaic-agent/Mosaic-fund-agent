"""Qdrant integration shims for the anomaly pipeline."""
from __future__ import annotations

import logging
from typing import Any

log = logging.getLogger(__name__)

try:
    from src.db.anomaly_vector import store_anomalies as _store_anomalies
except ImportError:
    _store_anomalies = None  # type: ignore[assignment]


def retrieve_similar_anomalies(
    symbol: str,
    regime: str,
    trade_date: Any,
    k: int = 5,
    category: str = "",
    same_asset_only: bool = False,
    exclude_window_days: int = 0,
) -> list[dict]:
    """Retrieve past anomaly events semantically similar to the given regime+context.

    Thin shim over src.db.anomaly_vector.retrieve_similar_anomalies — keeps the
    public import path stable at `from src.ml.anomaly import retrieve_similar_anomalies`
    while the implementation lives alongside the other Qdrant modules. If the
    anomaly_vector signature changes, update both this shim and the delegate.
    Returns [] gracefully if Qdrant is unavailable or collection is empty.
    """
    try:
        from src.db.anomaly_vector import retrieve_similar_anomalies as _retrieve
        return _retrieve(
            symbol=symbol,
            regime=regime,
            trade_date=trade_date,
            k=k,
            category=category,
            same_asset_only=same_asset_only,
            exclude_window_days=exclude_window_days,
        )
    except (ImportError, ConnectionError, OSError) as e:
        # Qdrant unavailable — expected in offline or non-vectorised environments
        log.debug("retrieve_similar_anomalies: Qdrant unavailable: %s", e)
        return []
    except Exception as e:
        # Programming error — log at WARNING so it surfaces during development
        log.warning("retrieve_similar_anomalies: unexpected error: %s", e)
        return []
