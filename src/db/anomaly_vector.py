"""
src/db/anomaly_vector.py
────────────────────────
Qdrant integration for anomaly detection results.

Two roles:
  WRITE  store_anomalies(df_flagged, symbol, category)
         Embeds each flagged row as a text description and upserts to the
         `market_anomalies` Qdrant collection. Called fire-and-forget from
         the anomaly pipeline after every run.

  READ   retrieve_similar_anomalies(symbol, regime, trade_date, k, ...)
         Semantic-similarity search over past anomaly events. Used by reports
         and alert pipelines to surface historical precedents for a new anomaly.

Collection schema
  name        : market_anomalies
  vector dim  : 768  (nomic-embed-text)
  distance    : COSINE
  payload idx : symbol (keyword), category (keyword), regime (keyword),
                trade_timestamp (float)

Text description format (embedded for each flagged row):
  "GOLDBEES (etfs) 2024-01-15: ⚡ Flash Crash / Black Swan (EXIT)
   final_z=-4.23 garch_vol=18.5% return=-2.14% z_resid=3.87 if_conf=0.91"
"""

from __future__ import annotations

import logging
import threading
import uuid
from datetime import date, datetime
from typing import Any

import pandas as pd

log = logging.getLogger(__name__)

_COLLECTION = "market_anomalies"
_EMBED_DIM = 768

# ── Qdrant lazy init (shared pattern with market_vector) ─────────────────────

_qdrant_available = True
try:
    from qdrant_client import QdrantClient
    from qdrant_client.models import (
        Distance,
        FieldCondition,
        Filter,
        KeywordIndexParams,
        KeywordIndexType,
        MatchValue,
        PayloadSchemaType,
        PointStruct,
        Range,
        VectorParams,
    )
except ImportError:
    _qdrant_available = False

_client: Any = None
_client_lock = threading.Lock()
_collection_ready = False


def _get_client() -> Any:
    global _client
    if not _qdrant_available:
        return None
    if _client is not None:
        return _client
    with _client_lock:
        if _client is not None:
            return _client
        try:
            import os
            from config.settings import settings

            host = os.environ.get("QDRANT_HOST") or getattr(settings, "qdrant_host", "localhost")
            port = int(os.environ.get("QDRANT_PORT") or getattr(settings, "qdrant_port", 6333))
            _client = QdrantClient(url=f"http://{host}:{port}", timeout=15.0)
            log.debug("AnomalyVector: Qdrant client at %s:%s", host, port)
        except Exception as e:
            log.debug("AnomalyVector: Qdrant init failed: %s", e)
            _client = None
    return _client


def _ensure_collection() -> bool:
    global _collection_ready
    if _collection_ready:
        return True
    client = _get_client()
    if client is None:
        return False
    try:
        names = {c.name for c in client.get_collections().collections}
        if _COLLECTION not in names:
            client.create_collection(
                collection_name=_COLLECTION,
                vectors_config=VectorParams(size=_EMBED_DIM, distance=Distance.COSINE),
            )
            log.info("AnomalyVector: created Qdrant collection '%s'", _COLLECTION)
            for field, schema in [
                ("symbol",          KeywordIndexParams(type=KeywordIndexType.KEYWORD, is_tenant=True)),
                ("category",        PayloadSchemaType.KEYWORD),
                ("regime",          PayloadSchemaType.KEYWORD),
                ("trade_timestamp", PayloadSchemaType.FLOAT),
                ("attributed_event_type", PayloadSchemaType.KEYWORD),
            ]:
                try:
                    client.create_payload_index(
                        collection_name=_COLLECTION,
                        field_name=field,
                        field_schema=schema,
                    )
                except Exception:
                    pass
        _collection_ready = True
        return True
    except Exception as e:
        log.warning("AnomalyVector: collection ensure failed: %s", e)
        return False


# ── Helpers ───────────────────────────────────────────────────────────────────

def _point_id(symbol: str, date_key: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"anomaly:{symbol}:{date_key}"))


def _to_ts(d: Any) -> float:
    if isinstance(d, datetime):
        return d.timestamp()
    if isinstance(d, date):
        return datetime(d.year, d.month, d.day).timestamp()
    try:
        return datetime.strptime(str(d)[:10], "%Y-%m-%d").timestamp()
    except Exception:
        return 0.0


def _safe(row: pd.Series, col: str, default: float = 0.0) -> float:
    try:
        v = row.get(col, default)
        return float(v) if v is not None and not (isinstance(v, float) and pd.isna(v)) else default
    except Exception:
        return default


def _row_text(symbol: str, category: str, row: pd.Series) -> str:
    trade_date = str(row.get("trade_date", ""))[:10]
    regime = str(row.get("regime", ""))
    final_z = _safe(row, "final_z")
    garch_vol = _safe(row, "garch_vol")
    daily_return = _safe(row, "daily_return")
    z_resid = _safe(row, "z_resid")
    if_conf = _safe(row, "if_confidence")
    return (
        f"{symbol} ({category}) {trade_date}: {regime} "
        f"final_z={final_z:.2f} garch_vol={garch_vol:.1f}% "
        f"return={daily_return:.2f}% z_resid={z_resid:.2f} if_conf={if_conf:.2f}"
    )


def _embed(texts: list[str]) -> list[list[float]]:
    try:
        from src.ml.correlation.news_rag import embed_batch
        return embed_batch(texts)
    except Exception as e:
        log.debug("AnomalyVector: embed_batch failed: %s", e)
        return [[0.0] * _EMBED_DIM for _ in texts]


# ── Write path ────────────────────────────────────────────────────────────────

# Attribution dict shape (see store_anomalies_with_attribution):
#   {"event_type": str, "label": str, "score": float, "confidence": str,
#    "strategy": str, "lag_days": int}
# or None for an anomaly the correlation engine looked at but could not explain
# (recorded as attributed_confidence="UNEXPLAINED" — a real negative signal,
# distinct from an anomaly that was never checked at all).

def _attribution_payload(attribution: dict | None) -> dict:
    if attribution is None:
        return {"attributed_confidence": "UNEXPLAINED"}
    return {
        "attributed_event_type": str(attribution.get("event_type", "")),
        "attributed_label":      str(attribution.get("label", "")),
        "attributed_score":      float(attribution.get("score", 0.0)),
        "attributed_confidence": str(attribution.get("confidence", "")),
        "attributed_strategy":   str(attribution.get("strategy", "")),
        "attributed_lag_days":   int(attribution.get("lag_days", 0)),
    }


def _do_store(
    df_flagged: pd.DataFrame,
    symbol: str,
    category: str,
    attributions: dict[str, dict | None] | None = None,
) -> None:
    if df_flagged.empty:
        return
    client = _get_client()
    if client is None or not _ensure_collection():
        return

    rows = list(df_flagged.itertuples(index=False))
    texts = [_row_text(symbol, category, pd.Series(r._asdict())) for r in rows]
    vectors = _embed(texts)

    points = []
    for i, r in enumerate(rows):
        row = pd.Series(r._asdict())
        trade_date_str = str(row.get("trade_date", ""))[:10]
        payload = {
            "data_type":       "anomaly",
            "symbol":          symbol,
            "category":        category,
            "trade_date":      trade_date_str,
            "trade_timestamp": _to_ts(row.get("trade_date")),
            "regime":          str(row.get("regime", "")),
            "final_z":         _safe(row, "final_z"),
            "final_z_abs":     _safe(row, "final_z_abs"),
            "z_robust":        _safe(row, "z_robust"),
            "z_resid":         _safe(row, "z_resid"),
            "garch_vol":       _safe(row, "garch_vol"),
            "if_confidence":   _safe(row, "if_confidence"),
            "daily_return":    _safe(row, "daily_return"),
            "close":           _safe(row, "close"),
            "is_changepoint":  bool(row.get("is_changepoint", False)),
            "cp_confirmed":    bool(row.get("cp_confirmed", False)),
            "text":            texts[i],
        }
        if attributions is not None and trade_date_str in attributions:
            payload.update(_attribution_payload(attributions[trade_date_str]))
        points.append(
            PointStruct(id=_point_id(symbol, trade_date_str), vector=vectors[i], payload=payload)
        )

    try:
        client.upsert(collection_name=_COLLECTION, points=points)
        log.info("AnomalyVector: stored %d anomaly points for %s", len(points), symbol)
    except Exception as e:
        log.warning("AnomalyVector: upsert failed: %s", e)


def store_anomalies_with_attribution(
    df_flagged: pd.DataFrame,
    symbol: str,
    category: str,
    attributions: dict[str, dict | None],
) -> None:
    """Fire-and-forget: store flagged anomalies together with what the
    correlation engine concluded caused each one (or None → UNEXPLAINED).

    Writing anomaly stats and attribution in the same upsert avoids a race
    between two separate writes — a later ``set_payload`` call could fire
    before the anomaly point itself has been created.
    """
    if df_flagged is None or df_flagged.empty or not _qdrant_available:
        return
    t = threading.Thread(
        target=_do_store,
        args=(df_flagged.copy(), symbol, category, attributions),
        daemon=True,
    )
    t.start()


def store_anomalies(df_flagged: pd.DataFrame, symbol: str, category: str = "") -> None:
    """Store flagged anomaly rows in Qdrant (fire-and-forget background thread)."""
    if df_flagged is None or df_flagged.empty or not _qdrant_available:
        return
    t = threading.Thread(
        target=_do_store,
        args=(df_flagged.copy(), symbol, category),
        daemon=True,
    )
    t.start()


# ── Read path ─────────────────────────────────────────────────────────────────

def retrieve_similar_anomalies(
    symbol: str,
    regime: str,
    trade_date: Any,
    k: int = 5,
    category: str = "",
    same_asset_only: bool = False,
) -> list[dict]:
    """
    Find past anomaly events semantically similar to the given regime + context.

    Builds a query text from symbol/category/regime and searches the
    `market_anomalies` collection by vector similarity.

    Args:
        symbol:          The asset symbol (e.g. "GOLDBEES").
        regime:          The anomaly regime label (e.g. "⚡ Flash Crash / Black Swan (EXIT)").
        trade_date:      The anomaly date — used to exclude future-leaking matches
                         that are within 30 days of the query date.
        k:               Number of similar events to return.
        category:        Optional asset category for richer query context.
        same_asset_only: If True, restrict results to the same symbol.

    Returns:
        List of dicts with keys: symbol, category, trade_date, regime,
        final_z, garch_vol, daily_return, similarity, text.
    """
    if not _qdrant_available:
        return []
    client = _get_client()
    if client is None or not _ensure_collection():
        return []

    query_text = f"{symbol} ({category}) {str(trade_date)[:10]}: {regime}"
    vectors = _embed([query_text])
    query_vec = vectors[0]
    if all(v == 0.0 for v in query_vec):
        return []

    # Exclude a 30-day window around the query date to avoid trivial self-matches
    ts = _to_ts(trade_date)
    exclusion_lo = ts - 30 * 86400
    exclusion_hi = ts + 30 * 86400

    try:
        must_conditions = []
        if same_asset_only:
            must_conditions.append(
                FieldCondition(key="symbol", match=MatchValue(value=symbol))
            )

        # Two searches: before and after the exclusion window, then merge
        results_all: list[dict] = []
        for (lo, hi) in [
            (0.0, exclusion_lo),
            (exclusion_hi, ts + 10 * 365 * 86400),
        ]:
            range_conds = list(must_conditions) + [
                FieldCondition(key="trade_timestamp", range=Range(gte=lo, lte=hi))
            ]
            hits = client.query_points(
                collection_name=_COLLECTION,
                query=query_vec,
                query_filter=Filter(must=range_conds),
                limit=k,
                with_payload=True,
            )
            for hit in hits.points:
                p = hit.payload or {}
                results_all.append({
                    "symbol":       p.get("symbol", ""),
                    "category":     p.get("category", ""),
                    "trade_date":   p.get("trade_date", ""),
                    "regime":       p.get("regime", ""),
                    "final_z":      p.get("final_z", 0.0),
                    "garch_vol":    p.get("garch_vol", 0.0),
                    "daily_return": p.get("daily_return", 0.0),
                    "similarity":   float(hit.score),
                    "text":         p.get("text", ""),
                    # Absent on points written before attribution existed —
                    # "" / 0.0 defaults mean "no attribution recorded",
                    # distinct from "UNEXPLAINED" (attribution was attempted
                    # and the correlation engine found no cause).
                    "attributed_event_type": p.get("attributed_event_type", ""),
                    "attributed_label":      p.get("attributed_label", ""),
                    "attributed_score":      p.get("attributed_score", 0.0),
                    "attributed_confidence": p.get("attributed_confidence", ""),
                })

        results_all.sort(key=lambda x: -x["similarity"])
        return results_all[:k]

    except Exception as e:
        log.warning("AnomalyVector: retrieve failed: %s", e)
        return []
