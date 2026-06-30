"""
src/db/market_vector.py
───────────────────────
Qdrant integration for multi-asset market data.

Converts each imported row to a text description, embeds it via Ollama
nomic-embed-text, and upserts to the `market_data` Qdrant collection.

Entry points (all fire-and-forget via a background thread):
  vectorize_prices(rows)          — OHLCV daily_prices rows
  vectorize_nav(rows)             — MF/ETF NAV rows
  vectorize_fx_rates(rows)        — FX rate rows
  vectorize_macro(rows)           — macro_indicators rows
  vectorize_cot(rows)             — COT gold positioning rows

Collection schema
  name        : market_data
  vector dim  : 768  (nomic-embed-text)
  distance    : COSINE
  payload idx : symbol (keyword), category (keyword), trade_timestamp (float)
"""

from __future__ import annotations

import logging
import threading
import uuid
from datetime import date, datetime
from typing import Any

log = logging.getLogger(__name__)

_COLLECTION = "market_data"
_EMBED_DIM = 768

# ── Qdrant lazy init ──────────────────────────────────────────────────────────

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
            log.debug("MarketVector: Qdrant client connected at %s:%s", host, port)
        except Exception as e:
            log.debug("MarketVector: Qdrant client init failed: %s", e)
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
            log.info("MarketVector: created Qdrant collection '%s'", _COLLECTION)
            for field, schema in [
                ("symbol", KeywordIndexParams(type=KeywordIndexType.KEYWORD, is_tenant=True)),
                ("category", PayloadSchemaType.KEYWORD),
                ("trade_timestamp", PayloadSchemaType.FLOAT),
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
        log.warning("MarketVector: collection ensure failed: %s", e)
        return False


# ── Helpers ───────────────────────────────────────────────────────────────────

def _point_id(symbol: str, date_key: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{symbol}:{date_key}"))


def _to_timestamp(d: Any) -> float:
    if isinstance(d, datetime):
        return d.timestamp()
    if isinstance(d, date):
        return datetime(d.year, d.month, d.day).timestamp()
    try:
        return datetime.strptime(str(d), "%Y-%m-%d").timestamp()
    except Exception:
        return 0.0


def _upsert(points: list[Any]) -> None:
    if not points:
        return
    client = _get_client()
    if client is None or not _ensure_collection():
        return
    try:
        client.upsert(collection_name=_COLLECTION, points=points)
        log.debug("MarketVector: upserted %d points", len(points))
    except Exception as e:
        log.warning("MarketVector: upsert failed: %s", e)


def _background(fn, *args, **kwargs) -> None:
    t = threading.Thread(target=fn, args=args, kwargs=kwargs, daemon=True)
    t.start()


# ── Embedding ─────────────────────────────────────────────────────────────────

def _embed(texts: list[str]) -> list[list[float]]:
    try:
        from src.ml.correlation.news_rag import embed_batch
        return embed_batch(texts)
    except Exception as e:
        log.debug("MarketVector: embed_batch failed: %s", e)
        return [[0.0] * _EMBED_DIM for _ in texts]


# ── Vectorizers ───────────────────────────────────────────────────────────────

def _do_vectorize_prices(rows: list[dict]) -> None:
    texts = [
        f"{r['symbol']} ({r['category']}) {r['trade_date']}: "
        f"open={float(r['open']):.2f} high={float(r['high']):.2f} "
        f"low={float(r['low']):.2f} close={float(r['close']):.2f} "
        f"volume={float(r['volume']):.0f}"
        for r in rows
    ]
    vectors = _embed(texts)
    points = [
        PointStruct(
            id=_point_id(r["symbol"], str(r["trade_date"])),
            vector=vectors[i],
            payload={
                "data_type": "price",
                "symbol": r["symbol"],
                "category": r["category"],
                "trade_date": str(r["trade_date"]),
                "trade_timestamp": _to_timestamp(r["trade_date"]),
                "open": float(r["open"]),
                "high": float(r["high"]),
                "low": float(r["low"]),
                "close": float(r["close"]),
                "volume": float(r["volume"]),
            },
        )
        for i, r in enumerate(rows)
    ]
    _upsert(points)


def _do_vectorize_nav(rows: list[dict]) -> None:
    texts = [
        f"{r['symbol']} (mf_nav) {r['nav_date']}: nav={float(r['nav']):.4f}"
        for r in rows
    ]
    vectors = _embed(texts)
    points = [
        PointStruct(
            id=_point_id(r["symbol"], str(r["nav_date"])),
            vector=vectors[i],
            payload={
                "data_type": "nav",
                "symbol": r["symbol"],
                "category": "mf_nav",
                "scheme_code": r.get("scheme_code", ""),
                "trade_date": str(r["nav_date"]),
                "trade_timestamp": _to_timestamp(r["nav_date"]),
                "nav": float(r["nav"]),
            },
        )
        for i, r in enumerate(rows)
    ]
    _upsert(points)


def _do_vectorize_fx_rates(rows: list[dict]) -> None:
    texts = [
        f"{r['symbol']} (fx_rates) {r['trade_date']}: "
        f"open={float(r['open']):.4f} high={float(r['high']):.4f} "
        f"low={float(r['low']):.4f} close={float(r['close']):.4f}"
        for r in rows
    ]
    vectors = _embed(texts)
    points = [
        PointStruct(
            id=_point_id(r["symbol"], str(r["trade_date"])),
            vector=vectors[i],
            payload={
                "data_type": "fx_rate",
                "symbol": r["symbol"],
                "category": "fx_rates",
                "trade_date": str(r["trade_date"]),
                "trade_timestamp": _to_timestamp(r["trade_date"]),
                "open": float(r["open"]),
                "high": float(r["high"]),
                "low": float(r["low"]),
                "close": float(r["close"]),
                "source": r.get("source", ""),
            },
        )
        for i, r in enumerate(rows)
    ]
    _upsert(points)


def _do_vectorize_macro(rows: list[dict]) -> None:
    texts = [
        f"{r['indicator_name']} ({r['country_code']}) {r['ref_year']}: "
        f"value={float(r['value']):.4f} [{r['indicator_code']}]"
        for r in rows
    ]
    vectors = _embed(texts)
    points = [
        PointStruct(
            id=_point_id(f"{r['country_code']}:{r['indicator_code']}", str(r["ref_year"])),
            vector=vectors[i],
            payload={
                "data_type": "macro",
                "symbol": r["indicator_code"],
                "category": "macro_indicators",
                "trade_date": str(r["ref_year"]),
                "trade_timestamp": _to_timestamp(date(int(r["ref_year"]), 1, 1)),
                "country_code": r["country_code"],
                "indicator_code": r["indicator_code"],
                "indicator_name": r["indicator_name"],
                "value": float(r["value"]),
                "source": r.get("source", ""),
                "is_forecast": int(r.get("is_forecast", 0)),
            },
        )
        for i, r in enumerate(rows)
    ]
    _upsert(points)


def _do_vectorize_cot(rows: list[dict]) -> None:
    texts = [
        f"GOLD COT {r['report_date']}: "
        f"mm_net={float(r['mm_net']):.0f} comm_net={float(r['comm_net']):.0f} "
        f"open_interest={float(r['open_interest']):.0f}"
        for r in rows
    ]
    vectors = _embed(texts)
    points = [
        PointStruct(
            id=_point_id("GOLD_COT", str(r["report_date"])),
            vector=vectors[i],
            payload={
                "data_type": "cot",
                "symbol": "GOLD_COT",
                "category": "cot",
                "trade_date": str(r["report_date"]),
                "trade_timestamp": _to_timestamp(r["report_date"]),
                "mm_long": float(r.get("mm_long", 0)),
                "mm_short": float(r.get("mm_short", 0)),
                "mm_net": float(r["mm_net"]),
                "comm_long": float(r.get("comm_long", 0)),
                "comm_short": float(r.get("comm_short", 0)),
                "comm_net": float(r["comm_net"]),
                "open_interest": float(r["open_interest"]),
                "source": r.get("source", ""),
            },
        )
        for i, r in enumerate(rows)
    ]
    _upsert(points)


# ── Public API (fire-and-forget) ──────────────────────────────────────────────

def vectorize_prices(rows: list[dict]) -> None:
    if rows and _qdrant_available:
        _background(_do_vectorize_prices, rows)


def vectorize_nav(rows: list[dict]) -> None:
    if rows and _qdrant_available:
        _background(_do_vectorize_nav, rows)


def vectorize_fx_rates(rows: list[dict]) -> None:
    if rows and _qdrant_available:
        _background(_do_vectorize_fx_rates, rows)


def vectorize_macro(rows: list[dict]) -> None:
    if rows and _qdrant_available:
        _background(_do_vectorize_macro, rows)


def vectorize_cot(rows: list[dict]) -> None:
    if rows and _qdrant_available:
        _background(_do_vectorize_cot, rows)
