"""
src/db/concall_vector.py
─────────────────────────
Qdrant integration for earnings concall transcript commentary.

Transcripts come from Screener.in's "Concalls" panel (free, BSE-hosted PDF
links — NOT Screener's own paywalled "AI Summary"; see
src.data_importer.tool_fetchers.earnings_scraper.fetch_screener_concall_links).
This module only handles chunking + embedding + Qdrant storage/search —
PDF download/text-extraction and the ClickHouse freshness check live in
src.tools.concall_transcripts, which is the entry point callers should use.

Collection schema
  name        : concall_transcripts
  vector dim  : 768  (nomic-embed-text)
  distance    : COSINE
  payload idx : symbol (keyword, is_tenant), period_label (keyword)

Chunking note: embed_text/embed_batch (src.ml.correlation.news_rag) hard-
truncate each string to 512 characters before embedding — there's no
token-aware windowing in that layer. Chunks here are sized to ~450 chars
(with a safety margin) specifically so nothing past the end of a chunk is
silently dropped from its own embedding.
"""

from __future__ import annotations

import logging
import re
import threading
import uuid
from typing import Any

log = logging.getLogger(__name__)

_COLLECTION = "concall_transcripts"
_EMBED_DIM = 768
_CHUNK_CHARS = 450
_CHUNK_OVERLAP = 50

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
        PointStruct,
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
            grpc_port = int(os.environ.get("QDRANT_GRPC_PORT") or getattr(settings, "qdrant_grpc_port", 6334))
            from src.utils.net_check import is_port_open
            if not is_port_open(host, port):
                log.debug("ConcallVector: Qdrant unreachable at %s:%s — skipping client init", host, port)
                _client = None
                return None
            _client = QdrantClient(host=host, port=port, grpc_port=grpc_port, prefer_grpc=True, timeout=15.0)
        except Exception as e:
            log.debug("ConcallVector: Qdrant client init failed: %s", e)
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
            log.info("ConcallVector: created Qdrant collection '%s'", _COLLECTION)
            try:
                client.create_payload_index(
                    collection_name=_COLLECTION, field_name="symbol",
                    field_schema=KeywordIndexParams(type=KeywordIndexType.KEYWORD, is_tenant=True),
                )
                client.create_payload_index(collection_name=_COLLECTION, field_name="period_label", field_schema="keyword")
            except Exception:
                pass
        _collection_ready = True
        return True
    except Exception as e:
        log.warning("ConcallVector: collection ensure failed: %s", e)
        return False


# ── Chunking ──────────────────────────────────────────────────────────────────

def chunk_transcript(text: str, chunk_chars: int = _CHUNK_CHARS, overlap_chars: int = _CHUNK_OVERLAP) -> list[str]:
    """
    Split transcript text into ~chunk_chars windows on sentence boundaries
    where possible, falling back to a hard cut. Small overlap keeps a
    sentence spanning a boundary searchable from either chunk.
    """
    text = re.sub(r"\s+", " ", text or "").strip()
    if not text:
        return []
    sentences = re.split(r"(?<=[.!?])\s+", text)

    chunks: list[str] = []
    current = ""
    for sent in sentences:
        if current and len(current) + len(sent) + 1 > chunk_chars:
            chunks.append(current.strip())
            # carry the tail of the previous chunk forward for continuity
            current = current[-overlap_chars:] + " " + sent
        else:
            current = (current + " " + sent).strip()
    if current.strip():
        chunks.append(current.strip())

    # A single sentence longer than chunk_chars won't have been split above — hard-cut it.
    final: list[str] = []
    for c in chunks:
        while len(c) > chunk_chars * 1.5:
            final.append(c[:chunk_chars])
            c = c[chunk_chars - overlap_chars:]
        final.append(c)
    return [c for c in final if c.strip()]


# ── Store ─────────────────────────────────────────────────────────────────────

def _point_id(symbol: str, period_label: str, chunk_index: int) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"concall:{symbol}:{period_label}:{chunk_index}"))


def store_transcript_chunks(
    symbol: str, period_label: str, pdf_url: str, full_text: str,
) -> int:
    """
    Chunk, embed, and upsert one concall transcript. Returns the number of
    chunks stored (0 if Qdrant/Ollama is unavailable or the text was empty).
    """
    chunks = chunk_transcript(full_text)
    if not chunks:
        return 0

    client = _get_client()
    if client is None or not _ensure_collection():
        return 0

    from src.ml.correlation.news_rag import embed_batch
    vectors = embed_batch(chunks)
    if not vectors or all(all(x == 0.0 for x in v) for v in vectors):
        log.warning("ConcallVector: embedding unavailable (Ollama unreachable) — skipping store for %s %s", symbol, period_label)
        return 0

    points = [
        PointStruct(
            id=_point_id(symbol, period_label, i),
            vector=vectors[i],
            payload={
                "symbol": symbol,
                "period_label": period_label,
                "chunk_index": i,
                "chunk_text": chunks[i],
                "pdf_url": pdf_url,
                "source": "screener_bse",
            },
        )
        for i in range(len(chunks))
    ]
    try:
        client.upsert(collection_name=_COLLECTION, points=points)
    except Exception as e:
        log.warning("ConcallVector: upsert failed for %s %s: %s", symbol, period_label, e)
        return 0
    return len(points)


# ── Search ────────────────────────────────────────────────────────────────────

def search_concall_commentary(symbol: str, query: str, k: int = 5, period_label: str = "") -> list[dict]:
    """
    Semantic search over a symbol's stored concall transcript chunks.

    Args:
        symbol:       NSE symbol — results are scoped to this company only.
        query:        Natural-language question, e.g. "margin guidance",
                      "demand outlook", "capex plans".
        k:            Number of chunks to return.
        period_label: Optional filter to one quarter, e.g. "Jul 2026".

    Returns:
        List of dicts: {chunk_text, period_label, pdf_url, chunk_index, similarity}.
    """
    client = _get_client()
    if client is None or not _ensure_collection():
        return []

    from src.ml.correlation.news_rag import embed_batch
    vectors = embed_batch([query])
    if not vectors or all(x == 0.0 for x in vectors[0]):
        return []

    must = [FieldCondition(key="symbol", match=MatchValue(value=symbol))]
    if period_label:
        must.append(FieldCondition(key="period_label", match=MatchValue(value=period_label)))

    try:
        hits = client.query_points(
            collection_name=_COLLECTION,
            query=vectors[0],
            query_filter=Filter(must=must),
            limit=k,
            with_payload=True,
        )
        return [{**(hit.payload or {}), "similarity": float(hit.score)} for hit in hits.points]
    except Exception as e:
        log.warning("ConcallVector: search failed for %s: %s", symbol, e)
        return []


def has_chunks(symbol: str, period_label: str) -> bool:
    """Cheap existence check — used by the cache-aside refresh to skip already-stored quarters."""
    client = _get_client()
    if client is None or not _ensure_collection():
        return False
    try:
        result = client.scroll(
            collection_name=_COLLECTION,
            scroll_filter=Filter(must=[
                FieldCondition(key="symbol", match=MatchValue(value=symbol)),
                FieldCondition(key="period_label", match=MatchValue(value=period_label)),
            ]),
            limit=1,
        )
        return len(result[0]) > 0
    except Exception as e:
        log.debug("ConcallVector: has_chunks check failed for %s %s: %s", symbol, period_label, e)
        return False
