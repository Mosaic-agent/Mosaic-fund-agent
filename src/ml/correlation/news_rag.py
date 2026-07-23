"""
src/ml/correlation/news_rag.py
──────────────────────────────
Embedding-based news retrieval and quality scoring for the correlation engine.

Uses Ollama `nomic-embed-text` (768-dim) for embeddings, stored in ClickHouse
`news_articles.embedding` column. Provides:
  - embed_text()           — embed a single text string via Ollama HTTP API
  - retrieve_articles()    — semantic search within a date window
  - score_news_quality()   — exemplar-based quality weight (replaces keyword tiers)
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import uuid
from datetime import date, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Optional

import numpy as np
import requests

log = logging.getLogger(__name__)

# Qdrant client dynamic/lazy import setup
_qdrant_available = True
try:
    from qdrant_client import QdrantClient
    from qdrant_client.models import (
        Distance, VectorParams, PointStruct, Filter, FieldCondition, Range,
        MatchValue, PayloadSchemaType, KeywordIndexParams, KeywordIndexType,
    )
except ImportError:
    _qdrant_available = False

_qdrant_client: Optional[QdrantClient] = None
_qdrant_collection_verified = False

def get_qdrant_client() -> Optional[QdrantClient]:
    """Lazy initialize Qdrant client."""
    global _qdrant_client
    if not _qdrant_available:
        return None
    if _qdrant_client is None:
        try:
            from config.settings import settings
            host = os.environ.get("QDRANT_HOST") or getattr(settings, "qdrant_host", "localhost")
            port = int(os.environ.get("QDRANT_PORT") or getattr(settings, "qdrant_port", 6333))
            _qdrant_client = QdrantClient(url=f"http://{host}:{port}", timeout=10.0)
        except Exception as e:
            log.debug("Failed to initialize QdrantClient: %s", e)
            _qdrant_client = None
    return _qdrant_client

def ensure_collection(client: QdrantClient, collection_name: str, dim: int = 768) -> bool:
    """Ensure the Qdrant collection exists."""
    global _qdrant_collection_verified
    if _qdrant_collection_verified:
        return True
    try:
        collections = client.get_collections().collections
        exists = any(c.name == collection_name for c in collections)
        if not exists:
            client.create_collection(
                collection_name=collection_name,
                vectors_config=VectorParams(size=dim, distance=Distance.COSINE),
            )
            log.info("Created Qdrant collection: %s", collection_name)
            try:
                client.create_payload_index(
                    collection_name=collection_name,
                    field_name="published_timestamp",
                    field_schema=PayloadSchemaType.FLOAT,
                )
                client.create_payload_index(
                    collection_name=collection_name,
                    field_name="category",
                    field_schema=PayloadSchemaType.KEYWORD,
                )
                # Exact-match index for retrieve_cached_news_for_symbol's
                # (symbol, published_date) scroll — published_timestamp is a
                # FLOAT range index and does not serve exact YYYY-MM-DD lookups.
                client.create_payload_index(
                    collection_name=collection_name,
                    field_name="published_date",
                    field_schema=PayloadSchemaType.KEYWORD,
                )
                log.info("Created Qdrant payload indexes for published_timestamp, category, published_date")
            except Exception as e:
                log.warning("Failed to create payload indexes in Qdrant: %s", e)
        _ensure_symbol_index(client, collection_name)
        _qdrant_collection_verified = True
        return True
    except Exception as e:
        log.warning("Qdrant collection check failed: %s", e)
        return False


_qdrant_symbol_index_verified = False


def _ensure_symbol_index(client: QdrantClient, collection_name: str) -> None:
    """Best-effort, run-once: tenant keyword index on `symbol`.

    Most news queries are symbol-scoped (per-stock correlation retrieval and the
    cache lookup), so `symbol` is the tenant field — same pattern as the
    `market_anomalies` / `mf_holdings` / `market_data` collections
    (see anomaly_vector.py, mf_vector.py, market_vector.py). Tenant indexing lets
    Qdrant co-locate each symbol's points and avoid a full-collection scan.
    """
    global _qdrant_symbol_index_verified
    if _qdrant_symbol_index_verified:
        return
    try:
        client.create_payload_index(
            collection_name=collection_name,
            field_name="symbol",
            field_schema=KeywordIndexParams(type=KeywordIndexType.KEYWORD, is_tenant=True),
        )
    except Exception:
        pass  # already exists — fine
    _qdrant_symbol_index_verified = True

def generate_point_id(url: str, title: str) -> str:
    """Generate a deterministic UUID v5 from URL or Title for Qdrant point ID."""
    key = url.strip() if url and url.strip() else title.strip()
    return str(uuid.uuid5(uuid.NAMESPACE_URL, key))


def _normalize_symbols(raw) -> list[str]:
    """Normalize a symbol payload into a list of upper-cased tickers.

    Accepts a single ticker ("RELIANCE"), a comma/semicolon-joined string
    ("GOLDBEES,SILVERBEES" as stored in ClickHouse etfs_impacted), a list, or
    None. Returns [] when empty so a Qdrant keyword array index has nothing to
    match rather than an empty-string element.
    """
    if not raw:
        return []
    if isinstance(raw, (list, tuple, set)):
        parts = [str(s) for s in raw]
    else:
        parts = str(raw).replace(";", ",").split(",")
    return [p.strip().upper() for p in parts if p and p.strip()]

def upsert_to_qdrant(articles: list[dict], vectors: list[list[float]]):
    """Upsert articles and their embeddings to Qdrant.
    
    Each article dict must have: title, source, url, published_at, category, sentiment.
    """
    client = get_qdrant_client()
    if not client or not articles or not vectors:
        return

    dim = len(vectors[0])
    if not ensure_collection(client, "news_articles", dim):
        return

    points = []
    from dateutil import parser as date_parser
    for i, art in enumerate(articles):
        title = art.get("title", "")
        url = art.get("url", "")
        if not title and not url:
            continue
            
        point_id = generate_point_id(url, title)
        
        # Parse published_at for clean range filtering
        pub_str = art.get("published_at", "")
        pub_ts = 0.0
        try:
            pub_dt = date_parser.parse(pub_str)
            pub_date_str = pub_dt.strftime("%Y-%m-%d")
            pub_ts = float(pub_dt.timestamp())
        except Exception:
            try:
                # Try fetched_at
                pub_dt = date_parser.parse(str(art.get("fetched_at", "")))
                pub_date_str = pub_dt.strftime("%Y-%m-%d")
                pub_ts = float(pub_dt.timestamp())
            except Exception:
                pub_date_str = date.today().strftime("%Y-%m-%d")
                import datetime
                pub_ts = float(datetime.datetime.combine(date.today(), datetime.time.min).timestamp())

        # Callers that already know the caller-local (e.g. IST) calendar date —
        # which can disagree with the UTC-naive parse above near midnight — may
        # pass it explicitly so cache-key lookups (exact match on published_date)
        # stay consistent with the date they filtered on when fetching.
        pub_date_str = art.get("published_date", pub_date_str)

        payload = {
            "title": title,
            "source": art.get("source", ""),
            "url": url,
            "published_at": pub_str,
            "published_date": pub_date_str,
            "published_timestamp": pub_ts,
            "category": art.get("category", ""),
            "sentiment": art.get("sentiment", "NEUTRAL"),
            # Stored as a LIST of upper-cased tickers. Sources vary: single-ticker
            # writers pass "RELIANCE"; the ClickHouse etfs_impacted column packs
            # several as "GOLDBEES,SILVERBEES". A Qdrant keyword index matches a
            # MatchValue against ANY element of an array, so a per-symbol filter
            # (`match=MatchValue("GOLDBEES")`) hits multi-symbol news too — which a
            # scalar "GOLDBEES,SILVERBEES" string would never match.
            "symbol": _normalize_symbols(art.get("symbol", "")),
        }
        points.append(PointStruct(id=point_id, vector=vectors[i], payload=payload))

    try:
        client.upsert(collection_name="news_articles", points=points)
        log.info("Successfully upserted %d points to Qdrant", len(points))
    except Exception as e:
        log.error("Failed to upsert to Qdrant: %s", e)

def retrieve_cached_news_for_symbol(symbol: str, published_date: str, limit: int = 30) -> list[dict]:
    """
    Exact-match lookup of previously-fetched news for a symbol on a given date
    straight from the Qdrant `news_articles` cache — no embedding call, no live
    fetch. Returns [] (not an error) if nothing is cached, signalling the caller
    to fall back to a live fetch.

    Args:
        symbol:         Ticker, upper/lower-cased freely (normalised internally).
        published_date: YYYY-MM-DD string matching the `published_date` payload field.
        limit:          Max cached articles to return.
    """
    client = get_qdrant_client()
    if not client or not _qdrant_available:
        return []
    if not ensure_collection(client, "news_articles", _EMBED_DIM):
        return []

    try:
        points, _ = client.scroll(
            collection_name="news_articles",
            scroll_filter=Filter(
                must=[
                    FieldCondition(key="symbol", match=MatchValue(value=symbol.upper())),
                    FieldCondition(key="published_date", match=MatchValue(value=published_date)),
                ]
            ),
            limit=limit,
            with_payload=True,
            with_vectors=False,
        )
    except Exception as e:
        log.debug("Qdrant cache lookup failed for %s/%s: %s", symbol, published_date, e)
        return []

    return [dict(p.payload or {}) for p in points]


# ── Configuration ─────────────────────────────────────────────────────────────

def _get_ollama_base() -> str:
    """Resolve Ollama base host dynamically from config/settings (.env) with container-to-host fallback."""
    try:
        from config.settings import settings
        default_host = settings.ollama_host.strip().rstrip("/") if hasattr(settings, "ollama_host") and settings.ollama_host else None
        fallback_host = settings.ollama_fallback_host.strip().rstrip("/") if hasattr(settings, "ollama_fallback_host") and settings.ollama_fallback_host else "http://localhost:11434"
    except Exception:
        default_host = None
        fallback_host = "http://localhost:11434"

    candidate = os.environ.get("OLLAMA_HOST")
    if candidate:
        candidate = candidate.strip().rstrip("/")
    elif default_host:
        candidate = default_host
    else:
        try:
            from config.settings import settings
            if settings.llm_base_url:
                base = settings.llm_base_url.strip().rstrip("/")
                if base.endswith("/v1"):
                    base = base[:-3]
                candidate = base
        except Exception:
            pass

    if not candidate:
        candidate = fallback_host

    # Container-to-host resolution fallback:
    # If the candidate URL targets the 'ollama' docker service name (e.g. http://ollama:11434)
    # but DNS resolution fails (running outside Docker container on macOS host),
    # dynamically rewrite 'ollama' to fallback_host target (default: localhost).
    if "://ollama" in candidate:
        import socket
        try:
            socket.gethostbyname("ollama")
        except (socket.gaierror, TimeoutError, OSError):
            fallback_target = fallback_host.split("://")[-1].split(":")[0] if "://" in fallback_host else "localhost"
            candidate = re.sub(r"://ollama(?=[:/].*|$)", "://" + fallback_target, candidate)

    return candidate

_EMBED_MODEL = "nomic-embed-text"
_EMBED_DIM = 768
_CACHE_DIR = Path("data/.cache/embeddings")

# Warn once per process when Ollama is unreachable; subsequent failures go to DEBUG.
_ollama_warned = False

# ── Embedding primitives ──────────────────────────────────────────────────────


def embed_text(text: str) -> list[float]:
    """Embed a single text string via Ollama HTTP API.

    Returns a 768-dim float vector. Raises on Ollama failure after 1 retry.
    Results are cached to disk by sha256(text) for 24h.
    """
    if not text or not text.strip():
        return [0.0] * _EMBED_DIM

    text = text.strip()[:512]  # nomic-embed-text has 512 token window

    # Check disk cache
    cache_key = hashlib.sha256(text.encode()).hexdigest()
    cache_path = _CACHE_DIR / f"{cache_key}.json"
    if cache_path.exists():
        try:
            age_hours = (os.path.getmtime(str(cache_path)) - os.time()) / 3600
        except Exception:
            age_hours = 0
        # Use cache if < 24h old
        import time
        age_hours = (time.time() - os.path.getmtime(str(cache_path))) / 3600
        if age_hours < 24:
            try:
                return json.loads(cache_path.read_text())
            except Exception:
                pass

    # Call Ollama
    url = f"{_get_ollama_base()}/api/embed"
    payload = {"model": _EMBED_MODEL, "input": text}
    try:
        resp = requests.post(url, json=payload, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        # Ollama returns {"embeddings": [[...]]} for /api/embed
        vec = data["embeddings"][0]
    except Exception as e:
        global _ollama_warned
        if not _ollama_warned:
            log.warning("Ollama embed unavailable — semantic scoring disabled (set OLLAMA_HOST to enable): %s", e)
            _ollama_warned = True
        else:
            log.debug("Ollama embed failed: %s", e)
        return [0.0] * _EMBED_DIM

    # Cache to disk
    try:
        _CACHE_DIR.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(json.dumps(vec))
    except Exception:
        pass

    return vec


# nomic-embed-text via Ollama's /api/embed becomes unreliable well before any
# documented limit — a 321-item batch reproducibly resets the connection with
# a 400 (the local model-runner subprocess appears to choke on it). Chunking
# here means every caller gets a safe batch size automatically instead of
# each one needing to remember its own wrapper — which is exactly how this
# broke: mf_vector.py had a 32-item chunking wrapper, anomaly_vector.py's
# equivalent didn't, and the unchunked call started failing at 321 items.
_OLLAMA_EMBED_BATCH = 32


def embed_batch(texts: list[str]) -> list[list[float]]:
    """Embed multiple texts via Ollama, chunked into safe sub-batches.

    Empty/whitespace-only entries are excluded from each request. Ollama's
    /api/embed rejects a batch containing even one empty string with a 400 —
    which otherwise fails the ENTIRE batch and forces a slow per-item
    fallback (one HTTP call per text) just because one entry was blank.
    """
    if not texts:
        return []

    if len(texts) > _OLLAMA_EMBED_BATCH:
        result: list[list[float]] = []
        for i in range(0, len(texts), _OLLAMA_EMBED_BATCH):
            result.extend(embed_batch(texts[i : i + _OLLAMA_EMBED_BATCH]))
        return result

    cleaned = [t.strip()[:512] if t else "" for t in texts]
    non_empty_idx = [i for i, t in enumerate(cleaned) if t]
    if not non_empty_idx:
        return [[0.0] * _EMBED_DIM for _ in texts]

    url = f"{_get_ollama_base()}/api/embed"
    payload = {"model": _EMBED_MODEL, "input": [cleaned[i] for i in non_empty_idx]}
    try:
        resp = requests.post(url, json=payload, timeout=120)
        resp.raise_for_status()
        embeddings = resp.json()["embeddings"]
    except Exception as e:
        global _ollama_warned
        if not _ollama_warned:
            log.warning("Ollama batch embed unavailable — semantic scoring disabled: %s", e)
            _ollama_warned = True
        else:
            log.debug("Ollama batch embed failed: %s", e)
        return [[0.0] * _EMBED_DIM for _ in texts]

    result = [[0.0] * _EMBED_DIM for _ in texts]
    for pos, idx in enumerate(non_empty_idx):
        result[idx] = embeddings[pos]
    return result


# ── Exemplar-based quality scoring ────────────────────────────────────────────

# ── Event-to-symbol relevance scoring ─────────────────────────────────────────

# Cache for symbol context embeddings (symbol → vector)
_symbol_context_cache: dict[str, np.ndarray] = {}


def score_event_relevance(event_text: str, symbol: str) -> float:
    """Score how relevant an event is to a specific symbol using cosine similarity.

    Compares the embedding of the event label/description against a symbol's
    domain context string (e.g. "GOLDBEES Nippon India ETF Gold BeES gold ETF
    India"). Returns a float in [0.3, 1.0] — floor at 0.3 ensures even weakly
    related macro events retain some influence.

    Falls back to 0.5 (the old hardcoded value) if embeddings are unavailable.
    """
    if not event_text or not event_text.strip():
        return 0.5

    # Build or retrieve symbol context embedding
    sym_upper = symbol.upper()
    if sym_upper not in _symbol_context_cache:
        try:
            from src.utils.symbol_mapper import get_company_name
            company = get_company_name(sym_upper)
            context = f"{sym_upper} {company}"
            cl = company.lower()
            # Enrich with asset-class keywords for ETFs
            if any(k in cl for k in ["etf", "bees", "index"]):
                if "gold" in cl:
                    context += " gold precious metals commodity India"
                elif "bank" in cl:
                    context += " banking financial services India"
                elif "nifty" in cl or "fang" in cl or "nyse" in cl:
                    context += " equity index India market"
                elif "silver" in cl:
                    context += " silver precious metals commodity India"
                elif "it" in cl:
                    context += " technology IT sector India"
                elif "liquid" in cl:
                    context += " debt liquid money market India"
            else:
                # Sector enrichment for individual stocks (heuristic from company name)
                if any(k in cl for k in ["bank", "finance", "life insurance", "prudential"]):
                    context += " banking financial services NBFC India RBI repo rate"
                elif any(k in cl for k in ["pharma", "laboratories", "biocon", "lupin", "cipla", "apollo", "hospital"]):
                    context += " pharmaceutical healthcare drugs FDA India"
                elif any(k in cl for k in ["motors", "auto", "leyland", "mrf", "tvs", "maruti", "hero", "eicher", "bajaj auto"]):
                    context += " automobile auto sector EV electric vehicle India"
                elif any(k in cl for k in ["reliance industries", "ongc", "oil", "petroleum", "petronet", "bpcl", "gail"]):
                    context += " oil gas petrochemicals crude WTI OPEC refining India"
                elif any(k in cl for k in ["tcs", "infosys", "wipro", "tech mahindra", "hcl tech", "mphasis", "persistent", "ltimindtree", "coforge", "kpit", "tata elxsi"]):
                    context += " technology IT software services USD India exports"
                elif any(k in cl for k in ["steel", "hindalco", "coal", "jsw", "ntpc", "power grid", "tata steel"]):
                    context += " metals mining commodities steel coal power utilities India"
                elif any(k in cl for k in ["hindustan unilever", "itc", "nestle", "britannia", "marico", "dabur", "godrej", "colgate", "varun", "tata consumer", "emami"]):
                    context += " FMCG consumer staples India rural demand"
                elif any(k in cl for k in ["airtel", "telecom"]):
                    context += " telecom data subscribers India spectrum"
                elif any(k in cl for k in ["paints", "cement", "ultratech", "asian paints", "berger", "pidilite", "havells", "siemens", "abb", "cummins", "larsen"]):
                    context += " capital goods infrastructure construction India capex"
                elif any(k in cl for k in ["adani"]):
                    context += " Adani group infrastructure ports green energy India"
            vec = np.array(embed_text(context), dtype=np.float32)
            _symbol_context_cache[sym_upper] = vec
        except Exception:
            return 0.5

    sym_vec = _symbol_context_cache[sym_upper]
    sym_norm = np.linalg.norm(sym_vec)
    if sym_norm == 0:
        return 0.5

    # Embed the event text
    event_vec = np.array(embed_text(event_text), dtype=np.float32)
    event_norm = np.linalg.norm(event_vec)
    if event_norm == 0:
        return 0.5

    sim = float(np.dot(sym_vec, event_vec) / (sym_norm * event_norm))

    # Map similarity [0, 1] → h_weight [0.3, 1.0]
    # Floor of 0.3 ensures even distantly-related macro events keep some weight
    return max(0.3, min(1.0, sim))


_EXEMPLARS_PATH = Path(__file__).parent.parent.parent.parent / "data" / "news_quality_exemplars.json"
_exemplar_cache: Optional[dict] = None


def _load_exemplars() -> dict:
    """Load and embed exemplar headlines. Cached in module memory."""
    global _exemplar_cache
    if _exemplar_cache is not None:
        return _exemplar_cache

    try:
        exemplars = json.loads(_EXEMPLARS_PATH.read_text())
    except Exception as e:
        log.debug("Could not load news quality exemplars (falling back to neutral weight=0.20): %s", e)
        _exemplar_cache = {"texts": [], "weights": [], "vectors": []}
        return _exemplar_cache

    texts = [e["text"] for e in exemplars]
    weights = [e["weight"] for e in exemplars]

    # Embed all exemplars
    vectors = embed_batch(texts)

    _exemplar_cache = {
        "texts": texts,
        "weights": np.array(weights, dtype=np.float32),
        "vectors": np.array(vectors, dtype=np.float32),
    }
    return _exemplar_cache


def score_news_quality(headline: str) -> float:
    """Score a news headline by semantic similarity to labelled exemplars.

    Returns a weight ∈ [0.0, 1.0]. Higher = more material/actionable.
    Falls back to 0.20 (neutral) if exemplars unavailable.
    """
    if not headline or not headline.strip():
        return 0.0

    exemplars = _load_exemplars()
    if len(exemplars["vectors"]) == 0:
        return 0.20  # fallback to keyword-era neutral weight

    query_vec = np.array(embed_text(headline), dtype=np.float32)
    if np.linalg.norm(query_vec) == 0:
        return 0.20

    # Cosine similarity against all exemplars
    exemplar_vecs = exemplars["vectors"]
    norms = np.linalg.norm(exemplar_vecs, axis=1)
    # Avoid division by zero
    valid = norms > 0
    if not valid.any():
        return 0.20

    sims = exemplar_vecs[valid] @ query_vec / (norms[valid] * np.linalg.norm(query_vec))

    # Weighted average of top-5 by similarity
    k = min(5, len(sims))
    top_k_idx = np.argpartition(sims, -k)[-k:]
    top_sims = sims[top_k_idx]

    # Convert similarities to positive weights (clip negatives)
    sim_weights = np.clip(top_sims, 0.0, 1.0)
    if sim_weights.sum() == 0:
        return 0.20

    top_quality_weights = exemplars["weights"][valid][top_k_idx]
    score = float(np.average(top_quality_weights, weights=sim_weights))

    return max(0.0, min(1.0, score))


# ── Semantic retrieval from ClickHouse ────────────────────────────────────────


def retrieve_articles(
    query: str,
    around_date: date,
    days: int = 7,
    k: int = 20,
    category_filter: Optional[str] = None,
    symbol: Optional[str] = None,
) -> list[dict]:
    """Retrieve top-k semantically relevant news articles within a date window.

    Queries Qdrant vector database if available, otherwise queries news_articles
    table in ClickHouse for rows with embeddings and ranks by cosine similarity.

    When ``symbol`` is provided, retrieval is two-pass: a precise symbol-scoped
    pass first (using the indexed `symbol` payload field), broadening to the
    symbol-less semantic search only when the precise pass is too thin — so
    tagged data is stock-specific while cold/untagged history still returns
    something. Omitting ``symbol`` preserves the original symbol-less behaviour.

    Returns list of dicts with keys: title, source, url, published_at,
    category, sentiment, similarity.
    """
    query_vec = embed_text(query)
    if all(v == 0.0 for v in query_vec):
        return []

    import datetime
    start_date = around_date - timedelta(days=days)
    end_date = around_date + timedelta(days=days)
    
    start_ts = float(datetime.datetime.combine(start_date, datetime.time.min).timestamp())
    end_ts = float(datetime.datetime.combine(end_date, datetime.time.max).timestamp())

    # 1. Try Qdrant retrieval first
    client = get_qdrant_client()
    if client and _qdrant_available:
        try:
            if ensure_collection(client, "news_articles", len(query_vec)):
                # Base conditions applied to every pass: date window + optional category.
                base_conditions = [
                    FieldCondition(
                        key="published_timestamp",
                        range=Range(gte=start_ts, lte=end_ts)
                    )
                ]
                if category_filter:
                    base_conditions.append(
                        FieldCondition(
                            key="category",
                            match=MatchValue(value=category_filter)
                        )
                    )

                def _search(extra_conditions: list) -> list[dict]:
                    res = client.query_points(
                        collection_name="news_articles",
                        query=query_vec,
                        query_filter=Filter(must=base_conditions + extra_conditions),
                        limit=k,
                        with_payload=True,
                    )
                    out = []
                    for hit in res.points:
                        p = hit.payload or {}
                        out.append({
                            "title": p.get("title", ""),
                            "source": p.get("source", ""),
                            "url": p.get("url", ""),
                            "published_at": p.get("published_at", ""),
                            "category": p.get("category", ""),
                            "sentiment": p.get("sentiment", "NEUTRAL"),
                            "similarity": float(hit.score),
                        })
                    return out

                # Pass 1 (precise): symbol-scoped, using the indexed `symbol` field.
                results_map: dict = {}
                if symbol:
                    for r in _search(
                        [FieldCondition(key="symbol", match=MatchValue(value=symbol.upper()))]
                    ):
                        results_map[r["url"] or r["title"]] = r
                symbol_hits = len(results_map)

                # Pass 2 (recall): broaden to the symbol-less semantic search when the
                # precise pass is thin — cold/untagged history, or symbol=None (in which
                # case this is the only pass, identical to the original behaviour).
                _SYMBOL_MIN = max(3, k // 4)
                broadened = symbol_hits < _SYMBOL_MIN
                if broadened:
                    for r in _search([]):
                        key = r["url"] or r["title"]
                        if key not in results_map:
                            results_map[key] = r

                qdrant_results = sorted(
                    results_map.values(), key=lambda x: -x["similarity"]
                )[:k]
                log.info(
                    "Retrieved %d articles from Qdrant (symbol=%s, symbol_hits=%d, broadened=%s)",
                    len(qdrant_results), symbol or "-", symbol_hits, broadened,
                )
                return qdrant_results
        except Exception as q_err:
            log.warning("Qdrant search failed: %s — falling back to ClickHouse in-memory RAG", q_err)

    # 2. ClickHouse Fallback (In-memory Python Cosine Similarity)
    log.debug("Using ClickHouse fallback for retrieve_articles")
    from src.db.pool import query_df

    # Build SQL — fetch articles with embeddings in the date window.
    # Use BOTH fetched_at and published_at for matching: an article about a duty
    # hike published May 18 but fetched June 19 should still match a May lookback.
    sql = """
        SELECT title, source, url, published_at, category, sentiment,
               embedding
        FROM market_data.news_articles FINAL
        WHERE length(embedding) > 0
          AND (
            toDate(fetched_at) BETWEEN {start:Date} AND {end:Date}
            OR toDate(parseDateTimeBestEffortOrNull(published_at)) BETWEEN {start:Date} AND {end:Date}
          )
    """
    params: dict = {"start": str(start_date), "end": str(end_date)}

    if category_filter:
        sql += " AND category = {cat:String}"
        params["cat"] = category_filter

    def _run(with_symbol: bool):
        # etfs_impacted is the CH column holding the ticker for stock/live news.
        q = sql + (" AND etfs_impacted = {sym:String}" if with_symbol else "")
        q += " LIMIT 500"  # cap for brute-force cosine
        p = dict(params)
        if with_symbol:
            p["sym"] = symbol.upper()
        return query_df(q, parameters=p)

    try:
        # Symbol-scoped first (mirrors the Qdrant Pass 1); broaden if it's empty
        # so untagged history still returns something (mirrors Pass 2).
        df = _run(with_symbol=bool(symbol))
        if bool(symbol) and (df is None or df.empty):
            df = _run(with_symbol=False)
    except Exception as e:
        log.warning("retrieve_articles ClickHouse query failed: %s", e)
        return []

    if df.empty:
        return []

    # Compute cosine similarities
    query_arr = np.array(query_vec, dtype=np.float32)
    q_norm = np.linalg.norm(query_arr)
    if q_norm == 0:
        return []

    results = []
    for _, row in df.iterrows():
        emb = row["embedding"]
        if not emb or len(emb) == 0:
            continue
        emb_arr = np.array(emb, dtype=np.float32)
        e_norm = np.linalg.norm(emb_arr)
        if e_norm == 0:
            continue
        sim = float(np.dot(query_arr, emb_arr) / (q_norm * e_norm))
        results.append({
            "title": row["title"],
            "source": row["source"],
            "url": row["url"],
            "published_at": row["published_at"],
            "category": row["category"],
            "sentiment": row["sentiment"],
            "similarity": sim,
        })

    # Sort by similarity descending, take top-k
    results.sort(key=lambda x: -x["similarity"])
    return results[:k]
