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
from datetime import date, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Optional

import numpy as np
import requests

log = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────

def _get_ollama_base() -> str:
    """Resolve Ollama base host dynamically."""
    env_host = os.environ.get("OLLAMA_HOST")
    if env_host:
        return env_host.rstrip("/")

    try:
        from config.settings import settings
        if settings.llm_base_url:
            base = settings.llm_base_url.strip().rstrip("/")
            if base.endswith("/v1"):
                base = base[:-3]
            return base
    except Exception:
        pass

    return "http://localhost:11434"

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


def embed_batch(texts: list[str]) -> list[list[float]]:
    """Embed multiple texts in a single Ollama call (batched)."""
    if not texts:
        return []

    cleaned = [t.strip()[:512] if t else "" for t in texts]
    url = f"{_get_ollama_base()}/api/embed"
    payload = {"model": _EMBED_MODEL, "input": cleaned}
    try:
        resp = requests.post(url, json=payload, timeout=120)
        resp.raise_for_status()
        data = resp.json()
        return data["embeddings"]
    except Exception as e:
        log.warning("Ollama batch embed failed: %s — falling back to individual", e)
        return [embed_text(t) for t in texts]


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
) -> list[dict]:
    """Retrieve top-k semantically relevant news articles within a date window.

    Queries news_articles table for rows with non-empty embeddings within
    [around_date - days, around_date + days], ranks by cosine similarity.

    Returns list of dicts with keys: title, source, url, published_at,
    category, sentiment, similarity.
    """
    from src.db.pool import query_df

    query_vec = embed_text(query)
    if all(v == 0.0 for v in query_vec):
        return []

    start_date = around_date - timedelta(days=days)
    end_date = around_date + timedelta(days=days)

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

    sql += " LIMIT 500"  # cap for brute-force cosine

    try:
        df = query_df(sql, parameters=params)
    except Exception as e:
        log.warning("retrieve_articles query failed: %s", e)
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
