"""
src/agents/intent_router.py
───────────────────────────
LLM-based intent router that replaces regex-based routing.

Uses a cheap, fast model (gpt-4o-mini / gemini-2.0-flash) to classify user
questions into one of the known sub-agent intents.  Results are cached by
normalised question hash so repeat/similar questions don't re-invoke the LLM.

Falls back to the legacy regex router when:
  - No cloud LLM API key is configured
  - The LLM call fails or times out
  - The model returns an unparseable response

Usage:
    from src.agents.intent_router import route_intent_llm

    intent = route_intent_llm("what is the macro outlook for gold?")
    # → "macro"
"""
from __future__ import annotations

import hashlib
import json
import logging
from functools import lru_cache
from typing import Any

logger = logging.getLogger(__name__)

# ── Valid intents (must match get_subagent registry in sub_agents.py) ──────────

VALID_INTENTS = frozenset({
    "main",
    "deepdive",
    "research",
    "india_equity",
    "signal",
    "macro",
    "mf",
    "intl_etf",
    "news",
    "code",
    "database",
})

# ── Router system prompt ──────────────────────────────────────────────────────

try:
    from pathlib import Path
    _ROUTER_SYSTEM_PROMPT = Path("src/prompts/router_system_prompt.txt").read_text(encoding="utf-8")
except Exception:
    logger.warning("Could not load router_system_prompt.txt — using static fallback")
    _ROUTER_SYSTEM_PROMPT = """\
You are an intent classifier for a financial intelligence platform focused on \
Indian equity and commodity markets. Classify the user's question into exactly \
one of the following intents:

| Intent        | Route when the user asks about                                    |
|---------------|-------------------------------------------------------------------|
| deepdive      | US stock SEC filings, 10-K/10-Q, XBRL, annual reports, EDGAR     |
| research      | Autonomous/deep/comprehensive multi-domain research on a topic    |
| india_equity  | Indian stock fundamentals, earnings, price, financials, MF hold.  |
| signal        | ETF signals, GOLDBEES ML pipeline, Kelly weight, GARCH, iNAV     |
| macro         | COMEX, FII/DII flows, macro themes, geopolitics, crude, gold/USD  |
| mf            | Mutual-fund holdings, NAV returns, fund consensus, fund managers  |
| intl_etf      | International ETFs (MAFANG, HNGSNGBEES, Hang Seng, Nasdaq ETF)   |
| news          | Latest news, headlines, news sentiment for a stock/ETF/market     |
| code          | Write/run/debug Python code, create scripts, ad-hoc analysis      |
| database      | ClickHouse queries, SQL, table schema, watermarks, row counts     |
| main          | Portfolio analysis, general questions, or anything not above      |

Special routing rules:
- "import", "refresh", "sync", "backfill" data → always "main" (import runs there)
- Questions about a SPECIFIC mutual fund (DSP / Nippon / Bajaj / Quant / ICICI Multi Asset, scheme codes, fund managers, MF holdings, fund cross-ownership, NAV returns, "which funds hold X", "smart-money consensus across funds") → "mf"
- Questions mentioning specific Indian companies (RELIANCE, TCS, HDFC) → "india_equity"
- "plot" or "chart" + macro keyword → "macro"; + ETF keyword → "signal"
- "plot" or "chart" + Indian stock name (RELIANCE, TCS, ADVENZYMES, etc.) → "india_equity"
- When uncertain, prefer "main" over guessing

Respond with ONLY a JSON object (no markdown, no explanation):
{"intent": "<intent>", "confidence": <0.0-1.0>}
"""


# ── Question normalisation & cache ────────────────────────────────────────────

def _normalise(question: str) -> str:
    """Lowercase, strip, collapse whitespace for cache key."""
    return " ".join(question.lower().split())


def _cache_key(question: str) -> str:
    return hashlib.sha256(_normalise(question).encode()).hexdigest()[:16]


# Simple in-memory LRU cache — survives the session, no persistence needed.
_intent_cache: dict[str, str] = {}
_MAX_CACHE = 256


# ── LLM router ───────────────────────────────────────────────────────────────

@lru_cache(maxsize=1)
def _get_router_llm() -> Any | None:
    """
    Build a cheap, fast LLM for routing. Returns None if no API key available.
    Uses gpt-4o-mini (OpenAI) or the configured cloud model.
    """
    from config.settings import settings

    # Treat known placeholder values as "no key" — these are used for local
    # OpenAI-compatible endpoints (LM Studio, Ollama) which can't serve real
    # OpenAI cloud calls.
    _PLACEHOLDERS = {"", "ollama", "local", "none", "null", "lm-studio", "lmstudio"}

    def _real_key(k: str | None) -> bool:
        return bool(k) and k.strip().lower() not in _PLACEHOLDERS

    # 1. Prefer cloud LLM if local is disabled
    if getattr(settings, "llm_local_disabled", False) and settings.llm_cloud_provider:
        cloud_provider = settings.llm_cloud_provider.strip().lower()
        if cloud_provider == "anthropic" and _real_key(settings.anthropic_api_key):
            try:
                from langchain_anthropic import ChatAnthropic
                return ChatAnthropic(
                    model=settings.llm_cloud_model or "claude-3-5-haiku-20241022",
                    api_key=settings.anthropic_api_key,
                    temperature=0,
                    max_tokens=50,
                )
            except Exception as exc:
                logger.debug("Router LLM (Anthropic Cloud) build failed: %s", exc)
        elif cloud_provider == "openai" and _real_key(settings.openai_api_key):
            try:
                from langchain_openai import ChatOpenAI
                return ChatOpenAI(
                    model="gpt-4o-mini",
                    api_key=settings.openai_api_key,
                    temperature=0,
                    max_tokens=50,
                    request_timeout=5,
                )
            except Exception as exc:
                logger.debug("Router LLM (OpenAI Cloud) build failed: %s", exc)
        elif cloud_provider == "openrouter" and _real_key(settings.openrouter_api_key):
            try:
                from langchain_openai import ChatOpenAI
                return ChatOpenAI(
                    model=settings.llm_cloud_model or "google/gemini-2.5-flash",
                    api_key=settings.openrouter_api_key,
                    base_url="https://openrouter.ai/api/v1",
                    temperature=0,
                    max_tokens=50,
                    request_timeout=10,
                    timeout=10,
                )
            except Exception as exc:
                logger.debug("Router LLM (OpenRouter Cloud) build failed: %s", exc)

    # 2. NVIDIA NIM — use the same endpoint/key as the main LLM
    nvidia_key = getattr(settings, "nvidia_api_key", "")
    if "nvidia" in getattr(settings, "llm_base_url", "").lower() and _real_key(nvidia_key):
        try:
            from langchain_openai import ChatOpenAI
            return ChatOpenAI(
                model=settings.llm_model,
                base_url=settings.llm_base_url,
                api_key=nvidia_key,
                temperature=0.6,
                max_tokens=50,
                timeout=30,
            )
        except Exception as exc:
            logger.debug("Router LLM (NVIDIA NIM) build failed: %s", exc)

    # 3. Local Ollama / LM Studio — use when base_url points to localhost
    if settings.llm_base_url and any(h in settings.llm_base_url for h in ("localhost", "127.0.0.1")):
        try:
            from langchain_openai import ChatOpenAI
            return ChatOpenAI(
                model=settings.llm_model,
                base_url=settings.llm_base_url,
                api_key=settings.openai_api_key or "ollama",
                temperature=0,
                max_tokens=50,
                request_timeout=120,
                timeout=120,
            )
        except Exception as exc:
            logger.debug("Router LLM (Local/Ollama) build failed: %s", exc)

    # 4. OpenRouter cloud — use a cheap routing model, NOT settings.llm_model
    #    (which may be a local Ollama name like 'mosaic-gemma4')
    if settings.llm_provider.lower() == "openrouter" and _real_key(settings.openrouter_api_key):
        try:
            from langchain_openai import ChatOpenAI
            return ChatOpenAI(
                model="google/gemma-3-12b-it",
                api_key=settings.openrouter_api_key,
                base_url="https://openrouter.ai/api/v1",
                temperature=0,
                max_tokens=50,
                request_timeout=10,
                timeout=10,
            )
        except Exception as exc:
            logger.debug("Router LLM (OpenRouter) build failed: %s", exc)

    if _real_key(settings.openai_api_key):
        try:
            from langchain_openai import ChatOpenAI
            if settings.llm_base_url:
                # Custom/local base URL (e.g. Ollama/LM Studio) requires a longer timeout
                # for the model to warm up or load into memory.
                return ChatOpenAI(
                    model=settings.llm_model,
                    base_url=settings.llm_base_url,
                    api_key=settings.openai_api_key,
                    temperature=0,
                    max_tokens=50,
                    request_timeout=120,
                    timeout=120,
                )
            else:
                return ChatOpenAI(
                    model="gpt-4o-mini",
                    api_key=settings.openai_api_key,
                    temperature=0,
                    max_tokens=50,
                    request_timeout=5,
                )
        except Exception as exc:
            logger.debug("Router LLM (OpenAI) build failed: %s", exc)

    if _real_key(settings.anthropic_api_key):
        try:
            from langchain_anthropic import ChatAnthropic
            return ChatAnthropic(
                model="claude-haiku-4-20250414",
                api_key=settings.anthropic_api_key,
                temperature=0,
                max_tokens=50,
            )
        except Exception as exc:
            logger.debug("Router LLM (Anthropic) build failed: %s", exc)

    if _real_key(getattr(settings, "google_api_key", None)):
        try:
            from langchain_google_genai import ChatGoogleGenerativeAI
            return ChatGoogleGenerativeAI(
                model="gemini-2.0-flash",
                google_api_key=settings.google_api_key,
                temperature=0,
                max_output_tokens=50,
            )
        except Exception as exc:
            logger.debug("Router LLM (Google) build failed: %s", exc)

    return None


def route_intent_llm(question: str) -> str:
    """
    Classify a question into a sub-agent intent using an LLM.

    Returns one of the VALID_INTENTS strings.
    Falls back to regex-based route_intent() on any failure.
    """
    key = _cache_key(question)

    # Check cache first
    if key in _intent_cache:
        logger.debug("Intent cache hit: %s → %s", key, _intent_cache[key])
        return _intent_cache[key]

    llm = _get_router_llm()
    if llm is None:
        logger.debug("No router LLM available — falling back to regex router")
        return _regex_fallback(question)

    try:
        from langchain_core.messages import SystemMessage, HumanMessage
        response = llm.invoke([
            SystemMessage(content=_ROUTER_SYSTEM_PROMPT),
            HumanMessage(content=question),
        ])
        text = str(response.content).strip()

        # Parse JSON response
        # Handle potential markdown wrapping: ```json ... ```
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
            text = text.strip()

        parsed = json.loads(text)
        intent = parsed.get("intent", "main").lower().strip()
        confidence = float(parsed.get("confidence", 0.0))

        if intent not in VALID_INTENTS:
            logger.warning("LLM router returned invalid intent %r — falling back", intent)
            return _regex_fallback(question)

        if confidence < 0.4:
            logger.info("LLM router low confidence (%.2f) for %r — falling back", confidence, intent)
            return _regex_fallback(question)

        # Cache the result
        if len(_intent_cache) >= _MAX_CACHE:
            # Evict oldest ~25% of cache
            keys_to_remove = list(_intent_cache.keys())[: _MAX_CACHE // 4]
            for k in keys_to_remove:
                _intent_cache.pop(k, None)
        _intent_cache[key] = intent

        logger.info("LLM router: %r → %s (confidence=%.2f)", question[:60], intent, confidence)
        return intent

    except json.JSONDecodeError as exc:
        logger.warning("LLM router returned non-JSON: %s — falling back", exc)
        return _regex_fallback(question)
    except Exception as exc:
        logger.warning("LLM router call failed: %s — falling back", exc)
        return _regex_fallback(question)


_golden_vectors = None
_tfidf_matcher = None
_STOPWORDS = frozenset({
    "what", "how", "are", "you", "the", "is", "for", "to", "of", "in", "on", "at", 
    "with", "this", "that", "these", "those", "have", "do", "does", "did", "can", 
    "could", "will", "would", "should", "me", "my", "your", "them", "their", 
    "they", "we", "our", "us", "about", "there", "here", "all", "any", "both", 
    "each", "few", "more", "most", "other", "some", "such", "than", "too", "very", 
    "who", "whom", "which", "where", "when", "why", "a", "an", "and", "or", "but"
})


class SimpleTFIDF:
    def __init__(self, documents: list[tuple[str, str]]):
        import collections, math, re
        self.documents = documents  # list of (question, intent)
        self.tokenized_docs = [
            [w for w in re.findall(r"\b\w+\b", doc[0].lower()) if len(w) > 1 and w not in _STOPWORDS]
            for doc in documents
        ]
        
        # Compute IDF
        self.idf = collections.defaultdict(float)
        num_docs = len(documents)
        doc_counts = collections.defaultdict(int)
        for doc in self.tokenized_docs:
            for term in set(doc):
                doc_counts[term] += 1
        for term, count in doc_counts.items():
            self.idf[term] = math.log((1 + num_docs) / (1 + count)) + 1
            
        # Compute doc vectors
        self.doc_vectors = []
        for doc in self.tokenized_docs:
            vector = collections.defaultdict(float)
            for term in doc:
                vector[term] += 1
            # Apply IDF
            for term in vector:
                vector[term] *= self.idf[term]
            # Normalise
            norm = math.sqrt(sum(v**2 for v in vector.values()))
            if norm > 0:
                for term in vector:
                    vector[term] /= norm
            self.doc_vectors.append(vector)

    def similarity(self, query: str) -> tuple[str, float]:
        import collections, math, re
        query_tokens = [w for w in re.findall(r"\b\w+\b", query.lower()) if len(w) > 1 and w not in _STOPWORDS]
        if not query_tokens:
            return "main", 0.0
            
        query_vector = collections.defaultdict(float)
        for term in query_tokens:
            query_vector[term] += 1
        for term in query_vector:
            query_vector[term] *= self.idf[term]
        norm = math.sqrt(sum(v**2 for v in query_vector.values()))
        if norm > 0:
            for term in query_vector:
                query_vector[term] /= norm
                
        best_score = 0.0
        best_intent = "main"
        
        for idx, doc_vector in enumerate(self.doc_vectors):
            # Cosine similarity
            score = sum(query_vector[term] * doc_vector[term] for term in query_vector if term in doc_vector)
            if score > best_score:
                best_score = score
                best_intent = self.documents[idx][1]
                
        return best_intent, best_score



def _embedding_similarity(question: str) -> tuple[str, float]:
    """Compare question embedding to GOLDEN_PAIRS embeddings using Ollama."""
    global _golden_vectors
    try:
        from src.ml.correlation.news_rag import embed_text
        from src.agents.golden_pairs import GOLDEN_PAIRS
        import numpy as np

        q_vec = embed_text(question)
        if not any(q_vec):  # All zeros = failed
            return "main", 0.0

        best_score = 0.0
        best_intent = "main"

        # Lazy load/cache golden vectors
        if _golden_vectors is None:
            _golden_vectors = []
            for q_text, intent in GOLDEN_PAIRS:
                vec = embed_text(q_text)
                _golden_vectors.append((vec, intent))

        # Compute cosine similarities
        q_norm = np.linalg.norm(q_vec)
        if q_norm == 0:
            return "main", 0.0

        for vec, intent in _golden_vectors:
            v_norm = np.linalg.norm(vec)
            if v_norm == 0:
                continue
            sim = np.dot(q_vec, vec) / (q_norm * v_norm)
            if sim > best_score:
                best_score = sim
                best_intent = intent

        return best_intent, float(best_score)
    except Exception as exc:
        logger.debug("_embedding_similarity failed: %s", exc)
        return "main", 0.0


def route_intent_rag(question: str) -> str | None:
    """
    Use local RAG/semantic search over GOLDEN_PAIRS database to discover intent.
    Checks Ollama embeddings first, falls back to TF-IDF similarity.
    Returns the mapped intent if confidence is high, else None.
    """
    global _tfidf_matcher
    try:
        from src.agents.golden_pairs import GOLDEN_PAIRS
    except ImportError:
        logger.warning("Could not load GOLDEN_PAIRS from src.agents.golden_pairs — RAG router disabled")
        return None

    # 1. Try Local Ollama Embedding similarity
    intent, score = _embedding_similarity(question)
    if score >= 0.82:
        logger.info("RAG Router (Embedding): %r → %s (score=%.3f)", question[:60], intent, score)
        return intent

    # 2. Try Pure-Python TF-IDF similarity
    if _tfidf_matcher is None:
        _tfidf_matcher = SimpleTFIDF(GOLDEN_PAIRS)
    
    intent, score = _tfidf_matcher.similarity(question)
    if score >= 0.50:
        logger.info("RAG Router (TF-IDF): %r → %s (score=%.3f)", question[:60], intent, score)
        return intent
        
    return None


def _regex_fallback(question: str) -> str:
    """RAG-based semantic intent search, then legacy regex fallback."""
    try:
        rag_intent = route_intent_rag(question)
        if rag_intent:
            return rag_intent
    except Exception as exc:
        logger.debug("_regex_fallback: RAG lookup failed (%s)", exc)

    from src.agents.sub_agents import _regex_route_intent
    return _regex_route_intent(question)


def clear_intent_cache() -> None:
    """Clear the intent cache (useful for testing)."""
    _intent_cache.clear()
    _get_router_llm.cache_clear()
