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
    "intl_etf",
    "news",
    "code",
    "database",
})

# ── Router system prompt ──────────────────────────────────────────────────────

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
| intl_etf      | International ETFs (MAFANG, HNGSNGBEES, Hang Seng, Nasdaq ETF)   |
| news          | Latest news, headlines, news sentiment for a stock/ETF/market     |
| code          | Write/run/debug Python code, create scripts, ad-hoc analysis      |
| database      | ClickHouse queries, SQL, table schema, watermarks, row counts     |
| main          | Portfolio analysis, general questions, or anything not above      |

Special routing rules:
- "import", "refresh", "sync", "backfill" data → always "main" (import runs there)
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
                    model="claude-3-5-haiku-20241022",
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

    # 2. Otherwise fall back to local/default API key providers
    # Prefer a small fast model for routing — gpt-4o-mini costs ~$0.0001 per call
    if _real_key(settings.openai_api_key):
        try:
            from langchain_openai import ChatOpenAI
            if settings.llm_base_url:
                return ChatOpenAI(
                    model=settings.llm_model,
                    base_url=settings.llm_base_url,
                    api_key=settings.openai_api_key,
                    temperature=0,
                    max_tokens=50,
                    request_timeout=5,
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


def _regex_fallback(question: str) -> str:
    """Import and call the legacy regex router (non-recursive)."""
    from src.agents.sub_agents import _regex_route_intent
    return _regex_route_intent(question)


def clear_intent_cache() -> None:
    """Clear the intent cache (useful for testing)."""
    _intent_cache.clear()
    _get_router_llm.cache_clear()
