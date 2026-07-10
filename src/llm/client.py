"""
src/llm/client.py
──────────────────
Singleton LLM factory for the Mosaic platform.

Provides ``get_llm(context)`` which returns a LangChain LLM instance wired
to the provider/endpoint configured in ``config/settings.py``.

Provider resolution order
─────────────────────────
1. ``LLM_BASE_URL`` set → local OpenAI-compatible server (Ollama, LM Studio, …)
2. ``LLM_PROVIDER=openrouter`` → OpenRouter cloud
3. ``LLM_PROVIDER=anthropic``  → Anthropic cloud
4. ``LLM_PROVIDER=google``     → Google Generative AI
5. Default                     → OpenAI cloud

Context modes
─────────────
* ``"default"``   — full ``LLM_TOKEN_BUDGET`` + temperature 0.2  (general use)
* ``"resolver"``  — minimal ``max_tokens=20`` + temperature 0  (symbol lookup)

Both modes follow the same provider chain; the *context* only affects
``max_tokens`` and ``temperature``.

Previously duplicated as:
  • ``_get_llm()``          in ``src/tools/summarization.py``
  • ``_get_resolver_llm()`` in ``src/tools/company_resolver.py``

[SENSITIVE] All API keys are loaded via ``config/settings.py`` from ``.env``.
            Never hard-code keys here.
"""

from __future__ import annotations

import logging
from typing import Any, Literal

logger = logging.getLogger(__name__)

# Module-level singleton cache: context → LLM instance.
# Populated lazily on first call per context; never evicted.
_INSTANCES: dict[str, Any] = {}

Context = Literal["default", "resolver"]


def get_llm(context: Context = "default") -> Any:
    """
    Return a cached LangChain LLM instance for *context*.

    Args:
        context: ``"default"`` for general-purpose chat (temperature 0.2,
                 full token budget) or ``"resolver"`` for single-token symbol
                 lookups (temperature 0, ``max_tokens=20``).

    Returns:
        A LangChain ``BaseChatModel`` instance, or ``None`` if no provider
        could be initialised (errors are logged as warnings).

    [SENSITIVE] API keys loaded from config/settings.py → .env
    """
    if context in _INSTANCES:
        return _INSTANCES[context]

    result = _build_llm(context)
    _INSTANCES[context] = result
    return result


def _build_llm(context: Context) -> Any:
    """Build a fresh LLM instance for *context* (no caching)."""
    try:
        from config.settings import settings

        is_resolver = context == "resolver"
        temperature = 0
        max_tokens = 20 if is_resolver else settings.llm_token_budget

        # Determine active provider, respecting llm_local_disabled for resolver
        if is_resolver:
            provider = (
                settings.llm_cloud_provider
                if settings.llm_local_disabled
                else settings.llm_provider
            ).strip().lower()
            use_local = settings.llm_base_url and not settings.llm_local_disabled and provider != "openrouter"
            model = settings.llm_cloud_model if settings.llm_local_disabled else settings.llm_model
        else:
            provider = settings.llm_provider.strip().lower()
            use_local = bool(settings.llm_base_url)
            model = settings.llm_model

        kwargs: dict[str, Any] = {"temperature": temperature, "max_tokens": max_tokens}

        # ── Local / custom OpenAI-compatible endpoint ─────────────────────────
        if use_local:
            from langchain_openai import ChatOpenAI
            return ChatOpenAI(
                model=model,
                base_url=settings.llm_base_url,
                api_key=settings.openai_api_key or "local",
                **kwargs,
            )

        # ── OpenRouter cloud ──────────────────────────────────────────────────
        if provider == "openrouter":
            from langchain_openai import ChatOpenAI
            return ChatOpenAI(
                model=model,
                base_url="https://openrouter.ai/api/v1",
                api_key=settings.openrouter_api_key,
                **kwargs,
            )

        # ── Anthropic cloud ───────────────────────────────────────────────────
        if provider == "anthropic":
            from langchain_anthropic import ChatAnthropic
            return ChatAnthropic(
                model=model,
                api_key=settings.anthropic_api_key,
                extra_headers={"anthropic-beta": "prompt-caching-2024-07-31"},
                **kwargs,
            )

        # ── Google Generative AI ──────────────────────────────────────────────
        if provider == "google":
            from langchain_google_genai import ChatGoogleGenerativeAI
            # Google uses max_output_tokens, not max_tokens
            google_kwargs = {"temperature": temperature, "max_output_tokens": max_tokens}
            return ChatGoogleGenerativeAI(
                model=model,
                google_api_key=settings.google_api_key,
                **google_kwargs,
            )

        # ── OpenAI cloud (default) ────────────────────────────────────────────
        from langchain_openai import ChatOpenAI
        return ChatOpenAI(
            model=model,
            api_key=settings.openai_api_key,
            **kwargs,
        )

    except Exception as exc:
        logger.warning("get_llm(context=%r): could not build LLM — %s", context, exc)
        return None
