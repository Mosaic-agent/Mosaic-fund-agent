"""
src/agents/sub_agents/infra.py
──────────────────────────────
Cross-cutting infrastructure for the sub-agent framework:

* Per-turn tool-call deduplication (ContextVar-scoped cache).
* Context-window trimmer for local-model LangGraph runs.
* Extended-thinking block extractor for Anthropic Claude responses.

These helpers are independent of any specific sub-agent and have no
LangChain runtime dependencies at import time — heavy imports happen
lazily inside the helper bodies so that this module loads cheaply.
"""
from __future__ import annotations

import contextvars
import json as _json
import logging
from typing import Any

logger = logging.getLogger(__name__)


# ── Per-turn tool-call deduplication ──────────────────────────────────────────
#
# LLMs occasionally re-issue an identical tool call within the same ReAct loop
# (same tool name + same args).  Re-running an expensive tool (anomaly correlation,
# ClickHouse query, news scrape) wastes seconds and burns context tokens with
# duplicate output.  We attach a per-run cache via a ContextVar so the same
# (tool_name, args_json) returns the cached result on the second hit instead of
# re-executing.
#
# - Cache is scoped to one `_SubAgent.run()` invocation via the context manager.
# - Tool instances are wrapped exactly once (idempotent via __dedup_wrapped__).
# - Outside an active context the wrapper is a no-op, so direct tool calls in
#   tests / scripts behave normally.

_dedup_cache: contextvars.ContextVar[dict | None] = contextvars.ContextVar(
    "_subagent_tool_dedup_cache", default=None
)


def _wrap_tool_for_dedup(tool: Any) -> Any:
    """Wrap a tool's underlying function so duplicate calls return cached results.

    Idempotent — re-wrapping the same tool instance is a no-op.

    Implementation note: StructuredTool is a frozen Pydantic model, so we cannot
    monkey-patch `invoke`. Instead we wrap `.func` (and `.coroutine` if present),
    which is what the tool's run path ultimately calls.
    """
    if getattr(tool, "__dedup_wrapped__", False):
        return tool

    tool_name = getattr(tool, "name", repr(tool))
    original_func = getattr(tool, "func", None)
    original_coro = getattr(tool, "coroutine", None)

    def _make_key(args: tuple, kwargs: dict) -> str | None:
        try:
            # Build a stable, hashable representation of the call args.
            payload = {"args": list(args), "kwargs": kwargs}
            return f"{tool_name}::{_json.dumps(payload, sort_keys=True, default=str)}"
        except Exception:
            return None

    if original_func is not None:
        def cached_func(*args: Any, **kwargs: Any) -> Any:
            cache = _dedup_cache.get()
            if cache is None:
                return original_func(*args, **kwargs)
            key = _make_key(args, kwargs)
            if key is None:
                return original_func(*args, **kwargs)
            if key in cache:
                logger.info(
                    "tool dedup: %s called twice with same args this turn — returning cached result",
                    tool_name,
                )
                return cache[key]
            result = original_func(*args, **kwargs)
            cache[key] = result
            return result

        # StructuredTool stores `func` as a Pydantic field — assignment is allowed,
        # but goes through Pydantic validation.  Use object.__setattr__ to bypass.
        object.__setattr__(tool, "func", cached_func)

    if original_coro is not None:
        async def cached_coro(*args: Any, **kwargs: Any) -> Any:
            cache = _dedup_cache.get()
            if cache is None:
                return await original_coro(*args, **kwargs)
            key = _make_key(args, kwargs)
            if key is None:
                return await original_coro(*args, **kwargs)
            if key in cache:
                logger.info(
                    "tool dedup: %s called twice with same args this turn — returning cached result",
                    tool_name,
                )
                return cache[key]
            result = await original_coro(*args, **kwargs)
            cache[key] = result
            return result

        object.__setattr__(tool, "coroutine", cached_coro)

    object.__setattr__(tool, "__dedup_wrapped__", True)
    return tool


# ── Context-window trimmer ─────────────────────────────────────────────────────

def _make_context_trimmer(context_window: int):
    """
    Returns a ``pre_model_hook`` for ``create_react_agent`` that keeps each
    LLM call within *context_window* tokens (approximated as chars / 4).

    Strategy (applied before every model call):
      1. Hard-truncate each ToolMessage to ≤ 20 % of context (biggest single
         source of overflow — SQL results, news dumps, chart ASCII).
      2. If the total message chars still exceed 60 % of context, evict the
         oldest AI+Tool round-trip (the pair of AIMessage-with-tool_calls +
         its ToolMessages) repeatedly until it fits.
      3. Return the trimmed list as ``llm_input_messages`` so the actual
         LangGraph state (used for the fallback synthesis path) is untouched.

    Only attached when running a local model; cloud models skip this.
    """
    max_input_chars = int(context_window * 0.50 * 4)   # 50 % of ctx for input
    max_tool_chars  = int(context_window * 0.10 * 4)   # 10 % per tool output

    def _hook(state: dict) -> dict:
        from langchain_core.messages import ToolMessage, AIMessage

        msgs = list(state.get("llm_input_messages") or state.get("messages") or [])

        # Step 1 — truncate oversized ToolMessage content
        result = []
        for m in msgs:
            if isinstance(m, ToolMessage):
                content = str(m.content)
                if len(content) > max_tool_chars:
                    trimmed_n = len(content) - max_tool_chars
                    content = (
                        content[:max_tool_chars]
                        + f"\n…[{trimmed_n} chars trimmed — use narrower queries to fit local context]"
                    )
                    m = m.model_copy(update={"content": content})
            result.append(m)

        # Step 2 — evict oldest AI+Tool round-trips until total fits
        def _total(ms):
            return sum(len(str(m.content)) for m in ms)

        while _total(result) > max_input_chars and len(result) > 2:
            evicted = False
            for i in range(1, len(result)):
                m = result[i]
                if isinstance(m, AIMessage) and getattr(m, "tool_calls", None):
                    j = i + 1
                    while j < len(result) and isinstance(result[j], ToolMessage):
                        j += 1
                    result = result[:i] + result[j:]
                    evicted = True
                    break
            if not evicted:
                break

        return {"llm_input_messages": result}

    return _hook


# ── Extended-thinking block printer ────────────────────────────────────────────

def _print_thinking_blocks(content: Any, label: str = "🧠 Analyst Reasoning") -> None:
    """
    Extract Anthropic extended-thinking blocks from a message content and
    print them to the console in a distinctive cyan panel.

    Called after the synthesis LLM responds so the user can see the
    cross-check reasoning before reading the final report.

    content: AIMessage.content (list of dicts, or plain str)
    """
    if not isinstance(content, list):
        return
    thinking_parts = [
        blk.get("thinking", "")
        for blk in content
        if isinstance(blk, dict) and blk.get("type") == "thinking" and blk.get("thinking")
    ]
    if not thinking_parts:
        return
    try:
        from rich.console import Console
        from rich.panel import Panel
        from rich.markdown import Markdown
        _c = Console()
        thinking_text = "\n\n---\n\n".join(thinking_parts)
        _c.print(Panel(
            Markdown(thinking_text),
            title=f"[bold cyan]{label}[/bold cyan]",
            border_style="cyan",
            expand=False,
        ))
    except Exception:
        # Non-critical — log as debug if Rich unavailable
        logger.debug("thinking: %s", "\n".join(thinking_parts[:200]))
