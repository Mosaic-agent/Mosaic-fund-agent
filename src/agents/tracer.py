"""
src/agents/tracer.py
────────────────────
Agent tracing: records every tool call, LLM invocation, and routing decision
to ``market_data.agent_traces`` in ClickHouse.

Provides two integration points:
  1. ``TracingCallbackHandler`` — LangChain callback that hooks into on_tool_start /
     on_tool_end / on_tool_error automatically for any agent using it.
  2. ``log_trace()`` — explicit helper for non-LangChain paths (direct tool calls,
     regex fallback routing, etc.).

Usage:
    # Automatic (add to any LangGraph agent):
    from src.agents.tracer import TracingCallbackHandler
    config = {"callbacks": [TracingCallbackHandler(agent="signal")]}
    agent.invoke({"messages": [...]}, config=config)

    # Explicit:
    from src.agents.tracer import log_trace
    log_trace(agent="macro", tool_name="scan_macro_events", latency_ms=342)
"""
from __future__ import annotations

import logging
import time
import uuid
from typing import Any

from langchain_core.callbacks import BaseCallbackHandler

logger = logging.getLogger(__name__)


def _generate_run_id() -> str:
    return uuid.uuid4().hex[:16]


# ── Explicit trace logging ───────────────────────────────────────────────────

def log_trace(
    *,
    agent: str,
    run_id: str = "",
    step_idx: int = 0,
    tool_name: str = "",
    args_json: str = "{}",
    result_json: str = "",
    latency_ms: int = 0,
    tokens_in: int = 0,
    tokens_out: int = 0,
    status: str = "ok",
    error_class: str = "",
    error_msg: str = "",
    parent_run_id: str = "",
) -> None:
    """
    Write a single trace row to market_data.agent_traces.

    Best-effort: errors are logged but never raised.
    """
    if not run_id:
        run_id = _generate_run_id()

    try:
        from src.db.pool import get_pool
        client = get_pool().get_client()
        client.insert(
            "market_data.agent_traces",
            [[
                run_id, agent, step_idx, tool_name,
                args_json[:4000], result_json[:4000],
                latency_ms, tokens_in, tokens_out,
                status, error_class, error_msg[:2000],
                parent_run_id,
            ]],
            column_names=[
                "run_id", "agent", "step_idx", "tool_name",
                "args_json", "result_json",
                "latency_ms", "tokens_in", "tokens_out",
                "status", "error_class", "error_msg",
                "parent_run_id",
            ],
        )
    except Exception as exc:
        logger.debug("Trace write failed (non-fatal): %s", exc)


# ── LangChain Callback Handler ───────────────────────────────────────────────

class TracingCallbackHandler(BaseCallbackHandler):
    """
    LangChain callback handler that records tool calls to ClickHouse.

    Attach to any agent via config={"callbacks": [TracingCallbackHandler(agent="name")]}.
    """

    def __init__(self, agent: str = "main", run_id: str = "") -> None:
        self.agent = agent
        self.run_id = run_id or _generate_run_id()
        self._step = 0
        self._tool_starts: dict[str, float] = {}  # tool_run_id → start_time

    def on_tool_start(
        self, serialized: dict[str, Any], input_str: str, *, run_id: Any = None, **kwargs: Any
    ) -> None:
        key = str(run_id) if run_id else str(self._step)
        self._tool_starts[key] = time.monotonic()
        self._step += 1

    def on_tool_end(self, output: Any, *, run_id: Any = None, **kwargs: Any) -> None:
        key = str(run_id) if run_id else str(self._step - 1)
        start = self._tool_starts.pop(key, None)
        latency_ms = int((time.monotonic() - start) * 1000) if start else 0

        tool_name = ""
        if hasattr(output, "name"):
            tool_name = output.name or ""

        result_str = ""
        if hasattr(output, "content"):
            result_str = str(output.content)[:500]
        else:
            result_str = str(output)[:500]

        log_trace(
            agent=self.agent,
            run_id=self.run_id,
            step_idx=self._step,
            tool_name=tool_name,
            latency_ms=latency_ms,
            result_json=result_str,
            status="ok",
        )

    def on_tool_error(self, error: BaseException, *, run_id: Any = None, **kwargs: Any) -> None:
        key = str(run_id) if run_id else str(self._step - 1)
        start = self._tool_starts.pop(key, None)
        latency_ms = int((time.monotonic() - start) * 1000) if start else 0

        log_trace(
            agent=self.agent,
            run_id=self.run_id,
            step_idx=self._step,
            tool_name="",
            latency_ms=latency_ms,
            status="error",
            error_class=type(error).__name__,
            error_msg=str(error)[:2000],
        )

    def on_llm_end(self, response: Any, **kwargs: Any) -> None:
        """Record token usage from LLM responses when available."""
        try:
            usage = None
            if hasattr(response, "llm_output") and response.llm_output:
                usage = response.llm_output.get("token_usage", {})
            if usage:
                log_trace(
                    agent=self.agent,
                    run_id=self.run_id,
                    step_idx=self._step,
                    tool_name="_llm",
                    tokens_in=usage.get("prompt_tokens", 0),
                    tokens_out=usage.get("completion_tokens", 0),
                    status="ok",
                )
        except Exception:
            pass  # non-fatal
