"""
src/agents/budget.py
────────────────────
Per-run budget enforcement for agent tool calls and LLM tokens.

Attach ``BudgetCallbackHandler`` to any LangGraph agent to enforce hard limits
on tool calls, LLM tokens, and wall-clock time.  When a budget is exceeded the
handler raises ``BudgetExceededError`` which the agent loop catches and surfaces
as a partial answer.

Usage:
    from src.agents.budget import BudgetCallbackHandler, BudgetExceededError

    budget = BudgetCallbackHandler(
        max_tool_calls=15,
        max_tokens=8000,
        max_wall_clock_s=120,
    )
    try:
        agent.invoke({"messages": [...]}, config={"callbacks": [budget]})
    except BudgetExceededError as exc:
        print(f"Budget exceeded: {exc}")
"""
from __future__ import annotations

import logging
import time
from typing import Any

from langchain_core.callbacks import BaseCallbackHandler

logger = logging.getLogger(__name__)


class BudgetExceededError(RuntimeError):
    """Raised when a per-run budget limit is hit."""
    pass


# Default per-tool call limits.  Keys are tool names, values are max calls per run.
# None = unlimited.
DEFAULT_TOOL_CAPS: dict[str, int] = {
    "fetch_all_comex_signals": 1,
    "collate_news_sentiment": 2,
    "run_deepdive_analysis": 1,
    "run_data_engineering_importer": 1,
}


class BudgetCallbackHandler(BaseCallbackHandler):
    """
    Callback handler that enforces per-run resource budgets.

    Parameters
    ----------
    max_tool_calls : int
        Max total tool invocations across all tools. Default 20.
    max_tokens : int
        Max total LLM tokens (prompt + completion). Default 100000.
    max_wall_clock_s : float
        Max wall-clock seconds for the entire run. Default 180.
    tool_caps : dict[str, int] | None
        Per-tool call limits.  Merged with DEFAULT_TOOL_CAPS.
    """

    def __init__(
        self,
        max_tool_calls: int = 20,
        max_tokens: int = 100_000,
        max_wall_clock_s: float | None = None,
        tool_caps: dict[str, int] | None = None,
    ) -> None:
        import os
        self.max_tool_calls = max_tool_calls
        self.max_tokens = max_tokens
        self.max_wall_clock_s = (
            max_wall_clock_s if max_wall_clock_s is not None
            else float(os.getenv("AGENT_TIMEOUT", "300.0"))
        )
        self.tool_caps = {**DEFAULT_TOOL_CAPS, **(tool_caps or {})}

        # Counters
        self.total_tool_calls = 0
        self.total_tokens = 0
        self._per_tool_counts: dict[str, int] = {}
        self._start_time = time.monotonic()

    # ── Guards ────────────────────────────────────────────────────────────────

    def _check_wall_clock(self) -> None:
        elapsed = time.monotonic() - self._start_time
        if elapsed > self.max_wall_clock_s:
            raise BudgetExceededError(
                f"Wall-clock budget exceeded: {elapsed:.0f}s > {self.max_wall_clock_s:.0f}s limit"
            )

    def _check_tool_calls(self, tool_name: str) -> None:
        if self.total_tool_calls >= self.max_tool_calls:
            raise BudgetExceededError(
                f"Total tool-call budget exceeded: {self.total_tool_calls} >= {self.max_tool_calls}"
            )
        cap = self.tool_caps.get(tool_name)
        if cap is not None:
            count = self._per_tool_counts.get(tool_name, 0)
            if count >= cap:
                raise BudgetExceededError(
                    f"Per-tool budget exceeded: {tool_name} called {count} times (cap={cap})"
                )

    def _check_tokens(self) -> None:
        if self.total_tokens >= self.max_tokens:
            raise BudgetExceededError(
                f"Token budget exceeded: {self.total_tokens} >= {self.max_tokens}"
            )

    # ── Callbacks ─────────────────────────────────────────────────────────────

    def on_tool_start(
        self, serialized: dict[str, Any], input_str: str, **kwargs: Any
    ) -> None:
        self._check_wall_clock()
        tool_name = serialized.get("name", "unknown")
        self._check_tool_calls(tool_name)
        self.total_tool_calls += 1
        self._per_tool_counts[tool_name] = self._per_tool_counts.get(tool_name, 0) + 1
        logger.debug(
            "Budget: tool_call #%d (%s, %d/%s)",
            self.total_tool_calls,
            tool_name,
            self._per_tool_counts[tool_name],
            self.tool_caps.get(tool_name, "∞"),
        )

    def on_llm_end(self, response: Any, **kwargs: Any) -> None:
        self._check_wall_clock()
        try:
            usage = None
            if hasattr(response, "llm_output") and response.llm_output:
                usage = response.llm_output.get("token_usage", {})
            if usage:
                self.total_tokens += usage.get("total_tokens", 0)
                logger.debug("Budget: tokens so far %d/%d", self.total_tokens, self.max_tokens)
                self._check_tokens()
        except BudgetExceededError:
            raise
        except Exception:
            pass  # non-fatal

    def on_tool_end(self, output: Any, **kwargs: Any) -> None:
        self._check_wall_clock()

    @property
    def summary(self) -> dict[str, Any]:
        """Return a summary of resource usage for this run."""
        return {
            "tool_calls": self.total_tool_calls,
            "tool_call_limit": self.max_tool_calls,
            "tokens": self.total_tokens,
            "token_limit": self.max_tokens,
            "elapsed_s": round(time.monotonic() - self._start_time, 1),
            "wall_clock_limit_s": self.max_wall_clock_s,
            "per_tool": dict(self._per_tool_counts),
        }
