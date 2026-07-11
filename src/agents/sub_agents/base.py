"""
src/agents/sub_agents/base.py
─────────────────────────────
Template-Method base class for every Mosaic sub-agent.

`_SubAgent` provides the lazy build, ReAct streaming, recursion fallback,
extended-thinking synthesis, chart placeholder substitution, and dedup
tool wrapping.  Concrete sub-agents only need to set `SYSTEM_PROMPT`,
`_get_tools()`, and optionally `RECURSION_LIMIT` / `_fallback()`.

This module is the heart of the framework — every subclass file in
this package imports `_SubAgent` from here.
"""
from __future__ import annotations

import logging
from typing import Any

from src.agents.sub_agents.infra import (
    _dedup_cache,
    _make_context_trimmer,
    _print_thinking_blocks,
    _wrap_tool_for_dedup,
)
from src.agents.sub_agents.prompts import NO_LLM_CALC_RULE

TABLE_FORMAT_RULE = (
    "\n\nTABLE FORMATTING MANDATE (apply strictly):\n"
    "When presenting structured data (allocations, consensus moves, flow metrics), ALWAYS use clean Markdown tables.\n"
    "Specifically:\n"
    "- Unified Macro Theme Allocations: Present exactly as a table with columns `Macro Theme | Combined Prev Weight | Combined Latest Weight | Net Flow Change`.\n"
    "- High-Conviction Equity Cross-Ownership: Present exactly as a table with columns `Security Name | Funds Count | Combined Prev % | Combined Latest % | Net Change | Conviction Rating`.\n"
    "Do NOT use bullet lists or prose for these datasets."
)

logger = logging.getLogger(__name__)


class _NullAgent:
    """Null Object for absent LLM — stream() fails fast into the existing error handler."""
    def stream(self, *args: Any, **kwargs: Any):
        raise RuntimeError("LLM unavailable — not support tool calling")


def _get_message_text(content: Any) -> str:
    """Extract string content from LangChain message content, which could be a list of blocks."""
    if isinstance(content, list):
        texts = []
        for block in content:
            if isinstance(block, dict):
                if block.get("type") == "text":
                    texts.append(block.get("text", ""))
            elif isinstance(block, str):
                texts.append(block)
        return "\n".join(texts)
    return str(content) if content else ""


class _SubAgent:
    """
    Lazy-initialised sub-agent base.

    The LangGraph ReAct agent (and the LLM) are built on the first call to
    ``run()`` to avoid unnecessary startup cost when the sub-agent is never
    invoked in a session.
    """

    #: Override in subclass
    SYSTEM_PROMPT: str = "You are a helpful assistant."
    #: Override in subclass — property or class-level list
    TOOLS: list = []
    #: Max LangGraph steps. Simple agents (news, signal) need ~8; equity/research
    #: need more due to parallel tool batches + optional import steps.
    #: None = LangGraph default (25). Override per subclass as needed.
    RECURSION_LIMIT: int | None = 20

    def __init__(self) -> None:
        self._agent: Any = None
        self._llm: Any = None
        import os
        self._built_caveman_level: str | None = os.environ.get("CAVEMAN_LEVEL")

    def _select_llm(self, llm_override: Any = None) -> Any:
        """
        Resolve the LLM to use for this agent.

        Override in subclasses to inject a domain-specific model before the
        base cloud-upgrade logic runs (e.g. CodeSubAgent uses CODE_LLM_PROVIDER).
        Returns None when no suitable LLM is available — _build() will then
        install a _NullAgent so run() never inspects self._agent for None.
        """
        from src.agents.mosaic_fund_agent import MosaicFundAgent
        tmp = object.__new__(MosaicFundAgent)
        tmp._checkpointer = None

        if llm_override is not None:
            return llm_override

        llm = tmp._build_llm()

        from config.settings import settings
        if llm is None or settings.llm_context_window < 12000:
            cloud_llm = tmp._build_cloud_llm()
            if cloud_llm is not None:
                if llm is None:
                    logger.info(
                        "%s: local LLM disabled — upgrading to cloud LLM",
                        self.__class__.__name__,
                    )
                else:
                    logger.info(
                        "%s: local context_window=%d < 12000 — upgrading to cloud LLM",
                        self.__class__.__name__, settings.llm_context_window,
                    )
                return cloud_llm
            if llm is None:
                logger.warning(
                    "%s: local LLM disabled and no cloud LLM configured — falling back",
                    self.__class__.__name__,
                )
            else:
                logger.info(
                    "%s: local context_window=%d < 12000 and no cloud LLM configured — falling back",
                    self.__class__.__name__, settings.llm_context_window,
                )
            return None

        return llm

    def _build(self, llm_override: Any = None) -> None:
        """Lazily build the LangGraph ReAct agent."""
        self._llm = self._select_llm(llm_override)
        if self._llm is None:
            self._agent = _NullAgent()
            return

        try:
            from langgraph.prebuilt import create_react_agent, ToolNode
            tools = self._get_tools()
            tools = [_wrap_tool_for_dedup(t) for t in tools]
            tool_node = ToolNode(tools)
            from src.utils.caveman import get_caveman_prompt
            from config.settings import settings

            # Always attach context trimmer using the appropriate context window size (local vs cloud)
            window = (
                settings.llm_cloud_context_window
                if (llm_override is not None or not settings.is_local_model)
                else settings.llm_context_window
            )
            pre_hook = _make_context_trimmer(window)
            logger.info(
                "%s: context trimmer attached (window=%d tokens)",
                self.__class__.__name__, window,
            )

            self._agent = create_react_agent(
                model=self._llm,
                tools=tool_node,
                prompt=self.SYSTEM_PROMPT + get_caveman_prompt() + NO_LLM_CALC_RULE + TABLE_FORMAT_RULE,
                pre_model_hook=pre_hook,
            )
            logger.info(
                "%s: agent built with parallel ToolNode (%d tools)",
                self.__class__.__name__, len(tools),
            )
        except Exception as exc:
            logger.error("%s: build failed: %s", self.__class__.__name__, exc)
            self._agent = _NullAgent()

    def _get_tools(self) -> list:
        """Return the tool list.  Subclasses can override for lazy imports."""
        return self.TOOLS

    def run(self, question: str, llm_override: Any = None, callbacks: list | None = None) -> str:
        """Invoke the sub-agent and return its text response.

        Parameters
        ----------
        llm_override:
            Cloud LLM to use instead of the default local model.
        callbacks:
            LangChain callbacks list (e.g. [RichConsoleCallbackHandler()]) for
            verbose tool-call tracing.  Passed directly to agent.invoke().
        """
        import os
        current_caveman = os.environ.get("CAVEMAN_LEVEL")
        if self._agent is None or current_caveman != getattr(self, "_built_caveman_level", None):
            self._build(llm_override=llm_override)
            self._built_caveman_level = current_caveman

        from src.tools.chart_tools import get_active_charts
        get_active_charts().clear()

        from langchain_core.messages import HumanMessage, ToolMessage, AIMessage
        from config.settings import settings
        is_local = (bool(settings.llm_base_url) and not settings.llm_local_disabled)
        limit = 8 if is_local else self.RECURSION_LIMIT
        config: dict = (
            {"recursion_limit": limit}
            if limit is not None
            else {}
        )
        if callbacks:
            config["callbacks"] = callbacks

        # Stream instead of invoke so we accumulate partial state at every step.
        # If the recursion limit fires mid-run, we still have all tool outputs
        # collected so far and can synthesise from them.
        msgs: list = []
        _recursion_hit = False
        # Open a per-run dedup cache so duplicate tool calls in this single turn
        # return cached results instead of re-executing.  Each .run() call sets
        # a fresh dict, so old caches are overwritten — no manual reset needed.
        _dedup_cache.set({})
        try:
            for state in self._agent.stream(
                {"messages": [HumanMessage(content=question)]},
                config=config,
                stream_mode="values",
            ):
                if isinstance(state, dict):
                    msgs = state.get("messages", msgs)
        except Exception as exc:
            err = str(exc).lower()
            if "recursion" in err:
                _recursion_hit = True
                logger.warning(
                    "%s: recursion limit hit — synthesising from %d partial messages",
                    self.__class__.__name__, len(msgs),
                )
            elif any(k in err for k in ("tool", "400", "invalid_request", "function", "tool_calls", "not support")):
                logger.info(
                    "%s: LLM tool-calling failed (%s), using programmatic fallback",
                    self.__class__.__name__, type(exc).__name__,
                )
                return self._confirm_fallback(question)
            else:
                logger.error("%s.run() failed: %s", self.__class__.__name__, exc)
                return f"Research incomplete: {exc}"

        try:
            # Collect tool outputs; skip pure-plumbing symbol-resolution calls.
            _SKIP_KEYS = ('"symbol"', '"nse_symbol"', '"yf_symbol"')
            tool_sections = []
            for m in msgs:
                if not isinstance(m, ToolMessage):
                    continue
                content = str(m.content).strip()
                if not content:
                    continue
                if content.startswith("{") and any(k in content for k in _SKIP_KEYS):
                    continue
                tool_sections.append(content)

            if tool_sections:
                # Prefer a final LLM synthesis if it already exists.
                last_ai = next(
                    (m for m in reversed(msgs) if isinstance(m, AIMessage) and _get_message_text(m.content).strip()),
                    None,
                )
                if last_ai and not _recursion_hit:
                    ai_text = _get_message_text(last_ai.content)

                    # Print any extended-thinking blocks from the final message
                    _print_thinking_blocks(last_ai.content)

                    from src.tools.chart_tools import get_active_charts
                    chart_by_type = get_active_charts().copy()

                    # Strip any box-drawing / chart characters the LLM may have
                    # reproduced despite instructions.  These corrupt Rich panels.
                    import re as _re
                    _CHART_LINE_RE = _re.compile(
                        r"^.*[┤┼┌┐┘└├┬┴─]{3,}.*$|"   # box-drawing heavy lines
                        r"^.*[████▓▓▒▒░░]{4,}.*$|"    # bar chart fill blocks
                        r"^.*▞▞.*▗▌.*$",               # plotext braille scatter
                        _re.MULTILINE,
                    )
                    ai_text = _CHART_LINE_RE.sub("", ai_text)
                    # Clean up empty ``` blocks left behind
                    ai_text = _re.sub(r"```\s*```", "", ai_text)
                    # Collapse runs of 3+ blank lines
                    ai_text = _re.sub(r"\n{3,}", "\n\n", ai_text)

                    # Replace placeholders for all charts in chart_by_type
                    for tname in list(chart_by_type.keys()):
                        placeholders = [f"[CHART:{tname}]"]
                        if tname.startswith("plot_") and tname.endswith("_chart"):
                            short_name = tname[5:-6]  # e.g., "plot_macd_chart" -> "macd"
                            placeholders.append(f"[CHART:{short_name}]")

                        for placeholder in placeholders:
                            if placeholder in ai_text:
                                ai_text = ai_text.replace(placeholder, chart_by_type.pop(tname))
                                break

                    # Fallbacks for specific standard sections if not explicitly replaced
                    if "price" in chart_by_type:
                        snap = _re.search(r"(#+\s*(?:\(?\d\)?\s*)?Company\s+Snapshot.*?)(?=\n\s*#|\Z)", ai_text, _re.I | _re.DOTALL)
                        if snap:
                            ai_text = ai_text[:snap.end()] + "\n\n" + chart_by_type.pop("price") + "\n" + ai_text[snap.end():]
                        else:
                            ai_text += "\n\n" + chart_by_type.pop("price")

                    if "shareholding" in chart_by_type:
                        own = _re.search(r"(#+\s*(?:\(?\d\)?\s*)?Institutional\s+Ownership.*?)(?=\n\s*[╭|])", ai_text, _re.I | _re.DOTALL)
                        if own:
                            ai_text = ai_text[:own.end()] + "\n\n" + chart_by_type.pop("shareholding") + "\n" + ai_text[own.end():]
                        else:
                            ai_text += "\n\n" + chart_by_type.pop("shareholding")

                    # Append any remaining charts (FII/DII, etc.) that weren't placed
                    for tname, chart_str in chart_by_type.items():
                        title = tname.replace("plot_", "").replace("_", " ").title()
                        ai_text += f"\n\n### {title}\n\n{chart_str}"

                    logger.info(
                        "%s: returning LLM synthesis (%d chars)",
                        self.__class__.__name__, len(ai_text),
                    )
                    return ai_text

                # Recursion limit hit (or no final AI message) — synthesise now.
                if self._llm:
                    try:
                        from langchain_core.messages import SystemMessage

                        # Use extended thinking for the synthesis call when the LLM
                        # is Anthropic Claude — gives a deeper reasoning pass over
                        # all collected tool data before writing the research note.
                        synth_llm = self._llm
                        try:
                            if synth_llm.__class__.__name__ == "ChatAnthropic" and hasattr(synth_llm, "model") and "claude" in str(getattr(synth_llm, "model", "")).lower():
                                synth_llm = synth_llm.bind(thinking={"type": "enabled", "budget_tokens": 8000})
                                logger.info("%s: extended thinking enabled for synthesis", self.__class__.__name__)
                        except Exception:
                            pass  # non-critical — fall through to normal LLM

                        combined = "\n\n---\n\n".join(tool_sections[:10])
                        from src.utils.caveman import get_caveman_prompt
                        sys_prompt = self.SYSTEM_PROMPT + get_caveman_prompt() + NO_LLM_CALC_RULE + TABLE_FORMAT_RULE + "\n\n" + (
                            "PARTIAL DATA SYNTHESIS RULES (apply strictly):\n"
                            "- Write ONLY the sections for which you have actual tool output data.\n"
                            "- OMIT any section entirely if no tool data was collected for it.\n"
                            "- NEVER write '(Data pending)', 'N/A', or placeholder text.\n"
                            "- Do not mention step limits, recursion, or missing data.\n"
                            "- Be concise and factual — only report what the tools returned."
                        )
                        synth = synth_llm.invoke([
                            SystemMessage(content=sys_prompt),
                            HumanMessage(content=f"Question: {question}\n\nData collected:\n{combined}"),
                        ])
                        # Print extended-thinking blocks if present
                        _print_thinking_blocks(synth.content, label="🧠 Analyst Reasoning (extended thinking)")
                        logger.info(
                            "%s: partial synthesis (%d tool outputs → %d chars)",
                            self.__class__.__name__, len(tool_sections), len(str(synth.content)),
                        )
                        return _get_message_text(synth.content) if isinstance(synth.content, list) else str(synth.content)
                    except Exception as synth_exc:
                        logger.warning("%s: synthesis call failed: %s", self.__class__.__name__, synth_exc)

                # Last resort — concatenate raw tool outputs.
                logger.info("%s: merged %d tool outputs programmatically", self.__class__.__name__, len(tool_sections))
                return "\n\n---\n\n".join(tool_sections)

            # No tool calls — return the last AI message directly.
            return _get_message_text(msgs[-1].content) if msgs else "No response from sub-agent."
        except Exception as exc:
            logger.error("%s: message processing failed: %s", self.__class__.__name__, exc)
            return f"Research incomplete: {exc}"

    def _confirm_fallback(self, question: str) -> str:
        """Prompt the user before switching to the programmatic data-gathering path."""
        import sys
        prompt = "\n[mosaic] LLM tool-calling unavailable — use programmatic data gathering instead? [Y/n] "
        try:
            # Open /dev/tty directly so the prompt works even when Rich's Live display
            # is active and has captured stdout/stdin.
            with open("/dev/tty", "r+") as tty:
                tty.write(prompt)
                tty.flush()
                ans = tty.readline().strip().lower()
        except OSError:
            # Non-interactive environment (piped, tests) — default to yes.
            sys.stdout.write(prompt + "\n")
            sys.stdout.flush()
            ans = ""
        if ans in ("", "y", "yes"):
            return self._fallback(question)
        return "Aborted. To enable full capability, configure a tool-calling LLM in your .env."

    def _fallback(self, question: str) -> str:
        """Programmatic fallback for when the LLM cannot call tools.  Override in subclasses."""
        return (
            "Your configured LLM does not support tool-calling.  "
            "Set `LLM_PROVIDER=openai` or `LLM_PROVIDER=anthropic` in your .env for full capability.  "
            "Alternatively use `./mosaic.sh chat` for the interactive REPL."
        )
