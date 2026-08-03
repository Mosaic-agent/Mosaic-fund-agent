"""
src/agents/declarative/declarative_runner.py
──────────────────────────────────────────────
Declarative Agent Runner — Execution engine for configuration-driven playbooks.

Features:
  - Manages ContextRun blackboard mapping step_ids to step outputs.
  - Renders Jinja2 templates and prompts using run context.
  - Executes auto_tools step fan-outs concurrently via ThreadPoolExecutor.
  - Intercepts tool errors using format_tool_error / format_agent_error.
"""
from __future__ import annotations

import concurrent.futures
import logging
from dataclasses import dataclass, field
from typing import Any, Callable

from jinja2 import Environment

from src.agents.declarative.declarative_spec import (
    AutoToolCallSpec,
    DeclarativeAgentSpec,
    StepSpec,
    StepType,
    load_agent_spec_from_dict,
    load_agent_spec_from_yaml,
)
from src.utils.error_utils import format_agent_error, format_tool_error

logger = logging.getLogger(__name__)

def safe_from_json(s: Any) -> Any:
    if not isinstance(s, str):
        return s
    try:
        import json
        return json.loads(s)
    except Exception:
        return {"status": "error", "message": "Failed to parse JSON string"}

def xml_tag(content: Any, tag_name: str) -> str:
    """Wrap content inside specified XML tag for Claude-native prompt structuring."""
    return f"<{tag_name}>\n{content}\n</{tag_name}>"

_jinja_env = Environment()
_jinja_env.filters["from_json"] = safe_from_json
_jinja_env.filters["xml_tag"] = xml_tag


@dataclass
class ContextRun:
    """Thread-safe execution blackboard for a declarative agent run."""
    input_params: dict[str, Any] = field(default_factory=dict)
    step_outputs: dict[str, Any] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)

    def get_render_context(self) -> dict[str, Any]:
        """Combine input parameters and prior step outputs for Jinja2 template rendering."""
        ctx: dict[str, Any] = {}
        ctx.update(self.input_params)
        ctx.update(self.step_outputs)
        ctx["context"] = {
            "inputs": self.input_params,
            "steps": self.step_outputs,
        }
        return ctx


# Default timeout for individual tool calls (seconds)
_DEFAULT_TOOL_TIMEOUT = 30


class DeclarativeAgentRunner:
    """Execution engine that runs a validated DeclarativeAgentSpec playbook."""

    def __init__(
        self,
        spec: DeclarativeAgentSpec | str | dict[str, Any],
        tool_registry: dict[str, Callable[..., Any]] | None = None,
        tool_timeout: int = _DEFAULT_TOOL_TIMEOUT,
    ) -> None:
        if isinstance(spec, str):
            self.spec = load_agent_spec_from_yaml(spec)
        elif isinstance(spec, dict):
            self.spec = load_agent_spec_from_dict(spec)
        else:
            self.spec = spec

        self.tool_registry = tool_registry or self._discover_tools()
        self.tool_timeout = tool_timeout
        self._callbacks: list[Any] = []  # Wired by DeclarativeSubAgentAdapter

    def _discover_tools(self) -> dict[str, Callable[..., Any]]:
        """Discover registered tools across Mosaic tool modules."""
        tools: dict[str, Callable[..., Any]] = {}

        # ── delegation / agent tools ───────────────────────────────────────
        try:
            from src.tools.agent_tools import ALL_TOOLS
            for t in ALL_TOOLS:
                tool_name = getattr(t, "name", getattr(t, "__name__", str(t)))
                tools[tool_name] = t
        except Exception as exc:
            logger.debug("Failed to load ALL_TOOLS from agent_tools: %s", exc)

        # ── equity data tools ──────────────────────────────────────────────
        _equity_modules = [
            ("src.tools.yahoo_finance",       ["get_yahoo_finance_data", "get_price_momentum"]),
            ("src.tools.earnings_scraper",    ["get_quarterly_results", "get_shareholding_pattern"]),
            ("src.tools.indian_equity_tools", ["get_stock_cashflow", "get_mf_holdings_for_stock",
                                               "get_db_price_summary", "get_fii_dii_summary"]),
            ("src.tools.news_search",         ["get_stock_news", "search_financial_news"]),
            ("src.tools.newsapi_search",      ["get_newsapi_stock_news"]),
            ("src.tools.market.equity",       ["search_anomaly_events"]),
            ("src.tools.chart_tools",         ["plot_price_chart", "plot_shareholding_bar",
                                               "plot_macd_chart", "plot_signal_scores"]),
            ("src.tools.agent_tools",         ["check_and_refresh_symbol_data"]),
        ]
        for module_path, names in _equity_modules:
            try:
                import importlib
                mod = importlib.import_module(module_path)
                for name in names:
                    obj = getattr(mod, name, None)
                    if obj is not None:
                        tools[name] = obj
            except Exception as exc:
                logger.debug("Failed to load tools from %s: %s", module_path, exc)

        # ── broker tools (optional — absent when Shoonya/NSE not configured) ─
        for tool_mod, tool_name in [
            ("src.tools.shoonya_tools",     "get_shoonya_quotes"),
            ("src.tools.nse_announcements", "get_nse_announcements"),
        ]:
            try:
                import importlib
                mod = importlib.import_module(tool_mod)
                obj = getattr(mod, tool_name, None)
                if obj is not None:
                    tools[tool_name] = obj
            except Exception:
                pass  # optional; missing silently

        return tools

    def render_string(self, template_str: str, ctx: ContextRun) -> str:
        """Render a Jinja2 template string using the current run context."""
        try:
            tmpl = _jinja_env.from_string(template_str)
            return tmpl.render(**ctx.get_render_context())
        except Exception as exc:
            logger.warning("Jinja2 rendering failed for '%s': %s", template_str[:50], exc)
            return template_str

    def _render_params(self, params: dict[str, Any], ctx: ContextRun) -> dict[str, Any]:
        """Recursively render Jinja2 strings inside parameter dictionaries."""
        rendered: dict[str, Any] = {}
        for k, v in params.items():
            if isinstance(v, str):
                rendered[k] = self.render_string(v, ctx)
            elif isinstance(v, dict):
                rendered[k] = self._render_params(v, ctx)
            else:
                rendered[k] = v
        return rendered

    def evaluate_condition(self, condition_str: str | None, ctx: ContextRun) -> bool:
        """Evaluate a boolean condition string using Jinja2 context."""
        if not condition_str:
            return True
        try:
            rendered = self.render_string(f"{{{{ {condition_str} }}}}", ctx).strip()
            return rendered.lower() in ("true", "1", "yes")
        except Exception as exc:
            logger.warning("Condition evaluation failed for '%s': %s", condition_str, exc)
            return True  # Default to executing step on condition error

    def _execute_single_tool_call(self, tc: AutoToolCallSpec, ctx: ContextRun) -> dict[str, Any]:
        """Execute a single deterministic tool call from an auto_tools step."""
        import time as _time

        if tc.condition and not self.evaluate_condition(tc.condition, ctx):
            return {"tool_name": tc.tool_name, "status": "skipped", "output": None}

        tool_func = self.tool_registry.get(tc.tool_name)
        if not tool_func:
            err_msg = f"Tool '{tc.tool_name}' not found in registry."
            impact = f"Tool '{tc.tool_name}' missing. Downstream steps depending on this tool will receive empty data."
            formatted_err = format_tool_error(tc.tool_name, err_msg, impact)
            if tc.fail_on_error:
                raise RuntimeError(formatted_err)
            return {"tool_name": tc.tool_name, "status": "error", "output": formatted_err}

        params = self._render_params(tc.params, ctx)
        start = _time.monotonic()
        try:
            # Handle LangChain @tool objects vs raw functions
            if hasattr(tool_func, "invoke"):
                result = tool_func.invoke(params)
            else:
                result = tool_func(**params)
            latency_ms = int((_time.monotonic() - start) * 1000)
            self._log_tool_trace(tc.tool_name, latency_ms, "ok")
            return {"tool_name": tc.tool_name, "status": "success", "output": result}
        except Exception as exc:
            latency_ms = int((_time.monotonic() - start) * 1000)
            self._log_tool_trace(tc.tool_name, latency_ms, "error", error_msg=str(exc))
            impact = f"Deterministic tool '{tc.tool_name}' execution failed. Data pipeline for this step is incomplete."
            formatted_err = format_tool_error(tc.tool_name, exc, impact)
            if tc.fail_on_error:
                raise RuntimeError(formatted_err) from exc
            logger.warning("auto_tools call '%s' failed (fail_on_error=False): %s", tc.tool_name, exc)
            return {"tool_name": tc.tool_name, "status": "error", "output": formatted_err}

    def _log_tool_trace(self, tool_name: str, latency_ms: int, status: str, error_msg: str = "") -> None:
        """Log tool execution to ClickHouse agent_traces via any attached callbacks."""
        for cb in self._callbacks:
            if hasattr(cb, "run_id"):
                try:
                    from src.agents.tracer import log_trace
                    log_trace(
                        agent=self.spec.agent_type,
                        run_id=cb.run_id,
                        tool_name=tool_name,
                        latency_ms=latency_ms,
                        status=status,
                        error_msg=error_msg[:2000],
                    )
                except Exception:
                    pass
                break

    def _execute_auto_tools(self, step: StepSpec, ctx: ContextRun) -> dict[str, Any]:
        """Execute an auto_tools step in parallel or sequentially with per-tool timeout."""
        results: list[dict[str, Any]] = []

        if step.parallel and len(step.tool_calls) > 1:
            max_workers = min(len(step.tool_calls), 8)
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_tc = {
                    executor.submit(self._execute_single_tool_call, tc, ctx): tc
                    for tc in step.tool_calls
                }
                # Wait up to the largest per-tool timeout so slow tools aren't cut short
                step_wall_timeout = max(
                    (tc.timeout or self.tool_timeout) for tc in step.tool_calls
                )
                done, not_done = concurrent.futures.wait(
                    future_to_tc.keys(), timeout=step_wall_timeout,
                )
                for f in done:
                    try:
                        results.append(f.result())
                    except Exception as exc:
                        tc = future_to_tc[f]
                        results.append({"tool_name": tc.tool_name, "status": "error", "output": str(exc)})
                for f in not_done:
                    tc = future_to_tc[f]
                    f.cancel()
                    effective_timeout = tc.timeout or self.tool_timeout
                    impact = f"Tool '{tc.tool_name}' timed out after {effective_timeout}s."
                    results.append({"tool_name": tc.tool_name, "status": "timeout", "output": format_tool_error(tc.tool_name, TimeoutError(impact), impact)})
                    logger.warning("auto_tools call '%s' timed out after %ds", tc.tool_name, effective_timeout)
        else:
            for tc in step.tool_calls:
                results.append(self._execute_single_tool_call(tc, ctx))

        # Format output into step result map
        output_map: dict[str, Any] = {
            "auto_tool_results": results,
            "status": "completed",
        }
        for res in results:
            tname = res["tool_name"]
            output_map[tname] = res["output"]
        return output_map

    def _execute_reason(self, step: StepSpec, ctx: ContextRun) -> dict[str, Any]:
        """Execute a reason LLM synthesis step."""
        rendered_prompt = self.render_string(step.prompt or "", ctx)
        logger.info("Executing declarative step '%s' [reason] — calling LLM for synthesis", step.id)
        try:
            from src.workflows.base import _get_llm
            llm = _get_llm()
            if llm is None:
                raise RuntimeError("No LLM configured — set LLM_PROVIDER/ANTHROPIC_API_KEY in .env")
            response = llm.invoke(rendered_prompt)
            from src.agents.sub_agents.base import _get_message_text
            output_text = _get_message_text(getattr(response, "content", response))
            return {"prompt": rendered_prompt, "output": output_text, "status": "completed"}
        except Exception as exc:
            impact = f"Reason step '{step.id}' failed. Qualitative LLM analysis for this step could not be completed."
            err_msg = format_agent_error(self.spec.agent_name, exc, impact)
            return {"prompt": rendered_prompt, "output": err_msg, "status": "error"}

    def _execute_template_output(self, step: StepSpec, ctx: ContextRun) -> dict[str, Any]:
        """Execute a template_output report rendering step."""
        rendered = self.render_string(step.template or "", ctx)
        return {"output": rendered, "format": step.format, "status": "completed"}

    def _execute_use_tools(self, step: StepSpec, ctx: ContextRun) -> dict[str, Any]:
        """Execute a use_tools step: bounded ReAct loop with mandatory + optional tools."""
        rendered_prompt = self.render_string(step.prompt or "", ctx)
        max_cycles = step.max_cycles
        mandatory = set(step.mandatory_tools)
        optional = set(step.optional_tools)
        all_allowed = mandatory | optional

        # Filter tool registry to only allowed tools
        available_tools = {k: v for k, v in self.tool_registry.items() if k in all_allowed}
        called_mandatory: set[str] = set()
        cycle_results: list[dict[str, Any]] = []

        for cycle in range(max_cycles):
            # Build a simple prompt listing available tools
            tool_list = ", ".join(sorted(available_tools.keys()))
            cycle_prompt = (
                f"{rendered_prompt}\n\n"
                f"Available tools: {tool_list}\n"
                f"Cycle {cycle + 1}/{max_cycles}. "
                f"Mandatory tools not yet called: {sorted(mandatory - called_mandatory) or 'all done'}."
            )

            try:
                from src.workflows.base import _get_llm
                llm = _get_llm()
                if llm is None:
                    raise RuntimeError("No LLM configured — set LLM_PROVIDER/ANTHROPIC_API_KEY in .env")

                # Bind tools if the LLM supports it
                tool_objects = [v for v in available_tools.values() if hasattr(v, "name")]
                if tool_objects and hasattr(llm, "bind_tools"):
                    bound_llm = llm.bind_tools(tool_objects)
                    response = bound_llm.invoke(cycle_prompt)
                else:
                    response = llm.invoke(cycle_prompt)

                # Check for tool calls in response
                tool_calls = getattr(response, "tool_calls", [])
                if not tool_calls:
                    # LLM chose to stop — return its text output
                    output_text = getattr(response, "content", str(response))
                    cycle_results.append({"cycle": cycle + 1, "output": output_text, "tools_called": []})
                    break

                # Execute each tool call
                tools_this_cycle = []
                for tc in tool_calls:
                    tc_name = tc.get("name", "")
                    tc_args = tc.get("args", {})
                    if tc_name not in available_tools:
                        continue
                    tool_func = available_tools[tc_name]
                    try:
                        if hasattr(tool_func, "invoke"):
                            result = tool_func.invoke(tc_args)
                        else:
                            result = tool_func(**tc_args)
                        tools_this_cycle.append({"tool": tc_name, "status": "success", "output": result})
                        called_mandatory.discard(tc_name)  # not needed, but clear
                        if tc_name in mandatory:
                            called_mandatory.add(tc_name)
                    except Exception as exc:
                        tools_this_cycle.append({"tool": tc_name, "status": "error", "output": str(exc)})

                cycle_results.append({"cycle": cycle + 1, "tools_called": tools_this_cycle})

                # Check if all mandatory tools called
                if called_mandatory >= mandatory:
                    break

            except Exception as exc:
                impact = f"use_tools step '{step.id}' cycle {cycle + 1} failed."
                err_msg = format_agent_error(self.spec.agent_name, exc, impact)
                cycle_results.append({"cycle": cycle + 1, "error": err_msg})
                break

        return {"cycles": cycle_results, "status": "completed"}

    def run(self, input_params: dict[str, Any] | None = None) -> dict[str, Any]:
        """Execute the complete declarative agent playbook."""
        ctx = ContextRun(input_params=input_params or {})

        # Apply sample_input defaults if not provided
        for k, v in self.spec.sample_input.items():
            if k not in ctx.input_params:
                ctx.input_params[k] = v

        for step in self.spec.steps:
            if step.condition and not self.evaluate_condition(step.condition, ctx):
                logger.info("Skipping step '%s' (condition evaluates to false)", step.id)
                ctx.step_outputs[step.id] = {"status": "skipped", "output": None}
                continue

            logger.info("Executing declarative step '%s' [%s]", step.id, step.step_type.value)
            if step.step_type == StepType.AUTO_TOOLS:
                res = self._execute_auto_tools(step, ctx)
            elif step.step_type == StepType.REASON:
                res = self._execute_reason(step, ctx)
            elif step.step_type == StepType.TEMPLATE_OUTPUT:
                res = self._execute_template_output(step, ctx)
            elif step.step_type == StepType.USE_TOOLS:
                res = self._execute_use_tools(step, ctx)
            else:
                # tasks, agent, loop — not yet implemented
                logger.warning(
                    "Step type '%s' for step '%s' is not yet implemented — skipping.",
                    step.step_type.value, step.id,
                )
                res = {"status": "not_implemented", "output": None}

            ctx.step_outputs[step.id] = res

        # Return final step output or complete context map
        last_step_id = self.spec.steps[-1].id
        final_output = ctx.step_outputs.get(last_step_id, {}).get("output", "")
        return {
            "agent_name": self.spec.agent_name,
            "agent_type": self.spec.agent_type,
            "output": final_output,
            "context": ctx.get_render_context(),
            "step_outputs": ctx.step_outputs,
        }
