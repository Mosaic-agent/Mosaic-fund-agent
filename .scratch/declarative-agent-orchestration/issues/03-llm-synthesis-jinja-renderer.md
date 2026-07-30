# 03 — reason LLM Synthesis & template_output Jinja2 Renderer

**What to build:** Extend the `DeclarativeAgentRunner` to support `reason` steps (LLM synthesis passes with prompt context and system instructions) and `template_output` steps (rendering final Markdown reports deterministically using Jinja2 templates directly from `ContextRun` step outputs).

**Blocked by:** 02 — Declarative Execution Engine & auto_tools Parallel Fetcher.

**Status:** done

- [x] Support `reason` step execution with LLM invocation and context prompt interpolation.
- [x] Support `use_tools` step execution with bounded `max_cycles` ReAct loops.
- [x] Implement `template_output` step using Jinja2 templates to render Markdown reports from `step_id` outputs without metric hallucination.
- [x] Add unit tests verifying end-to-end spec execution from batch tool pre-fetch to final Markdown report generation.
