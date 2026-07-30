# 02 — Declarative Execution Engine & auto_tools Parallel Fetcher

**What to build:** The core execution runner (`src/agents/declarative/declarative_runner.py`) that executes validated `DeclarativeAgentSpec` playbooks, maintains a thread-safe `ContextRun` blackboard mapping `step_id` outputs to reproducible keys, and executes `auto_tools` parallel batch tool fan-outs using Python thread pools without wasting LLM decision tokens.

**Blocked by:** 01 — Declarative Agent Specification & Pydantic Schema Validator.

**Status:** done

- [x] Implement `DeclarativeAgentRunner` class accepting a validated `DeclarativeAgentSpec`.
- [x] Implement `ContextRun` blackboard for storing intermediate outputs indexed by `step_id`.
- [x] Implement parallel `auto_tools` batch executor using `ThreadPoolExecutor` respecting per-tool `fail_on_error` flags.
- [x] Add unit tests in `tests/test_declarative_runner.py` verifying parallel tool pre-fetching and deterministic output storage.
