# 01 — Declarative Agent Specification & Pydantic Schema Validator

**What to build:** A Pydantic schema validation module (`src/agents/declarative/declarative_spec.py`) that loads declarative agent YAML/JSON contract files and validates every field, step type, tool call shape, loop constraint, and output template at startup so invalid configurations fail fast before execution starts.

**Blocked by:** None — can start immediately.

**Status:** done

- [x] Define `DeclarativeAgentSpec` root schema with `agent_name`, `agent_type`, `default_model`, `task_brief`, and `steps` list.
- [x] Define step type schemas for `reason`, `auto_tools`, `use_tools`, `tasks`, `agent`, `loop`, and `template_output`.
- [x] Enforce strict Pydantic validation rules for tool parameters, `max_cycles`, `fail_on_error`, and Jinja2 template syntax.
- [x] Add unit tests in `tests/test_declarative_spec.py` covering valid YAML playbooks and verifying fail-fast behavior on malformed specs.
