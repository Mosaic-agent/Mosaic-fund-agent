# 04 — Registry Adapter & Declarative YAML Playbooks (GOLDBEES & Equity)

**What to build:** Integrate `DeclarativeAgentRunner` into `src/agents/sub_agents/registry.py` via a `DeclarativeSubAgentAdapter` that checks for `config/agents/<intent>.yaml` playbooks with transparent fallback to Python `_SubAgent` classes, and ship initial declarative playbooks for GOLDBEES and India Equity.

**Blocked by:** 03 — reason LLM Synthesis & template_output Jinja2 Renderer.

**Status:** done

- [x] Create `config/agents/goldbees_pipeline.yaml` for GOLDBEES quantitative analysis.
- [x] Create `config/agents/india_equity.yaml` for 8-section stock research notes.
- [x] Implement `DeclarativeSubAgentAdapter` in `src/agents/sub_agents/registry.py` to auto-load YAML playbooks when present.
- [x] Add end-to-end integration test and CLI verification.
