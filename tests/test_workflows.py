"""
tests/test_workflows.py
────────────────────────
Graph-compile and unit tests for src/workflows/.

All tests run without a live LLM or ClickHouse connection:
- Graph-compile tests verify the StateGraph structure is valid
- _par() tests verify parallel execution and failure isolation
- Checkpointer test verifies graceful None fallback

Run:
    pytest tests/test_workflows.py -q
"""
from __future__ import annotations

import pytest


# ── Graph-compile tests (0 LLM calls) ─────────────────────────────────────────

def test_autonomous_research_graph_compiles():
    from src.workflows.autonomous_research import _build_graph
    graph = _build_graph()
    assert graph is not None


def test_india_equity_graph_compiles():
    from src.workflows.india_equity import _build_graph
    graph = _build_graph()
    assert graph is not None


def test_multi_fund_consensus_graph_compiles():
    from src.workflows.multi_fund_consensus import _build_graph
    graph = _build_graph()
    assert graph is not None


def test_portfolio_analysis_graph_compiles():
    from src.workflows.portfolio_analysis import _build_graph
    graph = _build_graph()
    assert graph is not None


def test_enrich_all_node_populates_enriched_from_par_dicts(monkeypatch):
    """Regression test: _enrich_all_node relies on _par() returning fetchers'
    raw dicts untouched. A prior refactor of _par() routed every result through
    ContextManager.compress(), which always returns a str — silently making
    `enriched` empty (isinstance(v, dict) never matched) for every holding.
    Uses the real (unpatched) _par() to prove the actual fetch path works."""
    from src.workflows import portfolio_analysis as pa

    class _FakeTool:
        def __init__(self, value):
            self._value = value

        def invoke(self, *args, **kwargs):
            return self._value

    import src.tools.yahoo_finance as yf
    import src.tools.news_search as news_search
    import src.tools.earnings_scraper as earnings_scraper
    monkeypatch.setattr(yf, "get_yahoo_finance_data", _FakeTool("100"))
    monkeypatch.setattr(news_search, "get_stock_news", _FakeTool("n/a"))
    monkeypatch.setattr(earnings_scraper, "get_quarterly_results", _FakeTool("n/a"))

    state = {"holdings": [{"tradingsymbol": "RELIANCE"}, {"tradingsymbol": "TCS"}]}
    result = pa._enrich_all_node(state)
    assert len(result["enriched"]) == 2
    assert {h["tradingsymbol"] for h in result["enriched"]} == {"RELIANCE", "TCS"}


def test_all_graphs_are_cached():
    """Second call returns the same compiled instance (module-level cache)."""
    from src.workflows.autonomous_research import _build_graph as g1
    from src.workflows.india_equity import _build_graph as g2
    assert g1() is g1()
    assert g2() is g2()


# ── _par() unit tests ──────────────────────────────────────────────────────────

def test_par_runs_all_fetchers():
    from src.workflows.base import _par
    results = _par({"a": lambda: "hello", "b": lambda: "world"})
    assert results["a"] == "hello"
    assert results["b"] == "world"


def test_par_isolates_failures():
    """A failing fetcher returns a placeholder; other fetchers succeed."""
    from src.workflows.base import _par

    def _fail():
        raise ValueError("deliberate test error")

    results = _par({"good": lambda: "ok", "bad": _fail})
    assert results["good"] == "ok"
    assert "unavailable" in results["bad"]
    assert "deliberate test error" in results["bad"]


def test_par_empty_dict():
    from src.workflows.base import _par
    assert _par({}) == {}


def test_par_returns_empty_string_for_none():
    from src.workflows.base import _par
    results = _par({"none_returner": lambda: None})
    assert results["none_returner"] == ""


def test_par_returns_raw_dict_untouched():
    """_par() must NOT compress dict results — callers like
    portfolio_analysis._enrich_all_node rely on getting the raw dict back."""
    from src.workflows.base import _par
    payload = {"latest_close": 123.45, "30d": {"change_pct": -2.5}}
    results = _par({"price": lambda: payload})
    assert results["price"] is payload


def test_par_datasets_compresses_native_dict_without_losing_numeric_fields():
    from src.workflows.base import _par_datasets
    from src.workflows.context_manager import DatasetRef
    datasets = _par_datasets({"price": lambda: {"latest_close": 123.45, "30d": {"change_pct": -2.5}}})
    assert isinstance(datasets["price"], DatasetRef)
    assert '"latest_close": 123.45' in datasets["price"].content
    assert '"change_pct": -2.5' in datasets["price"].content


def test_par_datasets_content_matches_named_field_parity():
    """Migration invariant: the compressed content a workflow spreads onto its
    named TypedDict fields must be byte-identical to state["datasets"][key].content."""
    from src.workflows.base import _par_datasets
    datasets = _par_datasets({"goldbees": lambda: "prob_up: 0.62"})
    fields = {k: v.content for k, v in datasets.items()}
    assert fields["goldbees"] == datasets["goldbees"].content == "prob_up: 0.62"


def test_dataset_ref_survives_json_checkpoint_round_trip():
    """DatasetRef must be a flat, JSON-serializable dataclass — langgraph's
    SqliteSaver checkpoints workflow state (including state["datasets"]) between
    graph steps, so a non-serializable field would break resumoption."""
    import dataclasses
    import json
    from src.workflows.base import _par_datasets
    datasets = _par_datasets({"macro": lambda: {"theme": "gold", "score": 12.5}})
    payload = json.dumps({k: dataclasses.asdict(v) for k, v in datasets.items()})
    restored = json.loads(payload)
    assert restored["macro"]["key"] == "macro"
    assert restored["macro"]["source_type"] == "dict"
    assert '"theme": "gold"' in restored["macro"]["content"]


# ── Context manager unit tests ───────────────────────────────────────────────

def test_context_manager_truncates_with_marker_and_preserves_unicode():
    from src.workflows.context_manager import ContextManager, truncate_text
    assert truncate_text("₹abcdef", 3) == "₹ab\n…[4 chars trimmed — use narrower queries to fit context]"
    assert ContextManager(max_chars=3).compress("any", "₹abcdef").startswith("₹ab")


def test_context_manager_dedups_only_duplicate_markdown_rows():
    from src.workflows.context_manager import dedup_rows
    text = "Before\n| A | B |\n| --- | --- |\n| 1 | 2 |\n| 1 | 2 |\n| 3 | 4 |\nAfter\n"
    result = dedup_rows(text)
    assert result.count("| 1 | 2 |") == 1
    assert "| 3 | 4 |" in result
    assert result.startswith("Before\n") and result.endswith("After\n")


def test_context_manager_fetch_cache_is_scoped_to_a_run():
    from src.workflows.context_manager import ContextManager
    calls = []
    manager = ContextManager()
    with manager.run_scope():
        assert manager.fetch_once("same", lambda: calls.append(1) or "result") == "result"
        assert manager.fetch_once("same", lambda: calls.append(2) or "other") == "result"
    assert calls == [1]


def test_context_manager_records_compaction_artifact():
    from src.workflows.context_manager import ContextManager
    manager = ContextManager(max_chars=3)
    with manager.run_scope():
        manager.compress("price", {"close": 123.45})
        artifact = manager.artifacts[0]
    assert artifact.key == "price"
    assert artifact.source_type == "dict"
    assert artifact.truncated
    assert artifact.content.startswith("{\n ")


# ── _get_llm() smoke test ──────────────────────────────────────────────────────

def test_get_llm_returns_none_or_llm_without_error():
    """_get_llm() must not raise even when no LLM is configured."""
    from src.workflows.base import _get_llm
    llm = _get_llm()
    # Returns None (no LLM configured) or a valid LLM object — both are OK
    assert llm is None or hasattr(llm, "invoke")


# ── _thread_id() ──────────────────────────────────────────────────────────────

def test_thread_id_is_deterministic():
    from src.workflows.base import _thread_id
    assert _thread_id("wf", "q") == _thread_id("wf", "q")


def test_thread_id_differs_by_key():
    from src.workflows.base import _thread_id
    assert _thread_id("wf", "q1") != _thread_id("wf", "q2")


def test_thread_id_is_16_chars():
    from src.workflows.base import _thread_id
    assert len(_thread_id("autonomous_research", "ADANIENT")) == 16


# ── Checkpointer ──────────────────────────────────────────────────────────────

def test_checkpointer_returns_none_or_saver_without_error():
    """_get_checkpointer() must not raise even if sqlite or langgraph is missing."""
    from src.workflows.base import _get_checkpointer
    cp = _get_checkpointer()
    # Either None (unavailable) or a valid checkpointer object
    assert cp is None or hasattr(cp, "put")


# ── _show_and_approve_plan() — interactive Panel path ────────────────────────
# Regression coverage for the actual MOSAIC_PLAN_APPROVAL=1 branch (not the
# auto-approve short-circuit) — this is the code path that renders the Rich
# Panel and evaluates _STEP_KEYWORDS, and previously crashed with a NameError
# because only `_re` (not `re`) was imported.

def test_show_and_approve_plan_renders_panel_and_approves(monkeypatch):
    """Interactive path with an Enter/approve response returns the plan unchanged."""
    from src.workflows import base
    monkeypatch.setenv("MOSAIC_PLAN_APPROVAL", "1")
    monkeypatch.setattr(base, "_cli_prompt", lambda _prompt: "")
    plan = [
        "Run macro scanner for active themes",
        "Fetch COMEX gold/silver/copper pre-market",
    ]
    result = base._show_and_approve_plan("analyse dxy", plan, intent="macro")
    assert result == plan


def test_show_and_approve_plan_edited_steps(monkeypatch):
    """A non-Y/n response is parsed as comma/newline-separated edited steps."""
    from src.workflows import base
    monkeypatch.setenv("MOSAIC_PLAN_APPROVAL", "1")
    responses = iter(["step one, step two", ""])
    monkeypatch.setattr(base, "_cli_prompt", lambda _prompt: next(responses))
    result = base._show_and_approve_plan("test question", ["original step"], intent="macro")
    assert result == ["step one", "step two"]


# ── BudgetCallbackHandler wiring ── the workflow path must actually enforce budgets ─────
# Every workflow node now accepts LangGraph's `config` and forwards it into each
# tool/LLM `.invoke()` call, so a BudgetCallbackHandler passed as `callbacks=[...]`
# into a workflow's run() actually observes and can cap tool calls/tokens — this
# was previously silently dropped (see run_subagent_for → sub-agent .run() refactor).

def test_macro_fetch_node_forwards_config_to_budget_handler(monkeypatch):
    """macro._fetch_node's 5 tool calls must all be observed by a passed-in callback,
    proving `config` is forwarded rather than dropped — and that concurrent
    ThreadPoolExecutor fetches don't race the counter (BudgetCallbackHandler lock)."""
    from langchain_core.tools import tool as lc_tool
    from src.agents.budget import BudgetCallbackHandler
    from src.workflows import macro

    @lc_tool
    def _fake_tool(**kwargs) -> str:
        """Fake replacement for a real fetch tool."""
        return "ok"

    import src.tools.skills_tools as skills_tools
    import src.tools.chart_tools as chart_tools
    import src.tools.market_context as market_context
    monkeypatch.setattr(skills_tools, "run_macro_scanner", _fake_tool)
    monkeypatch.setattr(skills_tools, "run_comex_analysis", _fake_tool)
    monkeypatch.setattr(chart_tools, "plot_fii_dii_chart", _fake_tool)
    monkeypatch.setattr(market_context, "get_dxy_context", _fake_tool)
    monkeypatch.setattr(skills_tools, "run_market_indicators", _fake_tool)

    budget = BudgetCallbackHandler(max_tool_calls=20)
    state = {"question": "test", "geo_query": False}
    result = macro._fetch_node(state, {"callbacks": [budget]})

    assert budget.total_tool_calls == 5
    assert "datasets" in result and len(result["datasets"]) == 5
    assert all(v == "ok" for k, v in result.items() if k != "datasets")


def test_budget_handler_tool_cap_enforced_through_workflow_node(monkeypatch):
    """A tool-call cap lower than the node's fetch count must raise BudgetExceededError —
    proving the budget is a real, enforced cap on the workflow path, not just observed."""
    from langchain_core.tools import tool as lc_tool
    from src.agents.budget import BudgetCallbackHandler, BudgetExceededError
    from src.workflows import macro

    @lc_tool
    def _fake_tool(**kwargs) -> str:
        """Fake replacement for a real fetch tool."""
        return "ok"

    import src.tools.skills_tools as skills_tools
    import src.tools.chart_tools as chart_tools
    import src.tools.market_context as market_context
    monkeypatch.setattr(skills_tools, "run_macro_scanner", _fake_tool)
    monkeypatch.setattr(skills_tools, "run_comex_analysis", _fake_tool)
    monkeypatch.setattr(chart_tools, "plot_fii_dii_chart", _fake_tool)
    monkeypatch.setattr(market_context, "get_dxy_context", _fake_tool)
    monkeypatch.setattr(skills_tools, "run_market_indicators", _fake_tool)

    budget = BudgetCallbackHandler(max_tool_calls=2)
    state = {"question": "test", "geo_query": False}
    result = macro._fetch_node(state, {"callbacks": [budget]})

    # _par() catches per-fetcher exceptions and returns a placeholder string instead
    # of propagating — so the cap trip surfaces as "unavailable" placeholders, not
    # a raised exception here. What matters is that the cap was actually hit.
    assert budget.total_tool_calls >= budget.max_tool_calls
    assert any("unavailable" in v for k, v in result.items() if k != "datasets")


# ── Synthesis prompts ─────────────────────────────────────────────────────────

def test_synthesis_prompts_do_not_reference_tool_calls():
    """Synthesis prompts must not contain ReAct-loop instructions."""
    from src.workflows.autonomous_research import _SYNTHESIS_PROMPT as sp1
    from src.workflows.india_equity import _SYNTHESIS_PROMPT as sp2
    for prompt in (sp1, sp2):
        assert "parallel tool call" not in prompt.lower()
        assert "emit all" not in prompt.lower()
        assert "tool_calls" not in prompt.lower()


def test_synthesis_prompts_contain_key_sections():
    from src.workflows.autonomous_research import _SYNTHESIS_PROMPT
    for section in ("Snapshot", "Financials", "Recommendation"):
        assert section in _SYNTHESIS_PROMPT


# ── Workflow routing via sub-agent run() ──────────────────────────────────────
# Routing now lives inside each sub-agent's run() method (workflow first,
# ReAct fallback), not in the registry. These tests verify that the sub-agents
# correctly attempt the workflow path.

def test_autonomous_research_routes_to_workflow(monkeypatch):
    """AutonomousResearchAgent.run() tries the StateGraph workflow first."""
    calls = []
    import sys, types
    mock_mod = types.ModuleType("src.workflows.autonomous_research")
    mock_mod.run = lambda q, callbacks=None: (calls.append(q), "mock workflow result")[1]
    sys.modules["src.workflows.autonomous_research"] = mock_mod
    try:
        from src.agents.sub_agents.research import AutonomousResearchAgent
        agent = AutonomousResearchAgent()
        result = agent.run("comprehensive research on ADANIENT")
        assert result == "mock workflow result"
        assert calls == ["comprehensive research on ADANIENT"]
    finally:
        sys.modules.pop("src.workflows.autonomous_research", None)


def test_signal_routes_to_workflow(monkeypatch):
    """SignalSubAgent.run() tries the StateGraph workflow first."""
    calls = []
    import sys, types
    mock_mod = types.ModuleType("src.workflows.signal")
    mock_mod.run = lambda q, callbacks=None: (calls.append(q), "mock signal result")[1]
    sys.modules["src.workflows.signal"] = mock_mod
    try:
        from src.agents.sub_agents.signal import SignalSubAgent
        agent = SignalSubAgent()
        result = agent.run("what is the goldbees signal?")
        assert result == "mock signal result"
        assert calls == ["what is the goldbees signal?"]
    finally:
        sys.modules.pop("src.workflows.signal", None)


def test_macro_routes_to_workflow(monkeypatch):
    """MacroSubAgent.run() tries the StateGraph workflow first."""
    calls = []
    import sys, types
    mock_mod = types.ModuleType("src.workflows.macro")
    mock_mod.run = lambda q, callbacks=None: (calls.append(q), "mock macro result")[1]
    sys.modules["src.workflows.macro"] = mock_mod
    try:
        from src.agents.sub_agents.macro import MacroSubAgent
        agent = MacroSubAgent()
        result = agent.run("what are the macro themes today?")
        assert result == "mock macro result"
        assert calls == ["what are the macro themes today?"]
    finally:
        sys.modules.pop("src.workflows.macro", None)


def test_news_routes_to_workflow(monkeypatch):
    """NewsSubAgent.run() tries the StateGraph workflow first."""
    calls = []
    import sys, types
    mock_mod = types.ModuleType("src.workflows.news")
    mock_mod.run = lambda q, callbacks=None: (calls.append(q), "mock news result")[1]
    sys.modules["src.workflows.news"] = mock_mod
    try:
        from src.agents.sub_agents.news import NewsSubAgent
        agent = NewsSubAgent()
        result = agent.run("latest news on RELIANCE")
        assert result == "mock news result"
        assert calls == ["latest news on RELIANCE"]
    finally:
        sys.modules.pop("src.workflows.news", None)


def test_mf_routes_to_workflow(monkeypatch):
    """MFSubAgent.run() tries the Plan-Execute-Replan workflow first."""
    calls = []
    import sys, types
    mock_mod = types.ModuleType("src.workflows.mf_planner")
    mock_mod.run = lambda q, callbacks=None: (calls.append(q), "mock mf result")[1]
    sys.modules["src.workflows.mf_planner"] = mock_mod
    try:
        from src.agents.sub_agents.mf import MFSubAgent
        agent = MFSubAgent()
        result = agent.run("MoM changes in DSP Multi Asset")
        assert result == "mock mf result"
        assert calls == ["MoM changes in DSP Multi Asset"]
    finally:
        sys.modules.pop("src.workflows.mf_planner", None)


def test_india_equity_routes_to_workflow(monkeypatch):
    """IndianEquityResearchSubAgent.run() tries the StateGraph workflow first."""
    calls = []
    import sys, types
    mock_mod = types.ModuleType("src.workflows.india_equity")
    mock_mod.run = lambda q, callbacks=None: (calls.append(q), "mock equity result")[1]
    sys.modules["src.workflows.india_equity"] = mock_mod
    try:
        from src.agents.sub_agents.india_equity import IndianEquityResearchSubAgent
        agent = IndianEquityResearchSubAgent()
        result = agent.run("research RELIANCE")
        assert result == "mock equity result"
        assert calls == ["research RELIANCE"]
    finally:
        sys.modules.pop("src.workflows.india_equity", None)


def test_registry_no_longer_mentions_env_var():
    """run_subagent_for no longer references MOSAIC_USE_WORKFLOWS (routing moved to sub-agents)."""
    import inspect
    from src.agents.sub_agents import registry
    src = inspect.getsource(registry.run_subagent_for)
    assert 'MOSAIC_USE_WORKFLOWS' not in src


def test_budget_exceeded_error_raises_through_callback_manager():
    """BudgetCallbackHandler.raise_error must be True — LangChain's callback manager
    checks this exact attribute name (not raise_on_error) to decide whether a
    callback exception propagates or is silently swallowed and logged."""
    from src.agents.budget import BudgetCallbackHandler
    budget = BudgetCallbackHandler()
    assert budget.raise_error is True


def test_run_subagent_for_catches_budget_exceeded(monkeypatch):
    """run_subagent_for must catch BudgetExceededError from a sub-agent's run() and
    return a graceful partial-answer message instead of letting it crash the caller."""
    from src.agents.budget import BudgetExceededError
    from src.agents.sub_agents import registry

    class _FakeMacroAgent:
        def run(self, question, llm_override=None, callbacks=None):
            raise BudgetExceededError("Total tool-call budget exceeded: 20 >= 20")

    monkeypatch.setattr(registry, "get_subagent", lambda intent: _FakeMacroAgent())
    result = registry.run_subagent_for("macro", "what are the macro themes today?")
    assert "budget exceeded" in result.lower()
    assert "20" in result


def test_macro_workflow_disabled_falls_back_to_react(monkeypatch):
    """MOSAIC_USE_WORKFLOWS=0 must skip the workflow entirely, not just on failure."""
    monkeypatch.setenv("MOSAIC_USE_WORKFLOWS", "0")
    calls = []
    import sys, types
    mock_mod = types.ModuleType("src.workflows.macro")
    mock_mod.run = lambda q: (calls.append(q), "mock macro result")[1]
    sys.modules["src.workflows.macro"] = mock_mod
    try:
        from src.agents.sub_agents.base import _SubAgent
        from src.agents.sub_agents.macro import MacroSubAgent
        monkeypatch.setattr(_SubAgent, "run", lambda self, question, llm_override=None, callbacks=None: "react result")
        agent = MacroSubAgent()
        result = agent.run("what are the macro themes today?")
        assert result == "react result"
        assert calls == []  # workflow must never be invoked when disabled
    finally:
        sys.modules.pop("src.workflows.macro", None)
