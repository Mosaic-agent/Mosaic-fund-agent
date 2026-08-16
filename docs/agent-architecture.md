# Agent Architecture

> Last updated: 2026-07-30 (Declarative agent orchestration, notice replacement trimmer, event bus observers, RAG architecture)

This document details the multi-agent orchestration layer of the Mosaic Fund Agent platform. For the broader system architecture (data pipeline, ClickHouse schema, ML, tools), see [architecture.md](architecture.md).

---

## Overview

The platform uses a **Multi-Agent Orchestrator** pattern built on [LangGraph](https://langchain-ai.github.io/langgraph/) `create_react_agent`. A main `MosaicFundAgent` delegates user queries to 10 specialised sub-agents via an **LLM-based Intent Router**, with middleware for **tracing** and **budget enforcement** on every invocation.

```
User Query
    │
    ├─── CLI: python src/main.py research / portfolio-wf
    │         ↓
    │    StateGraph Workflow (src/workflows/)
    │    Pure-Python nodes + 1-2 LLM calls — no agent overhead
    │
    └─── Chat / Agent path:
         ▼
┌─────────────────────────────┐
│  Intent Router              │
│  (LLM classifier →          │
│   regex fallback)           │
│  src/agents/intent_router.py│
└──────────┬──────────────────┘
           │  intent: "signal" | "macro" | "deepdive" | …
           ▼
┌──────────────────────────────────────────────────┐
│  run_subagent_for(question)                      │
│  src/agents/sub_agents/registry.py               │
│                                                  │
│  Middleware auto-attached:                        │
│  ┌───────────────────┐  ┌─────────────────────┐  │
│  │ TracingCallback    │  │ BudgetCallback      │  │
│  │ → agent_traces     │  │ 20 calls / 30k tok  │  │
│  │   table            │  │ / 180s wall-clock   │  │
│  └───────────────────┘  └─────────────────────┘  │
│                                                  │
│  Sub-Agent (lazy-init LangGraph ReAct)           │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐         │
│  │ToolNode  │ │System    │ │LLM       │         │
│  │(parallel)│ │Prompt    │ │(local or │         │
│  │          │ │+ rules   │ │ cloud)   │         │
│  └──────────┘ └──────────┘ └──────────┘         │
└──────────────────────────────────────────────────┘
           │
           ▼
       Response (Markdown text)
```

---

## Declarative Agent Orchestration & Playbooks

**Directory:** `src/agents/declarative/` & `config/agents/`

Mosaic supports **configuration-driven declarative playbooks** (`config/agents/*.yaml`) for predictable, zero-hallucination agent execution.

```
┌─────────────────────────────────────────────────────────────┐
│ Declarative Execution Pipeline                              │
├─────────────────────────────────────────────────────────────┤
│ 1. Pydantic Spec Validation  │ declarative_spec.py          │
│ 2. Parallel auto_tools Fetch │ ThreadPoolExecutor + 30s cap │
│ 3. LLM Reason Pass (XML tags)│ Claude-native prompt tags    │
│ 4. Jinja2 Template Rendering │ safe_from_json + xml_tag     │
└─────────────────────────────────────────────────────────────┘
```

### Key Components

1. **Pydantic Contract Validator (`declarative_spec.py`)**:
   - Parses YAML playbooks into strongly-typed `DeclarativeAgentSpec` instances.
   - Enforces strict validation on Jinja2 template syntax, step IDs, model tags, and step-type parameters (`auto_tools`, `reason`, `template_output`, `use_tools`).

2. **Multi-Threaded Execution Engine (`declarative_runner.py`)**:
   - Manages an in-memory thread-safe `ContextRun` blackboard mapping `step_id` outputs to template contexts.
   - Executes deterministic `auto_tools` tool calls concurrently via `ThreadPoolExecutor` with a default 30-second per-tool timeout.
   - Includes custom Jinja2 filters: `from_json` (safely parses JSON strings) and `xml_tag` (wraps content in XML tags).

3. **Sub-Agent Registry Adapter (`DeclarativeSubAgentAdapter` in `registry.py`)**:
   - `get_subagent(intent)` automatically detects `config/agents/<intent>.yaml` playbooks.
   - Extracts stock/ETF symbols accurately via `resolve_company_info` and regex heuristics.
   - Wires `callbacks` (such as `TracingCallbackHandler`) directly into the runner so all tool execution steps are logged to `market_data.agent_traces` in ClickHouse.

4. **Claude-Native XML Tag Structuring**:
   - Prompts and templates in playbooks (`india_equity.yaml`, `goldbees_pipeline.yaml`) wrap data boundaries and directives in explicit XML tags (`<task_context>`, `<source_priority>`, `<market_data>`, `<instructions>`).
   - Enforces price priority rules: **Shoonya Live LTP** $\rightarrow$ **ClickHouse NSE EOD** $\rightarrow$ **Yahoo Finance**.

---

## Notice Replacement Context Trimmer

**File:** `src/agents/sub_agents/infra.py` → `_make_context_trimmer()`

For multi-turn ReAct loops running on local models, Mosaic uses **Notice Replacement** to manage context window budgets without triggering agent amnesia:

- **Current Turn Tool Cap**: Hard-truncates current-turn `ToolMessage` outputs exceeding 10% of context capacity.
- **Notice Replacement Phase**: When total prompt tokens exceed 50% capacity, verbose historical `ToolMessage` outputs from prior turns are collapsed into compact metadata notices (`[Historical Tool Result Pruned: tool 'get_company_snapshot' (original size 4,200 chars)...]`).
- **Preserves Reasoning History**: Keeps 100% of past `AIMessage` thoughts and tool-call signatures intact.
- **Emergency Fallback**: Evicts the oldest full round-trip only as a last resort if prompt tokens still exceed capacity.

---

**File:** `src/agents/intent_router.py`

The router classifies free-form user questions into one of 10 intents, which map 1:1 to sub-agents.

### LLM Router (primary)

- Model: `gpt-4o-mini` (OpenAI) / `claude-haiku-4` (Anthropic) / `gemini-2.0-flash` (Google)
- Cost: ~$0.0001 per classification
- JSON-mode output: `{"intent": "signal", "confidence": 0.92}`
- LRU cache: 256 entries keyed by question hash — repeat questions are free
- `clear_intent_cache()` provided for testing

### Regex Router (fallback)

- **File:** `src/agents/sub_agents/routing.py` → `_regex_route_intent()` / `route_intent()`
- 12 compiled regex patterns (`_SIGNAL_RE`, `_DEEPDIVE_RE`, `_MACRO_RE`, `_MF_RE`, `_IMPORT_RE`, `_DB_RE`, `_CODE_RE`, `_NEWS_RE`, `_INTL_ETF_RE`, `_RESEARCH_RE`, `_GENERAL_RESEARCH_RE`, `_CLOUD_NEEDED_RE`)
- 3-case fast path (`_fast_path_intent`) fires before LLM: `import/refresh/sync` → `main`; explicit SQL → `database`; bare 1-2 word ticker → `india_equity` or `signal`
- **Table-driven dispatch** — `_regex_route_intent` uses three ordered tables instead of a 45-line if-elif block:
  - `_PRE_PLOT_TABLE` (4 entries) — deepdive, main, database, code (must fire before visualisation branch)
  - `_VIZ_ROUTE_TABLE` (3 entries) — intl_etf, macro, mf (checked only when query contains "plot"/"chart"/"show")
  - `_POST_PLOT_TABLE` (6 entries) — signal, intl_etf, mf, research, macro, news
  - Adding a new intent = one `insert()` at the correct table position; priority is explicit by index
- Activates when: no cloud API key configured, LLM call fails, or network error
- Deterministic, zero-cost, zero-latency

### Intent → Sub-Agent Map

| Intent | Sub-Agent Class | Trigger Examples |
|---|---|---|
| `deepdive` | `DeepDiveSubAgent` | "10-K filing for AAPL", "SEC filings" |
| `india_equity` | `IndianEquityResearchSubAgent` | "research RELIANCE", "TCS quarterly results" |
| `signal` | `SignalSubAgent` | "today's GOLDBEES signal", "run pipeline", "composite score" |
| `macro` | `MacroSubAgent` | "COMEX pre-market", "FII flows today", "macro risks" |
| `news` | `NewsSubAgent` | "HDFC Bank news", "market sentiment" |
| `code` | `CodeSubAgent` | "run this Python", "query ClickHouse" |
| `database` | `DatabaseSubAgent` | "show schema", "watermark status", "row counts" |
| `mf` | `MFSubAgent` | "DSP multi asset holdings", "which funds hold Reliance", "cross-fund consensus" |
| `intl_etf` | `IntlETFSubAgent` | "MAFANG performance", "Hang Seng premium", "run ETF premium / OU backtest on MON100", "PELT regime backtest", "confidence threshold 60" |
| `research` | `AutonomousResearchAgent` | "comprehensive research ADANIENT", "why is gold falling", "cross-asset analysis" |
| `main` | `MosaicFundAgent` (direct) | General / unclassifiable queries |

---

## Sub-Agent Base Class

**File:** `src/agents/sub_agents/base.py` → `_SubAgent`

All sub-agents inherit from `_SubAgent`, which provides:

```python
class _SubAgent:
    SYSTEM_PROMPT: str    # Domain-specific instructions (override in subclass)
    TOOLS: list           # Tool set (override _get_tools() in subclass)
    RECURSION_LIMIT: int  # Max LangGraph steps (default: 20)

    def _select_llm(llm_override)  # Hook: local → cloud-upgrade → None. Override to inject a domain-specific model before base logic runs (e.g. CodeSubAgent)
    def _build()          # Lazy-init: calls _select_llm(), installs _NullAgent on failure, wraps tools, builds ToolNode + create_react_agent
    def run(question)     # Invoke the ReAct agent and return text
    def _fallback()       # Programmatic data gathering when LLM can't call tools
```

### Key design decisions

1. **Lazy initialisation** — LLM and agent are built on first `run()` call, not at import time
2. **Parallel tool execution** — `ToolNode` runs all tool calls from a single `AIMessage` concurrently via `ThreadPoolExecutor`
3. **Hook Method for LLM selection** — `_select_llm(llm_override)` encapsulates the full local→cloud-upgrade→None resolution. Subclasses that need a domain-specific model (e.g. `CodeSubAgent` with `CODE_LLM_PROVIDER`) override only `_select_llm()` and call `super()._select_llm(llm_override)`. `_build()` assembly logic is never duplicated.
4. **Null Object for absent LLM** — `_build()` always assigns `self._agent`: a real LangGraph agent or `_NullAgent()`. `run()` needs no `None` guard; `_NullAgent.stream()` raises `"not support tool calling"` which hits the existing error handler → `_confirm_fallback()`. New no-LLM modes = one class to change.
5. **Strategy fallback** — When LLM tool-calling fails entirely (e.g. gemma4 with 4k context), `_fallback()` collects data via direct Python function calls, then a single LLM synthesis call produces the narrative

---

## Sub-Agents

### DeepDiveSubAgent

- **Purpose:** US stock SEC filings (10-K/10-Q), XBRL financials, peer valuation
- **Tools (~6):** `resolve_company`, Yahoo Finance tools, earnings scraper, `run_deepdive_analysis`, `query_clickhouse_db`
- **Behaviour:** Starts with `resolve_company`; if Indian stock detected → redirects to `IndianEquityResearchSubAgent`

### IndianEquityResearchSubAgent

- **Purpose:** NSE/BSE company research — price, earnings, cashflow, holdings, news, anomaly correlation
- **Tools (~19):** `resolve_company`, Yahoo Finance, `get_quarterly_results`, `get_stock_cashflow`, `get_db_price_summary`, `plot_price_chart`, `plot_shareholding_bar`, `plot_macd_chart`, `get_mf_holdings_for_stock`, `get_stock_news`, `get_newsapi_stock_news`, `query_clickhouse_db`, `import_symbol_data`, `check_and_refresh_symbol_data`, `search_anomaly_events`, `find_anomaly_correlations`, `publish_research_pdf`, `publish_consolidated_pdf`
- **Recursion limit:** 50 (needs more steps for parallel multi-tool batches + anomaly correlation)
- **Workflow:** Round 1 → resolve symbol; Round 2 → emit all data-fetching tools in parallel in one AIMessage
- **Fallback:** `_gather_indian_equity_data()` in `equity_gatherer.py` — programmatic data collection

### SignalSubAgent

- **Purpose:** ETF composite scores, ML prediction, Kelly weights, GARCH vol-targeting, iNAV, and anomaly explanation
- **Tools (~19):** `run_daily_signal_composite`, `run_goldbees_pipeline`, `run_etf_news_sentiment`, `run_risk_governor_analysis`, `run_premium_alerts`, `get_live_inav`, `query_clickhouse_db`, `explain_price_anomalies`, `search_anomaly_events`, `find_anomaly_correlations`, `plot_price_chart`, `plot_signal_scores`, `plot_signal_breakdown`, `plot_weight_recommendations`, `plot_garch_volatility_chart`, `plot_multi_price_chart`, `plot_macd_chart`, `get_shoonya_quotes`, `get_shoonya_live_tick`, `publish_consolidated_pdf`
- **Rule:** Never invent composite scores or regime labels — only narrate tool output
- **Anomaly tool:** `explain_price_anomalies` calls `run_composite_anomaly` (GARCH + IF + MAD-Z) on full OHLCV history, surfaces per-date `regime` + `Final Z`, correlates news, and appends ML forward context (`ml_prediction_asof`, `signal_composite_asof`) — always invoke `plot_price_chart` in parallel
- **Fallback (`_fallback`):** keyword-routed programmatic path for local models that can't emit tool-call JSON. Detects intent from the question and calls tools directly — *anomaly/spike/crash* → `explain_price_anomalies` + `plot_price_chart`; *signal/pipeline/goldbees* → `run_goldbees_pipeline`; *composite/score* → `run_daily_signal_composite`. Extracts symbol + time window (`30 days`, `3 months`, `1 year`) from the prompt, then runs an optional single LLM synthesis pass over the tool output.

### MacroSubAgent

- **Purpose:** COMEX pre-market, FII/DII flows, macro themes, geopolitics, market indicators, DXY
- **Tools (~12):** `run_macro_scanner`, `run_comex_analysis`, `query_clickhouse_db`, `run_whale_tracker`, `run_market_indicators`, `get_dxy_context`, `search_financial_news`, `get_db_news`, `find_anomaly_correlations`, `plot_fii_dii_chart`, `plot_price_chart`, `plot_dxy_chart`

### MFSubAgent

- **Purpose:** Indian mutual-fund analysis — holdings, NAV returns, cross-fund consensus, whale tracking (both the per-fund theme tracker AND the full-universe cross-AMC accumulation scanner)
- **Tools (~23):** `run_multi_asset_holdings_mom_yoy`, `run_multi_asset_consensus`, `run_whale_tracker`, `run_dsp_multi_asset_comparison`, `run_fund_mom_returns`, `run_dsp_multi_asset_importer`, `run_nippon_importer`, `run_icici_importer`, `run_all_multi_asset_importers`, `scan_whale_accumulation`, `get_whale_consensus`, `get_mf_holdings_for_stock`, `find_funds_holding`, `find_similar_funds`, `search_mf_exposure`, `plot_fund_holdings_chart`, `plot_price_chart`, `query_clickhouse_db`, `describe_db_table`, `list_db_tables`, `sample_db_table`, `search_db_metadata`, `get_stock_news`, `publish_consolidated_pdf`
- **Recursion limit:** 30
- **Routing keywords:** "mutual fund", "DSP multi asset", "cross-fund consensus", "which funds hold", "NAV return", "holding pattern", "institutional accumulation", "which consensus buys are technically attractive"
- **Fallback:** keyword-routed — `consensus/accumulation/institutions buying` → `scan_whale_accumulation` (checked before the theme branch below since both can match "whale"; `technical/rsi/breakout/oversold/drawdown` in the query sets `with_technicals=True`); `mom/yoy/changes` → `run_multi_asset_holdings_mom_yoy`; `which funds hold` → `get_mf_holdings_for_stock`; `whale/theme/thematic` → `run_whale_tracker`; `nav return` → `run_fund_mom_returns`

### NewsSubAgent

- **Purpose:** Latest news headlines, sentiment, and anomaly explanation per stock/ETF
- **Tools (~12):** `resolve_company`, `get_stock_news`, `get_newsapi_stock_news`, `search_financial_news`, `get_db_news`, `run_etf_news_sentiment`, `explain_price_anomalies`, `search_anomaly_events`, `find_anomaly_correlations`, `plot_price_chart`, `publish_research_pdf`, `publish_consolidated_pdf`

### CodeSubAgent

- **Purpose:** Ad-hoc Python execution, ClickHouse queries, script writing
- **Tools:** `CODE_TOOLS` + `query_clickhouse_db` + `CHART_TOOLS`
- **LLM hook:** Overrides `_select_llm()` — tries `_build_code_llm()` (`CODE_LLM_PROVIDER`) first; if unavailable falls through to `super()._select_llm()` (standard local → cloud upgrade). No `_build()` override needed.

### DatabaseSubAgent

- **Purpose:** ClickHouse schema inspection, watermarks, data freshness, NL → SQL
- **Tools:** `DB_TOOLS` + `CHART_TOOLS` (all chart tools included for post-query visualisation)

### IntlETFSubAgent

- **Purpose:** International ETFs (MAFANG, HNGSNGBEES, MON100, MASPTOP50, MAHKTECH, MONQ50) — performance, scarcity premium, PELT regime backtest, KMeans regimes, seasonality, correlation, LightGBM feature importance, drawdowns, OU mean-reversion
- **Tools (~14):** `INTL_ETF_TOOLS` (8: performance, premium, regimes, seasonality, correlation, drawdowns, lgbm, **run_ou_regime_backtest**) + `plot_intl_etf_performance` + `plot_intl_etf_premium` + `plot_ou_premium_chart` + `plot_price_chart` + `publish_research_pdf` + `publish_consolidated_pdf`
- **Routing rules:** RULE 1a — backtest / walk-forward / PELT / confidence-threshold keywords → `run_ou_regime_backtest` (ETF Premium Strategy / OU regime backtest); RULE 1b — OU / mean-reversion / half-life keywords → `plot_ou_premium_chart`

### AutonomousResearchAgent (`research`)

- **Purpose:** Multi-domain, self-directed 10-layer research framework: entity resolution → price/momentum → fundamentals → institutional footprint → macro → news intelligence → volatility/signals → correlation/ML → visualisation → synthesis
- **Tools (~30):** Union of Yahoo Finance, Indian equity, skills, macro, news, intl ETF, code execution, chart, and `AGENT_TOOLS` (delegation tools: `delegate_to_signal_agent`, `delegate_to_macro_agent`, `delegate_to_intl_etf_agent`, `delegate_to_news_agent`, `delegate_to_india_equity_agent`, `check_and_refresh_symbol_data`)
- **Recursion limit:** 50
- **Delegation:** Can hand off to specialised sub-agents for GOLDBEES ML, COMEX commodities, intl ETF deep dives, multi-source news sweeps, or full equity research notes

---

## Workflows (`src/workflows/`)

Workflows are **LangGraph `StateGraph` pipelines** for tasks with a fixed, known
structure. Unlike ReAct sub-agents (which resend the full system prompt on every
tool call), workflows use pure-Python nodes for all data fetch and reserve LLM calls
only for synthesis and adversarial verification.

### Why workflows instead of sub-agents for these tasks

| | ReAct sub-agent | StateGraph workflow |
|---|---|---|
| System prompt cost | Resent every step (×15–50) | Zero for data nodes |
| Parallelism | LLM must emit correct parallel tool calls | `ThreadPoolExecutor` — guaranteed |
| Section completeness | Silently skipped on token pressure | Every node always runs |
| Adversarial verify | Not built in | Dedicated `verify` node |
| Typical token cost | 15,000–42,000 | 4,000–9,800 |

### Workflow catalogue

| Workflow | File | Nodes | LLM calls | Est. tokens |
|---|---|---|---|---|
| `run_autonomous_research(question)` | `autonomous_research.py` | resolve → fetch_all → correlate → **verify** → synthesise | 2 | ~8,800 |
| `run_india_equity_research(question)` | `india_equity.py` | resolve → fetch_all (12 tools, guaranteed) → synthesise | 1 | ~7,000 |
| `run_multi_fund_consensus(period)` | `multi_fund_consensus.py` | fetch_all_funds (7 parallel) → fetch_consensus → synthesise | 1 | ~4,000 |
| `run_portfolio_analysis()` | `portfolio_analysis.py` | discover → enrich_all → score_all → **verify_high** → fetch_macro → synthesise | N+K+1 | ~9,800 |
| `run_signal(question)` | `signal.py` | resolve → build_plan → **[approval]** → fetch (6 parallel) → synthesise | 1 | ~4,000 |
| `run_macro(question)` | `macro.py` | build_plan → **[approval]** → fetch (all sources parallel) → synthesise | 1 | ~3,500 |
| `run_news(question)` | `news.py` | resolve → build_plan → **[approval]** → fetch (3 parallel) → aggregate (+ optional synth) | 0–1 | ~1,500 |
| `run_mf_planner(question)` | `mf_planner.py` | **plan** (LLM) → **[approval]** → executor ↺ replanner loop | 2–4 | ~6,000–12,000 |

### Shared infrastructure (`base.py`)

- **`_get_llm(prefer_cloud=True)`** — reuses `MosaicFundAgent._build_llm()` / `_build_cloud_llm()`; no duplicate LLM construction
- **`_par(fetchers: dict)`** — `ThreadPoolExecutor` fan-out: runs any dict of `{key: callable}` concurrently, returns `{key: result}`; failed fetchers return a `*key unavailable*` placeholder so synthesis always receives a complete state
- **`SYNTH_SUFFIX`** — the `NO_LLM_CALC_RULE` injected into every synthesis prompt

### How to invoke

```bash
# CLI — bypasses the agent entirely, no system-prompt overhead
python src/main.py research "comprehensive research on ADANIENT"
python src/main.py portfolio-wf

# Via chat — all 4 are @tool wrappers in SKILLS_TOOLS,
# callable from any sub-agent (e.g. AutonomousResearchAgent)
run_autonomous_research("research ADANIENT")
run_multi_fund_consensus_workflow("yoy")
```

### fetch_all parallelism detail (autonomous_research)

Six data groups run concurrently inside one `StateGraph` node:

| Group | Tools |
|---|---|
| price | `get_yahoo_finance_data`, `get_price_momentum`, `get_db_price_summary` |
| fundamentals | `get_quarterly_results`, `get_stock_cashflow` |
| institutional | `get_mf_holdings_for_stock`, `get_fii_dii_summary`, `plot_shareholding_bar` |
| macro | `run_macro_scanner`, `get_dxy_context` |
| news | `get_stock_news`, `get_newsapi_stock_news`, `search_financial_news` |
| volatility | `run_risk_governor_analysis`, `plot_price_chart`, `plot_macd_chart` |

### Adversarial verification nodes

Two workflows include an adversarial pass:

- **`verify` (autonomous_research)** — one LLM call: "generate 3 data-grounded bear cases that could invalidate a bullish thesis". Bear cases are injected into the synthesis prompt.
- **`verify_high` (portfolio_analysis)** — for each `HIGH`-conviction score, one LLM call tries to refute it. If refuted, conviction is downgraded to `MEDIUM` with the refutation reason appended to the rationale.

### Plan-Execute-Replan Pattern

**File:** `src/workflows/mf_planner.py`

A new agent pattern for open-ended mutual fund queries where the data needed depends on previous results. Unlike static parallel-fetch workflows, the MF Planner dynamically adapts its execution plan based on intermediate findings.

```
User question
     │
     ▼
┌──────────────┐
│  1. Planner  │  LLM decomposes question into 2–6 ordered steps
│  (1 LLM call)│  Pydantic schema: Plan { steps: list[str] }
└──────┬───────┘
       │
       ▼
┌──────────────┐
│  2. Approval │  _show_and_approve_plan() — Y/n/edit
│  (human)     │  Only in interactive CLI sessions
└──────┬───────┘
       │
       ▼
┌──────────────┐
│  3. Executor │  Mini-ReAct: one tool call per step
│  (0–1 LLM)  │  Uses full MF tool suite
└──────┬───────┘
       │
       ▼
┌──────────────┐     ┌─────────────────────────┐
│  4. Replanner│────▶│ action: "revise"         │
│  (1 LLM call)│     │ → rewrite remaining plan │
│              │     │ → loop back to Executor  │
│              │────▶│ action: "done"           │
│              │     │ → set response, → END    │
└──────────────┘     └─────────────────────────┘
```

**Self-improvement example:**
- Q: "Why is DSP trimming gold?"
- Initial plan: `[run_multi_asset_consensus, run MoM changes for DSP_MULTI_ASSET]`
- After step 1 → consensus shows Nippon ALSO trimming gold
- Replanner auto-adds: `[run MoM for NIPPON_INDIA_..., run_whale_tracker]`

**Pydantic schemas:**

```python
class Plan(BaseModel):
    steps: list[str]   # ordered list of 2–6 step descriptions

class ReplanDecision(BaseModel):
    action:       Literal["continue", "revise", "done"]
    revised_plan: list[str] = []   # populated when action="revise"
    response:     str = ""         # populated when action="done"
```

**Token savings:** ~6,000–12,000 vs ~25,000 for the ReAct equivalent (~55–76%).

**State:** `MFPlanExecute(MosaicState)` — extends the shared `MosaicState` TypedDict with `input`, `past_steps`, `step_count`, `max_steps` (default 8), `response`.

### Context Compression & Plan Store

#### ContextManager (`context_manager.py`)

> **Full reference:** [docs/context-manager.md](context-manager.md) — deterministic compaction mechanics, `DatasetRef` audit ledger, thread safety model, `contextvars` scoping, and `_par_datasets()` workflow integration (Issue [#156](https://github.com/Mosaic-agent/data_importer/issues/156)).

**File:** `src/workflows/context_manager.py`

Deterministic (no-LLM) context compression for StateGraph workflow fetch results. All compaction is rule-based — no summarisation that could lose numbers.

| Component | Purpose |
|---|---|
| `DatasetRef` | Frozen dataclass: prompt-ready fetch output + audit metadata (`original_chars`, `compacted_chars`, `rows_deduplicated`, `truncated`) |
| `ContextRun` | Thread-safe per-run ephemeral state (cache + artifacts list + `RLock`), scoped via `contextvars.ContextVar` |
| `truncate_text(text, max_chars=12_000)` | Head-truncate with explicit marker: `…[N chars trimmed — use narrower queries to fit context]` |
| `summarize_dict(raw)` | Lossless JSON serialization (no field derivation or dropping) |

`_par_datasets()` in `base.py` combines parallel fan-out (`_par()`) with automatic context compression, returning `dict[str, DatasetRef]` instead of raw values. Used by signal, macro, and news workflows.

#### PlanStore (`plan_store.py`)

**File:** `src/workflows/plan_store.py`

SQLite-backed plan persistence for workflow plan reuse and auditing.

| Function | Purpose |
|---|---|
| `save_plan(intent, question, steps)` | Store plan as JSON file + SQLite index entry at `output/plans/` |
| `find_similar_plans(question, intent, top_k)` | Jaccard token-overlap similarity search (no embedding model required) |
| `load_plan(plan_id)` | Retrieve stored plan by ID |

**Schema:** `plan_index` table — `id` (TEXT PK), `intent`, `question`, `steps_json`, `created_at`, `file_path`, `metadata_json`.

#### MosaicState (`state.py`)

**File:** `src/workflows/state.py`

Shared `TypedDict` ancestor for all workflow TypedDicts (`total=False` — subclasses only populate fields they use):

```python
class MosaicState(TypedDict, total=False):
    question: str
    plan: list[str]
    plan_id: str
    datasets: dict[str, DatasetRef]
```

#### Interactive Plan Approval (`_show_and_approve_plan()`)

**File:** `src/workflows/base.py`

Human-in-the-loop gate used by 4 workflows (signal, macro, news, mf_planner). Before executing the plan, a Rich terminal panel displays the ordered steps and prompts for Y/n/edit. Only active in interactive CLI sessions (`MOSAIC_INTERACTIVE_CHAT="1"`).

#### Shared Infrastructure Additions (`base.py`)

| Function | Purpose |
|---|---|
| `_par_datasets(fetchers, max_chars)` | Parallel fetch + context compression → `dict[str, DatasetRef]` |
| `_get_checkpointer()` | SQLite `SqliteSaver` for workflow resumption (same DB as chat sessions) |
| `_thread_id()` | Deterministic thread ID generation for checkpointer keying |
| `_show_and_approve_plan(question, plan, intent)` | Interactive Y/n/edit before workflow execution |

---

## Middleware

### TracingCallbackHandler

**File:** `src/agents/tracer.py`

Records every tool call and LLM invocation to `market_data.agent_traces` (ClickHouse).

| Column | Type | Description |
|---|---|---|
| `run_id` | String | 16-char hex per agent invocation |
| `agent` | String | Sub-agent intent (e.g. "signal") |
| `step_idx` | UInt16 | Sequential step within the run |
| `tool_name` | String | Tool function name |
| `latency_ms` | UInt32 | Wall-clock time for the step |
| `tokens_in` | UInt32 | Prompt tokens consumed |
| `tokens_out` | UInt32 | Completion tokens generated |
| `status` | String | "ok" or "error" |
| `error_class` | String | Exception class name (if failed) |

- **Best-effort writes** — errors are logged but never raised (tracing never blocks the agent)
- `log_trace()` helper for non-LangChain paths (e.g. router decisions)
- Table: `ReplacingMergeTree` partitioned by `toYYYYMM(created_at)`

### BudgetCallbackHandler

**File:** `src/agents/budget.py`

Enforces per-run resource limits. Raises `BudgetExceededError(RuntimeError)` on breach.

| Limit | Default | Purpose |
|---|---|---|
| `max_tool_calls` | 20 | Prevents runaway ReAct loops |
| `max_tokens` | 30,000 | Caps LLM spend per query |
| `max_wall_clock_s` | 180 | Hard timeout (3 minutes) |

**Per-tool caps** (`DEFAULT_TOOL_CAPS`):

| Tool | Max Calls | Rationale |
|---|---|---|
| `fetch_all_comex_signals` | 1 | Single-call design |
| `collate_news_sentiment` | 2 | At most 2 symbols |
| `run_pipeline` | 1 | Heavy ML workload |
| `run_deepdive_analysis` | 1 | Long-running SEC analysis |
| `run_data_engineering_importer` | 1 | Full import pipeline |

`.summary` property returns current usage: `{"tool_calls": 5, "tokens": 12340, "wall_clock_s": 22.1}`

### Middleware attachment

Both callbacks are **auto-attached** in `run_subagent_for()` — no per-agent wiring needed:

```python
# src/agents/sub_agents/registry.py — run_subagent_for()
callbacks = [TracingCallbackHandler(run_id, agent=intent), BudgetCallbackHandler()]
answer = subagent.run(question, callbacks=callbacks)
```

---

## LLM Response Cache (SQLite)

**File:** `src/utils/llm_cache.py`

A **SQLite-backed cache** (`output/.cache/llm_cache.db`) stores LLM responses keyed by prompt hash, so identical questions return instantly without re-hitting the model.

| Property | Value |
|---|---|
| Backend | SQLite (single-file, no server) |
| TTL | 24h (configurable) |
| Scope | Per-prompt response cache, distinct from the in-memory intent-router LRU and the joblib ML-model cache |

This is the **second storage engine** in the platform: ClickHouse holds market data (the quant engines read it), SQLite holds LLM responses (the agent layer reads it). Cache state is shown at startup, e.g. `llm_cache: enabled · ttl=24h · live=178 entries · size=1228kB`.

---

## Agentic Memory & Harness Architecture

The Mosaic platform is developed using **5 parallel agentic coding harnesses**, each with its own context injection mechanism, skills, and configuration. A layered memory hierarchy ensures consistent behaviour across all harnesses while allowing domain-specific extensions.

### 5-Layer Memory Hierarchy

```
┌────────────────────────────────────────────────────────┐
│ Level 1: Global User Mandate                          │
│ No LLM math · Zero-Trust Grounding · QIP Dilution     │
│ Check · DSP Conviction Signal · Single-Author Commits │
└───────────────────────────┬────────────────────────────┘
                            │
┌───────────────────────────▼────────────────────────────┐
│ Level 2: Harness Context Files                        │
│ AGENTS.md · GEMINI.md · docs/CLAUDE.md                │
│ Project architecture, DB schemas, design patterns     │
└───────────────────────────┬────────────────────────────┘
                            │
┌───────────────────────────▼────────────────────────────┐
│ Level 3: Subagent Definitions                         │
│ .agents/agents/*.md (21 YAML-frontmatter definitions) │
│ model, max_turns, tools, system prompts               │
└───────────────────────────┬────────────────────────────┘
                            │
┌───────────────────────────▼────────────────────────────┐
│ Level 4: Skill Specs & Commands                       │
│ .agents/skills/* (21 dirs) · .claude/commands/* (5)   │
│ CLI scripts, SQL queries, domain-specific parameters  │
└───────────────────────────┬────────────────────────────┘
                            │
┌───────────────────────────▼────────────────────────────┐
│ Level 5: Token-Compression Contracts                  │
│ .claude/skills/caveman* (7 files, skills-lock.json)   │
│ Cavecrew compressed output: investigator/builder/     │
│ reviewer delegation with 60–70% token savings         │
└────────────────────────────────────────────────────────┘
```

Each level overrides or specialises the one above. Global user mandates (Level 1) are injected by every harness and cannot be overridden by lower levels.

### Harness Configuration Matrix

| Harness | Context File | Config | Skills/Commands |
|---|---|---|---|
| **Claude Code** | `docs/CLAUDE.md` | `.claude/settings.local.json` (139 pre-approved patterns) | `.claude/commands/` (5: `/commit`, `/goldbees-pipeline`, `/intraday`, `/macro-strategy`, `/risk-governor`) + `.claude/skills/` (7 Cavecrew files + 8 Qdrant skill dirs) |
| **Codex (OpenAI)** | `AGENTS.md` | `.codex/config.toml` (MCP server: `ofin-pipeline`) | — |
| **Gemini CLI** | `GEMINI.md` | — | `docs/gemini-prompts.md` (20 structured prompts) |
| **Antigravity** | `AGENTS.md` + `GEMINI.md` (both auto-loaded) | `.antigravitycli/` (workspace registration) | `.agents/agents/` (21 agent defs) + `.agents/skills/` (21 skill dirs) |
| **Internal LangGraph** | `sub_agents/prompts.py` (`NO_LLM_CALC_RULE`) | `config/settings.py` | — |

### Context File Overlap

The three context files share ~70% of their content (architecture, CLI commands, ClickHouse schema, design patterns, mandatory rules). Each adds harness-specific extensions:

| Content | `AGENTS.md` | `GEMINI.md` | `docs/CLAUDE.md` |
|---|---|---|---|
| CLI commands & scripts | ✅ | ✅ | ✅ |
| ClickHouse DDL schemas | ✅ (full) | ✅ (full) | ✅ (summary) |
| Qdrant vector DB reference | ✅ | ✅ | ✅ (full with backfill) |
| Agent architecture cross-ref | ✅ | ✅ | ✅ |
| MCP tool mapping | — | — | ✅ |
| Antigravity slash commands | ✅ | ✅ | ✅ |
| Claude Code slash commands | ✅ | ✅ | ✅ |
| DSP fund scheme codes table | ✅ | — | — |
| Gemini prompt playbook | — | — (separate file) | — |

> **Note:** Future consolidation into a single source-of-truth file (e.g. `docs/project-context.md`) with harness-specific overlays would eliminate the maintenance burden of keeping three 25–30kB files in sync.

### Cross-Harness Consistency Rules

Six mandatory rules are enforced identically across all 5 harnesses:

1. **No LLM Calculations** — all numeric work in Python/SQL; the LLM only narrates tool output
2. **Zero-Trust Verification Protocol** — symbol-row locking, re-read mandate, overlay priority before citing any number
3. **No Co-Authored-By** in commit messages — single-author only
4. **QIP/dilution check** before flagging promoter sell-down — verify total share count expanded (QIP, preferential allotment, ESOP)
5. **DSP active-fund cross-ownership** as highest-conviction single-name signal (2+ active DSP funds × 24+ months)
6. **Pipeline grounding** — never invent composite scores, regime labels, or metrics beyond tool output

### Caveman/Cavecrew Token-Compression

**Source:** `JuliusBrussee/caveman` (GitHub), managed via `skills-lock.json`

Three compressed-output subagent contracts for Claude Code that reduce token usage by 60–70%:

| Role | Purpose | Output Format |
|---|---|---|
| `cavecrew-investigator` | Codebase research | File paths, line numbers, symbols — terse (~700 vs 2,000 tokens) |
| `cavecrew-builder` | Surgical 1–2 file edits | Edit confirmation + verification in compressed format |
| `cavecrew-reviewer` | Diff auditing | Line-level emoji severity flags (🔴 critical, 🟡 warning, 🟢 clean) |

### Known Discrepancies

| Issue | Detail | Status |
|---|---|---|
| venv path mismatch | `.codex/config.toml` and some `.claude/settings.local.json` entries reference `.venv_new/bin/python3`; `AGENTS.md`, `GEMINI.md`, and `ci.yml` standardise on `.venv/bin/python3` | Cosmetic |
| Broken Antigravity registration | `.antigravitycli/318cb5fd-...json` is a dangling reference | Cosmetic |
| Skill-to-command gap | 21 skill packages and 21 agent definitions, but only 5 Claude commands — high-value skills lack `.claude/commands/*.md` shortcuts | Feature gap |
| MCP fallback inconsistency | `macro-strategy.md` directs calling MCP `run_pipeline()`; `goldbees-pipeline.md` correctly mandates direct script execution | Documentation |

---

## Mandatory Rules

### No LLM Calculations

Every agent system prompt includes the `NO_LLM_CALC_RULE` (defined in `src/agents/sub_agents/prompts.py`):

> **NEVER compute, estimate, or derive any number** (returns, ratios, averages, percentages, scores, sums, differences, CAGR, PE, Kelly fractions, etc.) inside your response. ALL numeric work MUST be performed by a tool call (Python, SQL, or a dedicated function). You may ONLY narrate or format numbers that were returned verbatim by a tool.

This rule is injected at three levels:
1. `SubAgent._build()` — appended to every sub-agent's `SYSTEM_PROMPT` automatically
2. `AGENT_SYSTEM_PROMPT` — main MosaicFundAgent prompt
3. `AGENT_SYSTEM_PROMPT_COMPACT` — compact fallback for local models
4. `COMEX_SYSTEM_PROMPT` and `NEWS_SENTIMENT_SYSTEM_PROMPT` — standalone agent prompts

### Tool Loop Protection

- COMEX agent: direct function call for local LLMs (bypasses ReAct loop)
- News agent: single `collate_news_sentiment()` call (not a multi-tool loop)
- Budget handler: hard cap at 20 tool calls per run

---

## Name Resolution & Spelling Correction

**File:** `src/tools/company_resolver.py`

Before any stock research or deep-dive, the system uses the `resolve_company` tool to resolve human queries (like "reliance", "welspun living", or "adanai") to canonical symbols (like `RELIANCE`, `WELSPUNLIV`, `ADANIENT`) and markets (`India` vs `US`). 

The resolution flow follows a multi-tiered pipeline:

1. **Fast Local Lookup**: Checks for exact matches and fuzzy matches (similarity $\ge 0.85$) against a local alias dictionary (`_ALIAS`) and registered company list (`SYMBOL_TO_COMPANY`).
2. **LLM Ticker Suggestion**: Prompts the LLM to suggest the exchange ticker symbol from its training knowledge.
3. **Exact Ticker Validation**: Queries Yahoo Finance Search with the LLM-suggested ticker. If a matching candidate is found, it uses it. If the suggested ticker does not exist as an exact match in the returned results, the suggestion is discarded, preventing incorrect mappings (e.g., mapping `"WELSPUN"` to `"WELENT"`).
4. **Yahoo Search & Word-Overlap Ranking**: Performs a Yahoo Finance query using the user's original raw text. Results are scored and sorted by a word-overlap similarity ratio to prioritize the best match (e.g. prioritizing `"WELSPUNLIV.NS"` over `"WELENT.NS"` for `"welspun living"`).
5. **ClickHouse ngramDistance Fallback**: If Yahoo search returns no results (common for typos like `"welspn living"`), the resolver queries ClickHouse `market_data.mf_holdings` (12,000+ unique stock names) using `ngramDistance`. If a close match is found ($\le 0.65$), it recursively restarts the search with the corrected name.
6. **Interactive User Confirmation & Pre-Resolution**:
   - **Environment Check**: If running in an interactive CLI session (indicated by the `MOSAIC_INTERACTIVE_CHAT == "1"` environment variable set inside `run_chat_loop`), the CLI prompts the user to confirm the resolved company. If rejected, the user can input the correct name and select from the top 3 LLM-suggested matching options.
   - **Pre-Resolution**: The chat REPL loop pre-resolves potential company subjects in the user query *before* generating the visual AI execution plan. This ensures that the plan panel printed to the terminal uses the correct user-accepted canonical symbol.
   - **Turn-Level Caching & Question Rewriting**: Exact symbol matches and symbols already resolved/confirmed in the current chat turn are automatically bypassed. All sub-agent delegation tools (`delegate_to_*_agent`) automatically rewrite the queries using `rewrite_delegation_question` to replace the raw subject text with the final corrected symbol. This ensures that sub-agents do not trigger duplicate prompt prompts in the same turn.
7. **Auto-Import**: If the resolved symbol is listed in India and is missing from the local ClickHouse database (`daily_prices`), it triggers a parallel data backfill before launching sub-agents.

---

## Standalone Agents

These agents run outside the sub-agent routing framework:

### ComexAgent (`src/agents/comex_agent.py`)

- **Purpose:** Pre-market commodity signals for XAU, XAG, XPT, XPD, HG
- **Local path:** Direct `get_comex_signals()` call (no agent loop)
- **Cloud path:** LangGraph ReAct with `recursion_limit=6`
- **Tool:** `fetch_all_comex_signals` (single-call design with internal call counter)

### NewsSentimentAgent (`src/agents/news_sentiment_agent.py`)

- **Purpose:** Multi-source news sentiment (NewsAPI + Google News)
- **Local path:** Direct `collate_news_sentiment()` call
- **Cloud path:** LangGraph ReAct with `recursion_limit=6`
- **Tool:** `collate_news_sentiment` (deduplicates across sources, scores sentiment)

---

## MCP Server

**File:** `src/mcp_server.py`

Exposes 4 tools to external LLM clients (Claude Code, Gemini CLI) via stdio transport:

| MCP Tool | Maps To | Use When |
|---|---|---|
| `run_pipeline` | GOLDBEES ML pipeline | "run pipeline", "today's signal" |
| `get_latest_signal` | Last stored prediction | "latest signal", "last recommendation" |
| `evaluate_performance` | Hit-ratio from checkpoints | "how accurate", "evaluate" |
| `import_data` | Data import pipeline | "refresh data", "import stocks" |

---

## Request Flow Examples

### `ask "what is my GOLDBEES signal?"`

```
CLI ask()
 → route_intent_llm("what is my GOLDBEES signal?")
 → intent: "signal" (confidence: 0.95)
 → run_subagent_for("signal", question)
    ├── TracingCallbackHandler attached
    ├── BudgetCallbackHandler attached
    └── SignalSubAgent.run(question)
        → LangGraph ReAct stream
          Step 1: LLM → run_goldbees_pipeline()
          Step 2: Tool returns {prob_up, regime_signal, weights, ...}
          Step 3: LLM narrates tool output (no computation)
        → Markdown response
```

### `ask "explain GOLDBEES price anomalies"`

```
CLI ask()
 → intent: "signal"
 → SignalSubAgent.run(question)
    → LangGraph ReAct stream
      Step 1: LLM emits two parallel tool calls:
              explain_price_anomalies(symbol="GOLDBEES", days=60)
              plot_price_chart(symbol="GOLDBEES", days=60)
      Step 2a: explain_price_anomalies
               → fetch full OHLCV (ClickHouse, yfinance fallback)
               → fetch COT (cot_gold) + USDINR FX in parallel
               → run_composite_anomaly(df, df_cot, df_fx)
                   GARCH(1,1) + Isolation Forest + MAD-Z
                   → df_flagged: dates where final_z_abs > 2.5
                   → df_result:  per-date regime + final_z + garch_vol
               → filter flagged to last 60 days
               → per anomaly date:
                   search_financial_news(query, target_date)
                   ml_prediction_asof(date)      ← what ML expected
                   signal_composite_asof(date)   ← confirmed vs contradicted
               → append COMEX GC=F chart + GARCH vol chart
      Step 2b: plot_price_chart → ASCII price chart with 🔴 anomaly markers
      Step 3: LLM synthesises regime narrative + news correlation
    → Markdown report with table, per-date detail, charts
```

### `ask "research RELIANCE"`

```
CLI ask()
 → route_intent_llm("research RELIANCE")
 → intent: "india_equity"
 → run_subagent_for("india_equity", question)
    └── IndianEquityResearchSubAgent.run(question)
        → LangGraph ReAct stream
          Step 1: LLM → resolve_company("reliance")
          Step 2: Tool → symbol=RELIANCE, exchange=NSE
          Step 3: LLM emits 9 parallel tool calls:
                  get_yahoo_finance_data, get_price_momentum,
                  get_quarterly_results, get_stock_cashflow,
                  get_shareholding_pattern, get_mf_holdings_for_stock,
                  get_stock_news, get_newsapi_stock_news, plot_price_chart
          Step 4-9: ToolNode executes all concurrently
          Step 10: LLM synthesises Markdown report
        → Rich-formatted terminal output
```

### `ask "macro risks today"`

```
CLI ask()
 → route_intent_llm("macro risks today")
 → intent: "macro"
 → run_subagent_for("macro", question)
    └── MacroSubAgent.run(question)
        → LangGraph ReAct stream
          Step 1: LLM → run_macro_theme_scanner()
          Step 2: Tool → 8 themes with per-ETF impact scores
          Step 3: LLM → get_fii_dii_summary()
          Step 4: Tool → 5-day FII/DII net flows
          Step 5: LLM narrates macro landscape
        → Markdown response
```

---

## Architecture Diagram

```mermaid
graph TB
    subgraph "Entry Points"
        CLI["CLI<br/>src/main.py<br/>13 commands"]
        MCP["MCP Server<br/>src/mcp_server.py<br/>4 tools"]
    end

    subgraph "Routing"
        IR["LLM Intent Router<br/>gpt-4o-mini<br/>LRU cached"]
        RX["Regex Fallback<br/>15+ patterns"]
    end

    subgraph "Middleware"
        TRC["TracingCallback<br/>→ agent_traces"]
        BUD["BudgetCallback<br/>20 calls / 30k tokens"]
    end

    subgraph "Sub-Agents (LangGraph ReAct + ToolNode)"
        DD["DeepDive<br/>~6 tools"]
        IE["IndianEquity<br/>~19 tools"]
        SIG["Signal<br/>~19 tools"]
        MAC["Macro<br/>~12 tools"]
        MF["MF<br/>~12 tools"]
        NEWS["News<br/>~12 tools"]
        CODE["Code<br/>CODE_TOOLS"]
        DB["Database<br/>DB+CHART_TOOLS"]
        INTL["IntlETF<br/>~9 tools"]
        RES["Research<br/>~30 tools"]
    end

    subgraph "Workflows (StateGraph)"
        WF_SIG["Signal<br/>~4k tokens"]
        WF_MAC["Macro<br/>~3.5k tokens"]
        WF_NEWS["News<br/>~1.5k tokens"]
        WF_MF["MF Planner<br/>Plan-Execute-Replan"]
        WF_RES["Research<br/>~8.8k tokens"]
        WF_EQ["India Equity<br/>~7k tokens"]
        WF_CON["Consensus<br/>~4k tokens"]
        WF_PORT["Portfolio<br/>~9.8k tokens"]
    end

    subgraph "Workflow Infrastructure"
        CTX["ContextManager<br/>compression + dedup"]
        PS["PlanStore<br/>SQLite + Jaccard"]
        MS["MosaicState<br/>shared TypedDict"]
        AP["Plan Approval<br/>Y/n/edit gate"]
    end

    subgraph "Standalone Agents"
        COMEX["ComexAgent<br/>1 tool"]
        NSENT["NewsSentimentAgent<br/>1 tool"]
    end

    subgraph "Tool Layer"
        T["~80 @tool functions<br/>src/tools/"]
    end

    subgraph "Data Layer"
        REPO["MarketDataRepository"]
        CH[("ClickHouse<br/>market_data<br/>27 tables")]
        SQLITE[("SQLite<br/>LLM cache 24h TTL<br/>+ plan store")]
    end

    subgraph "Agentic Memory"
        L1["Global Rules<br/>user_global"]
        L2["Context Files<br/>AGENTS/GEMINI/CLAUDE.md"]
        L3["Agent Defs<br/>.agents/agents/ (21)"]
        L4["Skills<br/>.agents/skills/ (21)"]
    end

    CLI --> IR
    CLI --> WF_SIG & WF_MAC & WF_NEWS & WF_MF & WF_RES & WF_EQ & WF_CON & WF_PORT
    MCP --> CLI
    IR -->|fallback| RX
    IR --> DD & IE & SIG & MAC & MF & NEWS & CODE & DB & INTL & RES
    DD & IE & SIG & MAC & MF & NEWS & CODE & DB & INTL & RES -.->|auto| TRC & BUD
    DD & IE & SIG & MAC & MF & NEWS & CODE & DB & INTL & RES --> T
    WF_SIG & WF_MAC & WF_NEWS & WF_MF -.-> AP
    WF_SIG & WF_MAC & WF_NEWS & WF_MF & WF_RES & WF_EQ & WF_CON & WF_PORT -.-> CTX & MS
    WF_MF -.-> PS
    WF_SIG & WF_MAC & WF_NEWS & WF_MF & WF_RES & WF_EQ & WF_CON & WF_PORT --> T
    COMEX & NSENT --> T
    T --> REPO --> CH
    TRC --> CH
    PS --> SQLITE
    DD & IE & SIG & MAC & MF & NEWS & CODE & DB & INTL & RES -.->|cache| SQLITE
    L1 --> L2 --> L3 --> L4
```

---

## Adding a New Sub-Agent

1. Create `src/agents/sub_agents/<name>.py` with a subclass of `_SubAgent`:

```python
from __future__ import annotations
import logging
from .base import _SubAgent

logger = logging.getLogger(__name__)


class MySubAgent(_SubAgent):
    SYSTEM_PROMPT = "You are a specialist in ..."
    RECURSION_LIMIT = 20

    def _get_tools(self) -> list:
        from src.tools.my_tools import MY_TOOLS
        return MY_TOOLS

    # Optional: override only if this agent needs a domain-specific LLM
    # (e.g. a cheaper model for classification, a larger one for long reports)
    def _select_llm(self, llm_override=None):
        if llm_override is None:
            # inject domain-specific model here, then fall through
            pass
        return super()._select_llm(llm_override)
```

2. Import and register in `src/agents/sub_agents/registry.py`:
   - Add `from .my_module import MySubAgent` at the top
   - Add `"my_intent": MySubAgent` to `cls_map` inside `get_subagent()`

3. Re-export from `src/agents/sub_agents/__init__.py`:
   - Add `from .my_module import MySubAgent`
   - Add `"MySubAgent"` to `__all__`

4. Add the intent to routing:
   - `src/agents/sub_agents/routing.py` → add a compiled regex constant, then `insert()` a `(pattern, "my_intent")` tuple at the correct position in `_PRE_PLOT_TABLE`, `_VIZ_ROUTE_TABLE`, or `_POST_PLOT_TABLE`
   - `src/agents/intent_router.py` → add the intent to `_ROUTER_SYSTEM_PROMPT` for the LLM router

5. The `NO_LLM_CALC_RULE` and middleware (tracer + budget) are attached automatically — no per-agent wiring needed

---

## Observability Queries

```sql
-- Tool call frequency by agent (last 7 days)
SELECT agent, tool_name, count() AS calls
FROM market_data.agent_traces FINAL
WHERE created_at > now() - INTERVAL 7 DAY
GROUP BY agent, tool_name
ORDER BY calls DESC;

-- Avg latency by sub-agent
SELECT agent, avg(latency_ms) AS avg_ms, max(latency_ms) AS p100_ms
FROM market_data.agent_traces FINAL
WHERE status = 'ok'
GROUP BY agent;

-- Budget breaches
SELECT agent, run_id, error_msg, created_at
FROM market_data.agent_traces FINAL
WHERE error_class = 'BudgetExceededError'
ORDER BY created_at DESC
LIMIT 20;

-- Token consumption by day
SELECT toDate(created_at) AS day, sum(tokens_in + tokens_out) AS total_tokens
FROM market_data.agent_traces FINAL
GROUP BY day
ORDER BY day DESC;
```
