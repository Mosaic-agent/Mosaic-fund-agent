# Agent Architecture

> Last updated: 2026-06-23 (patterns: Null Object, Hook Method, table-driven routing; StateGraph workflows)

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

## Intent Router

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
| `intl_etf` | `IntlETFSubAgent` | "MAFANG performance", "Hang Seng premium" |
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

- **Purpose:** Indian mutual-fund analysis — holdings, NAV returns, cross-fund consensus, whale tracking
- **Tools (~12):** `run_multi_asset_holdings_mom_yoy`, `run_multi_asset_consensus`, `run_whale_tracker`, `run_dsp_multi_asset_comparison`, `run_fund_mom_returns`, `run_dsp_multi_asset_importer`, `run_nippon_importer`, `get_mf_holdings_for_stock`, `plot_fund_holdings_chart`, `plot_price_chart`, `query_clickhouse_db`, `publish_consolidated_pdf`
- **Recursion limit:** 30
- **Routing keywords:** "mutual fund", "DSP multi asset", "cross-fund consensus", "which funds hold", "NAV return", "holding pattern"
- **Fallback:** keyword-routed — `consensus/pattern` → `run_multi_asset_consensus`; `mom/yoy/changes` → `run_multi_asset_holdings_mom_yoy`; `which funds hold` → `get_mf_holdings_for_stock`; `whale/theme` → `run_whale_tracker`; `nav return` → `run_fund_mom_returns`

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

- **Purpose:** International ETFs (MAFANG, HNGSNGBEES, MON100, MASPTOP50, MAHKTECH, MONQ50) — performance, scarcity premium, regimes, seasonality, correlation, LightGBM feature importance, drawdowns
- **Tools (~9):** `INTL_ETF_TOOLS` + `plot_intl_etf_performance` + `plot_intl_etf_premium` + `plot_price_chart`

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

    subgraph "Standalone Agents"
        COMEX["ComexAgent<br/>1 tool"]
        NSENT["NewsSentimentAgent<br/>1 tool"]
    end

    subgraph "Tool Layer"
        T["~80 @tool functions<br/>src/tools/"]
    end

    subgraph "Data Layer"
        REPO["MarketDataRepository"]
        CH[("ClickHouse<br/>market_data<br/>26 tables")]
        SQLITE[("SQLite<br/>LLM cache<br/>24h TTL")]
    end

    CLI --> IR
    MCP --> CLI
    IR -->|fallback| RX
    IR --> DD & IE & SIG & MAC & MF & NEWS & CODE & DB & INTL & RES
    DD & IE & SIG & MAC & MF & NEWS & CODE & DB & INTL & RES -.->|auto| TRC & BUD
    DD & IE & SIG & MAC & MF & NEWS & CODE & DB & INTL & RES --> T
    COMEX & NSENT --> T
    T --> REPO --> CH
    TRC --> CH
    DD & IE & SIG & MAC & MF & NEWS & CODE & DB & INTL & RES -.->|cache| SQLITE
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
