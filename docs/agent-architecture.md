# Agent Architecture

> Last updated: 2026-06-01

This document details the multi-agent orchestration layer of the Mosaic Fund Agent platform. For the broader system architecture (data pipeline, ClickHouse schema, ML, tools), see [architecture.md](architecture.md).

---

## Overview

The platform uses a **Multi-Agent Orchestrator** pattern built on [LangGraph](https://langchain-ai.github.io/langgraph/) `create_react_agent`. A main `MosaicFundAgent` delegates user queries to 10 specialised sub-agents via an **LLM-based Intent Router**, with middleware for **tracing** and **budget enforcement** on every invocation.

```
User Query
    │
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
│  src/agents/sub_agents.py                        │
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

- **File:** `src/agents/sub_agents.py` → `route_intent()`
- 15+ compiled regex patterns (e.g. `_SIGNAL_RE`, `_DEEPDIVE_RE`, `_MACRO_RE`)
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
| `intl_etf` | `IntlETFSubAgent` | "MAFANG performance", "Hang Seng premium" |
| `research` | `ResearchSubAgent` | "compare gold vs silver", "cross-asset analysis" |
| `main` | `MosaicFundAgent` (direct) | General / unclassifiable queries |

---

## Sub-Agent Base Class

**File:** `src/agents/sub_agents.py` → `_SubAgent`

All sub-agents inherit from `_SubAgent`, which provides:

```python
class _SubAgent:
    SYSTEM_PROMPT: str    # Domain-specific instructions (override in subclass)
    TOOLS: list           # Tool set (override in subclass)
    RECURSION_LIMIT: int  # Max LangGraph steps (default: 20)

    def _build()          # Lazy-init: create LLM + create_react_agent + ToolNode
    def run(question)     # Invoke the ReAct agent and return text
    def _fallback()       # Programmatic data gathering when LLM can't call tools
```

### Key design decisions

1. **Lazy initialisation** — LLM and agent are built on first `run()` call, not at import time
2. **Parallel tool execution** — `ToolNode` runs all tool calls from a single `AIMessage` concurrently via `ThreadPoolExecutor`
3. **Cloud upgrade** — If local model context window < 12,000 tokens, automatically promotes to cloud LLM
4. **Strategy fallback** — When LLM tool-calling fails entirely (e.g. gemma4 with 4k context), `_fallback()` collects data via direct Python function calls, then a single LLM synthesis call produces the narrative

---

## Sub-Agents

### DeepDiveSubAgent

- **Purpose:** US stock SEC filings (10-K/10-Q), XBRL financials, peer valuation
- **Tools (~6):** `resolve_company`, Yahoo Finance tools, earnings scraper, `run_deepdive_analysis`, `query_clickhouse_db`
- **Behaviour:** Starts with `resolve_company`; if Indian stock detected → redirects to `IndianEquityResearchSubAgent`

### IndianEquityResearchSubAgent

- **Purpose:** NSE/BSE company research — price, earnings, cashflow, holdings, news
- **Tools (~15):** `resolve_company`, Yahoo Finance, earnings, cashflow, MF holdings, FII/DII, news, `plot_price_chart`
- **Recursion limit:** 40 (needs more steps for parallel multi-tool batches)
- **Workflow:** Round 1 → resolve symbol; Round 2 → emit all data-fetching tools in parallel
- **Fallback:** `_gather_indian_equity_data()` — programmatic data collection

### SignalSubAgent

- **Purpose:** ETF composite scores, ML prediction, Kelly weights, GARCH vol-targeting, iNAV
- **Tools (~14):** `run_daily_signal_composite`, `run_goldbees_pipeline`, `run_risk_governor_analysis`, `run_etf_news_sentiment`, `run_premium_alerts`, `get_live_inav`, `query_clickhouse_db`, 5× `plot_*` chart tools
- **Rule:** Never invent composite scores or regime labels — only narrate tool output

### MacroSubAgent

- **Purpose:** COMEX pre-market, FII/DII flows, macro themes, geopolitics
- **Tools (~10):** `fetch_all_comex_signals`, `run_macro_theme_scanner`, `collate_news_sentiment`, `get_fii_dii_summary`, `plot_*` charts

### NewsSubAgent

- **Purpose:** Latest news headlines and sentiment per stock/ETF
- **Tools (~5):** `get_stock_news`, `get_newsapi_stock_news`, `get_etf_news_sentiment`, `query_clickhouse_db`

### CodeSubAgent

- **Purpose:** Ad-hoc Python execution and ClickHouse queries
- **Tools (~5):** `exec_python_snippet`, `query_clickhouse_db`, `import_symbol_data`, `run_data_engineering_importer`

### DatabaseSubAgent

- **Purpose:** ClickHouse schema inspection, watermarks, data freshness
- **Tools (~5):** `query_clickhouse_db`, `describe_schema`, `show_watermarks`, `query_raw_db_table`

### IntlETFSubAgent

- **Purpose:** International ETF analysis (MAFANG, HNGSNGBEES, Nasdaq 100)
- **Tools (~8):** `get_intl_etf_performance`, `get_intl_etf_premium`, `plot_intl_etf_*`

### ResearchSubAgent / AutonomousResearchAgent

- **Purpose:** Multi-domain cross-asset research combining fundamentals, ML, macro, news, MF holdings
- **Tools (~30):** Union of most tool sets
- **Use case:** Complex comparative questions ("gold vs silver positioning", "best ETF this quarter")

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
# src/agents/sub_agents.py — run_subagent_for()
callbacks = [TracingCallbackHandler(run_id, agent=intent), BudgetCallbackHandler()]
answer = subagent.run(question, callbacks=callbacks)
```

---

## Mandatory Rules

### No LLM Calculations

Every agent system prompt includes the `NO_LLM_CALC_RULE` (defined in `src/agents/sub_agents.py`):

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
        IE["IndianEquity<br/>~15 tools"]
        SIG["Signal<br/>~14 tools"]
        MAC["Macro<br/>~10 tools"]
        NEWS["News<br/>~5 tools"]
        CODE["Code<br/>~5 tools"]
        DB["Database<br/>~5 tools"]
        INTL["IntlETF<br/>~8 tools"]
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
    end

    CLI --> IR
    MCP --> CLI
    IR -->|fallback| RX
    IR --> DD & IE & SIG & MAC & NEWS & CODE & DB & INTL & RES
    DD & IE & SIG & MAC & NEWS & CODE & DB & INTL & RES -.->|auto| TRC & BUD
    DD & IE & SIG & MAC & NEWS & CODE & DB & INTL & RES --> T
    COMEX & NSENT --> T
    T --> REPO --> CH
    TRC --> CH
```

---

## Adding a New Sub-Agent

1. Create a subclass of `_SubAgent` in `src/agents/sub_agents.py`:

```python
class MySubAgent(_SubAgent):
    SYSTEM_PROMPT = "You are a specialist in ..."
    RECURSION_LIMIT = 20

    @property
    def TOOLS(self):
        return [my_tool_1, my_tool_2]
```

2. Register the intent in `route_intent()` (regex) and `_ROUTER_SYSTEM_PROMPT` (LLM router)
3. Add the sub-agent to the `_SUBAGENT_MAP` dict
4. The `NO_LLM_CALC_RULE` and middleware (tracer + budget) are attached automatically

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
