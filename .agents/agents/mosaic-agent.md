---
name: mosaic-agent
description: Run the full Mosaic Fund ReAct agent for portfolio analysis, multi-pillar research Q&A, and Shoonya/Kite tool execution. Use when the user asks "run mosaic agent", "analyze portfolio", "ask portfolio [question]", or invokes /mosaic-agent.
tools:
  - run_command
  - view_file
  - search_web
  - read_url_content
  - grep_search
  - list_dir
model: inherit
temperature: 0.1
max_turns: 8
---

# Skill: Mosaic Fund Agent (ReAct Portfolio & Quant Q&A)

Orchestrates the full agentic quantitative research, asset-allocation, holdings analysis, and Shoonya/Kite broker tools using the `MosaicFundAgent` ReAct engine.

## Trigger

Use this skill when the user asks:
- "Analyze my portfolio"
- "Ask portfolio: what is my overall risk exposure?"
- "Run Mosaic agent on holdings"
- "/mosaic-agent [question]"

## What it does

Executes the `MosaicFundAgent` orchestrator (`src/agents/mosaic_fund_agent.py`):
1. **Holdings Import & Verification**: Authenticates via Kite/Shoonya to fetch live holdings and market positions.
2. **Parallel Asset Enrichment**: Runs multi-source enrichment (Shoonya live ticks, ClickHouse, yfinance, earnings, news) per holding.
3. **ReAct Quantitative Q&A**: Uses registered quant tools (`query_clickhouse_db`, `analyze_smallcap_patterns`, `analyze_midcap_patterns`, `analyze_largecap_patterns`, `get_live_inav`, `scan_etf_setups`, `run_composite_anomaly`, `get_shoonya_quotes`) to answer complex portfolio queries. MF-holdings questions (single-fund MoM/YoY, cross-fund consensus, whale tracking) are routed via `delegate_to_mf_agent` to the MF sub-agent's tool set rather than duplicated in the main tool list.
4. **Structured Report Generation**: Produces JSON and Markdown research notes summarizing risk, momentum, and position sizing.

## Cross-Fund Multi-Asset Consensus (Smart-Money Overlap)

When the user asks something like "what are multi-asset funds collectively buying", "any pattern across multi-asset funds", "smart-money consensus this month", or "trend across all multi asset MF", Mosaic delegates to the MF sub-agent's `run_multi_asset_consensus` tool (`src/scripts/portfolio/multi_asset_consensus.py`). It scans the 7 tracked multi-asset funds (Nippon, Nippon FoF, DSP, DSP Omni, Bajaj, Quant, ICICI) in `market_data.mf_holdings` and returns:
- Portfolio overlap — core holdings shared by ≥2 funds
- Consensus ADDS / TRIMS (MoM or YoY) — securities ≥2 funds moved the same direction
- Persistence-ranked asset-class rotation (12mo lookback, ≥3mo streak = persistent)
- Per-fund and cross-fund sector rotation

Caveat: the 7 funds disclose on different cadences (DSP can lag a month; ICICI is near-quarterly), so a "MoM" read can mix a true latest-month change with a stale prior-month one — check each fund's `as_of_month` before calling a result "this month's" trend.

```bash
# Direct CLI (bypasses the agent)
ALLOW_LOCAL_RUN=1 .venv/bin/python src/scripts/portfolio/multi_asset_consensus.py --period mom --top 30
```


## Usage

### Run Portfolio Analysis from CLI

```bash
ALLOW_LOCAL_RUN=1 .venv/bin/python src/main.py analyze --max 3
```

### Run ReAct Q&A over Portfolio / Market

```bash
ALLOW_LOCAL_RUN=1 .venv/bin/python src/main.py ask "<YOUR_QUESTION>"
```

### Direct Agent Execution

```bash
ALLOW_LOCAL_RUN=1 .venv/bin/python -c "from src.agents.mosaic_fund_agent import MosaicFundAgent; agent = MosaicFundAgent(); agent.run(max_holdings=5)"
```
