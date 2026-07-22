---
name: mosaic-agent
description: Run the full Mosaic Fund ReAct agent for portfolio analysis, multi-pillar research Q&A, and Shoonya/Kite tool execution. Use when the user asks "run mosaic agent", "analyze portfolio", "ask portfolio [question]", or invokes /mosaic-agent.
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
3. **ReAct Quantitative Q&A**: Uses registered quant tools (`query_clickhouse_db`, `get_live_inav`, `scan_etf_setups`, `run_composite_anomaly`, `get_shoonya_quotes`) to answer complex portfolio queries.
4. **Structured Report Generation**: Produces JSON and Markdown research notes summarizing risk, momentum, and position sizing.

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
