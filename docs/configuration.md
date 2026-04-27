# Configuration Reference

All settings are loaded from `.env` via Pydantic `BaseSettings`. Copy `.env.example` to `.env` and fill in your values.

## LLM

```
OPENAI_API_KEY=sk-...          # or use ANTHROPIC_API_KEY
ANTHROPIC_API_KEY=...
LLM_PROVIDER=openai            # "openai" | "anthropic"
LLM_MODEL=gpt-4o-mini
LLM_BASE_URL=                  # set for local model (LM Studio / Ollama)
LLM_CONTEXT_WINDOW=4096
```

**Local model (LM Studio / Ollama):**
```
LLM_BASE_URL=http://localhost:1234/v1
LLM_MODEL=DeepSeek-R1-Distill-Qwen-14B-GGUF
LLM_CONTEXT_WINDOW=4096
```
> Models < 30B struggle with multi-turn tool orchestration. COMEX and news agents bypass LangGraph for local models automatically.

## API Keys

```
NEWSAPI_KEY=...                # free at newsapi.org — 100 req/day
GOLD_API_KEY=...               # free at gold-api.com — COMEX spot prices
```

## Zerodha Kite MCP

```
KITE_MCP_URL=https://mcp.kite.trade/mcp
KITE_API_KEY=                  # only needed for self-hosted MCP
KITE_API_SECRET=
KITE_MCP_TIMEOUT=30
```

## ClickHouse (Data Hub & Connection Pool)

```
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=8123
CLICKHOUSE_DATABASE=market_data
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=

# Connection pool (src/db/pool.py — shared across all service modules)
CLICKHOUSE_POOL_MIN=2          # warm idle connections kept alive
CLICKHOUSE_POOL_MAX=10         # hard cap on total live connections
CLICKHOUSE_POOL_TIMEOUT=10.0   # seconds to wait for a free pool slot
```

All service modules import the pool singleton via `from src.db.pool import get_pool`.
Use `pool.query_df(sql)` for SELECT, `pool.execute(sql)` for DDL/INSERT,
or `with pool.acquire() as client:` for raw multi-statement access.

## Behaviour

```
NEWS_ARTICLES_PER_STOCK=5
NEWS_LOOKBACK_DAYS=7
MAX_HOLDINGS_PER_RUN=0         # 0 = no cap
SCRAPE_DELAY_SECONDS=2.0
OUTPUT_DIR=./output
LOG_LEVEL=INFO
```

## Check current config

```bash
python src/main.py config
```
