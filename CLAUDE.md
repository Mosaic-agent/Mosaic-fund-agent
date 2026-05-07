# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Mosaic-agent** (Mosaic Fund Agent) is a Python 3.11+ financial intelligence platform for Indian equity and commodity markets.
 It connects to a live Zerodha portfolio via MCP, enriches holdings with market data/news/earnings, runs ML forecasting and anomaly detection, and produces LLM-scored reports.

## Commands

### Setup
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
brew install libomp          # macOS only — required by LightGBM
cp .env.example .env         # then fill in API keys
docker compose up clickhouse -d
```

### Running the CLI
```bash
python src/main.py --help
python src/main.py analyze --max 3         # limit to 3 holdings
python src/main.py analyze                 # full live portfolio (requires Zerodha login)
python src/main.py ask "what is my riskiest holding?"
python src/main.py import --category stocks,etfs,mf,fii_dii,cot,fx_rates
python src/main.py import --full           # full backfill (ignores watermarks)
python src/main.py import --dry-run
python src/main.py signals --save --verbose  # composite signal aggregator
python src/main.py macro --max 3             # macro theme scanner
python src/main.py comex                     # COMEX pre-market gold/silver/copper
python src/main.py etf-news --max 3 --save  # ETF news sentiment
python src/main.py premium-alerts           # iNAV premium/discount alerts
python src/main.py ui                      # Streamlit at localhost:8501
python src/main.py config                  # show masked config
```

### Tests
```bash
# Unit tests (no external services needed)
python tests/test_tools.py
python tests/test_quant_signals.py
python tests/test_news_sentiment.py
python tests/test_cache.py
python tests/test_inav_cli.py
python tests/test_macro_theme_agent.py

# Integration tests (require live ClickHouse)
python tests/_test_importer.py

# Backtests / validation (prefixed with _ or _backtest_)
python tests/_validate_ml.py
python tests/_backtest_anomaly.py
```

### Docker
```bash
docker compose run mosaic analyze
docker compose up ui -d
docker compose up                          # full stack (clickhouse + ui + app)
```

## Architecture

### Request Flow (analyze command)
```
CLI (src/main.py)
  → PortfolioAgent.run()
      → KiteMCPClient  — authenticates with Zerodha via mcp.kite.trade
      → _parse_holdings()  — raw Kite response → List[Holding]
      → asset_analyzer.analyze_holding()  per holding (parallel)
            ↳ yahoo_finance tools  — OHLCV, price, 52w range
            ↳ earnings_scraper     — Screener.in → BSE fallback
            ↳ news_search          — GNews + NewsAPI sentiment
      → portfolio_analyzer.build_portfolio_report()
            ↳ LangGraph ReAct agent (create_react_agent)  — LLM scoring
      → output/  — JSON + HTML report files
```

### Key Layers

| Layer | Path | Role |
|-------|------|------|
| CLI | `src/main.py` | 13 Typer commands; entry point |
| Agents | `src/agents/` | LangChain/LangGraph orchestrators |
| Analyzers | `src/analyzers/` | `asset_analyzer` (per-holding), `portfolio_analyzer` (aggregate) |
| Tools | `src/tools/` | Pure functions returning dict/DataFrame; composable in agents or scripts |
| Importer | `src/importer/` | Delta-sync pipeline: fetchers → ClickHouse |
| DB Pool | `src/db/pool.py` | Thread-safe `CHPool` singleton (`get_pool()`); all service modules share pooled connections |
| ML | `src/ml/` | LightGBM 5-day forecast (`trend_predictor`), GARCH + Isolation Forest (`anomaly`) |
| Models | `src/models/portfolio.py` | Pydantic: `Holding`, `Portfolio`, `InstrumentType`, `Sentiment` |
| Config | `config/settings.py` | Pydantic `BaseSettings`; all settings loaded from `.env` |
| UI | `src/ui/app.py` | Streamlit data hub (5 tabs over ClickHouse data) |
| Utils | `src/utils/` | `sanity_checker` (ClickHouse anomaly rules) |

### Data Import Pipeline
```
CLI import command
  → src/importer/cli.py  (orchestrates by category)
      → src/importer/fetchers/<name>_fetcher.py  (external API → DataFrame)
      → src/importer/clickhouse.py               (watermark check → bulk insert)
          ReplacingMergeTree: re-importing same date is safe / idempotent
          Watermark: tracks last imported date per (source, symbol) to enable delta sync
```

**13 fetchers:** `yfinance`, `mfapi`, `cot` (CFTC Socrata), `nse_inav`, `fii_dii`, `imf_reserves`, `etf_aum`, `mf_holdings` (Morningstar), `fx_rates`, `nse_quote`, `yahoo_snapshot`, `expert_tweets`, plus news tools.

### LLM Configuration
`LLM_PROVIDER` (`openai` or `anthropic`) + `LLM_MODEL` control which model is used. Set `LLM_BASE_URL` to an OpenAI-compatible endpoint (Ollama, LM Studio) for local inference — no API key needed in that case.

### ClickHouse Schema
Database: `market_data`. Tables are auto-created on first import (DDL in `src/importer/clickhouse.py`). Primary tables: `daily_prices` (OHLCV), `mf_nav`, `fii_dii_flows`, `ml_predictions`, `signal_composite`, `inav_snapshots`, `import_watermarks`. All use `ReplacingMergeTree` — idempotent inserts are safe.

## User Context

Dhiraj's wife is a treasurer at a major Indian AMC. Her domain expertise shaped the
platform's fund/ETF signal design, macro theme mapping, and institutional flow
interpretation. Assume strong domain knowledge — skip basic MF/ETF/flow definitions.

## MCP Tools (ofin-pipeline server)

`run_pipeline`, `get_latest_signal`, `evaluate_performance`, `import_data` are MCP
tools — call them directly, not as shell commands.

| Tool | Call when user says |
|------|---------------------|
| `run_pipeline` | "run pipeline", "today's signal", "what should I do with GOLDBEES" |
| `get_latest_signal` | "latest signal", "last recommendation" |
| `evaluate_performance` | "evaluate", "how accurate", "hit ratio" |
| `import_data` | "refresh data", "update prices", "import" |

## Rules — Enforced in Every Session

### No LLM Calculations
Never compute any number inside an LLM response. All numeric work — returns, ratios,
aggregations, scores, Kelly fractions, PE ratios — must be computed in Python or SQL,
then narrated. The LLM only summarises pre-computed results.

- ✅ Run a script/query, then explain the output
- ❌ "The average MoM return is ~X%" — if you derived it yourself, do not state it

### No Co-Authored-By in Commits
Never add `Co-Authored-By: Claude ...` trailers to git commit messages.

### Grounding — Pipeline Outputs
The GOLDBEES pipeline produces a fixed output set. Never invent anything beyond these fields:
- `prob_up` — LightGBM classifier probability (0–1)
- `expected_return_pct` — predicted 5-day log return (%)
- `confidence_band` — [low%, high%] quantile bounds
- `regime_signal` — BUY / WATCH_LONG / HOLD / WATCH_SHORT / SELL
- `cv_auc` — model AUC (0.5 = random, >0.55 = useful)
- `cv_skill` — AUC − 0.5 (≤0 = no skill, Kelly disabled)
- `hit_ratio` — directional accuracy from walk-forward CV
- `weights.rg` — Rule-based Risk Governor weight
- `weights.kelly` — Kelly-optimal weight
- `weights.blended_50` — **recommended weight** (50% RG + 50% Kelly)
- `weights.blended_30` — conservative blend (70% RG + 30% Kelly)

Never invent:
- Composite scores (e.g. "69/100") or macro/sentiment/flow scores
- "ACCUMULATE" / "STRONG BUY" labels — use `regime_signal` as-is
- The RG weight is NOT the recommendation — `weights.blended_50` is

When a tool returns a `display_report` field, show it **verbatim** — do not reformat.

### Macro Scanner
- Net scores are article-counts, not % return forecasts
- Only cite prices/flows that appear in the Quant Overlay panel
- Score ≥ +16 = strong bullish | +8–+15 = moderate | ≤ −16 = strong bearish

### Number Sources
All market data comes from ClickHouse (live DB). Never substitute numbers from
training knowledge — gold prices, FII flows, USDINR etc. change daily and the
DB value is the only valid source.

### ClickHouse Queries
Always add `FINAL` to queries against `ReplacingMergeTree` tables to deduplicate:
```sql
SELECT ... FROM market_data.mf_holdings FINAL WHERE ...
```

### DSP Fund Holdings Schema
`market_data.mf_holdings` columns: `scheme_code`, `fund_name`, `as_of_month`,
`isin`, `security_name`, `asset_type`, `market_value_cr`, `pct_of_nav`, `imported_at`.
**Never use** `weight_pct` or `name` — those columns do not exist.
Coverage: 62 DSP funds Sep 2023–Mar 2026; Top 10 funds back to Jun 2022.

## Important Patterns

- **Tool registration:** Tools are lists of `@tool`-decorated functions (e.g., `YAHOO_TOOLS`, `NEWS_TOOLS`). `ALL_TOOLS` in `portfolio_agent.py` is the union passed to `create_react_agent`.
- **Scraping fallbacks:** Screener.in is primary for earnings; BSE/Yahoo Finance are fallbacks when blocked. `fake-useragent` rotates user-agents.
- **Caching:** NewsAPI/COMEX responses cached to `output/.cache/` with 1-hour TTL.
- **Output files:** Reports written to `./output/` as JSON.
- **`src/scripts/`:** Standalone analysis scripts organised by domain. Run from the project root: `python src/scripts/<subdir>/<name>.py`.
  - `dsp/` — DSP AMC import, analysis, and backtests
  - `fund_imports/` — Factory-pattern AMC importers; run via `python src/scripts/fund_imports/run.py <icici|nippon|icici-index|all> [--dry-run] [--test]`. `base.py` has the `BaseFundImporter` ABC; `importers/` has one class per AMC; `factory.py` has `create_importer(name)`.
  - `etf/` — ETF comparison, CAGR validation, risk analysis
  - `ml/` — ML prediction backfill and evaluation
  - `portfolio/` — portfolio tracking, health checks, opportunity scan
  - `market/` — macro themes, FII/DII, metals, sentiment, whale tracker
  - `db/` — ClickHouse backup, restore, and sanity checks
