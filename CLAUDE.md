# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**ofin-agent** (Mosaic Fund Agent) is a Python 3.11+ financial intelligence platform for Indian equity and commodity markets. It connects to a live Zerodha portfolio via MCP, enriches holdings with market data/news/earnings, runs ML forecasting and anomaly detection, and produces LLM-scored reports.

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
python src/main.py import --category stocks,etfs
python src/main.py import --full           # full backfill (ignores watermarks)
python src/main.py import --dry-run
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
