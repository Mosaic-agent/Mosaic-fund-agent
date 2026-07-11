# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) and Antigravity (agy) when working with code in this repository.

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
python src/main.py crossover --symbol GOLDBEES # MA crossover backtester
python src/main.py scan-setups              # ETF volume-volatility setups scanner
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
- **Start UI:** `./run.sh` (macOS/Linux) or `run.bat` (Windows)
- **Stop UI:** `./stop.sh` (macOS/Linux) or `stop.bat` (Windows)
- **Run CLI/Scripts in Docker:** Use the `mosaic.sh`/`mosaic.bat` wrappers:
  - `./mosaic.sh comex`
  - `./mosaic.sh ask "what is my riskiest holding?"`
  - `./mosaic.sh src/scripts/goldbees_report.py`
  - `./mosaic.sh src/scripts/db_metadata_init.py`
  - `./mosaic.sh src/scripts/news_rag_backfill.py --migrate-qdrant`


## Architecture

### Request Flow (analyze command)
```
CLI (src/main.py)
  → MosaicFundAgent.run()
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
| Signal Sources | `src/agents/signal_sources.py` | Strategy pattern: `SignalSource` ABC + 5 pillar classes + `GARCHAnomalySource`; `SIGNAL_ETFS` list |
| Analyzers | `src/analyzers/` | `asset_analyzer` (per-holding), `portfolio_analyzer` (aggregate) |
| Tools | `src/tools/` | Pure functions returning dict/DataFrame; composable in agents or scripts |
| Importer | `src/importer/` | Delta-sync pipeline: Fetcher adapters → ClickHouse |
| Fetcher ABC | `src/importer/base_fetcher.py` | Adapter interface: `fetch()`, `validate()`, `insert()`, `max_date()` |
| Fetcher Adapters | `src/importer/fetchers/adapters.py` | 5 concrete adapters + `FETCHER_REGISTRY` keyed by CLI category |
| Repository | `src/db/repository.py` | `MarketDataRepository`: typed reads, watermarks, `run_fetcher()` loop, event publish. Point-in-time variants: `ml_prediction_asof(date)`, `signal_composite_asof(symbol, date)` |
| DB Pool | `src/db/pool.py` | Thread-safe `CHPool` singleton (`get_pool()`); all service modules share pooled connections |
| Events | `src/events/` | `EventBus` singleton, `DataImportedEvent`, `Observer` ABC, 4 post-import hooks |
| ML | `src/ml/` | LightGBM 5-day forecast (`trend_predictor`), composite anomaly (`anomaly`), OU mean-reversion premium fit (`ou_estimator`). `run_composite_anomaly(df, df_cot, df_fx, df_corp_actions, symbol, category)` → 5-step pipeline: MAD-Z → GARCH(1,1) → Isolation Forest → PELT change-point → Company Event classification. Suppresses corporate actions from `is_anomaly` for ETFs only; requires ≥60 rows. **OU premium strategy (intl ETFs) full reference: [ou-mean-reversion.md](ou-mean-reversion.md).** |
| Models | `src/models/portfolio.py` | Pydantic: `Holding`, `Portfolio`, `InstrumentType`, `Sentiment` |
| Config | `config/settings.py` | Pydantic `BaseSettings`; all settings loaded from `.env` |
| UI | `src/ui/app.py` | Streamlit data hub (5 tabs over ClickHouse data) |
| Utils | `src/utils/` | `sanity_checker` (ClickHouse anomaly rules), `markdown_renderer` (terminal Markdown table formatter) |

### Data Import Pipeline
```
CLI import command
  → src/importer/cli.py  (orchestrates imports; intercepts stocks & us_stocks categories)
      ↳ Redirects to src/importer/parallel_importer.py (5 parallel worker threads with staggered jitter delays)
      → src/importer/fetchers/<name>_fetcher.py  (external API → DataFrame)
      → src/importer/clickhouse.py               (watermark check → bulk insert)
          ReplacingMergeTree: re-importing same date is safe / idempotent
          Watermark: tracks last imported date per (source, symbol) to enable delta sync

New pattern (Adapter + Repository):
  → MarketDataRepository.run_fetcher(fetcher)
      → fetcher.fetch(from_date, to_date)    ← Fetcher ABC (Adapter)
      → fetcher.validate(rows)
      → fetcher.insert(rows, ch)
      → ch.set_watermark(...)
      → EventBus.publish(DataImportedEvent)  ← triggers post-import hooks
```

**14 fetchers:** `yfinance`, `mfapi`, `cot` (CFTC Socrata), `nse_inav`, `fii_dii`, `imf_reserves`, `etf_aum`, `mf_holdings` (Morningstar), `fx_rates`, `nse_quote`, `yahoo_snapshot`, `expert_tweets`, `nse_corporate_actions` (NSE equity corporate actions — splits, bonuses, demergers, rights, dividends), plus news tools.

**5 Fetcher adapters** (Adapter pattern, `src/importer/fetchers/adapters.py`): `YFinanceFetcher`, `MFNavFetcher`, `FIIDIIFetcher`, `FXRatesFetcher`, `COTGoldFetcher`, `WorldBankMacroFetcher`, `IMFWEOFetcher`. Add new sources to `FETCHER_REGISTRY` — orchestrator loop picks up automatically.

### LLM Configuration
`LLM_PROVIDER` (`openai` or `anthropic`) + `LLM_MODEL` control which model is used. Set `LLM_BASE_URL` to an OpenAI-compatible endpoint (Ollama, LM Studio) for local inference — no API key needed in that case.

### ClickHouse Schema
Database: `market_data`. Tables are auto-created on first import (DDL in `src/importer/clickhouse.py`). Primary tables: `daily_prices` (OHLCV), `mf_nav`, `fii_dii_flows`, `ml_predictions`, `signal_composite`, `inav_snapshots`, `import_watermarks`, `macro_indicators` (World Bank / IMF WEO annual data), `corporate_actions` (NSE split/bonus/demerger/rights/dividend history — keyed by `(symbol, ex_date, action_type)`). All use `ReplacingMergeTree` — idempotent inserts are safe.

### Qdrant Vector DB
Database: `Qdrant` (served on port `6333` with built-in dashboard at `/dashboard`). Six collections (all 768-dim nomic-embed-text, COSINE distance). **Full reference incl. diagram, embedding pipeline, two-pass news retrieval, and read tools: [rag-architecture.md](rag-architecture.md).**
- `news_articles`: Financial news for RAG anomaly mapping + sentiment. `symbol` is a **list** of tickers (tenant index); `retrieve_articles(symbol=...)` does a two-pass symbol-scoped → semantic-fallback query. Written by `upsert_to_qdrant` / `_cache_articles_to_qdrant` / live fallback.
- `market_anomalies`: One point per (symbol × flagged date), GARCH/z-score signature **+ attribution** (`attributed_event_type`/`attributed_confidence`). Populated by `run_composite_anomaly(symbol=...)` / `store_anomalies_with_attribution`. Queried by `find_similar_anomaly_events` and the correlation precedent stage. Tenant: `symbol`.
- `mf_holdings`: One point per (fund × security × month), 22k+ holdings. Queried by `find_funds_holding`. Tenant: `isin`.
- `mf_fund_profiles`: One aggregated fingerprint per (fund × month) — equity/gold/bond/cash + top-5. Queried by `find_similar_funds`, `search_mf_exposure`. Tenant: `fund_name`.
- `clickhouse_metadata`: Table schemas + pre-baked SQL templates to prevent schema hallucination. Queried by `search_db_metadata`.
- `market_data`: One point per imported market row (OHLCV/NAV/FX/macro/COT), written fire-and-forget on import (`market_vector.py`). Tenant: `symbol`. **Write-only — no read path wired yet.**

Backfill: `python -m src.scripts.backfill_mf_qdrant` (MF), `python -m src.scripts.news_rag_backfill --migrate-qdrant` (news, idempotent), `python -m src.scripts.db_metadata_init` (schemas). Run once after first import.


## User Context

Dhiraj is a data enthusiast with deep interest in Indian capital markets and a network
of quant friends. Their collective expertise shaped the platform's fund/ETF signal
design, macro theme mapping, and institutional flow interpretation. Assume strong
domain knowledge — skip basic MF/ETF/flow definitions.

## MCP Tools (ofin-pipeline server)

`run_pipeline`, `get_latest_signal`, `evaluate_performance`, `import_data` are MCP
tools — call them directly, not as shell commands.

| Tool | Call when user says |
|------|---------------------|
| `run_pipeline` | "run pipeline", "today's signal", "what should I do with GOLDBEES" |
| `get_latest_signal` | "latest signal", "last recommendation" |
| `evaluate_performance` | "evaluate", "how accurate", "hit ratio" |
| `import_data` | "refresh data", "update prices", "import" |

## Antigravity (agy) & Claude Code Skills & Slash Commands

When working with Antigravity (`agy`) or Claude Code (`claude`), the following slash commands (skills) are available. Recommend them to the user as appropriate:

### Antigravity Slash Commands
| Command | Trigger / Purpose | What it does / When to recommend |
|---|---|---|
| `/goal` | "run a long task", "overnight run" | Recommend when the user wants to run a long-running, thorough task and the agent should keep going until the goal is fully achieved. |
| `/schedule` | "schedule this daily", "set cron" | Recommend when the user wants to run an instruction on a recurring schedule or set a one-time timer. |
| `/grill-me` | "interactive design", "clarify plan" | Recommend when the user wants to align on an implementation plan through an interactive interview to resolve design decisions. |
| `/commit` | "commit this", "commit changes" | Stage and commit the current working-tree changes using Conventional Commits. |
| `/daily-signal-composite` | "What should I buy today?", "run signals" | Run the composite signal aggregator to compute scores for all 18 ETFs. |
| `/data-engineering-importer` | "import all", "database repair" | Trigger the historical ClickHouse data engineering pipeline. |
| `/dsp-multi-asset-importer` | "import dsp data", "backfill dsp" | Backfill and validate DSP Multi Asset allocation fund holdings. |
| `/etf-news` | "etf news", "sentiment analysis" | Fetch and tag latest news articles by ETF category with sentiment. |
| `/goldbees-pipeline` | "run goldbees pipeline", "today's signal" | Run the full GOLDBEES ML prediction, Kelly sizing, and Risk Governor blend. |
| `/macro-scanner` | "macro trends today", "geopolitical events" | Scan live macro events and map their directional impact to ETFs. |
| `/macro-strategy` | "macro strategy", "positioning" | Deep dive into the 2026 macro supercycle, real assets, and domestic alpha. |
| `/risk-governor` | "position size", "garch vol" | Calculate GARCH-based position sizing and volatility targeting. |

### Claude Code Slash Commands
| Skill | Trigger | What it does |
|-------|---------|-------------|
| `/goldbees-pipeline` | "run goldbees", "pipeline signal" | Full GOLDBEES ML pipeline end-to-end |
| `/commit` | "commit this" | Stages and commits with a well-formed message |
| `/review` | "review PR #N" | Multi-agent pull request review |
| `/security-review` | "security review" | Reviews pending branch changes for vulnerabilities |
| `/simplify` | "simplify this code" | Reviews changed code for quality/efficiency |
| `/schedule` | "schedule this daily" | Creates a cron-scheduled remote agent routine |
| `/loop` | "loop every 5m" | Runs a prompt or command on a recurring interval |
| `/update-config` | "allow X", "add hook for Y" | Edits Claude Code settings.json/hooks |
| `/init` | "init CLAUDE.md" | Generates CLAUDE.md documentation for the repo |

Both agents share the same persistent memory guidelines for the project:
- `user_background.md` — AMC domain context
- `feedback_no_llm_calculations.md` — all numeric work in code, never in LLM
- `feedback_no_coauthor.md` — no Co-Authored-By in commits
- `feedback_qip_dilution_check.md` — verify QIP/dilution before flagging promoter sale
- `project_dsp_holdings_signal.md` — DSP active-fund cross-ownership is the primary institutional signal

## Rules — Enforced in Every Session

### No LLM Calculations
Never compute any number inside an LLM response. All numeric work — returns, ratios,
aggregations, scores, Kelly fractions, PE ratios — must be computed in Python or SQL,
then narrated. The LLM only summarises pre-computed results.

- ✅ Run a script/query, then explain the output
- ❌ "The average MoM return is ~X%" — if you derived it yourself, do not state it

### Commits and PR Workflow
- Never push directly to `main`. Always create a Pull Request (PR) for any changes.
- Never add a `Co-Authored-By:` trailer to git commit messages. Write clean, single-author commit messages only.

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

Let the LLM format the data and reports (e.g., into clean Markdown tables or structured panels) to ensure maximum readability and a premium layout. The LLM must still use the exact numbers returned by the tools verbatim (no self-calculations or metric inventions), but it should design and format the presentation layout dynamically.

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

→ MosaicFundAgent.run()
...
- **Tool registration:** Tools are lists of `@tool`-decorated functions (e.g., `YAHOO_TOOLS`, `NEWS_TOOLS`). `ALL_TOOLS` in `mosaic_fund_agent.py` is the union passed to `create_react_agent`.

- **Scraping fallbacks:** Screener.in is primary for earnings; BSE/Yahoo Finance are fallbacks when blocked. `fake-useragent` rotates user-agents.
- **Caching:** NewsAPI/COMEX responses cached to `output/.cache/` with 1-hour TTL. ML models cached to `output/.cache/ml_models/` keyed by `(max_trade_date, n_rows, n_splits, horizon)` — auto-invalidated by `ModelCacheInvalidator` when new price data arrives.
- **Output files:** Reports written to `./output/` as JSON.
- **Repository pattern:** `MarketDataRepository` (`src/db/repository.py`) is the single access point for all ClickHouse reads. Use `repo.fii_dii_5d()`, `repo.ohlcv()`, `repo.latest_ml_prediction()`, `repo.ml_prediction_asof(date)`, `repo.signal_composite_asof(symbol, date)` etc. — never write raw SQL in signal/ML code. The `*_asof(date)` variants are point-in-time queries used for historical anomaly context.
- **Anomaly pipeline:** `run_composite_anomaly` (`src/ml/anomaly.py`) — 5-step composite: (1) MAD robust Z-score, (2) GARCH(1,1) standardised residuals, (3) Isolation Forest confidence multiplier, (4) PELT change-point detection (`ruptures`, rbf cost on log-returns), (5) Company Event classification. Corporate action suppression: pass `df_corp_actions`; for ETFs (`category="etfs"`) split/bonus/demerger ex-dates are excluded from `df_flagged`; for stocks/commodities they remain in `df_flagged` (labelled `🏢 Price Driven by Company Event`) since corporate events on stocks are real analysable price movements. COT loaded only for gold; FX (USDINR) loaded for all. Falls back to naive `max(2.0, 2.5×std)` if <60 rows or `arch`/`ruptures` missing. Pass `symbol=` and `category=` to auto-store flagged anomalies in Qdrant `market_anomalies` collection.
- **Anomaly explanation tools:**
  - `explain_price_anomalies` (`src/tools/market/gold.py`, re-exported via `skills_tools.py`) — full GARCH report + per-date news correlation; gold/commodity-specific (loads COT + FX).
  - `search_anomaly_events(symbol, days, category)` (`src/tools/market/equity.py`) — equity-generic: detects same red-dot dates as chart, suppresses corp actions, runs **parallel** Google News searches per date (ThreadPoolExecutor, 5 workers); ±1 day fallback + NewsAPI for recent dates.
  - `get_corporate_actions(symbol)` (`src/tools/market/equity.py`) — fetches NSE corporate actions via `nse_corporate_actions_fetcher.py`, upserts to `market_data.corporate_actions`, returns history table.
- **Chart anomaly markers:** `_composite_anomaly_dates(symbol, category)` (`src/tools/chart_tools.py`) returns `(anomaly_dates, corp_action_dates)` — a session-level cache keyed by `(symbol, category, n_rows)`. `plot_price_chart` renders 🔴 genuine anomalies and 🏦 corporate action markers as separate scatter layers.
- **Strategy pattern:** Signal sources (`src/agents/signal_sources.py`) implement `SignalSource` ABC. The aggregator loops over `score_sources` list — add a new pillar by subclassing `SignalSource` and appending to the list.
- **Adapter pattern:** Data fetchers (`src/importer/fetchers/adapters.py`) implement `Fetcher` ABC. `MarketDataRepository.run_fetcher()` handles watermarks, dry-run, and event publishing. Add a new source by subclassing `Fetcher` and registering in `FETCHER_REGISTRY`.
- **Observer pattern:** `EventBus` (`src/events/bus.py`) fires `DataImportedEvent` after every live `run_fetcher()` insert. Four built-in observers: `ModelCacheInvalidator` (sync), `MLPredictionObserver`, `SignalAggregatorObserver`, `SanityCheckObserver` (all async). Register with `setup_observers()` at startup.
- **`src/scripts/`:** Standalone analysis scripts organised by domain. Run from the project root: `python src/scripts/<subdir>/<name>.py`.
  - `dsp/` — DSP AMC import, analysis, and backtests
  - `fund_imports/` — Factory-pattern AMC importers; run via `python src/scripts/fund_imports/run.py <icici|nippon|icici-index|all> [--dry-run] [--test]`. `base.py` has the `BaseFundImporter` ABC; `importers/` has one class per AMC; `factory.py` has `create_importer(name)`.
  - `etf/` — ETF comparison, CAGR validation, risk analysis
  - `ml/` — ML prediction backfill and evaluation
  - `portfolio/` — portfolio tracking, health checks, opportunity scan, parallel stock import (`import_stocks_parallel.py`)
  - `market/` — macro themes, FII/DII, metals, sentiment, whale tracker
  - `db/` — ClickHouse backup, restore, sanity checks, and data quality repairs (`fix_bad_data.py`)
