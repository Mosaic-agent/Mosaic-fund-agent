# Architecture

> Last updated: 2026-04-11

Mosaic Fund Agent is a multi-source financial intelligence platform for Indian equity and commodity markets. It ingests market data into ClickHouse, scores assets across six independent signal pillars, runs ML forecasting and anomaly detection, and surfaces actionable recommendations via CLI, scripts, and a Streamlit UI.

---

## High-Level Data Flow

```
External Data Sources
        │
        ▼
  Importers (src/importer/fetchers/)
        │  delta-sync, watermark-based
        ▼
  ClickHouse  (market_data database — 15 tables)
        │
        ├──▶  Tools (src/tools/)          ← real-time signals per asset
        ├──▶  ML  (src/ml/)               ← LightGBM forecast + anomaly
        ├──▶  Agents (src/agents/)        ← orchestrated multi-tool workflows
        │
        ▼
  Output
  ├── CLI tables (Rich console)
  ├── JSON reports  (output/)
  ├── HTML dashboards (React, self-contained)
  └── Streamlit UI  (localhost:8501)
```

---

## Directory Map

```
config/
  settings.py             Pydantic settings — LLM, ClickHouse, API keys, market constants

src/
  main.py                 Typer CLI — 13 commands (analyze, import, signals, macro, …)
  agents/                 LangGraph / LangChain orchestrated workflows
  analyzers/              Per-asset and portfolio-level enrichment
  clients/
    mcp_client.py         Zerodha Kite MCP (JSON-RPC 2.0)
  formatters/             JSON / HTML report rendering
  importer/
    cli.py                run_import() — entry point for all data imports
    clickhouse.py         Schema DDL, bulk inserts, watermark management
    registry.py           Symbol catalogs (stocks, ETFs, commodities, indices, FX)
    fetchers/             One file per external data source
  ml/
    trend_predictor.py    LightGBM 5-day return predictor
    anomaly.py            Composite anomaly detection (Z + GARCH + Isolation Forest)
  models/                 Pydantic data schemas
  tools/                  Standalone signal functions (no side effects)
  ui/
    app.py                Streamlit 5-tab data hub
  utils/                  Caching, symbol mapping, demo data helpers

scripts/                  Standalone runnable analysis scripts
docs/                     This documentation
skills/                   Gemini / Claude skill definitions
data-engineering-importer/  Data pipeline reference (importer guide + schema)
tests/
docker-compose.yml
```

---

## Importers

All importers are **watermark-based delta-sync**: each fetcher reads `import_watermarks.(source, symbol).last_date`, fetches only new rows, and writes back the watermark after a successful insert. Safe for repeated runs — `ReplacingMergeTree` handles duplicate dates.

| Fetcher | External Source | ClickHouse Table(s) | Cadence |
|---|---|---|---|
| `yfinance_fetcher` | Yahoo Finance | `daily_prices` | Daily |
| `mfapi_fetcher` | MFAPI.in (AMFI) | `mf_nav` | Daily |
| `cot_fetcher` | CFTC Socrata API + ZIP archives | `cot_gold` | Weekly (Fri) |
| `nse_inav_fetcher` | NSE website | `inav_snapshots` | Every 15 min (market hours) |
| `fii_dii_fetcher` | Sensibull oxide API | `fii_dii_flows`, `fii_dii_monthly`, `fii_dii_fno_daily` | Daily |
| `imf_reserves_fetcher` | IMF IFS REST API | `cb_gold_reserves` | Monthly |
| `etf_aum_fetcher` | Yahoo Finance | `etf_aum` | Daily |
| `mf_holdings_fetcher` | Morningstar (mstarpy) | `mf_holdings` | Monthly |
| `import_dsp_history.py` (script) | DSP website ZIP archives | `mf_holdings` | One-time backfill (Sep 2023–Mar 2026); writes per-fund watermark |
| News tools | NewsAPI + Google News RSS | `news_articles` | Twice daily |
| `signal_aggregator` | Reads ClickHouse | `signal_composite` | Daily / on-demand |
| `trend_predictor` | Reads ClickHouse | `ml_predictions` | Daily after close |

### Symbol Registry (`src/importer/registry.py`)

| Category | Count | Examples |
|---|---|---|
| Stocks | 50 | RELIANCE, TCS, HDFCBANK, INFY, ICICIBANK |
| ETFs | 30+ | GOLDBEES, SILVERBEES, NIFTYBEES, BANKBEES, ITBEES, CPSEETF |
| Commodities | 7 | GOLD (GC=F), SILVER (SI=F), COPPER (HG=F), CRUDEOIL, NGAS, PLATINUM, PALLADIUM |
| Indices | 10 | NIFTY50, SENSEX, BANKNIFTY, SP500, NASDAQ, US10Y, DXY |
| FX Pairs | 5 | USDINR, USDCNY, USDAED, USDSAR, USDKWD |
| MF Schemes | 12 | GOLDBEES (140088), SILVERBEES (149758), NIFTYBEES (140084) |
| MF Holdings Watchlist | 4 | DSP Multi Asset, Quant Multi Asset, ICICI Multi Asset, Bajaj Multi Asset |

---

## ClickHouse Tables

Database: `market_data`. All tables use `ReplacingMergeTree` for idempotent re-imports.

| Table | Key Columns | Purpose |
|---|---|---|
| `daily_prices` | symbol, category, trade_date, OHLCV, imported_at | OHLCV time series — stocks, ETFs, commodities, indices, FX |
| `mf_nav` | symbol, scheme_code, nav_date, nav | MF / ETF daily NAV (AMFI via MFAPI.in) |
| `inav_snapshots` | symbol, snapshot_at, inav, market_price, premium_discount_pct | ETF iNAV vs market price (intraday) |
| `cot_gold` | report_date, mm_long, mm_short, mm_net, open_interest | CFTC COT Gold positioning (weekly) |
| `cb_gold_reserves` | ref_period, country_code, reserves_tonnes | Central bank gold holdings (quarterly) |
| `etf_aum` | trade_date, symbol, aum_usd, implied_tonnes | ETF assets under management (daily) |
| `fx_rates` | symbol, trade_date, OHLC | Currency pair daily OHLC |
| `ml_predictions` | as_of, horizon_days, expected_return_pct, confidence_low/high, regime_signal, cv_r2_mean | LightGBM 5-day return forecasts for GOLDBEES |
| `mf_holdings` | scheme_code, as_of_month, isin, security_name, market_value_cr, pct_of_nav | Mutual fund portfolio compositions (monthly) |
| `fii_dii_flows` | trade_date, fii_net_cr, dii_net_cr | Daily FII/DII cash-market net flows (₹ Crore) |
| `fii_dii_monthly` | month_date, fii_net_cr, dii_net_cr, nifty_close | Monthly FII/DII aggregate + Nifty context |
| `fii_dii_fno_daily` | trade_date, fii_fut_net_oi, fii_opt_overall_net_oi | Daily F&O participant OI (futures + options) |
| `signal_composite` | as_of, etf_symbol, macro_score, sentiment_score, valuation_score, flow_score, ml_score, composite_score, action | Multi-pillar ETF scores 0–100 with BUY/HOLD/SELL action |
| `news_articles` | fetched_at, title, source, sentiment, etfs_impacted, category, impact_tier | ETF-tagged news + macro events |
| `import_watermarks` | source, symbol, last_date | Delta-sync state tracking |

---

## Tools (`src/tools/`)

Each tool is a standalone function that returns a dict or DataFrame. No database writes, no side effects. Can be called independently or composed inside agents.

| Tool | File | Signal Produced |
|---|---|---|
| **Quant Scorecard** | `quant_scorecard.py` | Gold + Silver 4-pillar 0–100 composite scores (Macro / Flows / Valuation / Momentum) |
| **Macro Event Scanner** | `macro_event_scanner.py` | 8 macro themes → per-ETF impact direction (+1 / -1) + conviction, sourced from live news |
| **iNAV Fetcher** | `inav_fetcher.py` | Live iNAV, market price, premium/discount % for any NSE ETF |
| **COMEX Fetcher** | `comex_fetcher.py` | Pre-market signals for XAU, XAG, XPT, XPD, HG vs prior close |
| **Who Is Selling** | `who_is_selling_agent.py` | FII / DII / Retail sell-off attribution and flow signals |
| **Premium Alerts** | `premium_alerts.py` | ETF iNAV premium/discount threshold breach alerts |
| **Domestic ETF Scanner** | `domestic_etf_scanner.py` | Z-score valuation + flow + momentum per ETF |
| **Market Context** | `market_context.py` | Live Nifty/BankNifty levels + market regime for LLM prompts |
| **News Search** | `news_search.py` | GNews RSS articles + keyword sentiment for any symbol |
| **NewsAPI Search** | `newsapi_search.py` | NewsAPI.org articles from premium Indian financial publications |
| **Earnings Scraper** | `earnings_scraper.py` | Quarterly results from Screener.in / Yahoo Finance |
| **Historic iNAV** | `historic_inav.py` | Historical iNAV snapshots for ETFs |
| **Valuation Alerts** | `valuation_alerts.py` | P/E, yield, P/B ratio threshold crossings |
| **Summarization** | `summarization.py` | LLM-generated risk and sentiment summaries per holding |
| **Zerodha MCP Tools** | `zerodha_mcp_tools.py` | Holdings, positions, orders via Zerodha Kite MCP |

### Quant Scorecard Pillars (`quant_scorecard.py`)

**Gold (GOLDBEES):**
| Pillar | Weight | Signal Sources | Scoring |
|---|---|---|---|
| Macro | 30% | DXY (yfinance) + US10Y yield | DXY ≤ 100 → 100; ≥ 110 → 0. Real yield 5D delta ≤ −0.10 → 100 |
| Flows | 30% | COT gold (ClickHouse `cot_gold`) | mm_net/OI ≤ 20% → 100; ≥ 35% → 0 |
| Valuation | 20% | iNAV snapshot (`inav_snapshots`) | Discount > 0.5% → 100; Premium > 0.5% → 0 |
| Momentum | 20% | LightGBM (`ml_predictions`) | Return ≥ +1% → 100; ≤ −1% → 0 |

**Silver (SILVERBEES) — additional signals:**
| Pillar | Weight | Signal Sources | Scoring |
|---|---|---|---|
| Macro | 30% | DXY + US10Y + Gold-Silver Ratio (yfinance) | GSR ≥ 90 → 100 (silver cheap); ≤ 55 → 0 |
| Flows | 30% | CFTC live TXT (`SILVER - COMMODITY`, code 084) | mm_net/OI ≤ 20% → 100; ≥ 35% → 0 |
| Valuation | 20% | SILVERBEES iNAV (`inav_snapshots`) | Same as gold |
| Momentum | 20% | SI=F 5-day realised return (yfinance fallback) | Return ≥ +2% → 100; ≤ −2% → 0 |

---

## ML (`src/ml/`)

### TrendPredictor (`trend_predictor.py`)

- **Target:** 5-day forward log return for GOLDBEES
- **Algorithm:** LightGBM with `TimeSeriesSplit` walk-forward cross-validation
- **25+ alpha features:**
  - Momentum: logret1, logret5, logret20, EMA crosses
  - Mean-reversion: price/MA ratio
  - Volatility: ATR, historical vol
  - Macro: DXY, USD/INR, US 10Y yield
  - Market microstructure: COT leverage ratio, iNAV spread
  - Flows: FII/DII 5-day rolling net
  - Seasonality: month sin/cos, day-of-week encoding
- **Output written to:** `ml_predictions` — expected_return_pct, confidence bounds, cv_r2_mean, regime_signal

### AnomalyDetector (`anomaly.py`)

Three-stage composite pipeline:

1. **Robust Z-score** — MAD-based, resistant to fat tails in gold returns
2. **GARCH(1,1) standardised residuals** — isolates true price shocks from routine volatility clustering
3. **Isolation Forest** — cross-asset feature confirmation

Regime labels output: `Crowded Long`, `Strong Trend`, `Flash Crash`, `Volatile Breakout`, `Blow-off Top`, `Normal`

Fire rate ~5% (vs. Random Forest's spurious 21% prior to GARCH replacement).

---

## Agents (`src/agents/`)

Agents orchestrate multiple tools into complete workflows using LangGraph / LangChain.

### PortfolioAgent (`portfolio_agent.py`)
Full Zerodha portfolio intelligence workflow.

```
Auth (Kite MCP) → Fetch Holdings
    ↓
Parallel enrichment per holding:
  Yahoo Finance (prices, metrics)
  + News (NewsAPI + GNews)
  + Earnings (Screener.in / Yahoo)
    ↓
Per-asset LLM scoring  (fallback: rule-based if no LLM)
    ↓
Portfolio aggregation + LLM summary
    ↓
JSON report + HTML dashboard
```

Entry: `run_full_analysis()` | Ad-hoc: `ask(question)` via ReAct loop

### ComexAgent (`comex_agent.py`)
Pre-market commodity signals for XAU, XAG, XPT, XPD, HG.

- **Local LLM path:** direct call to `get_comex_signals()` (avoids tool loop)
- **Cloud LLM path:** LangGraph with loop guard (max 2 tool calls)
- Signal thresholds: ±0.3% = neutral, ±1% = strong

### NewsSentimentAgent (`news_sentiment_agent.py`)
Multi-source news sentiment for any stock or ETF.

- Sources: NewsAPI.org + Google News RSS (gnews)
- Single-call design via `collate_news_sentiment()` to prevent tool loops
- Output: overall_sentiment (POSITIVE / NEUTRAL / NEGATIVE), per-article scores, deduplicated

### SignalAggregator (`signal_aggregator.py`)
Composite ETF signal — 6 pillars → 0–100 score → BUY / ACCUMULATE / HOLD / TRIM / AVOID

| Pillar | Weight | Source |
|---|---|---|
| Macro | 25% | `macro_event_scanner` → net signal across 8 themes |
| Sentiment | 15% | `news_articles` table — pos/neg ratio last 7 days |
| Valuation | 15% | `domestic_etf_scanner` — iNAV Z-score premium/discount |
| Flow | 25% | `fii_dii_flows` — 5D net; equity ETFs benefit, safe-haven inverse |
| ML | 15% | `ml_predictions` — LightGBM expected return (GOLDBEES only; others neutral) |
| Anomaly | 5% | `anomaly.py` — Flash Crash boost / Blow-off dampener |

Covers 18 core ETFs. Output optionally written to `signal_composite` table via `--save`.

> **Planned 7th pillar — DSP Smart Money (pending):** MoM delta of DSP Multi Asset gold/equity allocation (`mf_holdings` table) as a contrarian tactical signal. Source: `scripts/dsp_quant_strategy_analyzer.py`. GSR correlation R=0.68 identified as primary driver of DSP allocation shifts.

### VisualizationAgent (`visualization_agent.py`)
Generates a self-contained React HTML dashboard from a JSON portfolio report. No build step required.

---

## Scripts (`scripts/`)

Standalone scripts that run analyses against the live database and print Rich console output.

| Script | Purpose |
|---|---|
| `metals_quant_scorecard.py` | Run Gold + Silver quant scorecards side-by-side |
| `opportunity_scan.py` | Cross-asset DB scan — momentum, drawdown, RSI, iNAV, flows → ranked opportunity table |
| `gold_quant_scorecard.py` | Gold-only 4-pillar scorecard (GOLDBEES) |
| `fii_pattern_check.py` | FII historical buying/selling pattern analysis |
| `import_dsp_history.py` | One-time ETL backfill: 31-month DSP Multi Asset holdings (Sep 2023–Mar 2026) from DSP website ZIPs into `mf_holdings`; writes watermark on completion |
| `dsp_quant_strategy_analyzer.py` | Reverse-engineer DSP's trading rules by correlating monthly allocation deltas against Mosaic quant signals (DXY, COT, iNAV, GSR, ML). Identifies GSR as primary tactical lever (R=0.68). |
| `whale_tracker.py` | Large FII/DII flow detection and alerts |

---

## CLI Commands (`src/main.py`)

| Command | Purpose | Writes To |
|---|---|---|
| `analyze` | Full portfolio analysis (Zerodha → enrich → score → report) | JSON + HTML |
| `import` | Sync market data to ClickHouse (delta or full) | ClickHouse |
| `signals` | Run SignalAggregator for 18 ETFs | Console (+ DB with `--save`) |
| `macro` | Run macro event scanner (8 themes) | Console (+ DB with `--save`) |
| `etf-news` | ETF-specific news scanner | Console (+ DB with `--save`) |
| `comex` | Pre-market commodity signals | Console |
| `who-is-selling` | FII/DII/Retail flow analysis | Console |
| `premium-alerts` | iNAV premium/discount threshold alerts | Console |
| `news SYMBOL` | Multi-source sentiment for a symbol | Console |
| `ask "question"` | Free-form ReAct agent with tool access | Console |
| `dashboard` | Render React HTML dashboard from latest report | HTML file |
| `config` | Show current settings (API keys masked) | Console |

---

## Configuration (`config/settings.py`)

All settings are loaded from `.env`. See [docs/configuration.md](configuration.md) for full reference.

| Group | Key Settings |
|---|---|
| LLM | `llm_provider` (openai/anthropic), `llm_model`, `llm_base_url` (local), `llm_context_window` |
| API Keys | `openai_api_key`, `anthropic_api_key`, `newsapi_key`, `gold_api_key` |
| Zerodha | `kite_mcp_url`, `kite_api_key`, `kite_api_secret` |
| ClickHouse | `clickhouse_host`, `clickhouse_port`, `clickhouse_database`, `clickhouse_user/password` |
| Caching | `comex_cache_ttl_seconds` (3600), `newsapi_cache_ttl_seconds` (3600) |
| Market | `nse_suffix` (.NS), `market_timezone` (Asia/Kolkata), `market_open` (09:15), `market_close` (15:30) |
| App | `output_dir`, `log_level`, `news_articles_per_stock`, `news_lookback_days` |

---

## Design Patterns

### 1. Watermark Delta Sync
Every fetcher checks `import_watermarks.(source, symbol).last_date` before fetching. Only rows after `last_date - overlap_days` are fetched and inserted. `ReplacingMergeTree` deduplicates on re-import. Use `--full` to bypass watermarks.

### 2. Graceful Pillar Degradation
Every signal pillar degrades to `None` (not 0) when its data source is unavailable. The composite score re-weights across available pillars only, maintaining a valid 0–100 range. Missing pillars do not penalise the composite.

### 3. Dual LLM + Rule-Based Fallback
All agent scoring paths have a rule-based fallback when no LLM is configured. LLM provider (OpenAI / Anthropic / local via OpenAI-compatible endpoint) is selected at runtime via `llm_provider` setting.

### 4. Tool Loop Protection
- ComexAgent uses a direct function call for local LLMs (avoids ReAct loop overhead)
- NewsSentimentAgent uses a single `collate_news_sentiment()` call (not a tool loop)
- LangGraph agents have explicit loop guards (`max_iterations=2`)

### 5. iNAV Arbitrage Detection
NSE iNAV snapshots are captured every 15 minutes during market hours. `premium_discount_pct > +0.5%` triggers a premium alert; `< −0.25%` flags a discount opportunity. The SILVERBEES / GOLDBEES premium spread is a direct input to the quant scorecard valuation pillar.

---

## External Data Sources

| Source | Auth Required | Quota | Used For |
|---|---|---|---|
| Yahoo Finance (yfinance) | No | Soft rate limits | OHLCV, ETF AUM, FX, indices, GSR, silver momentum |
| Google News RSS (gnews) | No | None | Macro themes, ETF news |
| CFTC Socrata API | No | None | COT gold + silver positioning |
| CFTC direct TXT/ZIP | No | None | Silver COT (live `f_disagg.txt`) |
| NSE website | No | Soft | Live iNAV snapshots |
| Sensibull oxide API | No | None | FII/DII daily + monthly + F&O OI |
| IMF IFS REST API | No | None | Central bank gold reserves |
| MFAPI.in | No | None | MF / ETF NAV history |
| Morningstar (mstarpy) | No | None | MF portfolio holdings |
| NewsAPI.org | `newsapi_key` | 100 req/day (free) | Premium Indian financial news |
| gold-api.com | `gold_api_key` | Strict daily quota | COMEX live spot (with 1h cache) |
| Zerodha Kite MCP | Optional (hosted endpoint) | None | Live portfolio holdings + positions |
| OpenAI / Anthropic | `openai_api_key` / `anthropic_api_key` | Pay-per-token | LLM scoring, summaries, ReAct agent |
