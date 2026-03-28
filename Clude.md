# Mosaic Fund Agent — Knowledge Graph

## Project Overview

**Mosaic Fund Agent** is a Python-based AI-powered portfolio intelligence system for Indian stock markets.
It connects to a Zerodha brokerage account via MCP (Model Context Protocol), enriches each holding with
live market data, news sentiment, earnings, and commodity signals, then produces an LLM-scored portfolio
report with an auto-refreshing HTML dashboard.

- **Language**: Python 3.11+
- **CLI Framework**: typer
- **Agent Framework**: LangGraph + Deep Agents
- **LLM Providers**: OpenAI (GPT-4o-mini), Anthropic (Claude Haiku), or local (LM Studio/Ollama)
- **Cost**: ₹4–12/run (cloud) or free (local model)
- **Core Output**: JSON report (`./output/*.json`) + auto-refreshing HTML dashboard

---

## Architecture Diagram

```mermaid
flowchart TD
    User(["👤 User"])

    subgraph CLI ["CLI — src/main.py (typer)"]
        CLICmd["analyze [--demo --max N --quiet]\nask 'question'\nconfig\nnews\ncomex\ndashboard"]
    end

    subgraph Agent ["PortfolioAgent — portfolio_agent.py"]
        subgraph Pipeline ["analyze → Sequential Pipeline"]
            S1["① Fetch Holdings — KiteMCPClient → Zerodha Kite MCP"]
            S2["② Enrich Each Holding — asset_analyzer.py"]
            S3["③ LLM Scoring — summarization.py"]
            S4["④ Portfolio Aggregation — portfolio_analyzer.py"]
            S5["⑤ COMEX Pre-Market Signals — comex_fetcher.py"]
            S6["⑥ Format & Save — output.py + visualization_agent.py"]
            S1 --> S2 --> S3 --> S4 --> S5 --> S6
        end
        subgraph ReActLoop ["ask → LangGraph ReAct Loop"]
            ReAct["LLM reasons over registered tools"]
        end
    end

    subgraph Tools ["Enrichment Tools"]
        YF["yahoo_finance.py — price, P/E, sector, momentum"]
        NS["news_search.py — GNews RSS"]
        NA["newsapi_search.py — NewsAPI.org"]
        ES["earnings_scraper.py — Screener.in"]
        IV["inav_fetcher.py — NSE live iNAV (ETF)"]
        HI["historic_inav.py — AMFI 30-day NAV (ETF)"]
        CF["comex_fetcher.py — gold-api.com"]
    end

    User --> CLICmd
    CLICmd -->|analyze| S1
    CLICmd -->|ask| ReAct
    S2 --- YF & NS & NA & ES & IV & HI
    S5 --- CF
```

---

## Module Dependency Graph

```
src/main.py (CLI entry point)
├── config/settings.py (Settings — Pydantic BaseSettings from .env)
├── src/ml/anomaly.py (composite anomaly detection — used by Streamlit UI)
├── src/agents/portfolio_agent.py (PortfolioAgent — main orchestrator)
│   ├── src/analyzers/asset_analyzer.py (analyze_holding)
│   │   ├── src/tools/yahoo_finance.py (fetch_yahoo_data, fetch_price_history)
│   │   ├── src/tools/news_search.py (fetch_news_for_symbol)
│   │   ├── src/tools/newsapi_search.py (fetch_newsapi_articles)
│   │   ├── src/tools/earnings_scraper.py (fetch_from_screener)
│   │   ├── src/tools/inav_fetcher.py (get_etf_inav)
│   │   ├── src/tools/historic_inav.py (get_historic_inav)
│   │   ├── src/tools/summarization.py (summarize_asset / summarize_asset_demo)
│   │   └── src/utils/symbol_mapper.py (get_company_name, to_nse_yahoo)
│   ├── src/analyzers/portfolio_analyzer.py (build_portfolio_report)
│   │   └── src/tools/summarization.py (summarize_portfolio)
│   ├── src/clients/mcp_client.py (KiteMCPClient — Zerodha Kite MCP)
│   ├── src/models/portfolio.py (Holding, Portfolio, PortfolioReport)
│   └── src/utils/demo_data.py (get_demo_holdings)
├── src/agents/comex_agent.py (ComexAgent)
│   └── src/tools/comex_fetcher.py (get_comex_signals)
├── src/agents/news_sentiment_agent.py (NewsSentimentAgent)
│   ├── src/tools/news_search.py
│   └── src/tools/newsapi_search.py
├── src/agents/visualization_agent.py (VisualizationAgent — HTML dashboard)
├── src/formatters/output.py (print_report_to_console, save_json_report)
└── src/utils/report_loader.py (load_latest_report — for `ask` command)
```

---

## CLI Commands (`src/main.py`)

| Command | Description | Key Options |
|---------|-------------|-------------|
| `analyze` | Full portfolio analysis pipeline | `--demo`, `--max N`, `--quiet`, `--no-dashboard` |
| `dashboard` | Generate HTML dashboard from latest report | — |
| `ask` | Free-form Q&A via ReAct agent over portfolio | `question: str` |
| `config` | Display current settings (sensitive fields masked) | — |
| `news` | Multi-source news sentiment analysis | `symbol`, `--company` |
| `comex` | COMEX pre-market commodity signals | — |

---

## Data Models (`src/models/portfolio.py`)

### Enums

| Enum | Values |
|------|--------|
| `Exchange` | NSE, BSE |
| `InstrumentType` | EQ, ETF, BE |
| `Sentiment` | POSITIVE, NEGATIVE, NEUTRAL, MIXED |

### Core Models

| Model | Key Fields | Purpose |
|-------|------------|---------|
| `Holding` | symbol, exchange, quantity, average_price, last_price, pnl | Raw broker holding |
| `Position` | symbol, exchange, quantity, buy_price, sell_price, pnl | Open position |
| `Portfolio` | holdings, positions, equity, available_margin | Full account state |
| `NewsItem` | title, source, url, published_at, sentiment | Single news article |
| `QuarterlyResult` | period, revenue_cr, net_profit_cr, yoy_revenue_growth_pct | Earnings data |
| `YahooFinanceData` | symbol, sector, industry, market_cap, pe_ratio, current_price, fifty_two_week_high/low, price_history | Market data |
| `AssetAnalysis` | holding, yahoo_data, news, quarterly_result, summary, risk_score, sentiment_score, insights | Per-holding enrichment |
| `PortfolioSummary` | total_invested, current_value, overall_pnl_pct, health_score, diversification_score | Aggregated metrics |
| `PortfolioReport` | summary, holdings_analysis, sector_allocation, portfolio_risks, actionable_insights, rebalancing_signals | Final output |

### Computed Properties on `Holding`

- `invested_value` = quantity × average_price
- `current_value` = quantity × last_price
- `pnl_percent` = ((last_price − average_price) / average_price) × 100
- `yahoo_symbol` = symbol + ".NS" (default NSE)

---

## Agents (`src/agents/`)

### PortfolioAgent (`portfolio_agent.py`)

Main orchestrator for the full analysis pipeline.

| Method | Purpose |
|--------|---------|
| `__init__(demo_mode=False)` | Init LLM + ReAct agent; graceful fallback to rule-based |
| `run_full_analysis(console)` | 6-step pipeline: Fetch → Enrich → Score → Aggregate → COMEX → Format |
| `ask(question)` | ReAct agent Q&A with latest report context injection |
| `_fetch_holdings_async()` | Async Kite MCP fetch with auto-login on 401 |
| `_build_llm()` | Priority: local OpenAI-compat > Anthropic > OpenAI |

**Tool Registry**: `ALL_TOOLS = ZERODHA_TOOLS + YAHOO_TOOLS + NEWS_TOOLS + EARNINGS_TOOLS + SUMMARIZATION_TOOLS`

### ComexAgent (`comex_agent.py`)

Commodity pre-market signals (XAU, XAG, XPT, XPD, HG).

| Method | Purpose |
|--------|---------|
| `run()` | Local → direct call; Cloud → deep-agent (recursion_limit=6) |
| `_run_direct()` | Bypass agent, call `get_comex_signals()` directly |

**Loop Guard**: `_MAX_TOOL_CALLS = 2` with thread-local counter.  
**Tools**: `fetch_all_comex_signals`, `fetch_single_commodity`, `get_comex_pre_market_context`

### NewsSentimentAgent (`news_sentiment_agent.py`)

Multi-source news sentiment with deduplication.

| Method | Purpose |
|--------|---------|
| `run(symbol, company_name)` | Local → direct; Cloud → deep-agent |
| `_run_direct(symbol, company_name)` | Direct `collate_news_sentiment` invocation |

**Tools**: `collate_news_sentiment`, `get_newsapi_stock_news`, `get_stock_news`  
**Deduplication**: Normalizes titles via `_norm()` to merge across NewsAPI + GNews.

### VisualizationAgent (`visualization_agent.py`)

HTML dashboard generator — zero-build React 18 + Tailwind from CDN.

| Method | Purpose |
|--------|---------|
| `generate(report)` | Build data → render HTML → write to disk |
| `_build_dashboard_data(report)` | Flatten report into React-optimized structure |
| `_render_html(data)` | Inject JSON into HTML template via `window.__PORTFOLIO_DATA__` |
| `open_in_browser(path)` | Open `file://` URL in default browser |

**React Components**: `MetricCard`, `SectionTitle`, `ComexPanel`, `SvgSectorChart`, `SvgInavChart`, `HoldingCard`, `BulletList`, `Dashboard`

---

## Tools (`src/tools/`)

### Data Fetchers

| Module | Function | External API | Returns |
|--------|----------|-------------|---------|
| `yahoo_finance.py` | `fetch_yahoo_data(symbol, exchange="NSE")` | Yahoo Finance | `YahooFinanceData` |
| `yahoo_finance.py` | `fetch_price_history(symbol, period="3mo")` | Yahoo Finance | `list[OHLCV dicts]` |
| `earnings_scraper.py` | `fetch_from_screener(symbol)` | Screener.in + Yahoo fallback | `QuarterlyResult \| None` |
| `news_search.py` | `fetch_news_for_symbol(symbol, company_name)` | Google News RSS (GNews) | `list[NewsItem]` |
| `newsapi_search.py` | `fetch_newsapi_articles(symbol, company_name)` | NewsAPI.org | `list[NewsItem]` |
| `inav_fetcher.py` | `get_etf_inav(symbol)` | NSE API + Yahoo fallback | `dict[inav, market_price, premium_discount_pct, label]` |
| `inav_fetcher.py` | `get_portfolio_etf_inav(symbols)` | NSE API | `dict[symbol → iNAV data]` |
| `historic_inav.py` | `get_historic_inav(symbol, days=30)` | MFAPI.in + Yahoo | `dict[records, trend, sparkline]` |
| `comex_fetcher.py` | `get_comex_signals(symbols)` | gold-api.com + Yahoo | `dict[commodities, signals, summary]` |
| `zerodha_mcp_tools.py` | `fetch_portfolio_holdings()` | Zerodha Kite MCP | `dict[holdings]` |

### LangChain `@tool` Decorators

| Tool Name | Module | Input |
|-----------|--------|-------|
| `get_quarterly_results` | earnings_scraper | `input_str: str` (symbol) |
| `get_stock_news` | news_search | `input_str: str` (symbol\|company) |
| `get_newsapi_stock_news` | newsapi_search | `input_str: str` (symbol\|company) |
| `get_yahoo_finance_data` | yahoo_finance | `input_str: str` (symbol) |
| `get_price_momentum` | yahoo_finance | `input_str: str` (symbol) |
| `fetch_portfolio_holdings` | zerodha_mcp_tools | `_` (no input) |
| `fetch_open_positions` | zerodha_mcp_tools | `_` (no input) |
| `fetch_account_profile` | zerodha_mcp_tools | `_` (no input) |
| `initiate_kite_login` | zerodha_mcp_tools | `_` (no input) |

### LLM Analysis (`summarization.py`)

| Function | Purpose |
|----------|---------|
| `summarize_asset(asset_data)` | LLM-scored per-holding analysis → risk_score, sentiment_score, insights |
| `summarize_portfolio(portfolio_data)` | LLM-scored portfolio-level → health_score, diversification, risks, actions |
| `summarize_asset_demo(asset_data)` | Rule-based scoring (no LLM needed) for demo mode |

---

## Analyzers (`src/analyzers/`)

### `asset_analyzer.py`

**Single-holding enrichment pipeline**: `analyze_holding(holding, use_llm=True)`

Flow: Yahoo Finance → News (GNews + NewsAPI) → Quarterly Results → iNAV (ETF only) → Historic iNAV (ETF only) → LLM Scoring → `AssetAnalysis`

### `portfolio_analyzer.py`

**Portfolio-level aggregation**: `build_portfolio_report(portfolio, holdings_analysis, comex_signals, use_llm=True)`

Key computations:
- Sector allocation via `SYMBOL_SECTOR_FALLBACK` (120+ symbol → sector mappings)
- Concentration risk via HHI (Herfindahl-Hirschman Index)
- Diversification score (0–100)
- Portfolio health score
- COMEX-to-ETF linkage via `_STATIC_COMEX_MAP` (e.g., GOLDBEES → XAU)

---

## Clients (`src/clients/`)

### `KiteMCPClient` (`mcp_client.py`)

Async HTTP client for Zerodha Kite MCP server using JSON-RPC 2.0 protocol.

| Method | Purpose |
|--------|---------|
| `get_holdings()` | Fetch portfolio holdings |
| `get_positions()` | Fetch open positions |
| `get_margins()` | Fetch account margins |
| `get_quotes(symbols)` | Get live quotes |
| `get_ltp(symbols)` | Get last traded prices |
| `login()` | Initiate OAuth browser login |

- Uses `httpx.AsyncClient` with session cookie management
- Context manager support (`async with KiteMCPClient() as client`)
- Auto-login on 401 responses

---

## Formatters (`src/formatters/`)

### `output.py`

Rich terminal output + JSON persistence.

| Function | Purpose |
|----------|---------|
| `print_report_to_console(report)` | 9-section Rich terminal display |
| `save_json_report(report)` | Save to `./output/portfolio_YYYYMMDD_HHMMSS.json` |

**Console Sections**: COMEX signals → Portfolio overview → Holdings table → Per-holding panels → Sector chart → Risks → Insights → Rebalancing signals

**Specialized Panels**: iNAV premium/discount, Historic iNAV sparklines, COMEX commodity signals

---

## ML Module (`src/ml/`)

### `anomaly.py`

Self-contained composite anomaly detection for daily OHLCV time series.  
Independent of the UI — importable from CLI, agents, or tests.

**3-step pipeline** exposed via `run_composite_anomaly(df, rf_lags, contamination, z_threshold)`:

| Step | Function | Output columns |
|------|----------|-----------------|
| 1 | `robust_zscore(s)` | `z_return`, `z_range`, `z_robust` |
| 2 | `fit_rf_residuals(df, rf_lags, train_frac)` | `rf_pred`, `residual`, `z_resid`, `z_resid_abs` |
| 3 | `fit_isolation_forest(df, contamination)` | `if_confidence` (0→1), `if_label` (-1/1) |
| — | `classify_regime(df)` | `final_z`, `final_z_abs`, `regime` |

**Formula:** `Final_Z = Z_robust × (1 + IF_confidence)`

**Regime matrix:**

| Z_robust | Z_resid | Regime |
|---|---|---|
| High | Low  | 📈 Strong Trend (HODL) |
| Low  | High | ⚡ Flash Crash / Black Swan (EXIT) |
| High | High | 🔥 Volatile Breakout |
| Low  | Low  | ✅ Normal |

**RF features:** `lag_1..lag_N`, `ma7`, `ma30`, `vol_lag1` (lag count configurable, default 5)

**Returns:** `(df_result, df_flagged, r2_train)` — full DataFrame with all signals, flagged subset, RF R².

---

## Utils (`src/utils/`)

| Module | Key Functions | Purpose |
|--------|---------------|---------|
| `cache.py` | `cache_get(key, ttl)`, `cache_set(key, data)`, `cache_clear()`, `cache_age_seconds(key)` | Disk-based TTL cache (stdlib only) |
| `demo_data.py` | `get_demo_holdings()` | 5 sample holdings (3 stocks + 2 ETFs) |
| `report_loader.py` | `load_latest_report()`, `_compact_context(report)` | Token-aware context builder (8K/32K/cloud scaling) |
| `symbol_mapper.py` | `get_company_name(symbol)`, `to_nse_yahoo(symbol)`, `to_bse_yahoo(symbol)`, `from_yahoo(yahoo_symbol)` | 160+ NSE symbol ↔ Yahoo ↔ company name mappings |

---

## Configuration (`config/settings.py`)

Pydantic `BaseSettings` loading from `.env` file:

| Category | Key Fields |
|----------|------------|
| **LLM** | `llm_provider` (openai/anthropic/local), `openai_api_key`, `anthropic_api_key`, `llm_model`, `llm_base_url` |
| **Zerodha MCP** | `kite_mcp_url`, `kite_mcp_timeout` |
| **APIs** | `newsapi_key`, `gold_api_key` |
| **Cache** | `cache_dir`, `comex_cache_ttl` (3600s), `newsapi_cache_ttl` (3600s) |
| **Output** | `output_dir` (./output) |
| **Market** | Market hours, timezone (Asia/Kolkata) |

**Validation**: Warns on missing API keys; masks sensitive fields for `config` command display.

---

## External Services & APIs

| Service | Endpoint | Auth | Module(s) | Notes |
|---------|----------|------|-----------|-------|
| **Zerodha Kite MCP** | `mcp.kite.trade` | OAuth 2.0 (browser) | mcp_client, zerodha_mcp_tools | Account-level rate limits |
| **Yahoo Finance** | yfinance library | None (free) | yahoo_finance, historic_inav | Soft limits |
| **NewsAPI.org** | newsapi.org/v2 | API Key | newsapi_search | 100 req/day (free tier) |
| **Google News** | RSS via GNews | None (free) | news_search | Soft limits |
| **Screener.in** | screener.in/company | Web scraping | earnings_scraper | Polite delays applied |
| **NSE API** | nseindia.com/api | Custom headers | inav_fetcher | Rate limited |
| **MFAPI.in** | mfapi.in/mf | None (free) | historic_inav | No key required |
| **gold-api.com** | gold-api.com/api | `x-access-token` | comex_fetcher | Free tier |
| **OpenAI** | api.openai.com | API Key | summarization, all agents | Pay-per-token |
| **Anthropic** | api.anthropic.com | API Key | summarization, all agents | Pay-per-token |
| **Local LLM** | `LLM_BASE_URL` | None | summarization, all agents | Unlimited / free |

---

## Security Features

### Prompt Injection Protection (`comex_fetcher.py`)

- `_safe_str(val)` — Detects "ignore previous instructions", "SYSTEM:", "act as" → returns `[SANITIZED]`
- `_safe_price(val)` — Rejects non-numeric and negative values
- `_safe_symbol(val)` — Whitelist: XAU, XAG, XPT, XPD, HG only
- `_safe_timestamp(val)` — Validates ISO 8601 format
- ASCII control character stripping, regex pattern detection, string length limits

### Sensitive Data

- All API keys stored in `.env`, loaded via Pydantic Settings
- `config` command masks sensitive fields in display
- No keys logged or serialized to report JSON

### Agent Loop Guards

- `_MAX_TOOL_CALLS = 2` per agent invocation (comex_agent)
- `recursion_limit=6` on LangGraph agents
- Thread-local call counters prevent infinite loops

---

## Caching Strategy

| Cache Key | TTL | Source |
|-----------|-----|--------|
| COMEX signals | 3600s (1h) | `comex_cache_ttl` setting |
| NewsAPI articles | 3600s (1h) | `newsapi_cache_ttl` setting |
| NSE ETF list | Per-process | Module-level variable |

Storage: Disk-based JSON files in `cache_dir` with filename sanitization. Pure stdlib implementation (no external dependencies).

---

## Key Architectural Patterns

1. **Two-Mode Routing**: Local model → direct function call (no token waste); Cloud model → LangGraph deep-agent with loop guards
2. **LLM Fallback**: Attempts LLM init; silently falls back to rule-based scoring (`summarize_asset_demo`) on failure
3. **Tool Aggregation**: Agents expose tool lists (`COMEX_TOOLS`, `NEWS_TOOLS`, etc.) that `PortfolioAgent` combines into `ALL_TOOLS`
4. **Token-Aware Context**: `_compact_context()` scales injected report context based on model capacity (8K/32K/cloud)
5. **Zero-Build Frontend**: React 18 from CDN + Tailwind + pure SVG charts; data injected as `window.__PORTFOLIO_DATA__`
6. **Deduplication**: News articles normalized and merged across NewsAPI + GNews sources

---

## Data Flow: Full Analysis Run

```
User runs: python -m src.main analyze
│
├─ 1. Fetch Holdings
│   ├─ Demo mode → get_demo_holdings() (5 sample holdings)
│   └─ Live mode → KiteMCPClient.get_holdings() → Zerodha MCP
│       └─ 401? → Browser OAuth login → retry
│
├─ 2. Enrich Each Holding (per holding)
│   ├─ fetch_yahoo_data(symbol) → YahooFinanceData
│   ├─ fetch_news_for_symbol(symbol) → list[NewsItem] (GNews)
│   ├─ fetch_newsapi_articles(symbol) → list[NewsItem] (NewsAPI, cached 1h)
│   ├─ fetch_from_screener(symbol) → QuarterlyResult
│   ├─ [ETF only] get_etf_inav(symbol) → iNAV data
│   ├─ [ETF only] get_historic_inav(symbol) → 30-day history + sparkline
│   └─ summarize_asset(data) or summarize_asset_demo(data) → AssetAnalysis
│
├─ 3. COMEX Pre-Market Signals
│   └─ ComexAgent().run() → XAU, XAG, XPT, XPD, HG signals
│
├─ 4. Portfolio Aggregation
│   └─ build_portfolio_report(portfolio, holdings_analysis, comex)
│       ├─ Sector allocation (SYMBOL_SECTOR_FALLBACK — 120+ entries)
│       ├─ Concentration risk (HHI)
│       ├─ Diversification score (0–100)
│       ├─ COMEX-to-ETF linkage (_STATIC_COMEX_MAP)
│       └─ LLM portfolio insights or rule-based fallback
│
└─ 5. Output
    ├─ save_json_report() → ./output/portfolio_*.json
    ├─ print_report_to_console() → Rich terminal panels
    └─ VisualizationAgent().generate() → HTML dashboard + browser open
```

---

## Test Coverage (`tests/`)

| Test File | Type | Coverage |
|-----------|------|----------|
| `test_tools.py` | Unit (11 tests) | Yahoo Finance, symbol mapper, earnings, news, models, sector allocation, config, iNAV, premium/discount, historic iNAV, COMEX signals |
| `test_cache.py` | Unit + Integration | Cache round-trip, TTL expiry, cache clear, NewsAPI cache hit speedup (>50×) |
| `test_inav_cli.py` | Visual + Mocked | 9 ETF scenarios with mocked iNAV/market prices, Rich panel rendering |
| `test_news_sentiment.py` | Smoke (live APIs) | `collate_news_sentiment` + `NewsSentimentAgent._run_direct` |
| `_compare_inav_sources.py` | Script | NSE vs Yahoo Finance iNAV comparison for 8 ETFs |
| `_fetch_live_prices.py` | Script | Live Yahoo Finance data for 9 ETFs |
| `_test_comex.py` | Script | COMEX signals smoke test (XAU, XAG, HG) |
| `_test_nse_parse.py` | Script | NSE API ETF list parsing validation |

### Key Test Scenarios

- **iNAV Premium/Discount thresholds**: 11 boundary scenarios — > +0.25% = PREMIUM, < −0.25% = DISCOUNT, else FAIR VALUE
- **COMEX signal thresholds**: > ±1.0% = STRONG, ±0.3–1.0% = normal, within ±0.3% = NEUTRAL
- **Prompt injection guards**: 10 unit tests for `_safe_str`, `_safe_price`, `_safe_symbol`, `_safe_timestamp`
- **Cache speedup**: Asserts second call is >50× faster than first (disk cache hit)

---

## Dependencies (`requirements.txt`)

| Category | Packages |
|----------|----------|
| **Agent Framework** | `langchain`, `langchain-core`, `langchain-openai`, `langchain-anthropic`, `langgraph`, `deepagents` |
| **MCP Client** | `mcp`, `httpx`, `httpx-sse` |
| **Market Data** | `yfinance`, `pandas`, `numpy` |
| **Web Scraping** | `beautifulsoup4`, `requests`, `lxml`, `fake-useragent` |
| **News** | `gnews`, `newsapi-python` |
| **Config** | `pydantic`, `pydantic-settings`, `python-dotenv` |
| **Output / CLI** | `rich`, `typer` |
| **ML / Anomaly** | `scikit-learn>=1.4.0` (IsolationForest, RandomForestRegressor), `altair>=5.0.0` (Vega-Lite charts in UI) |

---

## File Index

| Path | Purpose |
|------|---------|
| `src/main.py` | CLI entry point — 6 commands via typer |
| `config/settings.py` | Pydantic BaseSettings from .env (40+ fields) |
| `src/agents/portfolio_agent.py` | Main orchestrator — full analysis + ask Q&A |
| `src/agents/comex_agent.py` | COMEX commodity signals agent |
| `src/agents/news_sentiment_agent.py` | Multi-source news sentiment agent |
| `src/agents/visualization_agent.py` | HTML dashboard generator (React 18) |
| `src/analyzers/asset_analyzer.py` | Per-holding enrichment pipeline |
| `src/analyzers/portfolio_analyzer.py` | Portfolio aggregation + scoring |
| `src/clients/mcp_client.py` | Zerodha Kite MCP async client (JSON-RPC 2.0) |
| `src/models/portfolio.py` | All Pydantic data models + enums |
| `src/tools/yahoo_finance.py` | Yahoo Finance data + momentum |
| `src/tools/news_search.py` | Google News via GNews RSS |
| `src/tools/newsapi_search.py` | NewsAPI.org premium news |
| `src/tools/earnings_scraper.py` | Screener.in quarterly results |
| `src/tools/inav_fetcher.py` | Live ETF iNAV from NSE |
| `src/tools/historic_inav.py` | 30-day iNAV history from AMFI/MFAPI.in |
| `src/tools/comex_fetcher.py` | COMEX commodity prices + signals |
| `src/tools/summarization.py` | LLM analysis + rule-based fallback |
| `src/tools/zerodha_mcp_tools.py` | Zerodha MCP LangChain tool wrappers |
| `src/formatters/output.py` | Rich console output + JSON report |
| `src/utils/cache.py` | Disk-based TTL cache (stdlib only) |
| `src/utils/demo_data.py` | 5 sample holdings for demo mode |
| `src/utils/report_loader.py` | Latest report loader + token-aware context compaction |
| `src/utils/symbol_mapper.py` | 160+ NSE ↔ Yahoo ↔ company name mappings |
| `docs/architecture.mmd` | Mermaid architecture diagram |
| `src/ml/__init__.py` | Package marker |
| `src/ml/anomaly.py` | Composite anomaly detection — Robust Z + RF Residuals + Isolation Forest |
| `src/ui/__init__.py` | Package marker |
| `src/ui/app.py` | Streamlit 4-tab UI — Import / SQL Query / Explorer / Anomaly Detection |
