# Mosaic Fund Agent

Ask your Zerodha portfolio a question. Get an actual answer.

Pulls live holdings from Zerodha Kite, enriches each position with news, results, and iNAV data, scores risk and sentiment via LLM, and outputs a terminal report + self-contained HTML dashboard.

A built-in **Streamlit data hub** lets you import and store historical market data in ClickHouse, run arbitrary SQL, and run composite anomaly detection and ML forecasting — no code required.

> **Not financial advice.** This is a personal research tool. Always verify before acting on any output.

Licensed under the [Apache License 2.0](LICENSE).

---

## Features

- **Portfolio analysis** — per-holding risk scores, news sentiment, quarterly results, sector breakdown
- **ETF premium/discount** — live iNAV vs market price for every ETF holding
- **COMEX pre-market signals** — Gold, Silver, Copper, Platinum, Palladium vs previous close
- **Who Is Selling?** — institutional sell-off attribution (retail panic / institutional exit / speculator crowding)
- **LightGBM 5-day forecast** — walk-forward forward-return predictor with 14 alpha features
- **Composite anomaly detection** — Robust Z (MAD) + Random Forest residuals + Isolation Forest
- **FII/DII institutional flows** — daily cash + monthly (Sep 2018→present) + F&O participant OI
- **Macro & geopolitical scanner** — 9 themes (war, Fed/RBI, crude, INR, trade war, gold, risk-off, electrification) mapped to ETF impact
- **Macro Strategy Agent** — specialized specialist for 2026 "Baton Pass" analysis, institutional tracking, and valuation re-rating
- **ETF-impact news scanner** — 10 ETF categories tagged with sentiment scores, Google News RSS + Yahoo Finance, no API key
- **HTML dashboard** — self-contained, auto-refreshes every 5 minutes
- **Streamlit UI** — import, SQL explorer, charts, anomaly detection, quant scorecard, market news

---

## Usage

### Macro Strategy Agent (2026 Specialist)

The `macro-strategy-agent` is a specialized specialist designed to identify structural shifts in the 2026 market, specifically the **"Baton Pass" from paper to real assets**. It tracks 9 macro themes, institutional "Whale" moves, and performs valuation re-rating checks.

**Invoke the agent:**
```bash
# General delegation (main agent will route to specialist)
gemini ask "analyze the latest moves in the electrification theme"

# Explicit delegation (@ syntax)
@macro-strategy-agent analyze the outlook for Gold vs Nifty for 2026
@macro-strategy-agent check valuation for L&T and Hindalco
@macro-strategy-agent identify top PSU holdings in Quant Multi-Asset fund
```

### Portfolio analysis


- Python 3.11+
- Zerodha account (or use `--demo` mode)
- OpenAI / Anthropic API key **or** a local LLM (LM Studio / Ollama)
- Docker (for ClickHouse — required only for data hub features)

### Install

```bash
git clone https://github.com/Mosaic-agent/Mosaic-fund-agent.git
cd Mosaic-fund-agent
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

**macOS — OpenMP for LightGBM:**
```bash
brew install libomp
```

### Configure

```bash
cp .env.example .env   # fill in your keys
```

Minimum keys to get started:
```
OPENAI_API_KEY=sk-...
NEWSAPI_KEY=...
GOLD_API_KEY=...
```

See [docs/configuration.md](docs/configuration.md) for all options including local LLM setup.

### Start ClickHouse

```bash
docker compose up clickhouse -d
python src/main.py config   # verify connection
```

---

## Usage

### Portfolio analysis

```bash
python src/main.py analyze --demo        # no login needed
python src/main.py analyze               # live portfolio
python src/main.py analyze --max 3       # test with 3 holdings
```

### Ask a question

```bash
python src/main.py ask "which holdings have the worst news sentiment?"
python src/main.py ask "am I overexposed to IT sector?"
python src/main.py ask "which ETFs are trading at a premium?"
```

### Market news (no API key)

```bash
# Scan macro & geopolitical events → map to ETF directional impact
python src/main.py macro                 # print to terminal
python src/main.py macro --save          # scan + persist to ClickHouse

# Fetch ETF-tagged news with sentiment scores
python src/main.py etf-news              # all categories
python src/main.py etf-news --category "Gold ETFs"   # single category
python src/main.py etf-news --save       # scan + persist to ClickHouse
```

After saving, open the **📰 Market News** tab in the Streamlit UI to browse news by date, theme, and category — sourced from ClickHouse.

### Other commands

```bash
python src/main.py comex                 # COMEX pre-market signals
python src/main.py news GOLDBEES         # news for a single symbol
python src/main.py dashboard            # open HTML dashboard
python src/main.py config               # show current settings (masked)
```

---

## Data Hub & Streamlit UI

```bash
docker compose up -d                     # ClickHouse + UI together
# or
python src/main.py ui                    # UI only (ClickHouse must already be running)
```

Open **http://localhost:8501**.

### Import data

```bash
python src/main.py import --category commodities
python src/main.py import --category etfs --category mf
python src/main.py import --category fii_dii    # FII/DII flows

# Full backfill (ignores watermarks)
python src/main.py import --category stocks --full

# Preview without writing
python src/main.py import --category etfs --dry-run
```

Subsequent runs are **delta-synced** — only new data is fetched.

See [docs/import-schema.md](docs/import-schema.md) for all categories, the full ClickHouse schema, and recommended cron schedules.

---

## Documentation

| Doc | What's in it |
|---|---|
| [docs/import-schema.md](docs/import-schema.md) | All import categories, ClickHouse tables, cron schedule |
| [docs/data-sources.md](docs/data-sources.md) | APIs and data sources used |
| [docs/anomaly-detection.md](docs/anomaly-detection.md) | How the 3-step anomaly pipeline works |
| [docs/ml-forecast.md](docs/ml-forecast.md) | LightGBM feature set, regime signals, accuracy tracking |
| [docs/configuration.md](docs/configuration.md) | All `.env` settings |

---

## Project Structure

```
config/settings.py              Pydantic settings
src/
  main.py                       CLI (typer)
  agents/                       portfolio, comex, news, visualization
  analyzers/                    asset_analyzer, portfolio_analyzer
  clients/mcp_client.py         Zerodha Kite MCP (JSON-RPC 2.0)
  importer/                     Delta-sync importer
    cli.py                      run_import() logic
    clickhouse.py               Schema DDL + bulk inserts + watermarks
    registry.py                 Symbol registry
    fetchers/                   One file per data source
  ml/
    anomaly.py                  Composite anomaly detection
    trend_predictor.py          LightGBM 5-day predictor
  tools/
    who_is_selling_agent.py     Sell-off attribution
    market_context.py           FII/DII context for LLM prompt
    macro_event_scanner.py      8-theme macro/geopolitical → ETF impact scanner
    etf_news_scanner.py         10-category ETF news tagger (gnews + yfinance)
  ui/app.py                     Streamlit 8-tab data hub
tests/
docker-compose.yml
```

---

## Tests

```bash
python tests/test_tools.py          # unit tests (no API keys for 10/11)
python tests/_test_importer.py      # integration (requires ClickHouse)
```

---

## Known Limitations

- **NewsAPI free tier:** 100 req/day — top holdings by weight are prioritised
- **iNAV:** NSE API live only 9:15 AM – 3:30 PM IST
- **LightGBM:** Requires ≥ 120 clean training rows; CV R² improves as history accumulates
- **Local LLMs:** Models < 30B struggle with multi-turn orchestration
- **Anomaly detection:** Requires ≥ 60 rows per symbol — run an import first
