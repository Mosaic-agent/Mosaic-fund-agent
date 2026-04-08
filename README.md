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
- **HTML dashboard** — self-contained, auto-refreshes every 5 minutes
- **Streamlit UI** — import, SQL explorer, charts, anomaly detection, quant scorecard

---

## Quick Start

### Prerequisites

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
  ui/app.py                     Streamlit 5-tab data hub
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
