# 📊 Portfolio Insight — Zerodha Portfolio Intelligence Agent

A fully agentic AI system that reads your **Zerodha Kite** portfolio, enriches every holding with live market data, recent news, and quarterly results — then generates a rich terminal report with risk scores, sentiment, sector allocation, and actionable insights. Built for Indian equity markets (NSE & BSE).

No manual data entry. No spreadsheets. Just run one command.

---

## 🧠 What It Does

```
python src/main.py analyze
              ↓
  Zerodha Kite MCP (free hosted)
      → fetches your live holdings
              ↓
  For each stock/ETF:
      ├── Yahoo Finance (.NS / .BO)   → price, P/E, sector, 52W range, momentum
      ├── NewsAPI.org (free tier)     → top 5 recent Indian financial news
      ├── Screener.in scraper         → latest quarterly results (YoY growth)
      ├── NSE iNAV API (ETFs only)    → live iNAV + premium/discount
      └── AMFI via MFAPI (ETFs only)  → 30-day historic NAV trend + sparkline
              ↓
  LangGraph ReAct Agent + LLM
      → risk score (1–10) per holding
      → sentiment score (–1 to +1) per holding
      → 5 bullet investment insights per holding
              ↓
  Portfolio Analyzer
      → sector allocation breakdown
      → concentration & diversification scores
      → overall portfolio health score (0–100)
      → actionable rebalancing insights
              ↓
  COMEX Pre-Market Signals  (gold-api.com + Yahoo Finance futures)
      → live spot prices: Gold, Silver, Platinum, Palladium, Copper
      → day-over-day change vs previous close
      → STRONG BULLISH / BULLISH / NEUTRAL / BEARISH / STRONG BEARISH
      → overall commodity signal + affected NSE ETFs highlighted
              ↓
  Rich terminal report  +  JSON file (./output/)
```

---

## 🏗️ Architecture

```
portfolio_insight/
├── config/
│   └── settings.py          # All config — sensitive fields clearly marked
├── src/
│   ├── main.py              # CLI entry point (analyze / ask / config)
│   ├── clients/
│   │   └── mcp_client.py    # Async HTTP client for Kite MCP server
│   ├── models/
│   │   └── portfolio.py     # Pydantic models: Holding, Portfolio, Report
│   ├── tools/
│   │   ├── zerodha_mcp_tools.py  # LangChain tools: get_holdings, get_positions
│   │   ├── yahoo_finance.py      # Free market data via yfinance (.NS / .BO)
│   │   ├── news_search.py        # NewsAPI free tier — Indian financial news
│   │   ├── earnings_scraper.py   # Screener.in scraper + Yahoo Finance fallback
│   │   ├── inav_fetcher.py       # ETF iNAV — NSE API (15s) + Yahoo fallback
│   │   ├── historic_inav.py      # 30-day historic iNAV from AMFI via MFAPI.in
│   │   ├── comex_fetcher.py      # COMEX signals — gold-api.com + Yahoo futures
│   │   └── summarization.py      # LLM risk/sentiment scoring & insights
│   ├── agents/
│   │   └── portfolio_agent.py    # LangGraph ReAct agent + orchestration
│   ├── analyzers/
│   │   ├── asset_analyzer.py     # Per-holding enrichment pipeline (+ iNAV for ETFs)
│   │   └── portfolio_analyzer.py # Sector allocation, concentration, health score
│   ├── formatters/
│   │   └── output.py             # Rich terminal display + JSON file writer
│   └── utils/
│       └── symbol_mapper.py      # NSE symbol ↔ Yahoo Finance ↔ company name
├── tests/
│   └── test_tools.py        # Test suite (11 tests — 10 free, 1 requires GOLD_API_KEY)
├── output/                  # Generated JSON reports (git-ignored)
├── .env.example             # Config template — copy to .env
├── .env                     # Your actual keys — NEVER commit this
└── requirements.txt         # Python dependencies
```

---

## ⚡ Quick Start

### 1. Clone & set up environment

```bash
cd /path/to/portfolio_insight
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure API keys

```bash
cp .env.example .env
```

Open `.env` and fill in:

| Variable | Where to get it | Required? |
|---|---|---|
| `OPENAI_API_KEY` | [platform.openai.com/api-keys](https://platform.openai.com/api-keys) | ✅ (or Anthropic) |
| `ANTHROPIC_API_KEY` | [console.anthropic.com](https://console.anthropic.com) | ✅ (or OpenAI) |
| `NEWSAPI_KEY` | [newsapi.org/register](https://newsapi.org/register) — free | ✅ recommended |
| `GOLD_API_KEY` | [gold-api.com](https://gold-api.com/) — free | ✅ recommended |
| `KITE_API_KEY` | Only for self-hosted MCP | ❌ not needed for hosted |
| `KITE_API_SECRET` | Only for self-hosted MCP | ❌ not needed for hosted |

> **Using the hosted Kite MCP server** (`https://mcp.kite.trade/mcp`) requires **no Kite API key** — you authenticate via browser OAuth when the agent runs.

### 3. Verify your configuration

```bash
python src/main.py config
```

This shows all settings with sensitive fields masked — never exposes raw keys.

### 4. Try the demo first (no Zerodha login needed)

```bash
# Run on sample data (RELIANCE, TCS, HDFCBANK, INFY, NIFTYBEES, GOLDBEES)
# Uses real live data from Yahoo Finance, NSE, Screener.in — no Zerodha auth
python src/main.py analyze --demo
```

### 5. Run on your real portfolio

```bash
# Full portfolio analysis — will prompt for Zerodha browser login on first run
python src/main.py analyze

# Limit to 3 holdings first (recommended to verify setup)
python src/main.py analyze --max 3

# Save JSON only, skip terminal display
python src/main.py analyze --quiet

# Ask a freeform question about your portfolio
python src/main.py ask "Which of my stocks has the highest risk?"
python src/main.py ask "Which ETFs are trading at a premium?"
```

---

## 🔐 Zerodha Kite Authentication

The agent uses the **free hosted MCP server** at `https://mcp.kite.trade/mcp`.

When you run `analyze` for the first time (or when your session expires), the agent will print a browser URL:

```
[AUTH REQUIRED] Please open this URL in your browser to login:
https://kite.zerodha.com/connect/login?...
Press ENTER after completing authentication...
```

1. Open the URL in your browser
2. Log in with your Zerodha credentials
3. Press ENTER in the terminal — the agent continues automatically

---

## 💬 CLI Commands

### `analyze` — Full portfolio intelligence report

```bash
python src/main.py analyze [OPTIONS]

Options:
  --max INTEGER    Limit to top N holdings (0 = all)  [default: 0]
  --json/--no-json Save JSON report to ./output/      [default: True]
  --quiet          Skip terminal display, JSON only
  --help
```

### `ask` — Free-form question about your portfolio

```bash
python src/main.py ask "Which of my stocks has the highest risk?"
python src/main.py ask "What is my technology sector exposure?"
python src/main.py ask "Which holdings have negative news sentiment?"
```

### `config` — Show current configuration (non-sensitive)

```bash
python src/main.py config
```

---

## 📤 Output Format

Reports are saved to `./output/portfolio_report_YYYYMMDD_HHMMSS.json`:

```json
{
  "generated_at": "2026-02-22T14:30:00",
  "portfolio_summary": {
    "total_value": "₹5,23,450.00",
    "total_invested": "₹4,85,000.00",
    "total_pnl": "₹38,450.00",
    "total_pnl_percent": "7.93%",
    "health_score": 72.5,
    "diversification_score": 65.0,
    "num_holdings": 8,
    "stock_count": 6,
    "etf_count": 2,
    "direct_equity_allocation_pct": 78.5,
    "etf_allocation_pct": 21.5
  },
  "holdings_analysis": [
    {
      "symbol": "RELIANCE",
      "exchange": "NSE",
      "instrument_type": "STOCK",
      "sector": "Energy",
      "sentiment_score": 0.35,
      "risk_score": 4.0,
      "summary": "Reliance continues to demonstrate strength across its telecom and retail verticals...",
      "key_insights": [
        "Jio platform adds 5M subscribers YoY — telecom remains the growth engine",
        "Retail segment revenue up 18% YoY driven by fashion and grocery expansion",
        "...3 more bullets"
      ],
      "risk_signals": ["Oil price volatility", "High capex cycle ongoing"],
      "key_news": [...],
      "latest_results": {
        "period": "Dec 2025",
        "revenue_cr": 236000.0,
        "net_profit_cr": 18540.0,
        "revenue_yoy_pct": 8.2,
        "profit_yoy_pct": 11.5
      }
    }
  ],
  "sector_allocation": {
    "Technology": 35.2,
    "Financial Services": 28.1,
    "Energy": 18.4,
    "FMCG": 10.0,
    "Index ETF": 8.3
  },
  "portfolio_risks": [
    "High technology sector concentration (35%) — exposed to IT slowdown risk",
    "3 of 8 holdings show negative 30-day momentum"
  ],
  "actionable_insights": [
    "Consider trimming TCS/Infosys position — both show BEARISH momentum",
    "Add exposure to Healthcare or Consumer sectors to improve diversification",
    "..."
  ]
}
```

---

## 🛠️ Data Sources

| Data Type | Source | Cost | Limit |
|---|---|---|---|
| Portfolio holdings | Zerodha Kite MCP (hosted) | **Free** | Live data |
| Stock prices & metrics | Yahoo Finance (`.NS` / `.BO`) | **Free** | Unlimited |
| Price history & momentum | Yahoo Finance | **Free** | Unlimited |
| Indian financial news | NewsAPI.org | **Free** | 100 req/day |
| Quarterly results | Screener.in (scraper) | **Free** | Rate-limited |
| Quarterly results (fallback) | Yahoo Finance financials | **Free** | Unlimited |
| ETF iNAV (live, every 15s) | NSE API (`nseindia.com/api/etf`) | **Free** | Live during mkt hrs |
| ETF iNAV (fallback) | Yahoo Finance navPrice | **Free** | Delayed |
| Historic iNAV (30 days) | MFAPI.in — official AMFI data | **Free** | Daily NAV records |
| COMEX live spot prices | gold-api.com (`/price/{symbol}`) | **Free** | Real-time |
| COMEX previous close | Yahoo Finance futures (GC=F, SI=F…) | **Free** | Unlimited |
| AI analysis & scoring | OpenAI GPT-4o-mini (default) | ~$0.01/run | Pay-per-use |

> **Estimated LLM cost per full run:** ~$0.05–0.15 for a 10-15 stock portfolio using `gpt-4o-mini`.  
> Switch to `claude-3-haiku-20240307` (set `LLM_PROVIDER=anthropic`) for similar cost.

---

## 📈 ETF iNAV Analysis

For every ETF in your portfolio, the agent automatically fetches the **Indicative NAV (iNAV)** and calculates whether it is trading at a **premium or discount** to its fair value.

### How It Works

| Step | Detail |
|---|---|
| **1. ETF Detection** | Checks static symbol list (GOLDBEES, NIFTYBEES, etc.) + Yahoo Finance `quoteType` for unknowns |
| **2. iNAV — Primary** | NSE API `nseindia.com/api/etf?symbol=…` — updated every **15 seconds** during market hours |
| **3. iNAV — Fallback** | Yahoo Finance `navPrice` / `regularMarketPrice` (delayed ~15 min) |
| **4. Premium / Discount** | `(market_price − iNAV) / iNAV × 100` — shown in terminal and JSON |

### Premium / Discount Labels

| Label | Threshold | Meaning |
|---|---|---|
| 🟡 **PREMIUM** | `> +0.25%` | ETF trading above its underlying NAV — often caused by demand surge |
| 🟢 **DISCOUNT** | `< −0.25%` | ETF trading below NAV — potential buying opportunity |
| 🔵 **FAIR VALUE** | `−0.25% to +0.25%` | Trading close to NAV — neutral |

### Sample Terminal Output (ETF Holdings)

```
╭─────────────── iNAV Analysis — GOLDBEES ───────────────╮
│ iNAV (per unit):      ₹61.2300                         │
│ Market Price:         ₹61.80                           │
│ Premium / Discount:   +0.93%  ◀  PREMIUM               │
│ Data Source:          NSE                              │
│                                                        │
│ ⚠ ETF trading above NAV — consider waiting to buy more │
╰────────────────────────────────────────────────────────╯

╭─────────────── iNAV Analysis — NIFTYBEES ──────────────╮
│ iNAV (per unit):      ₹289.1500                        │
│ Market Price:         ₹289.23                          │
│ Premium / Discount:   +0.03%  ◀  FAIR VALUE            │
│ Data Source:          NSE                              │
│                                                        │
│ ✅ ETF trading close to fair value.                    │
╰────────────────────────────────────────────────────────╯
```

### iNAV in JSON Output

```json
{
  "symbol": "GOLDBEES",
  "instrument_type": "ETF",
  "inav_analysis": {
    "symbol": "GOLDBEES",
    "is_etf": true,
    "inav": 61.23,
    "market_price": 61.80,
    "premium_discount_pct": 0.93,
    "premium_discount_label": "PREMIUM",
    "source": "NSE",
    "note": "Positive premium_discount_pct = ETF trading above iNAV (premium)."
  }
}
```

---

## 🔒 Security — Handling Sensitive Configuration

All sensitive fields are loaded **exclusively from `.env`** and never hard-coded.

Every field in [config/settings.py](config/settings.py) is annotated with either `# [SENSITIVE]` or `# [NON-SENSITIVE]`.

**Sensitive fields** (never commit these):
- `OPENAI_API_KEY` — LLM provider key
- `ANTHROPIC_API_KEY` — LLM provider key  
- `NEWSAPI_KEY` — News search API key
- `GOLD_API_KEY` — COMEX pre-market signals ([gold-api.com](https://gold-api.com/))
- `KITE_API_KEY` — Only for self-hosted Kite MCP
- `KITE_API_SECRET` — Only for self-hosted Kite MCP

**`.env` is in `.gitignore`** — it will never be accidentally committed.

```bash
# Safe: shows masked values
python src/main.py config

# Output example:
# OpenAI API Key    │  sk-p****90   │  ⚠ YES
# NewsAPI Key       │  abc1****yz   │  ⚠ YES
```

---

## 🧪 Running Tests

Tests 1–10 run **without any API keys**. TEST 11 requires `GOLD_API_KEY` (free at [gold-api.com](https://gold-api.com/)) and will skip gracefully without it:

```bash
python tests/test_tools.py
```

```
TEST 1:  Yahoo Finance Tool         ✓  RELIANCE.NS live price, sector, momentum
TEST 2:  Symbol Mapper              ✓  NSE ↔ Yahoo ↔ company name conversions
TEST 3:  Earnings Scraper           ✓  Screener.in live INFY Dec 2025 results
TEST 4:  News Tool (no key)         ✓  Graceful empty return without API key
TEST 5:  Pydantic Portfolio Models  ✓  P&L, ETF detection, portfolio totals
TEST 6:  Sector Allocation          ✓  Concentration risk, diversification score
TEST 7:  Config Masking             ✓  Sensitive field warnings & masking
TEST 8:  iNAV Fetcher               ✓  ETF detection, live iNAV, premium/discount batch
TEST 9:  iNAV Premium/Discount      ✓  11 boundary scenarios (PREMIUM/DISCOUNT/FAIR VALUE)
TEST 10: Historic iNAV              ✓  30-day AMFI data, sparkline, trend analysis
TEST 11: COMEX Signals              ✓  Live gold-api.com XAU/XAG/HG + prompt-injection guards

RESULTS: 11 passed, 0 failed
```

---

## ⚙️ Configuration Reference

All settings can be overridden in `.env`. Non-sensitive defaults are safe to commit.

| Variable | Default | Sensitive? | Description |
|---|---|---|---|
| `OPENAI_API_KEY` | — | ⚠️ Yes | OpenAI API key |
| `ANTHROPIC_API_KEY` | — | ⚠️ Yes | Anthropic API key |
| `LLM_PROVIDER` | `openai` | No | `openai` or `anthropic` |
| `LLM_MODEL` | `gpt-4o-mini` | No | Model name |
| `KITE_MCP_URL` | `https://mcp.kite.trade/mcp` | No | MCP endpoint |
| `KITE_API_KEY` | — | ⚠️ Yes | Self-hosted only |
| `KITE_API_SECRET` | — | ⚠️ Yes | Self-hosted only |
| `KITE_MCP_TIMEOUT` | `30` | No | Request timeout (s) |
| `NEWSAPI_KEY` | — | ⚠️ Yes | NewsAPI.org key |
| `GOLD_API_KEY` | — | ⚠️ Yes | COMEX pre-market signals (gold-api.com) |
| `NEWS_ARTICLES_PER_STOCK` | `5` | No | Max articles per stock |
| `NEWS_LOOKBACK_DAYS` | `7` | No | Days back for news (max 30 on free tier) |
| `MAX_HOLDINGS_PER_RUN` | `0` | No | Holdings cap (0 = all) |
| `SCRAPE_DELAY_SECONDS` | `2.0` | No | Polite delay between scrapes |
| `OUTPUT_DIR` | `./output` | No | Report output folder |
| `LOG_LEVEL` | `INFO` | No | `DEBUG` / `INFO` / `WARNING` |

---

## 🗺️ Supported Symbols

The symbol mapper covers **100+ NSE large/mid-cap stocks** and common ETFs including:

**Indices & ETFs:** `NIFTYBEES`, `JUNIORBEES`, `GOLDBEES`, `BANKBEES`, `LIQUIDBEES`

**Sectors:** IT (TCS, INFY, WIPRO, HCL), Banking (HDFC, ICICI, SBI, KOTAK), Pharma (SUNPHARMA, DRREDDY, CIPLA), FMCG (HUL, ITC, NESTLE), Auto (MARUTI, TATAMOTOR, BAJAJ-AUTO), Energy (RELIANCE, ONGC, BPCL), and more.

Unknown symbols gracefully fall back to Yahoo Finance lookup with `.NS` suffix.

---

## 📋 Requirements

- Python 3.11+
- Active Zerodha account (for Kite MCP authentication)
- OpenAI or Anthropic API key (paid, ~$0.05–0.15 per full run)
- NewsAPI free key (100 req/day — optional but recommended)
- Internet access for Yahoo Finance and Screener.in
