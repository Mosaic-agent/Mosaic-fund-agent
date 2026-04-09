# Data Sources

All data sources used by Mosaic Fund Agent are free unless noted.

## Market & Price Data

| What | Source | Notes |
|---|---|---|
| Stock / ETF / commodity OHLCV | Yahoo Finance `.NS`, `GC=F`, etc. | Free, no rate limit |
| ETF iNAV — live | NSE API | Free, 15-second refresh (9:15 AM – 3:30 PM IST) |
| ETF iNAV — historic / NAV | MFAPI.in (AMFI official) | Free |
| COMEX spot prices | gold-api.com | Free with API key |
| COMEX previous close | Yahoo Finance futures | Free |

## Fundamental & Flow Data

| What | Source | Notes |
|---|---|---|
| Indian financial news | NewsAPI.org | Free: 100 req/day |
| Indian financial news | Google News RSS (GNews) | Free, no key — used by `etf-news` and `macro` scanners |
| Yahoo Finance news | yfinance ticker.news | Free, no key — used by `etf-news` scanner |
| Quarterly results | Screener.in (scraped) | Free, polite delays |
| CFTC COT (hedge fund positioning) | publicreporting.cftc.gov | Free, no auth |
| Central bank gold reserves | IMF IFS REST API | Free, no auth |
| Gold ETF AUM flows | Yahoo Finance (totalAssets) | Free |
| Fund portfolio holdings | Morningstar (mstarpy) | Current snapshot; run monthly to build time-series |
| FII / DII institutional flows | Sensibull oxide API | Free, no auth; ~6 months rolling daily + 7+ years monthly |

## Portfolio & Brokerage

| What | Source | Notes |
|---|---|---|
| Live portfolio holdings | Zerodha Kite MCP (hosted) | Free, OAuth login |

## Infrastructure

| What | Source | Notes |
|---|---|---|
| Historical storage | ClickHouse (Docker) | Free, self-hosted |
| LLM scoring | OpenAI / Anthropic / Local | ~₹4–12/run cloud; free local |

## ETF iNAV Interpretation

- **Premium (> +0.25%)** — ETF more expensive than underlying. Wait before buying.
- **Discount (< −0.25%)** — ETF cheaper than underlying. Potential buying opportunity.
- **Fair value** — within ±0.25% of NAV.

Schedule periodic iNAV imports to build a time-series:

```bash
# crontab — every 15 min during market hours (IST)
*/15 9-15 * * 1-5 cd /path/to/project && .venv/bin/python src/main.py import --category inav
```
