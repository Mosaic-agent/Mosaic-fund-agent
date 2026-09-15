# Data Sources

All data sources used by Mosaic Fund Agent are free unless noted.

## Market & Price Data

| What | Source | Notes |
|---|---|---|
| Stock / ETF / commodity OHLCV | Shoonya (primary for NSE) / nselib / Yahoo Finance (fallback) | Shoonya API used as primary daily EOD reader for Indian stocks/ETFs (requires API key & TOTP); nselib for direct NSE fallback; Yahoo Finance `.NS` for global fallback and commodities (`GC=F`) |
| Live quotes & tick streams | Shoonya API / WebSocket | Used during market hours for real-time stock/ETF prices and order flow (requires Shoonya API login) |
| Currency exchange rates (FX) | Yahoo Finance | Free, daily OHLC for USDINR, USDCNY, USDAED, USDSAR, USDKWD. Used in macro, anomaly, and currency transmission models |
| ETF iNAV — live | Multi-source: NSE API / Kite / Direct AMC Feeds | Free, 15-second refresh during market hours (9:15 AM – 3:30 PM IST). Supported adapters: NSE Official API, Kite Inav, Nippon India, Mirae Asset, and Motilal Oswal direct feeds |
| ETF iNAV — historic / NAV | MFAPI.in (AMFI official) | Free, historical daily NAV tracking for tracked ETF schemes |
| MF scheme lookup by name / NAV MoM returns (any AMC) | MFAPI.in (AMFI official) | Free; use `src/scripts/portfolio/fund_mom_returns.py --search "<name>"` then `--scheme <code>`; fetched live, no ClickHouse import step exists for actively-managed schemes. **No expense ratio field** — not available from this source or tracked anywhere else in the codebase. |
| Sectoral & Factor Indices | nselib (direct NSE) / Yahoo Finance | Sectoral, thematic, and factor indices not available on Yahoo Finance ingested via nselib |
| NSE Official EOD Archive | NSE EOD bulk archive | Authoritative EOD Bhavcopy data directly from NSE |
| NSE Official Delivery Data | NSE delivery archive | Security-wise deliverable quantity and delivery percentages from NSE |
| US Stocks OHLCV | Yahoo Finance | Daily price history for tracked US equities |
| COMEX spot prices | gold-api.com | Free with API key |
| COMEX previous close | Yahoo Finance futures | Free |

## Fundamental, Macro & Institutional Flow Data

| What | Source | Notes |
|---|---|---|
| Indian Macro Indicators | RBI / MOSPI public series | Monthly/quarterly CPI, GDP, IIP, Repo Rate, 10Y G-Sec Yield, Bank Credit Growth, Forex Reserves, WPI into `market_data.indian_macro_indicators` |
| Multilateral Macro Data | World Bank WDI & IMF WEO APIs | Annual global macro indicators (GDP growth, inflation, debt-to-GDP) and IMF World Economic Outlook projections into `market_data.macro_indicators` |
| AMFI Category-Wise Flows & AUM | AMFI official portal | Monthly category-wise inflows, outflows, net flows, and closing AUM across Equity, Debt, Hybrid, and Passive/ETF categories into `market_data.amfi_category_flows` |
| NSE Bulk & Block Deals | NSE official API / nselib | Large institutional, promoter, and bulk/block deal filings into `market_data.bulk_block_deals` |
| FII / DII institutional flows | Sensibull oxide API | Free, no auth; rolling daily cash flows, monthly aggregates, and F&O OI into `market_data.fii_dii_flows`, `fii_dii_monthly`, and `fii_dii_fno_daily` |
| Indian financial news | NewsAPI.org & Google News RSS | Category-tagged news feeds with sentiment scoring across ETF sleeves |
| Quarterly results (Indian Stocks) | Screener.in (scraped) | Free, polite delays; P&L, balance sheet, and shareholding pattern |
| US Stock Fundamentals | Yahoo Finance | Quarterly earnings (`stock_earnings`), insider transactions (`stock_insider_trades`), and valuation snapshots (`stock_valuation`) |
| CFTC COT (hedge fund positioning) | publicreporting.cftc.gov (Socrata API) | Free, no auth; weekly Gold COT positioning (Managed Money vs. Commercials) |
| Central bank gold reserves | IMF IFS REST API | Free, no auth; monthly gold reserve holdings for major central banks |
| Gold ETF AUM flows | Yahoo Finance (totalAssets) | Free; daily AUM tracking for global gold ETFs (GLD, IAU, SGOL, PHYS) |
| Expert Macro Tweets | Nitter RSS proxies | Scrapes updates of macro experts (e.g. Ritesh Jain) for pre-market sentiment |

## AMC Statutory Portfolio Disclosures (14+ AMCs)

| AMC / Provider | Source | Coverage & Ingestion Strategy |
|---|---|---|
| DSP Mutual Fund | dspim.com portfolio ZIP archives | Full portfolio backfill & monthly updates into `market_data.mf_holdings` |
| Nippon India MF | nipponindiamf.com | Dynamic monthly XLS discovery and extraction (2017 → present) |
| ICICI Prudential MF & Index | icicipruamc.com & Azure Blob | Monthly portfolio filings and index constituent holdings |
| Bajaj Finserv AMC | bajajamc.com | Monthly and fortnightly portfolio XLSX parsing via AJAX endpoints |
| Quant MF & Quant SIF | quantmutual.com | Monthly regulatory portfolio filings and SIF holdings |
| HDFC, Kotak, Axis, Motilal Oswal | Official AMC portals | Automated monthly regulatory Excel/XLSX portfolio extraction |
| Invesco, Canara Robeco, Mirae Asset, Helios, Abakkus | Official AMC portals | Monthly statutory portfolio disclosures |
| Morningstar Snapshot | Morningstar sal-service API / mstarpy | Current portfolio snapshot across multi-asset funds; auto-vectorized into Qdrant |

## Portfolio & Brokerage

| What | Source | Notes |
|---|---|---|
| Live portfolio & orders | Zerodha Kite MCP (hosted) | OAuth login; live holdings, margins, profile, positions, and orders |
| Live trading & market feed | Shoonya API (Finvasia) | Primary API for tick data, WebSocket streaming, and intraday monitor |

## Infrastructure

| What | Source | Notes |
|---|---|---|
| Timeseries & Relational Storage | ClickHouse (Docker) | 37+ ReplacingMergeTree tables for EOD prices, holdings, flows, and ML predictions |
| Vector Database & RAG | Qdrant (Docker) | Vector collections for semantic search over MF holdings, fund profiles, and macro documents |
| LLM Scoring & Agents | OpenAI / Anthropic / Google Gemini | Agent reasoning, qualitative synthesis, and composite scoring |

## ETF iNAV Interpretation

- **Premium (> +0.25%)** — ETF more expensive than underlying. Wait before buying.
- **Discount (< −0.25%)** — ETF cheaper than underlying. Potential buying opportunity.
- **Fair value** — within ±0.25% of NAV.

Schedule periodic iNAV imports to build a time-series:

```bash
# crontab — every 15 min during market hours (IST)
*/15 9-15 * * 1-5 docker run --rm --network ofin-agent_default --env-file .env mosaic-fund-agent python src/main.py import --category inav
```
