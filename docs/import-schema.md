# Import Categories & ClickHouse Schema

## Import Categories

Run imports via CLI: `python src/main.py import --category <name>`

| Category | Source | Symbols | ClickHouse Table |
|---|---|---|---|
| `stocks` | Yahoo Finance (`.NS`) | 50 NSE large/mid-caps | `daily_prices` |
| `etfs` | Yahoo Finance (`.NS`) | 15 NSE ETFs | `daily_prices` |
| `commodities` | Yahoo Finance (futures) | Gold, Silver, Copper, Crude Oil, etc. | `daily_prices` |
| `indices` | Yahoo Finance | Nifty50, Sensex, S&P500, Nasdaq, etc. | `daily_prices` |
| `mf` | MFAPI.in (AMFI official) | NAV history for 13 ETF schemes | `mf_nav` |
| `inav` | NSE API (live) | 15 ETFs — iNAV + market price + premium/discount | `inav_snapshots` |
| `cot` | CFTC Socrata API (free) | Weekly Gold COT — Managed Money + Commercials | `cot_gold` |
| `cb_reserves` | IMF IFS REST API (free) | Monthly gold reserves for 9 central banks | `cb_gold_reserves` |
| `etf_aum` | Yahoo Finance (free) | Daily AUM snapshot for GLD, IAU, SGOL, PHYS | `etf_aum` |
| `fx_rates` | Yahoo Finance (free) | Daily OHLC for USDINR, USDCNY, USDAED, USDSAR, USDKWD | `fx_rates` |
| `mf_holdings` | Morningstar via mstarpy | Current portfolio snapshot for DSP, Quant, ICICI multi-asset funds | `mf_holdings` |
| `fii_dii` | Sensibull oxide API | Daily + monthly FII/DII institutional cash flows + F&O OI | `fii_dii_flows`, `fii_dii_monthly`, `fii_dii_fno_daily` |

### Delta-sync

All imports are **watermark-based** — only new data since the last successful run is fetched (3-day overlap for late corrections). Use `--full` to ignore watermarks and re-fetch all history.

```bash
python src/main.py import --category commodities --full   # full re-import
python src/main.py import --category etfs --dry-run       # preview, no DB writes
```

## ClickHouse Schema

Database: `market_data`. All tables use `ReplacingMergeTree` for idempotent re-imports.

| Table | Engine | Partition | Order Key | Purpose |
|---|---|---|---|---|
| `daily_prices` | ReplacingMergeTree(imported_at) | toYYYYMM(trade_date) | (symbol, trade_date) | OHLCV for stocks, ETFs, commodities, indices |
| `mf_nav` | ReplacingMergeTree(imported_at) | toYYYYMM(nav_date) | (symbol, nav_date) | Daily MF/ETF NAV from AMFI via MFAPI.in |
| `inav_snapshots` | ReplacingMergeTree(snapshot_at) | toYYYYMM(snapshot_at) | (symbol, snapshot_at) | Live iNAV + premium/discount snapshots |
| `import_watermarks` | ReplacingMergeTree(updated_at) | — | (source, symbol) | Delta-sync watermarks |
| `cot_gold` | ReplacingMergeTree | — | (report_date) | Weekly CFTC COT — mm_net, comm_net, open_interest |
| `cb_gold_reserves` | ReplacingMergeTree | toYYYYMM(ref_period) | (ref_period, country_code) | Monthly central bank gold reserves (metric tonnes) |
| `etf_aum` | ReplacingMergeTree | toYYYYMM(trade_date) | (trade_date, symbol) | Daily ETF AUM (USD) + implied gold tonnes |
| `fx_rates` | ReplacingMergeTree(imported_at) | toYYYYMM(trade_date) | (symbol, trade_date) | Daily OHLC for 5 USD pairs |
| `ml_predictions` | ReplacingMergeTree(created_at) | — | (as_of, horizon_days) | LightGBM forecast log |
| `mf_holdings` | ReplacingMergeTree(imported_at) | toYYYYMM(as_of_month) | (scheme_code, as_of_month, isin) | Monthly MF portfolio holdings snapshot |
| `fii_dii_flows` | ReplacingMergeTree(imported_at) | — | (trade_date) | Daily FII/DII cash-market net flows (₹ Crore) |
| `fii_dii_monthly` | ReplacingMergeTree(imported_at) | — | (month_date) | Monthly FII/DII aggregate + Nifty (Sep 2018→present) |
| `fii_dii_fno_daily` | ReplacingMergeTree(imported_at) | — | (trade_date) | Daily F&O participant OI (futures + options, 4 categories) |

### Querying tips

All tables use `ReplacingMergeTree`. Use `FINAL` to get deduplicated results:

```sql
SELECT trade_date, fii_net_cr, dii_net_cr
FROM market_data.fii_dii_flows FINAL
ORDER BY trade_date DESC
LIMIT 30;
```

For large tables, `argMax` is faster than `FINAL`:

```sql
SELECT trade_date, argMax(close, imported_at) AS close
FROM market_data.daily_prices
WHERE symbol = 'GOLDBEES'
GROUP BY trade_date
ORDER BY trade_date DESC;
```

To force deduplication (e.g. after a double-import):

```sql
OPTIMIZE TABLE market_data.daily_prices FINAL;
```

## Recommended Cron Schedule

```bash
# iNAV — every 15 min during market hours (IST)
*/15 9-15 * * 1-5  cd /path/to/project && .venv/bin/python src/main.py import --category inav

# EOD prices — after NSE close
30 15 * * 1-5  cd /path/to/project && .venv/bin/python src/main.py import --category nse_eod

# FII/DII flows — daily after market close
0 16 * * 1-5   cd /path/to/project && .venv/bin/python src/importer/fetchers/fii_dii_fetcher.py --insert

# COT — Fridays after 3:30 PM ET (CFTC release)
30 22 * * 5    cd /path/to/project && .venv/bin/python src/main.py import --category cot

# IMF reserves — monthly, run weekly to catch publishing lag
0 9 * * 1      cd /path/to/project && .venv/bin/python src/main.py import --category cb_reserves

# ETF AUM — daily after US market close
0 23 * * 1-5   cd /path/to/project && .venv/bin/python src/main.py import --category etf_aum

# ML forecast — daily after Indian market close
30 15 * * 1-5  cd /path/to/project && .venv/bin/python src/ml/trend_predictor.py

# MF holdings snapshot — 5th of each month after AMFI disclosure
0 10 5 * *     cd /path/to/project && .venv/bin/python src/main.py import --category mf_holdings
```
