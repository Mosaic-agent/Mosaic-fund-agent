# ClickHouse Schema Reference

The `market_data` database uses `ReplacingMergeTree` for all primary tables to ensure idempotent imports.

## Database: `market_data`

### `daily_prices`
OHLCV data for stocks, ETFs, commodities, and indices.
- **Engine**: `ReplacingMergeTree(imported_at)`
- **Partition**: `toYYYYMM(trade_date)`
- **Order Key**: `(symbol, trade_date)`
- **Categories**: `stocks`, `etfs`, `commodities`, `indices`

### `mf_nav`
Daily NAV for mutual funds and ETFs from MFAPI.in.
- **Engine**: `ReplacingMergeTree(imported_at)`
- **Partition**: `toYYYYMM(nav_date)`
- **Order Key**: `(symbol, nav_date)`

### `import_watermarks`
Tracks the last successfully imported date per (source, symbol).
- **Engine**: `ReplacingMergeTree(updated_at)`
- **Order Key**: `(source, symbol)`

### `fx_rates`
Daily USD FX pairs (USDINR, USDCNY, etc.).
- **Engine**: `ReplacingMergeTree(imported_at)`
- **Partition**: `toYYYYMM(trade_date)`
- **Order Key**: `(symbol, trade_date)`

### `inav_snapshots`
Intraday iNAV snapshots from NSE.
- **Engine**: `ReplacingMergeTree(snapshot_at)`
- **Partition**: `toYYYYMM(snapshot_at)`
- **Order Key**: `(symbol, snapshot_at)`

### `cot_gold`
CFTC Commitments of Traders (Managed Money positioning).
- **Engine**: `ReplacingMergeTree(_ver)`
- **Order Key**: `(report_date)`

### `cb_gold_reserves`
IMF Central Bank gold reserves.
- **Engine**: `ReplacingMergeTree(_ver)`
- **Order Key**: `(ref_period, country_code)`

### `etf_aum`
Gold ETF AUM and implied tonnes.
- **Engine**: `ReplacingMergeTree(_ver)`
- **Order Key**: `(trade_date, symbol)`

### `ml_predictions`
Log of ML forecast outputs.
- **Engine**: `ReplacingMergeTree(created_at)`
- **Order Key**: `(as_of, horizon_days)`

### `mf_holdings`
Portfolio disclosures for mutual funds.
- **Engine**: `ReplacingMergeTree(imported_at)`
- **Partition**: `toYYYYMM(as_of_month)`
- **Order Key**: `(scheme_code, as_of_month, isin)`

## Querying Tips
Since tables use `ReplacingMergeTree`, use `argMax` or `FINAL` to get the latest version of a row:
```sql
SELECT trade_date, argMax(close, imported_at) as close
FROM market_data.daily_prices
WHERE symbol = 'GOLDBEES'
GROUP BY trade_date
ORDER BY trade_date DESC
```
Avoid `FINAL` on large tables if performance is slow; `argMax` is generally faster.
