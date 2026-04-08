---
name: data-engineering-importer
description: Data engineering for the historical market data pipeline. Use when you need to manage ClickHouse schema, add new data sources (fetchers), backfill historical data, or repair corrupted partitions.
---

# Data Engineering & Importer

## Overview
This skill manages the historical data ingestion pipeline from multiple sources into ClickHouse. It covers the end-to-end lifecycle of data fetchers, watermark-based sync logic, and ClickHouse schema maintenance.

## Core Workflows

### 1. Adding a New Data Source
To add a new data source (e.g., Sentiment from a specific API):
1. **Scaffold Fetcher**: Run `python data-engineering-importer/scripts/add_new_fetcher.py <name>`.
2. **Define Table**: Add the DDL to `src/importer/clickhouse.py`.
3. **Register Source**: Update `src/importer/registry.py` with the new symbols or categories.
4. **Wire into CLI**: Update `run_import()` in `src/importer/cli.py`.

### 2. Managing Historical Backfills
To perform a full historical backfill:
```bash
python -m src.main import <category> --lookback 3650 --full
```
Use `--dry-run` to preview the range before committing to ClickHouse.

### 3. Data Validation & Repair
- **Validate Watermarks**: Run `python data-engineering-importer/scripts/validate_watermarks.py`.
- **Repair Partition**: If a month has corrupted data, run:
  ```bash
  python data-engineering-importer/scripts/repair_clickhouse_partition.py <table_name> <YYYYMM>
  ```
- **Reset Watermark**: To force a re-sync for a specific symbol, you can manually update `market_data.import_watermarks` in ClickHouse:
  ```sql
  ALTER TABLE market_data.import_watermarks DELETE WHERE symbol = 'SYMBOL'
  ```

## Reference Material
- See [references/clickhouse_schema.md](references/clickhouse_schema.md) for table definitions.
- See [references/importer_guide.md](references/importer_guide.md) for delta-sync logic and category list.

## Sensibull Oxide API (FII/DII Data)

For FII/DII cash-market flow data, use the Sensibull data backend — **not** the NSE API (historical endpoint returns 404).

**Endpoint**: `GET https://oxide.sensibull.com/v1/compute/cache/fii_dii_daily`
- No params → current rolling month (~20 trading days)
- `?year_month=2026-March` → specific month (Sensibull format: `YYYY-MonthName`)
- Response: `{ year_month, key_list: ["2025-October", ..., "2026-April"], data: { "2026-04-01": { cash: { fii: {buy, sell, buy_sell_difference}, dii: {buy, sell, buy_sell_difference} } } } }`
- `key_list` exposes ~6 available months (rolling window)

**Month key comparison pitfall**: Month names don't sort alphabetically (`April < March`). Always convert to `(year, month_int)` tuples for comparison.

**Overlap between buckets**: The current-month bucket contains late dates from the prior calendar month — always deduplicate by `trade_date` after merging multiple months.

**Backfill command** (all available history):
```bash
python src/importer/fetchers/fii_dii_fetcher.py --from 2025-10-01 --insert
```
This fetches all 7 month buckets (127 rows, Oct 2025 → present) and inserts into `market_data.fii_dii_flows`.

## Usage Scenarios

| User Request | Action |
|--------------|--------|
| "Add a new data source for US Bond yields" | Scaffold fetcher, define DDL, register in registry. |
| "Backfill GOLDBEES data for the last 5 years" | `python -m src.main import etfs --lookback 1825 --full` |
| "The daily prices seem wrong for Jan 2024" | DROP PARTITION '202401' and re-import. |
| "I added a new ETF to the tracking list" | Add to `ETFS` in `src/importer/registry.py` and run `import`. |
| "Import all FII/DII history" | `python src/importer/fetchers/fii_dii_fetcher.py --from 2025-10-01 --insert` |
| "Get FII/DII for last 1 month" | `python src/importer/fetchers/fii_dii_fetcher.py --from YYYY-MM-DD` (no `--insert` for dry-run) |
