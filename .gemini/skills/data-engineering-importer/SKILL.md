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

## Usage Scenarios

| User Request | Action |
|--------------|--------|
| "Add a new data source for US Bond yields" | Scaffold fetcher, define DDL, register in registry. |
| "Backfill GOLDBEES data for the last 5 years" | `python -m src.main import etfs --lookback 1825 --full` |
| "The daily prices seem wrong for Jan 2024" | DROP PARTITION '202401' and re-import. |
| "I added a new ETF to the tracking list" | Add to `ETFS` in `src/importer/registry.py` and run `import`. |
