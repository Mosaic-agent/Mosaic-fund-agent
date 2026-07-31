---
name: data-engineering-importer
description: Data engineering for the historical market data pipeline. Use when you need to manage ClickHouse schema, add new data sources (fetchers), backfill historical data, or repair corrupted partitions.
---

# Data Engineering & Importer

## Overview
This skill manages the historical data ingestion pipeline from multiple sources into ClickHouse. It covers the end-to-end lifecycle of data fetchers, watermark-based sync logic, and ClickHouse schema maintenance.

## Core Workflows

### 1. Adding a New Data Source (Adapter Pattern)
Preferred path — uses the `Fetcher` ABC (`src/importer/base_fetcher.py`):

```python
# src/importer/fetchers/adapters.py
class MySentimentFetcher(Fetcher):
    source_name = "sentiment_api"
    symbol_key  = "ALL"
    overlap_days = 1

    def fetch(self, from_date, to_date):
        # call external API, return list[dict]
        ...

    def insert(self, rows, ch):
        return ch.insert_news_articles(rows)   # or a new insert method

# Register — orchestrator picks it up automatically
get_registry()["sentiment"] = MySentimentFetcher()
```

Then `repo.run_fetcher(get_registry()["sentiment"])` handles watermarks, dry-run, and fires `DataImportedEvent` with no extra wiring.

All general data categories (`stocks`, `etfs`, `fii_dii`, `world_bank`, `imf_weo`, `amfi_flows`, `fx_rates`, `cot`, `cb_reserves`, etc.) are 100% adapter-driven. Adding a new category simply requires instantiating its `Fetcher` in `_build_registry()` inside `src/importer/fetchers/adapters.py`.

### 2. Nippon India AMC Importer — Dynamic URL Discovery

The Nippon importer (`src/scripts/fund_imports/importers/nippon.py`) auto-discovers monthly XLS files from 2024 onward. **No manual URL additions are needed for new months.**

- Historical entries (Jan 2017 – Dec 2023): static `XLS_FILES` list (irregular URL formats)
- From 2024: `_discover_recent_months()` probes `mf.nipponindiaim.com` via HEAD requests at runtime, tries multiple filename variants per month (handles April/June/July full names, no-day November quirk)
- Delta sync: already-imported months are skipped automatically

```bash
python src/scripts/fund_imports/run.py nippon          # delta sync (new months only)
python src/scripts/fund_imports/run.py nippon --full   # reimport all months
python src/scripts/fund_imports/run.py nippon --dry-run
```

### 3. Managing Historical Backfills
To perform a full historical backfill:
```bash
# Correct syntax — category is a --flag, not a positional arg
python src/main.py import --category etfs --full --lookback 3650
python src/main.py import --category etfs,mf,cot --full
python src/main.py import --dry-run   # preview without writing
```

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
- Schema DDL lives in `src/importer/clickhouse.py` (search for `_DDL_` constants)
- Category list and symbols: `src/importer/registry.py`
- Delta-sync logic: `src/importer/cli.py` → `run_import()`

## ClickHouse Connection Pattern
All service modules use the shared pool at `src/db/pool.py` — never call `clickhouse_connect.get_client()` directly in `src/`. The importer (`src/importer/clickhouse.py`) is the only exception: it owns one long-lived connection for bulk inserts.

```python
# Correct pattern for any new code in src/
from src.db.pool import get_pool
pool = get_pool()
df = pool.query_df("SELECT ...")       # one-shot SELECT
pool.execute("INSERT INTO ...")        # DDL / INSERT
with pool.acquire() as client:         # raw multi-statement access
    client.query(...)
```

New pool config vars (`.env`): `CLICKHOUSE_POOL_MIN` (default 2), `CLICKHOUSE_POOL_MAX` (default 10), `CLICKHOUSE_POOL_TIMEOUT` (default 10.0s).

## Usage Scenarios

| User Request | Action |
|--------------|--------|
| "Add a new data source for US Bond yields" | Scaffold fetcher, define DDL, register in registry. |
| "Backfill GOLDBEES data for the last 5 years" | `python src/main.py import --category etfs --lookback 1825 --full` |
| "The daily prices seem wrong for Jan 2024" | DROP PARTITION '202401' and re-import. |
| "I added a new ETF to the tracking list" | Add to `ETFS` in `src/importer/registry.py` and run `import`. |
