# ClickHouse DB Management

> Database: `market_data` · Engine: ClickHouse 24 (Docker) · Tables: 15 × ReplacingMergeTree

---

## Backup Strategy

Three tiers — run manually or via your preferred scheduler.

| Tier | Command | Where stored | When to use |
|---|---|---|---|
| **1 — Native** | `python scripts/db_backup.py --mode native` | `clickhouse-backups` Docker volume | Regular snapshots; full restore from single file |
| **2 — Parquet** | `python scripts/db_backup.py --mode parquet` | `output/db-backups/parquet/YYYYMMDD/` | Insurance for irreplaceable tables; portable |
| **3 — Volume tar** | See below | Any path on host | Before `docker volume rm`, OS reinstalls, migrations |

**Recommended cadence:**
- `--mode full` (both tiers) after every significant data import session
- Tier 3 cold snapshot before any Docker or host infrastructure change

---

## Data Classification

### Irreplaceable — back up before any destructive operation

| Table | Why irreplaceable | Covered by |
|---|---|---|
| `mf_holdings` | DSP 31-month historical backfill from DSP website ZIPs | Native + Parquet |
| `inav_snapshots` | Intraday NSE iNAV history; NSE serves live data only | Native + Parquet |
| `signal_composite` | Computed signals log; regeneration is lossy without archived inputs | Native + Parquet |
| `news_articles` | Ephemeral; articles expire from sources | Native + Parquet |
| `import_watermarks` | Delta-sync state; loss forces full re-import of all categories | Native + Parquet |

### Re-importable — recoverable via `python src/main.py import`

| Table | Recovery command |
|---|---|
| `daily_prices` | `import --category stocks,etfs,commodities,indices --full` |
| `mf_nav` | `import --category mf --full` |
| `fx_rates` | `import --category fx_rates --full` |
| `cot_gold` | `import --category cot --full` |
| `cb_gold_reserves` | `import --category cb_reserves --full` |
| `etf_aum` | `import --category etf_aum --full` |
| `fii_dii_flows` / `fii_dii_monthly` / `fii_dii_fno_daily` | `import --category fii_dii --full` |
| `ml_predictions` | Re-run ML forecast script |

---

## Backup Commands

```bash
# Full backup (native snapshot + parquet export of irreplaceable tables)
python scripts/db_backup.py

# Native only
python scripts/db_backup.py --mode native

# Parquet export only
python scripts/db_backup.py --mode parquet

# List all backups
python scripts/db_backup.py --list

# Keep only last 14 days of backups
python scripts/db_backup.py --keep-days 14

# Preview without writing
python scripts/db_backup.py --dry-run
```

### Tier 3 — Cold Docker volume snapshot

Stop ClickHouse first for a consistent snapshot:
```bash
docker compose stop clickhouse

# Snapshot to a local tar
docker run --rm \
  -v mosaic-fund-agent_clickhouse-data:/data \
  -v $(pwd)/output/db-backups:/backup \
  alpine tar czf /backup/clickhouse-volume-$(date +%Y%m%d).tar.gz -C /data .

docker compose start clickhouse
```

Restore from tar:
```bash
docker compose stop clickhouse

docker run --rm \
  -v mosaic-fund-agent_clickhouse-data:/data \
  -v $(pwd)/output/db-backups:/backup \
  alpine sh -c "rm -rf /data/* && tar xzf /backup/clickhouse-volume-YYYYMMDD.tar.gz -C /data"

docker compose start clickhouse
```

---

## Restore Commands

```bash
# List available backups
python scripts/db_restore.py --list

# Restore full DB from native backup
python scripts/db_restore.py --from-native backup_20260412_220000

# Restore from native backup (dry-run — shows SQL only)
python scripts/db_restore.py --from-native backup_20260412_220000 --dry-run

# Restore all precious tables from parquet export
python scripts/db_restore.py --from-parquet 20260412

# Restore only mf_holdings from parquet export
python scripts/db_restore.py --from-parquet 20260412 --table mf_holdings
```

### Restore Runbook

1. **Assess damage** — is ClickHouse running? `docker compose ps`
2. **Start fresh if needed** — `docker compose up clickhouse -d` (creates empty volume)
3. **Choose backup tier:**
   - Prefer Native if full DB loss
   - Prefer Parquet if only irreplaceable tables need recovery and re-importable data can be re-fetched
4. **Run restore** — see commands above
5. **Verify row counts** — restore script prints counts automatically; or run monitoring queries below
6. **Re-import re-importable tables** if needed — `python src/main.py import --category all`
7. **Verify watermarks** — `SELECT * FROM market_data.import_watermarks ORDER BY source, symbol`

---

## Monitoring Queries

```sql
-- Row counts and sizes for all tables
SELECT
    name,
    formatReadableQuantity(total_rows) AS rows,
    formatReadableSize(total_bytes) AS size
FROM system.tables
WHERE database = 'market_data'
ORDER BY total_bytes DESC;

-- Parts per table (high part count = needs OPTIMIZE)
SELECT
    table,
    count() AS parts,
    sum(rows) AS rows,
    formatReadableSize(sum(bytes_on_disk)) AS disk
FROM system.parts
WHERE database = 'market_data' AND active
GROUP BY table
ORDER BY parts DESC;

-- Watermark status (delta-sync state)
SELECT source, symbol, last_date
FROM market_data.import_watermarks FINAL
ORDER BY source, symbol;

-- Native backup status
SELECT name, status, start_time, formatReadableSize(compressed_size) AS size
FROM system.backups
ORDER BY start_time DESC
LIMIT 10;
```

---

## Maintenance Schedule

### After each import session
```bash
# Trigger immediate part merges on high-churn tables
docker exec -it <clickhouse-container> clickhouse-client \
  --query "OPTIMIZE TABLE market_data.daily_prices FINAL"
```

### Weekly (high-churn tables)
```sql
OPTIMIZE TABLE market_data.daily_prices FINAL;
OPTIMIZE TABLE market_data.inav_snapshots FINAL;
OPTIMIZE TABLE market_data.fii_dii_fno_daily FINAL;
```

### Monthly (all tables)
```sql
OPTIMIZE TABLE market_data.mf_nav FINAL;
OPTIMIZE TABLE market_data.fx_rates FINAL;
OPTIMIZE TABLE market_data.news_articles FINAL;
OPTIMIZE TABLE market_data.signal_composite FINAL;
-- (repeat for remaining tables)
```

---

## Retention Policies

ClickHouse doesn't enforce automatic TTL unless configured. Manual cleanup recommendations:

| Table | Keep | Rationale |
|---|---|---|
| `news_articles` | 90 days | Ephemeral; older news has no analytical value |
| `inav_snapshots` | 1 year | Intraday data; older iNAV history rarely queried |
| `signal_composite` | 6 months | Rolling signal history; older records superseded |
| `ml_predictions` | 6 months | Model outputs; retrain supersedes old forecasts |
| All others | Indefinitely | Historical market data is valuable long-term |

Delete old rows manually when needed:
```sql
ALTER TABLE market_data.news_articles DELETE
WHERE fetched_at < now() - INTERVAL 90 DAY;

ALTER TABLE market_data.inav_snapshots DELETE
WHERE snapshot_at < now() - INTERVAL 365 DAY;
```

---

## Docker Volume Safety

The `clickhouse-data` and `clickhouse-backups` volumes are separate — this is intentional. A corrupted `clickhouse-data` volume does not affect `clickhouse-backups`.

```bash
# List volumes
docker volume ls | grep mosaic

# Inspect volume path on host (macOS: inside Docker VM, not directly accessible)
docker volume inspect mosaic-fund-agent_clickhouse-data

# NEVER run without understanding impact:
# docker volume rm mosaic-fund-agent_clickhouse-data  ← destroys all DB data
```

On macOS, Docker volumes live inside the Docker VM and are not directly accessible on the host filesystem. Use the Tier 3 cold snapshot method above to extract data to a host path.
