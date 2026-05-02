"""
db_backup.py — ClickHouse backup utility for Mosaic Fund Agent.

Tiers:
  native  — BACKUP DATABASE via ClickHouse's built-in mechanism (stored in clickhouse-backups volume)
  parquet — Export irreplaceable tables to Parquet files under output/db-backups/parquet/
  full    — Both (default)

Usage:
  python scripts/db_backup.py                     # full backup
  python scripts/db_backup.py --mode native        # native only
  python scripts/db_backup.py --mode parquet       # parquet export only
  python scripts/db_backup.py --list               # list existing native backups
  python scripts/db_backup.py --keep-days 14       # prune backups older than 14 days
  python scripts/db_backup.py --dry-run            # print what would run, no writes
"""

import os
import sys
import argparse
import requests
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, os.getcwd())
from config.settings import settings

import clickhouse_connect
from rich.console import Console
from rich.table import Table

console = Console()

DATABASE = "market_data"

# Tables exported to Parquet — cannot be recovered from external APIs
PRECIOUS_TABLES = [
    "mf_holdings",       # DSP 31-month backfill — irreplaceable
    "inav_snapshots",    # Historical intraday iNAV — NSE live-only source
    "import_watermarks", # Delta-sync state — loss forces full re-import of everything
    "signal_composite",  # Computed composite signals log
    "news_articles",     # Ephemeral; gone from sources after TTL
]

PARQUET_BASE = Path("output/db-backups/parquet")


def _client():
    return clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=DATABASE,
    )


def _http_url():
    auth = ""
    if settings.clickhouse_user and settings.clickhouse_password:
        auth = f"{settings.clickhouse_user}:{settings.clickhouse_password}@"
    return f"http://{auth}{settings.clickhouse_host}:{settings.clickhouse_port}/"


def list_backups(client):
    """Print a table of all native backups stored on the backups disk."""
    try:
        rows = client.query(
            "SELECT name, status, start_time, end_time, "
            "formatReadableSize(uncompressed_size) AS uncompressed, "
            "formatReadableSize(compressed_size) AS compressed "
            "FROM system.backups "
            "WHERE status = 'BACKUP_COMPLETE' "
            "ORDER BY start_time DESC"
        ).result_rows
    except Exception as e:
        console.print(f"[red]Could not query system.backups: {e}[/red]")
        return

    t = Table(title="Native Backups", show_header=True, header_style="bold cyan")
    t.add_column("Name")
    t.add_column("Status")
    t.add_column("Started")
    t.add_column("Uncompressed")
    t.add_column("Compressed")
    for r in rows:
        t.add_row(str(r[0]), str(r[1]), str(r[2]), str(r[4]), str(r[5]))
    console.print(t)

    # Also list parquet exports
    if PARQUET_BASE.exists():
        dates = sorted(PARQUET_BASE.iterdir(), reverse=True)
        if dates:
            pt = Table(title="Parquet Exports", show_header=True, header_style="bold magenta")
            pt.add_column("Date")
            pt.add_column("Tables")
            pt.add_column("Size")
            for d in dates[:10]:
                files = list(d.glob("*.parquet"))
                total = sum(f.stat().st_size for f in files)
                pt.add_row(
                    d.name,
                    ", ".join(f.stem for f in files),
                    f"{total / 1024 / 1024:.1f} MB",
                )
            console.print(pt)


def run_native_backup(client, dry_run=False):
    """Run BACKUP DATABASE via ClickHouse's built-in mechanism."""
    name = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    sql = f"BACKUP DATABASE {DATABASE} TO Disk('backups', '{name}') SETTINGS async=false"
    if dry_run:
        console.print(f"[dim][dry-run] Would execute: {sql}[/dim]")
        return name
    try:
        console.print(f"[cyan]Running native backup → {name} ...[/cyan]")
        client.command(sql)
        console.print(f"[green]✓ Native backup complete: {name}[/green]")
        return name
    except Exception as e:
        console.print(f"[red]✗ Native backup failed: {e}[/red]")
        return None


def run_parquet_export(dry_run=False):
    """Export precious tables to Parquet files."""
    date_str = datetime.now().strftime("%Y%m%d")
    out_dir = PARQUET_BASE / date_str
    if not dry_run:
        out_dir.mkdir(parents=True, exist_ok=True)

    base_url = _http_url()
    results = []

    for table in PRECIOUS_TABLES:
        query = f"SELECT * FROM {DATABASE}.{table} FORMAT Parquet"
        out_path = out_dir / f"{table}.parquet"

        if dry_run:
            console.print(f"[dim][dry-run] Would export {table} → {out_path}[/dim]")
            results.append((table, "dry-run", "-"))
            continue

        try:
            console.print(f"  Exporting [bold]{table}[/bold] ...")
            resp = requests.get(base_url, params={"query": query}, timeout=120, stream=True)
            resp.raise_for_status()
            out_path.write_bytes(resp.content)
            size_mb = out_path.stat().st_size / 1024 / 1024
            results.append((table, "ok", f"{size_mb:.2f} MB"))
        except Exception as e:
            console.print(f"[red]  ✗ {table}: {e}[/red]")
            results.append((table, "error", str(e)))

    t = Table(title=f"Parquet Export — {date_str}", header_style="bold magenta")
    t.add_column("Table")
    t.add_column("Status")
    t.add_column("Size")
    for row in results:
        color = "green" if row[1] == "ok" else ("dim" if row[1] == "dry-run" else "red")
        t.add_row(f"[{color}]{row[0]}[/{color}]", row[1], row[2])
    console.print(t)
    return out_dir if not dry_run else None


def prune_old_native_backups(client, keep_days, dry_run=False):
    """Drop native backups older than keep_days."""
    cutoff = datetime.now() - timedelta(days=keep_days)
    try:
        rows = client.query(
            "SELECT name, start_time FROM system.backups "
            "WHERE status = 'BACKUP_COMPLETE' ORDER BY start_time"
        ).result_rows
    except Exception:
        return

    pruned = 0
    for name, start_time in rows:
        if start_time < cutoff:
            sql = f"DROP BACKUP Disk('backups', '{name}')"
            if dry_run:
                console.print(f"[dim][dry-run] Would drop: {name}[/dim]")
            else:
                try:
                    client.command(sql)
                    console.print(f"[dim]Pruned old backup: {name}[/dim]")
                    pruned += 1
                except Exception as e:
                    console.print(f"[yellow]Could not prune {name}: {e}[/yellow]")
    if pruned:
        console.print(f"[dim]Pruned {pruned} backup(s) older than {keep_days} days.[/dim]")


def prune_old_parquet_exports(keep_days, dry_run=False):
    """Remove Parquet export directories older than keep_days."""
    if not PARQUET_BASE.exists():
        return
    cutoff = datetime.now() - timedelta(days=keep_days)
    for d in PARQUET_BASE.iterdir():
        try:
            dir_date = datetime.strptime(d.name, "%Y%m%d")
        except ValueError:
            continue
        if dir_date < cutoff:
            if dry_run:
                console.print(f"[dim][dry-run] Would remove parquet dir: {d}[/dim]")
            else:
                import shutil
                shutil.rmtree(d)
                console.print(f"[dim]Removed old parquet export: {d.name}[/dim]")


def main():
    parser = argparse.ArgumentParser(description="ClickHouse backup utility")
    parser.add_argument("--mode", choices=["native", "parquet", "full"], default="full")
    parser.add_argument("--list", action="store_true", help="List existing backups")
    parser.add_argument("--keep-days", type=int, default=30,
                        help="Prune backups older than N days (default: 30)")
    parser.add_argument("--dry-run", action="store_true", help="Print actions without executing")
    args = parser.parse_args()

    client = _client()

    if args.list:
        list_backups(client)
        client.close()
        return

    if args.mode in ("native", "full"):
        run_native_backup(client, dry_run=args.dry_run)
        prune_old_native_backups(client, args.keep_days, dry_run=args.dry_run)

    if args.mode in ("parquet", "full"):
        run_parquet_export(dry_run=args.dry_run)
        prune_old_parquet_exports(args.keep_days, dry_run=args.dry_run)

    client.close()


if __name__ == "__main__":
    main()
