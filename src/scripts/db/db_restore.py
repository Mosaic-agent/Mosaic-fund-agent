"""
db_restore.py — ClickHouse restore utility for Mosaic Fund Agent.

Usage:
  python scripts/db_restore.py --list                          # show available backups
  python scripts/db_restore.py --from-native backup_20260412_220000
  python scripts/db_restore.py --from-native backup_20260412_220000 --dry-run
  python scripts/db_restore.py --from-parquet 20260412        # restore precious tables from parquet
  python scripts/db_restore.py --from-parquet 20260412 --table mf_holdings  # single table
"""

import os
import sys
import argparse
from pathlib import Path

sys.path.insert(0, os.getcwd())
from config.settings import settings

import clickhouse_connect
from rich.console import Console
from rich.table import Table

console = Console()

DATABASE = "market_data"
PARQUET_BASE = Path("output/db-backups/parquet")


def _client():
    return clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=DATABASE,
    )


def list_all(client):
    """Print all available native backups and parquet exports."""
    console.print("\n[bold cyan]Native Backups[/bold cyan]")
    try:
        rows = client.query(
            "SELECT name, status, start_time, "
            "formatReadableSize(compressed_size) AS size "
            "FROM system.backups ORDER BY start_time DESC"
        ).result_rows
        if rows:
            t = Table(show_header=True, header_style="bold cyan")
            t.add_column("Name")
            t.add_column("Status")
            t.add_column("Started")
            t.add_column("Compressed Size")
            for r in rows:
                t.add_row(str(r[0]), str(r[1]), str(r[2]), str(r[3]))
            console.print(t)
        else:
            console.print("  [dim]No native backups found.[/dim]")
    except Exception as e:
        console.print(f"  [yellow]Could not query system.backups: {e}[/yellow]")

    console.print("\n[bold magenta]Parquet Exports[/bold magenta]")
    if PARQUET_BASE.exists():
        dates = sorted(PARQUET_BASE.iterdir(), reverse=True)
        if dates:
            pt = Table(show_header=True, header_style="bold magenta")
            pt.add_column("Date")
            pt.add_column("Tables")
            pt.add_column("Total Size")
            for d in dates[:20]:
                files = list(d.glob("*.parquet"))
                total = sum(f.stat().st_size for f in files)
                pt.add_row(d.name, ", ".join(f.stem for f in files), f"{total/1024/1024:.1f} MB")
            console.print(pt)
        else:
            console.print("  [dim]No parquet exports found.[/dim]")
    else:
        console.print("  [dim]No parquet exports directory found.[/dim]")


def restore_native(client, backup_name, dry_run=False):
    """Restore the full database from a native backup."""
    sql = (
        f"RESTORE DATABASE {DATABASE} "
        f"FROM Disk('backups', '{backup_name}') "
        "SETTINGS allow_non_empty_tables=true"
    )
    if dry_run:
        console.print(f"[dim][dry-run] Would execute:\n  {sql}[/dim]")
        return

    console.print(f"[cyan]Restoring from native backup: {backup_name} ...[/cyan]")
    console.print("[yellow]⚠  This will overwrite existing data in conflicting tables.[/yellow]")
    try:
        client.command(sql)
        console.print(f"[green]✓ Restore complete from {backup_name}[/green]")
        _print_row_counts(client)
    except Exception as e:
        console.print(f"[red]✗ Restore failed: {e}[/red]")
        raise


def restore_parquet(client, date_str, table_filter=None, dry_run=False):
    """Restore one or all precious tables from a Parquet export directory."""
    export_dir = PARQUET_BASE / date_str
    if not export_dir.exists():
        console.print(f"[red]No parquet export found for date: {date_str}[/red]")
        console.print(f"Available: {[d.name for d in sorted(PARQUET_BASE.iterdir(), reverse=True)[:5]]}")
        return

    files = list(export_dir.glob("*.parquet"))
    if table_filter:
        files = [f for f in files if f.stem == table_filter]
        if not files:
            console.print(f"[red]No parquet file for table '{table_filter}' in {date_str}[/red]")
            return

    import pandas as pd

    for parquet_file in sorted(files):
        table = parquet_file.stem
        full_table = f"{DATABASE}.{table}"
        if dry_run:
            console.print(f"[dim][dry-run] Would restore {full_table} from {parquet_file}[/dim]")
            continue

        console.print(f"  Restoring [bold]{full_table}[/bold] from {parquet_file.name} ...")
        try:
            df = pd.read_parquet(parquet_file)
            if df.empty:
                console.print(f"  [yellow]  Skipped (empty parquet)[/yellow]")
                continue
            client.insert_df(full_table, df)
            console.print(f"  [green]  ✓ {len(df):,} rows inserted[/green]")
        except Exception as e:
            console.print(f"  [red]  ✗ {table}: {e}[/red]")


def _print_row_counts(client):
    """Print current row counts for all tables as a post-restore sanity check."""
    try:
        rows = client.query(
            "SELECT name, formatReadableQuantity(total_rows) AS rows, "
            "formatReadableSize(total_bytes) AS size "
            "FROM system.tables WHERE database = 'market_data' "
            "ORDER BY total_rows DESC"
        ).result_rows
        t = Table(title="Post-Restore Row Counts", header_style="bold green")
        t.add_column("Table")
        t.add_column("Rows", justify="right")
        t.add_column("Size", justify="right")
        for r in rows:
            t.add_row(str(r[0]), str(r[1]), str(r[2]))
        console.print(t)
    except Exception:
        pass


def main():
    parser = argparse.ArgumentParser(description="ClickHouse restore utility")
    parser.add_argument("--list", action="store_true", help="List available backups")
    parser.add_argument("--from-native", metavar="NAME", help="Restore from named native backup")
    parser.add_argument("--from-parquet", metavar="DATE", help="Restore from parquet export (YYYYMMDD)")
    parser.add_argument("--table", metavar="TABLE", help="Restore only this table (parquet mode)")
    parser.add_argument("--dry-run", action="store_true", help="Print actions without executing")
    args = parser.parse_args()

    client = _client()

    if args.list:
        list_all(client)
    elif args.from_native:
        restore_native(client, args.from_native, dry_run=args.dry_run)
    elif args.from_parquet:
        restore_parquet(client, args.from_parquet, table_filter=args.table, dry_run=args.dry_run)
    else:
        parser.print_help()

    client.close()


if __name__ == "__main__":
    main()
