import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from rich.console import Console
from rich.table import Table

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

try:
    from config.settings import settings
except ImportError as e:
    print(f"Error importing settings: {e}")
    sys.exit(1)

def audit_freshness():
    console = Console()
    console.print("[bold cyan]🔍 Database Freshness Audit Scanner[/bold cyan]\n")

    try:
        from src.db.pool import get_client
        client = get_client()
    except Exception as e:
        console.print(f"[red]Error connecting to ClickHouse: {e}[/red]")
        sys.exit(1)

    # Categories to check in daily_prices
    daily_price_categories = ['stocks', 'etfs', 'indices', 'commodities', 'us_stocks']
    
    results = []

    # 1. Check daily_prices categories
    for cat in daily_price_categories:
        query = f"SELECT max(trade_date) FROM market_data.daily_prices FINAL WHERE category = '{cat}'"
        try:
            res = client.query(query)
            max_date = res.result_rows[0][0] if res.result_rows else None
            results.append((cat.capitalize(), "market_data.daily_prices", max_date))
        except Exception as e:
            results.append((cat.capitalize(), "market_data.daily_prices", f"Error: {e}"))

    # 2. Check fx_rates category
    try:
        res = client.query("SELECT max(trade_date) FROM market_data.fx_rates FINAL")
        max_date = res.result_rows[0][0] if res.result_rows else None
        results.append(("FX_Rates", "market_data.fx_rates", max_date))
    except Exception as e:
        results.append(("FX_Rates", "market_data.fx_rates", f"Error: {e}"))

    # 3. Check inav category
    try:
        res = client.query("SELECT max(toDate(snapshot_at)) FROM market_data.inav_snapshots FINAL")
        max_date = res.result_rows[0][0] if res.result_rows else None
        results.append(("iNAV", "market_data.inav_snapshots", max_date))
    except Exception as e:
        results.append(("iNAV", "market_data.inav_snapshots", f"Error: {e}"))

    # 4. Check indian_macro category
    try:
        res = client.query("SELECT max(as_of_date) FROM market_data.indian_macro_indicators FINAL")
        max_date = res.result_rows[0][0] if res.result_rows else None
        results.append(("Indian_Macro", "market_data.indian_macro_indicators", max_date))
    except Exception as e:
        results.append(("Indian_Macro", "market_data.indian_macro_indicators", f"Error: {e}"))

    # 5. Check fii_dii category
    try:
        res = client.query("SELECT max(trade_date) FROM market_data.fii_dii_flows FINAL")
        max_date = res.result_rows[0][0] if res.result_rows else None
        results.append(("FII_DII", "market_data.fii_dii_flows", max_date))
    except Exception as e:
        results.append(("FII_DII", "market_data.fii_dii_flows", f"Error: {e}"))

    # 6. Check AMC Mutual Fund Holdings category
    try:
        res = client.query("SELECT max(as_of_month) FROM market_data.mf_holdings FINAL")
        max_date = res.result_rows[0][0] if res.result_rows else None
        results.append(("AMC_Holdings", "market_data.mf_holdings", max_date))
    except Exception as e:
        results.append(("AMC_Holdings", "market_data.mf_holdings", f"Error: {e}"))

    # Print results
    table = Table(title="Category Freshness Audit", show_header=True, header_style="bold magenta")
    table.add_column("Category", style="cyan")
    table.add_column("Table", style="dim")
    table.add_column("Max Date/Record Date", justify="center")
    table.add_column("Status", justify="center")

    today = date.today()
    # Find last business day: if today is Monday (0) or Sunday (6) or Saturday (5)
    # Actually, simpler threshold: max_date >= today - 1 business day (approx 3 days to account for weekend)
    # A category is stale if the last date is older than 1 business day (2-3 calendar days on weekends, 1 day on weekdays)
    # Let's compute if it's older than 1 business day from May 28, 2026 (Thursday).
    # Since today is Thursday, May 28, the limit is Wednesday, May 27. So if max_date < May 27, 2026, it is STALE.
    # For iNAV, it must be today (May 28). If older than today, it is considered stale for iNAV.
    
    stale_categories = []
    
    for cat, tbl_name, max_date in results:
        if isinstance(max_date, str) and max_date.startswith("Error"):
            table.add_row(cat, tbl_name, str(max_date), "[bold red]ERROR[/bold red]")
            stale_categories.append(cat.lower())
            continue
            
        if max_date is None:
            table.add_row(cat, tbl_name, "No Data", "[bold red]STALE (No Data)[/bold red]")
            stale_categories.append(cat.lower())
            continue
            
        # Determine stale threshold
        # Today is Thursday May 28, 2026.
        # Threshold: Any category with a max(trade_date) older than 1 business day (or today for iNAV) is considered STALE.
        if cat == "iNAV":
            is_stale = (max_date < today)
            threshold_desc = "Today"
        elif cat == "Indian_Macro":
            # Indian macro indicators are monthly (e.g. auto sales, insurance).
            # Consider stale if older than 60 days.
            limit_date = today - timedelta(days=60)
            is_stale = (max_date < limit_date)
            threshold_desc = f"On/After {limit_date}"
        elif cat == "AMC_Holdings":
            # AMC holdings disclosures are monthly.
            # Consider stale if older than 45 days.
            limit_date = today - timedelta(days=45)
            is_stale = (max_date < limit_date)
            threshold_desc = f"On/After {limit_date}"
        else:
            # Older than 1 business day
            # If today is Thursday, 1 business day ago is Wednesday.
            # If today is Monday, 1 business day ago is Friday (3 days).
            # Let's do a robust business day subtraction
            # Or just check day of week:
            days_to_subtract = 1
            if today.weekday() == 0: # Monday
                days_to_subtract = 3
            elif today.weekday() == 6: # Sunday
                days_to_subtract = 2
            elif today.weekday() == 5: # Saturday
                days_to_subtract = 1
            limit_date = today - timedelta(days=days_to_subtract)
            is_stale = (max_date < limit_date)
            threshold_desc = f"On/After {limit_date}"

        status = "[bold red]STALE[/bold red]" if is_stale else "[bold green]FRESH[/bold green]"
        if is_stale:
            # Map standard name to cli import category name
            import_name = cat.lower()
            if import_name == "stocks": import_name = "stocks"
            elif import_name == "etfs": import_name = "etfs"
            elif import_name == "indices": import_name = "indices"
            elif import_name == "commodities": import_name = "commodities"
            elif import_name == "us_stocks": import_name = "us_stocks"
            elif import_name == "fx_rates": import_name = "fx_rates"
            elif import_name == "inav": import_name = "inav"
            elif import_name == "indian_macro": import_name = "indian_macro"
            elif import_name == "fii_dii": import_name = "fii_dii"
            elif import_name == "amc_holdings": import_name = "dsp,nippon,icici,quant,bajaj,hdfc,kotak,abakkus,helios,invesco,canara,mirae,axis,motilal"
            stale_categories.append(import_name)

        table.add_row(cat, tbl_name, str(max_date), status)

    console.print(table)
    
    if stale_categories:
        console.print(f"\n[bold yellow]⚠ Stale categories identified:[/bold yellow] {', '.join(stale_categories)}")
        # Print action command
        console.print(f"[bold cyan]Action required:[/bold cyan] Run targeted import:")
        console.print(f"  python src/main.py import --category {','.join(stale_categories)}")
    else:
        console.print("\n[bold green]✅ All database categories are FRESH![/bold green]")
        
    audit_qdrant_freshness(console)
    client.close()

def audit_qdrant_freshness(console):
    """Compare latest dates in Qdrant collections vs ClickHouse source tables."""
    try:
        from qdrant_client import QdrantClient
        import os
        host = os.environ.get("QDRANT_HOST", "localhost")
        port = int(os.environ.get("QDRANT_PORT", "6333"))
        qc = QdrantClient(url=f"http://{host}:{port}", timeout=10)
        # Quick connectivity check
        qc.get_collections()
    except Exception:
        console.print("\n[yellow]⚠ Qdrant not available — skipping vector staleness check[/yellow]")
        return

    from src.db.pool import get_client
    ch = get_client()

    checks = [
        # (qdrant_collection, ch_query_for_max_date, payload_date_field, label)
        ("mf_holdings",      "SELECT max(as_of_month) FROM market_data.mf_holdings FINAL",
         "as_of_month",      "as_of_timestamp",   "MF Holdings"),
        ("mf_fund_profiles", "SELECT max(as_of_month) FROM market_data.mf_holdings FINAL",
         "as_of_month",      "as_of_timestamp",   "MF Fund Profiles"),
        ("news_articles",    "SELECT max(toString(toDate(published_at))) FROM market_data.news_articles FINAL",
         "published_date",   "published_timestamp", "News Articles"),
        ("market_anomalies", "SELECT max(toString(trade_date)) FROM market_data.daily_prices FINAL WHERE category='etfs'",
         "trade_date",       "trade_timestamp",   "Market Anomalies"),
        ("market_data",      "SELECT max(toString(trade_date)) FROM market_data.daily_prices FINAL",
         "trade_date",       "trade_timestamp",   "Market Data"),
    ]

    table = Table(title="Qdrant vs ClickHouse Staleness", show_header=True, header_style="bold blue")
    table.add_column("Collection", style="cyan")
    table.add_column("Qdrant Latest", justify="center")
    table.add_column("ClickHouse Latest", justify="center")
    table.add_column("Qdrant Points", justify="right")
    table.add_column("Status", justify="center")

    for coll_name, ch_query, date_field, ts_field, label in checks:
        # Get ClickHouse max date
        try:
            ch_res = ch.query(ch_query)
            ch_date = str(ch_res.result_rows[0][0]) if ch_res.result_rows else "?"
        except Exception:
            ch_date = "ERROR"

        # Get Qdrant collection info
        try:
            info = qc.get_collection(coll_name)
            point_count = info.points_count
            if point_count == 0:
                qdrant_date = "EMPTY"
            else:
                # Scroll to get a sample of recent points by ordering
                try:
                    hits = qc.scroll(
                        collection_name=coll_name,
                        limit=1,
                        order_by=ts_field,
                        with_payload=[date_field],
                    )
                    if hits[0]:
                        qdrant_date = str(hits[0][0].payload.get(date_field, "?"))
                    else:
                        qdrant_date = "?"
                except Exception:
                    # Fallback: scroll without ordering
                    hits = qc.scroll(
                        collection_name=coll_name,
                        limit=1,
                        with_payload=[date_field],
                    )
                    qdrant_date = str(hits[0][0].payload.get(date_field, "?")) if hits[0] else "?"
        except Exception:
            point_count = 0
            qdrant_date = "N/A"

        is_stale = (
            ch_date not in ("ERROR", "?")
            and qdrant_date not in ("N/A", "EMPTY", "?")
            and str(qdrant_date) < str(ch_date)
        )
        if qdrant_date in ("N/A", "EMPTY"):
            status = "[yellow]EMPTY[/yellow]"
        elif is_stale:
            status = "[bold red]STALE[/bold red]"
        else:
            status = "[bold green]SYNCED[/bold green]"

        table.add_row(label, str(qdrant_date), str(ch_date), str(point_count), status)

    console.print("\n")
    console.print(table)
    qc.close()

if __name__ == "__main__":
    audit_freshness()
