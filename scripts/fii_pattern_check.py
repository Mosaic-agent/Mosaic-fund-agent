
import os
import sys
from datetime import date, timedelta
from rich.console import Console
from rich.table import Table

# Add src to path
sys.path.append(os.path.join(os.getcwd(), "src"))

try:
    from config.settings import settings
    from importer.clickhouse import ClickHouseImporter
except ImportError as e:
    print(f"Error importing project modules: {e}")
    sys.exit(1)

console = Console()

def get_fii_data():
    with ClickHouseImporter(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        database=settings.clickhouse_database,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
    ) as ch:
        # Cash Flows
        cash_query = """
            SELECT 
                trade_date, 
                fii_net_cr, 
                dii_net_cr 
            FROM market_data.fii_dii_flows FINAL 
            ORDER BY trade_date DESC 
            LIMIT 5
        """
        cash_result = ch._client.query(cash_query)
        
        # F&O Data
        fno_query = """
            SELECT 
                trade_date, 
                fii_fut_nifty_net_oi, 
                fii_fut_banknifty_net_oi, 
                fii_opt_call_net_oi, 
                fii_opt_put_net_oi 
            FROM market_data.fii_dii_fno_daily FINAL 
            ORDER BY trade_date DESC 
            LIMIT 5
        """
        fno_result = ch._client.query(fno_query)

        return cash_result.result_rows, fno_result.result_rows

def display_data(cash_rows, fno_rows):
    # Cash Table
    cash_table = Table(title="FII/DII Cash Flows (₹ Cr)", show_header=True, header_style="bold magenta")
    cash_table.add_column("Date", style="dim")
    cash_table.add_column("FII Net", justify="right")
    cash_table.add_column("DII Net", justify="right")
    cash_table.add_column("Total", justify="right")

    for row in cash_rows:
        trade_date, fii, dii = row
        total = fii + dii
        fii_style = "green" if fii > 0 else "red"
        dii_style = "green" if dii > 0 else "red"
        total_style = "bold white" if total > 0 else "bold red"
        cash_table.add_row(
            str(trade_date),
            f"[{fii_style}]{fii:,.2f}[/{fii_style}]",
            f"[{dii_style}]{dii:,.2f}[/{dii_style}]",
            f"[{total_style}]{total:,.2f}[/{total_style}]"
        )
    
    console.print(cash_table)
    console.print("\n")

    # F&O Table
    fno_table = Table(title="FII F&O Positioning (OI Contracts)", show_header=True, header_style="bold cyan")
    fno_table.add_column("Date", style="dim")
    fno_table.add_column("Nifty Fut Net", justify="right")
    fno_table.add_column("BNF Fut Net", justify="right")
    fno_table.add_column("Call OI", justify="right")
    fno_table.add_column("Put OI", justify="right")
    fno_table.add_column("PCR (OI)", justify="right")

    for row in fno_rows:
        dt, n_fut, b_fut, call, put = row
        pcr = put / call if call != 0 else 0
        pcr_style = "green" if pcr < 0.8 else "red" if pcr > 1.3 else "yellow"
        
        fno_table.add_row(
            str(dt),
            f"{n_fut:,.0f}",
            f"{b_fut:,.0f}",
            f"{call:,.0f}",
            f"{put:,.0f}",
            f"[{pcr_style}]{pcr:.2f}[/{pcr_style}]"
        )
    
    console.print(fno_table)

if __name__ == "__main__":
    try:
        cash, fno = get_fii_data()
        if not cash:
            console.print("[yellow]No FII data found in database.[/yellow]")
        else:
            display_data(cash, fno)
    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
