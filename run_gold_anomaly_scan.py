
import pandas as pd
import clickhouse_connect
from src.ml.anomaly import run_composite_anomaly
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from config.settings import settings

def run_gold_anomaly():
    console = Console()
    client = clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        database=settings.clickhouse_database,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
    )
    
    # Fetch GOLDBEES data
    query = """
    SELECT 
        trade_date, 
        argMax(open, imported_at) as open, 
        argMax(high, imported_at) as high, 
        argMax(low, imported_at) as low, 
        argMax(close, imported_at) as close, 
        argMax(volume, imported_at) as volume
    FROM market_data.daily_prices
    WHERE symbol = 'GOLDBEES' AND category = 'etfs'
    GROUP BY trade_date
    ORDER BY trade_date ASC
    """
    df = client.query_df(query)
    client.close()
    
    if df.empty:
        console.print("[red]Error: No GOLDBEES data found in ClickHouse.[/red]")
        return

    # Run composite anomaly pipeline
    df_res, df_flagged, _ = run_composite_anomaly(df, z_threshold=2.0)
    
    # Get latest regime
    latest = df_res.iloc[-1]
    
    console.print(Panel(f"[bold gold1]GOLDBEES Anomaly Scan Report[/bold gold1]\n"
                        f"As of: {latest['trade_date'].date()}\n"
                        f"Current Close: ₹{latest['close']:.2f}\n"
                        f"Regime: [bold]{latest['regime']}[/bold]\n"
                        f"Final Z-Score: {latest['final_z']:.2f} (Shock Intensity)", 
                        border_style="gold1"))

    # Display flagged anomalies in the last 60 days
    last_60 = df_flagged[df_flagged['trade_date'] > (pd.Timestamp.now() - pd.Timedelta(days=60))]
    
    if not last_60.empty:
        table = Table(title="Recent Anomalies (Last 60 Days)", header_style="bold magenta")
        table.add_column("Date", justify="center")
        table.add_column("Price", justify="right")
        table.add_column("Return %", justify="right")
        table.add_column("Final Z", justify="right")
        table.add_column("Regime", justify="left")
        
        for _, row in last_60.iterrows():
            table.add_row(
                str(row['trade_date'].date()),
                f"₹{row['close']:.2f}",
                f"{row['daily_return']:.2f}%",
                f"{row['final_z']:.2f}",
                row['regime']
            )
        console.print(table)
    else:
        console.print("[dim italic]No major anomalies detected in the last 60 days.[/dim italic]")

if __name__ == "__main__":
    run_gold_anomaly()
