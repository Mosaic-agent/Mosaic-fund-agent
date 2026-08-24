from __future__ import annotations
import argparse, sys
from pathlib import Path
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(ROOT_DIR))

from src.db.pool import get_pool
from src.data_importer.fetchers.bulk_block_deals_fetcher import fetch_nse_bulk_and_block_deals

console = Console(color_system='truecolor', force_terminal=True)

def import_bulk_deals(period: str = '1Y') -> None:
    console.print(Panel(
        f'[bold cyan]📥 Fetching NSE Bulk & Block Deals (Period: {period})...[/bold cyan]',
        title='[bold yellow]🏛️ NSE BULK & BLOCK DEALS IMPORTER[/bold yellow]',
        border_style='yellow'
    ))

    records = fetch_nse_bulk_and_block_deals(period=period)
    if not records:
        console.print('[bold red]❌ No bulk/block deal records fetched.[/bold red]')
        return

    console.print(f'[bold green]✓ Fetched {len(records):,} records from NSE.[/bold green] Ingesting into ClickHouse...')
    pool = get_pool()
    client = pool.get_client()

    cols = ['deal_date', 'deal_type', 'symbol', 'security_name', 'client_name', 'buy_sell', 'quantity', 'trade_price', 'value_cr', 'remarks']
    data = [
        [
            r['deal_date'], r['deal_type'], r['symbol'], r['security_name'],
            r['client_name'], r['buy_sell'], r['quantity'], r['trade_price'],
            r['value_cr'], r['remarks']
        ]
        for r in records
    ]

    client.insert('market_data.bulk_block_deals', data, column_names=cols)
    console.print(f'[bold green]✅ Successfully inserted {len(data):,} records into market_data.bulk_block_deals![/bold green]\n')

    res_stats = client.query("""SELECT deal_type, count(), round(sum(value_cr), 2) as total_val_cr, min(deal_date), max(deal_date) FROM market_data.bulk_block_deals FINAL GROUP BY deal_type""")
    t_stats = Table(title='📊 BULK & BLOCK DEALS DATABASE SUMMARY', title_style='bold cyan')
    t_stats.add_column('Deal Type', style='bold white')
    t_stats.add_column('Total Deals', justify='right', style='cyan')
    t_stats.add_column('Total Turnover (₹ Cr)', justify='right', style='green')
    t_stats.add_column('From Date', justify='center', style='yellow')
    t_stats.add_column('To Date', justify='center', style='yellow')

    for row in res_stats.result_rows:
        t_stats.add_row(str(row[0]), f'{row[1]:,}', f'₹{row[2]:,.2f} Cr', str(row[3]), str(row[4]))

    console.print(t_stats)

    q_top = """SELECT deal_date, deal_type, symbol, client_name, buy_sell, quantity, trade_price, value_cr FROM market_data.bulk_block_deals FINAL WHERE deal_date >= today() - 14 ORDER BY value_cr DESC LIMIT 10"""
    df_top = client.query_df(q_top)

    if not df_top.empty:
        t_top = Table(title='🚨 TOP 10 LARGEST RECENT TRANSACTIONS (LAST 14 DAYS)', title_style='bold magenta')
        t_top.add_column('Date', style='bold white')
        t_top.add_column('Type', style='yellow')
        t_top.add_column('Symbol', style='bold cyan')
        t_top.add_column('Client / Institutional Entity', style='white')
        t_top.add_column('Side', style='bold')
        t_top.add_column('Qty', justify='right', style='magenta')
        t_top.add_column('Price (₹)', justify='right', style='white')
        t_top.add_column('Value (₹ Cr)', justify='right', style='bold green')

        for _, r in df_top.iterrows():
            side_style = '[bold green]BUY[/bold green]' if r['buy_sell'] == 'BUY' else '[bold red]SELL[/bold red]'
            t_top.add_row(
                str(r['deal_date']), str(r['deal_type']), str(r['symbol']),
                str(r['client_name'])[:40], side_style,
                f"{r['quantity']:,.0f}", f"₹{r['trade_price']:,.2f}", f"₹{r['value_cr']:,.2f} Cr"
            )
        console.print(t_top)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Import NSE Bulk and Block Deals into ClickHouse')
    parser.add_argument('--period', type=str, default='1Y', help='Lookback period: 1D, 1W, 1M, 6M, 1Y')
    args = parser.parse_args()
    import_bulk_deals(period=args.period)