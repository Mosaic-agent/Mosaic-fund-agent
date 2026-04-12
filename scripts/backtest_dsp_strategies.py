import os
import sys
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from rich.console import Console
from rich.table import Table

sys.path.append(os.path.join(os.getcwd(), "src"))
try:
    from config.settings import settings
    import clickhouse_connect
except ImportError as e:
    print(f"Error importing project modules: {e}")
    sys.exit(1)

console = Console()
logging.basicConfig(level=logging.ERROR)

def fetch_data(client):
    """Fetch daily prices for necessary instruments from ClickHouse."""
    query = """
    SELECT trade_date, symbol, argMax(close, imported_at) as close
    FROM market_data.daily_prices
    WHERE symbol IN ('GC=F', 'SI=F', 'NIFTYBEES', 'LIQUIDBEES', 'SMALL250')
      AND trade_date >= '2022-06-01'
    GROUP BY trade_date, symbol
    ORDER BY trade_date ASC
    """
    df = client.query_df(query)
    
    missing_symbols = []
    found_symbols = df['symbol'].unique() if not df.empty else []
    for sym in ['GC=F', 'SI=F', 'NIFTYBEES', 'LIQUIDBEES', 'SMALL250']:
        if sym not in found_symbols:
            missing_symbols.append(sym)
            
    if missing_symbols:
        import yfinance as yf
        # map our internal symbols to yf symbols if needed. NIFTYBEES -> NIFTYBEES.NS
        yf_symbols = []
        for s in missing_symbols:
            if s == 'NIFTYBEES': yf_symbols.append('NIFTYBEES.NS')
            elif s == 'LIQUIDBEES': yf_symbols.append('LIQUIDBEES.NS')
            elif s == 'SMALL250': yf_symbols.append('SMALL250.NS')
            else: yf_symbols.append(s)
            
        data = yf.download(yf_symbols, start='2022-06-01', progress=False)['Close']
        if len(yf_symbols) == 1:
            data = pd.DataFrame({yf_symbols[0]: data})
        data = data.stack().reset_index()
        data.columns = ['trade_date', 'symbol', 'close']
        
        # map back
        mapping = {'NIFTYBEES.NS': 'NIFTYBEES', 'LIQUIDBEES.NS': 'LIQUIDBEES', 'SMALL250.NS': 'SMALL250'}
        data['symbol'] = data['symbol'].replace(mapping)
        
        df = pd.concat([df, data])

    df['trade_date'] = pd.to_datetime(df['trade_date']).dt.normalize()
    pivot_df = df.pivot(index='trade_date', columns='symbol', values='close')
    pivot_df = pivot_df.ffill()
    
    # Only dropna on the core symbols for Strategy A to maximize the date range
    core_symbols = [s for s in ['GC=F', 'SI=F', 'NIFTYBEES', 'LIQUIDBEES'] if s in pivot_df.columns]
    pivot_df = pivot_df.dropna(subset=core_symbols)
    return pivot_df

def calculate_metrics(returns_series, risk_free_rate=0.06):
    """Calculate annualized return, volatility, Sharpe, and Max Drawdown."""
    days = len(returns_series)
    if days == 0:
        return 0, 0, 0, 0
    
    cumulative = (1 + returns_series).cumprod()
    total_return = cumulative.iloc[-1] - 1
    
    years = days / 252
    cagr = (1 + total_return) ** (1 / years) - 1 if years > 0 else 0
    
    volatility = returns_series.std() * np.sqrt(252)
    
    sharpe = (cagr - risk_free_rate) / volatility if volatility > 0 else 0
    
    rolling_max = cumulative.cummax()
    drawdowns = (cumulative - rolling_max) / rolling_max
    max_dd = drawdowns.min()
    
    return cagr, volatility, sharpe, max_dd

def backtest_strategy_a(df):
    """
    Strategy A: GSR Mean-Reversion Scalper
    Baseline: 60% NIFTYBEES, 20% LIQUIDBEES, 20% GOLDBEES (GC=F proxy)
    Tactical: 
      If GSR < 60: 80% NIFTYBEES, 20% LIQUIDBEES, 0% GC=F
      If GSR > 85: 40% NIFTYBEES, 20% LIQUIDBEES, 40% GC=F
    """
    if 'GC=F' not in df.columns or 'SI=F' not in df.columns:
         return pd.Series(dtype=float), pd.Series(dtype=float)

    df['GSR'] = df['GC=F'] / df['SI=F']
    
    # Calculate daily returns only for the assets we trade
    trade_assets = [c for c in ['NIFTYBEES', 'LIQUIDBEES', 'GC=F'] if c in df.columns]
    returns = df[trade_assets].pct_change().dropna()
    
    # Align GSR (use previous day's GSR to determine today's weights to avoid look-ahead)
    gsr_lagged = df['GSR'].shift(1).dropna()
    
    # Align dates
    common_dates = returns.index.intersection(gsr_lagged.index)
    returns = returns.loc[common_dates]
    gsr_lagged = gsr_lagged.loc[common_dates]
    
    strat_returns = []
    bench_returns = []
    
    for date in common_dates:
        gsr = gsr_lagged.loc[date]
        
        # Benchmark weights
        w_eq_b = 0.60
        w_bd_b = 0.20
        w_au_b = 0.20
        
        # Strategy weights
        if gsr < 60:
            w_eq_s = 0.80
            w_bd_s = 0.20
            w_au_s = 0.00
        elif gsr > 85:
            w_eq_s = 0.40
            w_bd_s = 0.20
            w_au_s = 0.40
        else:
            w_eq_s = 0.60
            w_bd_s = 0.20
            w_au_s = 0.20
            
        r_eq = returns.loc[date, 'NIFTYBEES'] if 'NIFTYBEES' in returns.columns else 0
        r_bd = returns.loc[date, 'LIQUIDBEES'] if 'LIQUIDBEES' in returns.columns else 0
        r_au = returns.loc[date, 'GC=F'] if 'GC=F' in returns.columns else 0
        
        strat_ret = (w_eq_s * r_eq) + (w_bd_s * r_bd) + (w_au_s * r_au)
        bench_ret = (w_eq_b * r_eq) + (w_bd_b * r_bd) + (w_au_b * r_au)
        
        strat_returns.append(strat_ret)
        bench_returns.append(bench_ret)
        
    return pd.Series(strat_returns, index=common_dates), pd.Series(bench_returns, index=common_dates)


def run_backtest():
    console.print("[bold cyan]Running Deep Quantitative Backtests (DSP RV Strategies)[/bold cyan]")
    
    try:
        client = clickhouse_connect.get_client(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            username=settings.clickhouse_user,
            password=settings.clickhouse_password,
            database=settings.clickhouse_database
        )
    except Exception as e:
        console.print(f"[red]Failed to connect to ClickHouse: {e}[/red]")
        return

    console.print("Fetching daily price data...")
    df = fetch_data(client)
    
    if df.empty:
        console.print("[red]No price data available for backtest.[/red]")
        return
        
    console.print(f"Data loaded from {df.index.min().date()} to {df.index.max().date()}")

    # Strategy A
    strat_a_ret, bench_a_ret = backtest_strategy_a(df)
    
    # Calculate Metrics
    sa_cagr, sa_vol, sa_sharpe, sa_dd = calculate_metrics(strat_a_ret)
    ba_cagr, ba_vol, ba_sharpe, ba_dd = calculate_metrics(bench_a_ret)

    table = Table(title="Strategy A: GSR Mean-Reversion Scalper vs Benchmark (60/20/20)", show_header=True, header_style="bold magenta")
    table.add_column("Metric", style="dim")
    table.add_column("Strategy A (GSR Pivot)", justify="right")
    table.add_column("Benchmark (Static)", justify="right")
    table.add_column("Outperformance", justify="right")

    table.add_row("CAGR", f"{sa_cagr*100:.2f}%", f"{ba_cagr*100:.2f}%", f"{(sa_cagr - ba_cagr)*100:+.2f}%")
    table.add_row("Volatility", f"{sa_vol*100:.2f}%", f"{ba_vol*100:.2f}%", f"{(sa_vol - ba_vol)*100:+.2f}%")
    table.add_row("Sharpe Ratio", f"{sa_sharpe:.2f}", f"{ba_sharpe:.2f}", f"{(sa_sharpe - ba_sharpe):+.2f}")
    table.add_row("Max Drawdown", f"{sa_dd*100:.2f}%", f"{ba_dd*100:.2f}%", f"{(sa_dd - ba_dd)*100:+.2f}%")

    console.print(table)
    
    if sa_cagr > ba_cagr and sa_sharpe > ba_sharpe:
        console.print("\n[bold green]Conclusion: Strategy A successfully generates alpha and improves risk-adjusted returns by dynamically pivoting based on GSR extremes.[/bold green]")
    else:
        console.print("\n[yellow]Conclusion: Strategy A did not significantly outperform the static benchmark in this specific time window.[/yellow]")

if __name__ == "__main__":
    run_backtest()