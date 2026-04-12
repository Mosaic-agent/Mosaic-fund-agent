import os
import sys
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from rich.console import Console
from rich.table import Table
from rich.progress import Progress

# Add src to path
sys.path.append(os.path.join(os.getcwd(), "src"))

try:
    from config.settings import settings
    import clickhouse_connect
except ImportError as e:
    print(f"Error importing project modules: {e}")
    sys.exit(1)

console = Console()
logging.basicConfig(level=logging.ERROR)

def _clamp01(value: float, low: float, high: float) -> float:
    """Linear interpolation between [low, high] -> [1.0, 0.0], clamped to [0, 1]."""
    if high == low: return 0.5
    raw = (high - value) / (high - low)
    return float(np.clip(raw, 0.0, 1.0))

def get_dsp_allocation_history(client):
    """Fetch monthly asset allocation for DSP_MULTI_ASSET."""
    query = """
    SELECT
        as_of_month,
        sumIf(pct_of_nav, asset_type = 'equity') AS equity_pct,
        sumIf(pct_of_nav, asset_type = 'gold') AS gold_pct,
        sumIf(pct_of_nav, asset_type = 'bond') AS bond_pct,
        sumIf(pct_of_nav, asset_type = 'silver') AS silver_pct
    FROM market_data.mf_holdings FINAL
    WHERE fund_name = 'DSP_MULTI_ASSET'
    GROUP BY as_of_month
    ORDER BY as_of_month ASC
    """
    df = client.query_df(query)
    df['as_of_month'] = pd.to_datetime(df['as_of_month'])
    return df

def get_historical_quant_data(client, dates):
    """Reconstruct quant signals for each date in the list."""
    results = []
    
    # 1. Fetch all DXY, TNX, Gold and Silver history
    dxy_tnx_df = client.query_df("""
        SELECT trade_date, symbol, argMax(close, imported_at) as close
        FROM market_data.daily_prices
        WHERE symbol IN ('DX-Y.NYB', '^TNX', 'GC=F', 'SI=F')
        GROUP BY trade_date, symbol
    """)
    if dxy_tnx_df.empty:
        # Fallback to yfinance if not in ClickHouse
        import yfinance as yf
        min_date = (min(dates) - timedelta(days=30)).strftime('%Y-%m-%d')
        max_date = (max(dates) + timedelta(days=5)).strftime('%Y-%m-%d')
        tickers = ["DX-Y.NYB", "^TNX", "GC=F", "SI=F"]
        data = yf.download(tickers, start=min_date, end=max_date, progress=False)['Close']
        dxy_tnx_df = data.stack().reset_index()
        dxy_tnx_df.columns = ['trade_date', 'symbol', 'close']
    
    dxy_tnx_df['trade_date'] = pd.to_datetime(dxy_tnx_df['trade_date']).dt.normalize()
    dxy_prices = dxy_tnx_df[dxy_tnx_df['symbol'] == 'DX-Y.NYB'].set_index('trade_date')['close'].sort_index()
    tnx_prices = dxy_tnx_df[dxy_tnx_df['symbol'] == '^TNX'].set_index('trade_date')['close'].sort_index()
    gc_prices = dxy_tnx_df[dxy_tnx_df['symbol'] == 'GC=F'].set_index('trade_date')['close'].sort_index()
    si_prices = dxy_tnx_df[dxy_tnx_df['symbol'] == 'SI=F'].set_index('trade_date')['close'].sort_index()

    # 2. Fetch COT Gold history
    cot_df = client.query_df("""
        SELECT report_date, mm_net, open_interest
        FROM market_data.cot_gold
    """)
    if not cot_df.empty:
        cot_df['report_date'] = pd.to_datetime(cot_df['report_date']).dt.normalize()
        cot_df['cot_pct'] = (cot_df['mm_net'] / cot_df['open_interest']) * 100
        cot_df = cot_df.set_index('report_date').sort_index()

    # 3. Fetch iNAV GOLDBEES history
    inav_df = client.query_df("""
        SELECT toDate(snapshot_at) as trade_date, argMax(premium_discount_pct, snapshot_at) as premium_discount_pct
        FROM market_data.inav_snapshots
        WHERE symbol = 'GOLDBEES'
        GROUP BY trade_date
    """)
    if not inav_df.empty:
        inav_df['trade_date'] = pd.to_datetime(inav_df['trade_date']).dt.normalize()
        inav_df = inav_df.set_index('trade_date').sort_index()

    # 4. Fetch ML Predictions history
    ml_df = client.query_df("""
        SELECT as_of, argMax(expected_return_pct, created_at) as expected_return_pct
        FROM market_data.ml_predictions
        GROUP BY as_of
    """)
    if not ml_df.empty:
        ml_df['as_of'] = pd.to_datetime(ml_df['as_of']).dt.normalize()
        ml_df = ml_df.set_index('as_of').sort_index()

    for dt in dates:
        dt = pd.to_datetime(dt).normalize()
        
        # Macro Scores
        dxy_val = dxy_prices.asof(dt) if not dxy_prices.empty else None
        tnx_val = tnx_prices.asof(dt) if not tnx_prices.empty else None
        gc_val = gc_prices.asof(dt) if not gc_prices.empty else None
        si_val = si_prices.asof(dt) if not si_prices.empty else None
        gsr_val = gc_val / si_val if (gc_val and si_val) else None
        
        # Real Yield Delta 5D
        tnx_hist = tnx_prices[tnx_prices.index <= dt].tail(6)
        ry_delta5 = None
        if len(tnx_hist) == 6:
            ry_delta5 = tnx_hist.iloc[-1] - tnx_hist.iloc[0]
        
        macro_score = None
        if dxy_val is not None and ry_delta5 is not None:
            dxy_s = _clamp01(dxy_val, 100.0, 110.0) * 100
            yield_s = _clamp01(ry_delta5, -0.10, +0.10) * 100
            macro_score = (dxy_s + yield_s) / 2
        
        # Flows Score
        cot_val = cot_df.asof(dt)['cot_pct'] if not cot_df.empty else None
        flows_score = _clamp01(cot_val, 20.0, 35.0) * 100 if cot_val is not None else None
        
        # Valuation Score
        inav_val = inav_df.asof(dt)['premium_discount_pct'] if not inav_df.empty else None
        valuation_score = _clamp01(inav_val, -0.5, 0.5) * 100 if inav_val is not None else None
        
        # Momentum Score
        ml_val = ml_df.asof(dt)['expected_return_pct'] if not ml_df.empty else None
        momentum_score = _clamp01(ml_val, -1.0, 1.0) * 100 if ml_val is not None else None
        
        # Composite Score (re-weighted)
        scores = [s for s in [macro_score, flows_score, valuation_score, momentum_score] if s is not None]
        comp_score = np.mean(scores) if scores else None
        
        results.append({
            'as_of_month': dt,
            'dxy': dxy_val,
            'real_yield_delta5': ry_delta5,
            'gsr': gsr_val,
            'cot_pct': cot_val,
            'inav_disc': inav_val,
            'ml_pred': ml_val,
            'macro_score': macro_score,
            'flows_score': flows_score,
            'valuation_score': valuation_score,
            'momentum_score': momentum_score,
            'composite_score': comp_score
        })
    
    return pd.DataFrame(results)

def run_analysis():
    console.print("[bold cyan]DSP Multi Asset Quant Strategy Analyzer[/bold cyan]")
    
    client = clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_database
    )
    
    # 1. Get DSP History
    dsp_df = get_dsp_allocation_history(client)
    if dsp_df.empty:
        console.print("[red]No DSP allocation history found in ClickHouse.[/red]")
        return
    
    # 2. Get Quant History
    dates = dsp_df['as_of_month'].tolist()
    with Progress() as progress:
        task = progress.add_task("[green]Reconstructing Quant Signals...", total=len(dates))
        quant_df = get_historical_quant_data(client, dates)
        progress.update(task, advance=len(dates))
    
    # 3. Join
    final_df = dsp_df.merge(quant_df, on='as_of_month', how='inner')
    
    # Calculate Deltas
    final_df['delta_gold'] = final_df['gold_pct'].diff()
    final_df['delta_equity'] = final_df['equity_pct'].diff()
    final_df['delta_dxy'] = final_df['dxy'].diff()
    final_df['delta_yield'] = final_df['real_yield_delta5'].diff()
    
    # 4. Correlation Analysis
    console.print("\n[bold]Correlation: Delta DSP Gold % vs Quant Signals[/bold]")
    corr_table = Table(show_header=True, header_style="bold magenta")
    corr_table.add_column("Signal", style="dim")
    corr_table.add_column("Correlation (R)", justify="right")
    corr_table.add_column("Relationship Strength")
    
    signals = {
        'Composite Quant Score': 'composite_score',
        'Macro Score (DXY/Yield)': 'macro_score',
        'DXY Level': 'dxy',
        'US Real Yield Δ': 'real_yield_delta5',
        'Flows (COT %)': 'cot_pct',
        'Valuation (iNAV)': 'inav_disc',
        'ML Prediction': 'ml_pred',
        'Gold-Silver Ratio (GSR)': 'gsr'
    }
    
    results = []
    for label, col in signals.items():
        if col in final_df.columns and not final_df[col].dropna().empty:
            valid_df = final_df[['delta_gold', col]].dropna()
            if len(valid_df) > 5:
                r = valid_df.corr().iloc[0, 1]
                results.append((label, r))
    
    # Sort by absolute correlation
    results.sort(key=lambda x: abs(x[1]) if not np.isnan(x[1]) else 0, reverse=True)
    
    for label, r in results:
        strength = "N/A"
        if not np.isnan(r):
            abs_r = abs(r)
            if abs_r > 0.7: strength = "[bold green]Very Strong[/bold green]"
            elif abs_r > 0.4: strength = "[green]Moderate[/green]"
            elif abs_r > 0.2: strength = "[yellow]Weak[/yellow]"
            else: strength = "Negligible"
            
            corr_table.add_row(label, f"{r:.3f}", strength)
        else:
            corr_table.add_row(label, "N/A", "N/A")
            
    console.print(corr_table)

    # 5. Identify Rules (Reverse Engineering)
    console.print("\n[bold green]Identified DSP Trading Rules (Reverse Engineered)[/bold green]")
    rules_found = False
    
    # Rule 1: Gold vs DXY/Yields
    gold_macro_corr = final_df[['delta_gold', 'macro_score']].dropna().corr().iloc[0,1]
    if abs(gold_macro_corr) > 0.3:
        direction = "increases" if gold_macro_corr > 0 else "decreases"
        console.print(f"• [bold]Macro Driven:[/bold] DSP {direction} Gold exposure when our Macro Score (DXY/Yield) improves (R={gold_macro_corr:.2f}).")
        rules_found = True
        
    # Rule 2: The "Bearish Pivot" Threshold
    # Look for the largest drop in Gold
    if not final_df['delta_gold'].dropna().empty:
        pivot_idx = final_df['delta_gold'].idxmin()
        pivot_row = final_df.loc[pivot_idx]
        if pivot_row['delta_gold'] < -5:
            console.print(f"• [bold]High-Conviction Pivot:[/bold] Largest Gold reduction ({pivot_row['delta_gold']:.1f}%) occurred on {pivot_row['as_of_month'].strftime('%Y-%m')}.")
            console.print(f"  - Quant Status then: DXY={pivot_row['dxy']:.2f}, MacroScore={pivot_row['macro_score']:.1f}, GSR={pivot_row['gsr']:.2f}")
            rules_found = True

    if not rules_found:
        console.print("No statistically significant quantitative rules identified yet. Need more data points.")

    # 6. Full Data Table (Summary)
    console.print("\n[bold]Historical Allocation vs Quant Scores[/bold]")
    summary_table = Table(show_header=True)
    summary_table.add_column("Month")
    summary_table.add_column("Equity %", justify="right")
    summary_table.add_column("Gold %", justify="right")
    summary_table.add_column("GSR", justify="right")
    summary_table.add_column("Macro Score", justify="right")
    
    for _, row in final_df.tail(12).iterrows():
        summary_table.add_row(
            row['as_of_month'].strftime('%Y-%m'),
            f"{row['equity_pct']:.1f}",
            f"{row['gold_pct']:.1f}",
            f"{row['gsr']:.2f}" if not np.isnan(row['gsr']) else "N/A",
            f"{row['macro_score']:.1f}" if not np.isnan(row['macro_score']) else "N/A"
        )
    console.print(summary_table)

if __name__ == "__main__":
    run_analysis()
