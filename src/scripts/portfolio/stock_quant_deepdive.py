import os
import sys
import argparse
from pathlib import Path
from datetime import datetime

# Ensure project root is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.workflows.india_equity import run as run_equity_workflow
from src.tools.company_resolver import resolve_company_info
from src.scripts.market.ma_crossover_backtest import run_crossover_backtest
from src.db.pool import query_df

def make_markdown_table(df):
    headers = list(df.columns)
    lines = []
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
    for _, row in df.iterrows():
        row_str = []
        for val in row:
            if isinstance(val, float):
                row_str.append(f"{val:.2f}")
            elif val is None:
                row_str.append("N/A")
            else:
                row_str.append(str(val))
        lines.append("| " + " | ".join(row_str) + " |")
    return "\n".join(lines)

def run_deepdown_analysis(query_or_symbol: str, artifact_dir: str):
    print(f"Resolving company info for: {query_or_symbol}...")
    info = resolve_company_info(query_or_symbol)
    sym = info.get("symbol")
    if not sym:
        print(f"Error: Could not resolve symbol for {query_or_symbol}")
        sys.exit(1)
    
    print(f"Canonical Symbol: {sym} ({info.get('company_name')}) on {info.get('exchange')}")
    
    # 1. Run the main 8-section research note workflow
    print("\n[Phase 1] Running Indian Equity Research Workflow...")
    base_report = run_equity_workflow(query_or_symbol)
    
    # 2. Run MA Crossover Backtest (20d vs 50d SMA)
    print("\n[Phase 2] Running 20d vs 50d SMA Crossover Backtest...")
    try:
        metrics = run_crossover_backtest(sym, fast=20, slow=50, ma_type="sma", plot=True)
        crossover_summary = f"""
### 📊 Moving Average Crossover Backtest (20d vs 50d SMA)
* **Strategy Return:** {metrics.get('strategy_return', 0.0):.2f}% (CAGR: {metrics.get('strategy_cagr', 0.0):.2f}%)
* **Benchmark Return:** {metrics.get('benchmark_return', 0.0):.2f}% (CAGR: {metrics.get('benchmark_cagr', 0.0):.2f}%)
* **Win Rate:** {metrics.get('win_rate', 0.0)*100:.1f}% ({metrics.get('total_trades', 0)} trades)
* **Sharpe Ratio:** {metrics.get('sharpe_ratio', 0.0):.2f}
* **Strategy Max Drawdown:** {metrics.get('max_drawdown', 0.0):.2f}%
* **Benchmark Max Drawdown:** {metrics.get('benchmark_max_dd', 0.0):.2f}%
"""
    except Exception as e:
        crossover_summary = f"\n*Crossover Backtest failed: {e}*"
    
    # 3. Fetch Sector/Industry Peers from ClickHouse
    print("\n[Phase 3] Querying Peer Valuations from ClickHouse...")
    peer_summary = ""
    try:
        # Get peers from stock_valuation that are near this stock
        # We can query all records from stock_valuation to compare
        query = f"""
        SELECT 
            symbol,
            snapshot_date,
            market_cap / 10000000 AS market_cap_cr,
            trailing_pe,
            forward_pe,
            price_to_book,
            price_to_sales,
            return_on_equity * 100 AS roe_pct,
            profit_margin * 100 AS net_margin_pct,
            revenue_growth * 100 AS rev_growth_pct,
            earnings_growth * 100 AS eps_growth_pct,
            recommendation
        FROM market_data.stock_valuation FINAL
        ORDER BY market_cap_cr DESC
        """
        df = query_df(query)
        if not df.empty:
            # We want to include our symbol and top peers
            # Let's filter top 10 stocks by market cap in the same size range or key stocks
            df_latest = df.sort_values('snapshot_date', ascending=False).groupby('symbol').first().reset_index()
            # If our symbol is not in the DB valuation table yet, we can append it from yfinance data
            if sym not in df_latest['symbol'].values:
                # Add a dummy row or fetch from yfinance
                pass
            
            # Filter to show some comparison stocks
            df_latest = df_latest.sort_values('market_cap_cr', ascending=False)
            peer_table = make_markdown_table(df_latest.head(15))
            peer_summary = f"""
### 📊 ClickHouse Stock Valuation Peer Matrix
Here are the latest valuation snapshots of major listed Indian stocks in our database:

{peer_table}
"""
    except Exception as e:
        peer_summary = f"\n*Peer Comparison failed: {e}*"
        
    # 4. Save Combined Report as Artifact
    print("\n[Phase 4] Saving combined report to artifacts...")
    report_content = f"""# Quant & Valuation Deep Dive: {info.get('company_name')} ({sym})
**Generated on:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  

---

{base_report}

---

{crossover_summary}

---

{peer_summary}
"""
    
    out_path = Path(artifact_dir) / f"{sym.lower()}_deep_down_analysis.md"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(report_content)
    print(f"\nSUCCESS: Research report saved successfully to: {out_path.absolute()}")
    print(f"LINK: [deep_down_analysis](file://{out_path.absolute()})")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run complete stock quant deep-dive analysis workflow")
    parser.add_argument("query", type=str, help="Stock name, partial name, or NSE symbol")
    parser.add_argument("--artifact-dir", type=str, required=True, help="Directory to save report artifact")
    args = parser.parse_args()
    
    run_deepdown_analysis(args.query, args.artifact_dir)
