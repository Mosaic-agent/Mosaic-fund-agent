import os
import sys
import argparse
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
# Ensure project root is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.tools.company_resolver import resolve_company_info
from src.scripts.market.ma_crossover_backtest import run_crossover_backtest
from src.db.pool import query_df, get_pool

# Imports for tool invocations
from src.tools.yahoo_finance import get_yahoo_finance_data, get_price_momentum
from src.tools.earnings_scraper import get_quarterly_results, get_shareholding_pattern
from src.tools.indian_equity_tools import get_stock_cashflow, get_db_price_summary, get_mf_holdings_for_stock
from src.tools.news_search import get_stock_news
from src.tools.newsapi_search import get_newsapi_stock_news
from src.tools.chart_tools import plot_price_chart, plot_shareholding_bar
from src.tools.market.equity import search_anomaly_events
from src.tools.market.correlation_tools import find_anomaly_correlations

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

def run_dcf_valuation_helper(sym, exc, cn):
    import yfinance as yf
    try:
        suffix = ".BO" if exc.upper() == "BSE" else ".NS"
        yf_sym = f"{sym}{suffix}"
        print(f"Running DCF valuation model for {yf_sym}...")
        ticker = yf.Ticker(yf_sym)
        info = ticker.info
        
        # 1. Fetch Key Valuation Inputs
        pool = get_pool()
        price_query = f"SELECT close FROM market_data.daily_prices WHERE symbol = '{sym}' ORDER BY trade_date DESC LIMIT 1"
        price_df = pool.query_df(price_query)
        latest_close = float(price_df['close'].iloc[0]) if not price_df.empty else float(info.get('currentPrice') or 0.0)
        
        if latest_close == 0.0:
            return "\n*DCF Valuation skipped: Could not resolve current close price*"
            
        mcap = float(info.get('marketCap') or 0.0)
        if mcap == 0.0:
            # Fallback to ClickHouse stock_valuation
            val_df = pool.query_df(f"SELECT market_cap FROM market_data.stock_valuation FINAL WHERE symbol = '{sym}'")
            mcap = float(val_df['market_cap'].iloc[0]) if not val_df.empty else 0.0
            
        if mcap == 0.0:
            return "\n*DCF Valuation skipped: Could not resolve market capitalization*"
            
        shares_outstanding = mcap / latest_close
        
        # Cash Flow & Balance Sheet Cash Cushion
        total_cash = float(info.get('totalCash') or 0.0)
        total_debt = float(info.get('totalDebt') or 0.0)
        net_cash = total_cash - total_debt
        
        # Base Free Cash Flow
        latest_fcf = float(info.get('freeCashflow') or 0.0)
        if latest_fcf == 0.0:
            # Fallback to cashflow table or ticker financials
            try:
                latest_fcf = float(ticker.cashflow.loc['Free Cash Flow'].iloc[0])
            except:
                latest_fcf = 0.0
                
        if latest_fcf <= 0.0:
            # If FCF is negative or zero, use a placeholder or skip
            return f"\n*DCF Valuation skipped: Free Cash Flow is negative or zero (FY FCF: ₹{latest_fcf/10000000:.2f} Cr)*"
            
        # 2. Run DCF Model
        scenarios = [
            {"name": "Conservative Case (High Risk/Discount)", "growth": 0.10, "wacc": 0.13, "g_terminal": 0.045},
            {"name": "Base Case (Standard Growth Recovery)", "growth": 0.14, "wacc": 0.115, "g_terminal": 0.05},
            {"name": "Aggressive Case (High Growth/Low WACC)", "growth": 0.18, "wacc": 0.10, "g_terminal": 0.055}
        ]
        
        results = []
        for sc in scenarios:
            name = sc["name"]
            g = sc["growth"]
            wacc = sc["wacc"]
            g_t = sc["g_terminal"]
            
            # Project Cash Flows for 5 years
            cf = []
            pv_cf = []
            current_fcf = latest_fcf
            
            for yr in range(1, 6):
                current_fcf = current_fcf * (1 + g)
                cf.append(current_fcf)
                pv = current_fcf / ((1 + wacc) ** yr)
                pv_cf.append(pv)
                
            # Terminal Value at Year 5
            terminal_value = cf[-1] * (1 + g_t) / (wacc - g_t)
            pv_terminal_value = terminal_value / ((1 + wacc) ** 5)
            
            # Enterprise Value & Equity Value (EV + Net Cash)
            enterprise_value = sum(pv_cf) + pv_terminal_value
            equity_value = enterprise_value + net_cash
            
            # Intrinsic value per share
            intrinsic_val = equity_value / shares_outstanding
            margin_of_safety = (1 - (latest_close / intrinsic_val)) * 100
            
            results.append({
                "Scenario": name,
                "Proj Growth": f"{g*100:.1f}%",
                "WACC (Discount)": f"{wacc*100:.1f}%",
                "Terminal Growth": f"{g_t*100:.1f}%",
                "PV of Cash Flows (Cr)": f"₹{sum(pv_cf)/10000000:.2f}",
                "PV of Terminal Val (Cr)": f"₹{pv_terminal_value/10000000:.2f}",
                "Intrinsic Value": f"₹{intrinsic_val:.2f}",
                "Margin of Safety": f"{margin_of_safety:.1f}%"
            })
            
        df_res = pd.DataFrame(results)
        
        # Build ASCII comparison bar chart
        base_iv = results[1]["Intrinsic Value"].replace("₹", "")
        cons_iv = results[0]["Intrinsic Value"].replace("₹", "")
        aggr_iv = results[2]["Intrinsic Value"].replace("₹", "")
        
        ascii_chart = f"""
```text
Intrinsic Value vs. Current LTP (₹{latest_close:.2f})
================================================================================
Conservative IV | [₹{cons_iv}]  {sc_bar(float(cons_iv), float(aggr_iv))} {cons_iv}
Base Case IV    | [₹{base_iv}]  {sc_bar(float(base_iv), float(aggr_iv))} {base_iv}
Current LTP     | [₹{latest_close:.2f}]  {sc_bar(float(latest_close), float(aggr_iv))} {latest_close:.2f}
Aggressive IV   | [₹{aggr_iv}]  {sc_bar(float(aggr_iv), float(aggr_iv))} {aggr_iv}
================================================================================
```
"""
        
        dcf_summary = f"""
### 💎 Intrinsic Value & 2-Stage DCF Valuation
* **Base Free Cash Flow:** ₹{latest_fcf/10000000:.2f} Cr
* **Net Cash Buffer:** ₹{net_cash/10000000:.2f} Cr (Cash: ₹{total_cash/10000000:.2f} Cr, Debt: ₹{total_debt/10000000:.2f} Cr)

{ascii_chart}

{make_markdown_table(df_res)}
"""
        return dcf_summary
        
    except Exception as e:
        return f"\n*DCF Valuation failed: {e}*"

def sc_bar(val, max_val):
    if max_val <= 0:
        return ""
    length = int(min(40, max(1, (val / max_val) * 40)))
    return "██" * (length // 2) + ("■" if length % 2 else "")

def run_deepdown_analysis(query_or_symbol: str, artifact_dir: str):
    print(f"Resolving company info for: {query_or_symbol}...")
    info = resolve_company_info(query_or_symbol)
    sym = info.get("symbol")
    if not sym:
        print(f"Error: Could not resolve symbol for {query_or_symbol}")
        sys.exit(1)
    
    cn = info.get("company_name") or query_or_symbol
    exc = info.get("exchange") or "NSE"
    inp = f"{sym}:{exc}"
    
    print(f"Resolved: {sym} ({cn}) on {exc}")
    
    # Check and refresh price data first
    from src.tools.agent_tools import check_and_refresh_symbol_data
    print("Checking database baseline freshness...")
    check_and_refresh_symbol_data.invoke({"symbol": sym, "auto_import": True})

    # Fetch 12 data sources in parallel via ThreadPoolExecutor (NO LLM, purely data)
    print("\nFetching data sources in parallel (no local LLM)...")
    
    tasks = {
        "price":        lambda: get_yahoo_finance_data.invoke({"input_str": inp}),
        "momentum":     lambda: get_price_momentum.invoke({"input_str": inp}),
        "quarterly":    lambda: get_quarterly_results.invoke({"input_str": inp}),
        "cashflow":     lambda: get_stock_cashflow.invoke({"input_str": inp}),
        "shareholding": lambda: f"{plot_shareholding_bar.invoke({'symbol': sym})}\n{get_shareholding_pattern.invoke({'symbol': sym})}",
        "mf_holdings":  lambda: get_mf_holdings_for_stock.invoke({"company_name_or_symbol": cn}),
        "db_price":     lambda: get_db_price_summary.invoke({"symbol": sym}),
        "news_gnews":   lambda: get_stock_news.invoke({"input_str": f"{sym}|{cn}"}),
        "news_api":     lambda: get_newsapi_stock_news.invoke({"input_str": f"{sym}|{cn}"}),
        "price_chart":  lambda: plot_price_chart.invoke({"symbol": sym, "days": 365}),
        "anomalies":    lambda: search_anomaly_events.invoke({"symbol": sym, "days": 365}),
        "correlations": lambda: find_anomaly_correlations.invoke({"symbol": sym, "lookback_days": 365}),
    }
    
    fetched_data = {}
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = {executor.submit(fn): name for name, fn in tasks.items()}
        for fut in futures:
            name = futures[fut]
            try:
                fetched_data[name] = str(fut.result())
                print(f" ✓ Completed: {name}")
            except Exception as e:
                fetched_data[name] = f"Error: {e}"
                print(f" ✗ Failed: {name} ({e})")
                
    # 2. Run MA Crossover Backtest (20d vs 50d SMA)
    print("\nRunning 20d vs 50d SMA Crossover Backtest...")
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
    
    # 3. Compute Intrinsic Value DCF Sizing
    print("\nRunning DCF Valuation Model...")
    dcf_valuation_summary = run_dcf_valuation_helper(sym, exc, cn)
    
    # 4. Fetch Sector/Industry Peers from ClickHouse
    print("\nQuerying Peer Valuations from ClickHouse...")
    peer_summary = ""
    try:
        query = """
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
            df_latest = df.sort_values('snapshot_date', ascending=False).groupby('symbol').first().reset_index()
            df_latest = df_latest.sort_values('market_cap_cr', ascending=False)
            peer_table = make_markdown_table(df_latest.head(15))
            peer_summary = f"""
### 📊 ClickHouse Stock Valuation Peer Matrix
{peer_table}
"""
    except Exception as e:
        peer_summary = f"\n*Peer Comparison failed: {e}*"
        
    # 5. Save Combined Report as Artifact
    print("\nSaving raw data report to artifacts...")
    
    base_report_data = "\n\n---\n\n".join(
        f"### {k}\n{fetched_data.get(k, '')}" for k in (
            "price", "momentum", "quarterly", "cashflow", "shareholding",
            "mf_holdings", "db_price", "news_gnews", "news_api",
            "price_chart", "anomalies", "correlations"
        ) if fetched_data.get(k)
    )
    
    report_content = f"""# Quant & Valuation Deep Down Data: {cn} ({sym})
**Generated on:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  

---

{base_report_data}

---

{crossover_summary}

---

{dcf_valuation_summary}

---

{peer_summary}
"""
    
    out_path = Path(artifact_dir) / f"{sym.lower()}_deep_down_analysis.md"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(report_content)
    print(f"\nSUCCESS: Data report saved successfully to: {out_path.absolute()}")
    print(f"LINK: [deep_down_analysis](file://{out_path.absolute()})")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run CLI-only stock quant deep-dive analysis workflow")
    parser.add_argument("query", type=str, help="Stock name, partial name, or NSE symbol")
    parser.add_argument("--artifact-dir", type=str, required=True, help="Directory to save report artifact")
    args = parser.parse_args()
    
    run_deepdown_analysis(args.query, args.artifact_dir)
