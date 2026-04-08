
import sys
from pathlib import Path
from rich.console import Console
from rich.panel import Panel

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from src.analyzers.asset_analyzer import analyze_holding
from src.analyzers.portfolio_analyzer import build_portfolio_report
from src.formatters.output import print_report_to_console
from src.models.portfolio import Portfolio, Holding

# This mock uses the data we already retrieved to avoid further 400 errors
RAW_HOLDINGS = [
    {"tradingsymbol":"ADVENZYMES","exchange":"NSE","quantity":1274,"average_price":368.395682,"last_price":296},
    {"tradingsymbol":"ALKYLAMINE","exchange":"NSE","quantity":12,"average_price":2405.458333,"last_price":1382.7},
    {"tradingsymbol":"BAJFINANCE","exchange":"BSE","quantity":964,"average_price":822.940819,"last_price":916.35},
    {"tradingsymbol":"BECTORFOOD","exchange":"BSE","quantity":215,"average_price":267.702093,"last_price":190.25},
    {"tradingsymbol":"CHOLAFIN","exchange":"NSE","quantity":225,"average_price":1325.105555,"last_price":1541},
    {"tradingsymbol":"EMBASSY","exchange":"BSE","quantity":4,"average_price":423.265,"last_price":436.84},
    {"tradingsymbol":"GARFIBRES","exchange":"NSE","quantity":982,"average_price":798.732281,"last_price":612.25},
    {"tradingsymbol":"GLAND","exchange":"NSE","quantity":74,"average_price":2051.155405,"last_price":1727.9},
    {"tradingsymbol":"GODIGIT","exchange":"BSE","quantity":1716,"average_price":368.287441,"last_price":326.65},
    {"tradingsymbol":"GOLDBEES","exchange":"NSE","quantity":36861,"average_price":77.376333,"last_price":125.43},
    {"tradingsymbol":"GOLDCASE","exchange":"NSE","quantity":23332,"average_price":21.352006,"last_price":23.84},
    {"tradingsymbol":"GRWRHITECH","exchange":"NSE","quantity":257,"average_price":3303.279377,"last_price":3739.6},
    {"tradingsymbol":"HNGSNGBEES","exchange":"NSE","quantity":2611,"average_price":518.43018,"last_price":523.72},
    {"tradingsymbol":"ICICIPRULI","exchange":"BSE","quantity":4,"average_price":697.0125,"last_price":539.85},
    {"tradingsymbol":"INDIGRID-IV","exchange":"NSE","quantity":4935,"average_price":144.398865,"last_price":168.09},
    {"tradingsymbol":"MAFANG","exchange":"NSE","quantity":4849,"average_price":154.531313,"last_price":155.94},
    {"tradingsymbol":"MON100","exchange":"BSE","quantity":1,"average_price":175.72,"last_price":256.36},
    {"tradingsymbol":"NEWGEN","exchange":"NSE","quantity":30,"average_price":645.485,"last_price":459.45},
    {"tradingsymbol":"NIFTYBEES","exchange":"NSE","quantity":831,"average_price":276.131432,"last_price":271.05},
    {"tradingsymbol":"NUVAMA","exchange":"NSE","quantity":70,"average_price":1467.085714,"last_price":1283},
    {"tradingsymbol":"PGINVIT-IV","exchange":"NSE","quantity":155,"average_price":95.309677,"last_price":92.11},
    {"tradingsymbol":"PSUBNKBEES","exchange":"NSE","quantity":1061,"average_price":95.686475,"last_price":96.75},
    {"tradingsymbol":"SETFNIF50","exchange":"NSE","quantity":291,"average_price":266.53,"last_price":256.7},
    {"tradingsymbol":"SILVERBEES","exchange":"NSE","quantity":3238,"average_price":157.482483,"last_price":231.49},
    {"tradingsymbol":"SILVERCASE","exchange":"BSE","quantity":1,"average_price":20.53,"last_price":24.52},
    {"tradingsymbol":"SOLARA","exchange":"NSE","quantity":52,"average_price":635.689423,"last_price":475.45},
    {"tradingsymbol":"TEJASNET","exchange":"NSE","quantity":181,"average_price":691.273756,"last_price":438.6},
    {"tradingsymbol":"TTKPRESTIG","exchange":"NSE","quantity":1,"average_price":597.3,"last_price":485.2},
    {"tradingsymbol":"VAIBHAVGBL","exchange":"NSE","quantity":2550,"average_price":318.594294,"last_price":206.82},
    {"tradingsymbol":"WELCORP","exchange":"NSE","quantity":1,"average_price":968.35,"last_price":937.5},
    {"tradingsymbol":"WELSPUNLIV","exchange":"NSE","quantity":1982,"average_price":171.043405,"last_price":123.62},
    {"tradingsymbol":"ZENTEC","exchange":"NSE","quantity":14,"average_price":1451.128571,"last_price":1453.1}
]

def main():
    console = Console()
    
    # Convert raw to Holding objects
    holdings = [Holding(**h) for h in RAW_HOLDINGS]
    
    # Enrich each holding (rule-based scoring)
    # Must happen before Portfolio so equity reflects live prices from Yahoo Finance.
    analysis_list = []
    with console.status("[cyan]Quant Analysis: Fetching market data & iNAV…[/cyan]"):
        for h in holdings:
            try:
                # Fetches live Yahoo Finance price + iNAV for ETFs
                analysis = analyze_holding(h, use_llm_scoring=False)
                analysis_list.append(analysis)
            except Exception as e:
                console.print(f"[dim yellow]Warning: Skipping {h.tradingsymbol} due to error: {e}[/dim yellow]")
                continue
    
    # Build Portfolio model using live current_value from enriched analysis.
    # Falling back to RAW_HOLDINGS last_price only for holdings that failed enrichment.
    enriched_symbols = {a.symbol for a in analysis_list}
    live_equity = sum(
        a.current_value for a in analysis_list
    ) + sum(
        h.current_value for h in holdings if h.tradingsymbol not in enriched_symbols
    )
    portfolio = Portfolio(
        holdings=holdings,
        positions=[],
        equity=live_equity,
        available_margin=0.0,
    )
    
    # Build and display report.
    # comex_signals={} — COMEX pre-market signals not fetched in this offline run.
    report = build_portfolio_report(portfolio, analysis_list, use_llm_scoring=False, comex_signals={})
    
    console.print(Panel("[bold green]Live Portfolio Quant Analysis — Mosaic Fund Agent[/bold green]", border_style="green"))
    print_report_to_console(report.model_dump(), console=console)

if __name__ == "__main__":
    main()
