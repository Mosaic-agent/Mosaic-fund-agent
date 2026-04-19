
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
    {"tradingsymbol":"RELIANCE","exchange":"NSE","quantity":10,"average_price":2500.0,"last_price":2900.0},
    {"tradingsymbol":"TCS","exchange":"NSE","quantity":5,"average_price":3200.0,"last_price":3800.0}
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
