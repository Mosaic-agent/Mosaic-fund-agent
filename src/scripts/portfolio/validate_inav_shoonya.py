import sys
import os
import argparse
from datetime import datetime

# Add project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from src.importer.fetchers.shoonya_fetcher import get_shoonya_api
from src.tools.inav_fetcher import get_etf_inav
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()

def main():
    parser = argparse.ArgumentParser(description="Validate ETF iNAV using live Shoonya market price")
    parser.add_argument("--symbol", default="GOLDBEES", help="ETF Symbol (default: GOLDBEES)")
    args = parser.parse_args()

    symbol = args.symbol.upper()
    console.print(f"[bold magenta]🔍 Validating Indicative NAV (iNAV) for {symbol}...[/bold magenta]\n")

    # 1. Fetch live iNAV from NSE API
    try:
        inav_data = get_etf_inav(symbol)
        if not inav_data or not inav_data.get("is_etf"):
            console.print(f"[bold red]Error:[/bold red] {symbol} is not recognized as an ETF or no iNAV data returned from NSE API.")
            sys.exit(1)
    except Exception as e:
        console.print(f"[bold red]Error fetching iNAV from NSE API:[/bold red] {e}")
        sys.exit(1)

    # 2. Get live quote from Shoonya
    api = get_shoonya_api()
    if not api:
        console.print("[bold red]Error:[/bold red] Could not authenticate with Shoonya. Please run the login script first:")
        print("  python src/scripts/portfolio/shoonya_login.py")
        sys.exit(1)

    # Search for token
    try:
        search_res = api.searchscrip(exchange="NSE", searchtext=symbol)
        if not search_res or search_res.get("stat") != "Ok" or not search_res.get("values"):
            console.print(f"[bold red]Error:[/bold red] Could not find Shoonya symbol for {symbol}")
            sys.exit(1)
        
        # Match exact symbol
        token = None
        for val in search_res["values"]:
            if val.get("tsym") == f"{symbol}-EQ":
                token = val.get("token")
                break
        
        if not token:
            token = search_res["values"][0].get("token")
            
        quote = api.get_quotes(exchange="NSE", token=token)
        if not quote or quote.get("stat") != "Ok":
            console.print(f"[bold red]Error:[/bold red] Failed to fetch quotes from Shoonya for token {token}")
            sys.exit(1)
            
    except Exception as e:
        console.print(f"[bold red]Shoonya API error:[/bold red] {e}")
        sys.exit(1)

    # 3. Parse values
    shoonya_ltp = float(quote.get("lp", 0))
    nse_inav = float(inav_data.get("inav", 0))
    nse_market_price = float(inav_data.get("market_price", 0))
    
    if nse_inav <= 0:
        console.print(f"[bold red]Error:[/bold red] Invalid iNAV returned: {nse_inav}")
        sys.exit(1)

    # Calculate real-time premium/discount
    shoonya_premium_pct = ((shoonya_ltp - nse_inav) / nse_inav) * 100
    nse_premium_pct = ((nse_market_price - nse_inav) / nse_inav) * 100 if nse_market_price > 0 else 0.0

    shoonya_label = "PREMIUM" if shoonya_premium_pct >= 0 else "DISCOUNT"
    nse_label = "PREMIUM" if nse_premium_pct >= 0 else "DISCOUNT"

    discrepancy = shoonya_ltp - nse_market_price
    discrepancy_pct = (discrepancy / nse_market_price) * 100 if nse_market_price > 0 else 0.0

    # 4. Display results in a table
    table = Table(title=f"📊 iNAV Validation Report: {symbol}", show_header=True, header_style="bold cyan")
    table.add_column("Metric", style="bold white")
    table.add_column("Value / Price", justify="right")
    table.add_column("Premium/Discount %", justify="right")
    table.add_column("Status / Source", justify="center")

    table.add_row(
        "Indicative NAV (iNAV)", 
        f"₹{nse_inav:.4f}", 
        "-", 
        "[green]NSE API[/green]"
    )
    table.add_row(
        "Shoonya Live Price (LTP)", 
        f"₹{shoonya_ltp:.2f}", 
        f"{shoonya_premium_pct:+.2f}%", 
        f"[yellow]Shoonya ({shoonya_label})[/yellow]"
    )
    table.add_row(
        "NSE API Market Price", 
        f"₹{nse_market_price:.2f}" if nse_market_price > 0 else "N/A", 
        f"{nse_premium_pct:+.2f}%" if nse_market_price > 0 else "-", 
        "[green]NSE API[/green]"
    )

    console.print(table)
    
    # Summary card
    summary_text = (
        f"• [bold]Shoonya LTP vs NSE iNAV:[/bold] {shoonya_premium_pct:+.4f}% ({shoonya_label.lower()})\n"
        f"• [bold]NSE Live price discrepancy:[/bold] {discrepancy:+.2f} INR ({discrepancy_pct:+.2f}% delta vs Shoonya LTP)\n"
        f"• [bold]Current bid/ask on Shoonya:[/bold] Bid: ₹{quote.get('bp1')} (Qty: {quote.get('bq1')}) | Ask: ₹{quote.get('sp1')} (Qty: {quote.get('sq1')})"
    )
    
    console.print(Panel(
        summary_text,
        title="[bold yellow]⚡ Real-Time Premium/Discount Analytics[/bold yellow]",
        border_style="yellow"
    ))

if __name__ == "__main__":
    main()
