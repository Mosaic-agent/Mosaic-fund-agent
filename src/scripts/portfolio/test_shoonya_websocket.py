import sys
import os
import time
import threading
from datetime import datetime

# Add project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from src.importer.fetchers.shoonya_fetcher import get_shoonya_api
from rich.console import Console

console = Console()

# Global variables for synchronization
ticks_received = 0
max_ticks = 5
ticks_event = threading.Event()

def event_handler_feed_update(tick_data):
    global ticks_received, max_ticks, ticks_event
    
    # Check if we have valid tick data
    if not tick_data:
        return
        
    ticks_received += 1
    
    # Pretty print the tick data
    ltp = tick_data.get("lp")  # Last traded price
    volume = tick_data.get("v")  # Volume
    ltt = tick_data.get("ltt")  # Last traded time
    
    # Sometimes it's a touchline or a depth update
    console.print(f"[green]Tick #{ticks_received}:[/green] LTP = ₹{ltp} | Vol = {volume} | Time = {ltt} | Raw = {tick_data}")
    
    if ticks_received >= max_ticks:
        ticks_event.set()

def event_handler_order_update(order_data):
    pass

def main():
    global ticks_received, ticks_event
    
    console.print("[bold magenta]🔌 Connecting to Shoonya WebSocket...[/bold magenta]")
    
    api = get_shoonya_api()
    if not api:
        console.print("[bold red]Error:[/bold red] Could not authenticate with Shoonya. Please run the login script first.")
        sys.exit(1)

    # Search for GOLDBEES token
    console.print("Resolving GOLDBEES token...")
    try:
        search_res = api.searchscrip(exchange="NSE", searchtext="GOLDBEES")
        if not search_res or search_res.get("stat") != "Ok" or not search_res.get("values"):
            console.print("[bold red]Error:[/bold red] Could not find GOLDBEES on Shoonya")
            sys.exit(1)
            
        token = None
        for val in search_res["values"]:
            if val.get("tsym") == "GOLDBEES-EQ":
                token = val.get("token")
                break
        
        if not token:
            token = search_res["values"][0].get("token")
            
        tsym = next((x.get("tsym") for x in search_res["values"] if x.get("token") == token), "GOLDBEES-EQ")
        console.print(f"Found instrument: [bold cyan]{tsym}[/bold cyan] (Token: [bold cyan]{token}[/bold cyan])")
        
    except Exception as e:
        console.print(f"[bold red]Error searching script:[/bold red] {e}")
        sys.exit(1)

    # 1. Define callback for when connection opens
    def open_callback():
        console.print("[bold green]✓ WebSocket connected successfully! Subscribing to GOLDBEES feed...[/bold green]")
        api.subscribe([f"NSE|{token}"])

    def error_callback(err):
        console.print(f"[bold red]WebSocket Error callback details:[/bold red] {err}")

    def close_callback(*args):
        console.print(f"[bold yellow]WebSocket Closed callback details:[/bold yellow] {args}")

    # 2. Start WebSocket
    try:
        import websocket
        websocket.enableTrace(True)
        api.start_websocket(
            order_update_callback=event_handler_order_update,
            subscribe_callback=event_handler_feed_update,
            socket_open_callback=open_callback,
            socket_error_callback=error_callback,
            socket_close_callback=close_callback
        )
    except Exception as e:
        console.print(f"[bold red]Failed to start WebSocket:[/bold red] {e}")
        sys.exit(1)

    console.print(f"Waiting for {max_ticks} live broadcast ticks (or 15s timeout)...")
    
    # Wait for ticks or timeout
    success = ticks_event.wait(timeout=15.0)
    
    # 3. Clean up and close connection
    console.print("\n[bold yellow]🔌 Disconnecting from WebSocket...[/bold yellow]")
    try:
        api.close_websocket()
    except Exception:
        pass
        
    if success:
        console.print(f"[bold green]✓ Success! Received {ticks_received} live ticks from Shoonya broadcast.[/bold green]")
    else:
        if ticks_received > 0:
            console.print(f"[bold yellow]Timeout reached after receiving {ticks_received} ticks.[/bold yellow]")
        else:
            console.print("[bold red]Timeout reached. No ticks received. (Is the market open and actively trading?)[/bold red]")

if __name__ == "__main__":
    main()
