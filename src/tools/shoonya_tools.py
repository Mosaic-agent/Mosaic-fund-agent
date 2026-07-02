"""
src/tools/shoonya_tools.py
───────────────────────────
Provides tools to fetch live stock/ETF prices and details from Shoonya API
and WebSocket feeds. Exposes:
  - get_shoonya_quotes: fetch live quotes from REST API
  - get_shoonya_live_tick: fetch a live tick from WebSocket feed

Ensures all data (including errors and timeouts) is returned in valid JSON format.
"""

import json
import logging
import threading
from typing import Any
from langchain_core.tools import tool

logger = logging.getLogger(__name__)

def _resolve_token(api, symbol: str) -> tuple[str, str] | None:
    """Resolve symbol to exchange token and trading symbol (e.g. 'GOLDBEES' -> ('14428', 'GOLDBEES-EQ'))."""
    clean_sym = symbol.strip().upper().replace(".NS", "").replace(".BO", "")
    try:
        search_res = api.searchscrip(exchange="NSE", searchtext=clean_sym)
        if not search_res or search_res.get("stat") != "Ok" or not search_res.get("values"):
            return None
            
        token = None
        tsym = None
        target_tsym = f"{clean_sym}-EQ"
        for val in search_res["values"]:
            if val.get("tsym") == target_tsym:
                token = val.get("token")
                tsym = val.get("tsym")
                break
                
        if not token:
            token = search_res["values"][0].get("token")
            tsym = search_res["values"][0].get("tsym")
            
        return token, tsym
    except Exception as e:
        logger.debug("Shoonya symbol resolution error for %s: %s", symbol, e)
        return None


@tool
def get_shoonya_quotes(symbol: str) -> str:
    """
    Fetch the latest live quotes (LTP, open, high, low, close, volume, bid/ask depth)
    for any Indian NSE stock or ETF from the Shoonya REST API.

    Use this tool when you need real-time quotes or order book depth during market hours.
    
    Args:
        symbol: The stock or ETF symbol (e.g., 'GOLDBEES', 'RELIANCE', 'TCS').
    """
    from src.importer.fetchers.shoonya_fetcher import get_shoonya_api
    api = get_shoonya_api()
    if not api:
        return json.dumps({
            "status": "error",
            "message": "Shoonya API is not authenticated or credentials not configured."
        }, indent=2)
        
    res = _resolve_token(api, symbol)
    if not res:
        return json.dumps({
            "status": "error",
            "message": f"Could not find NSE token for symbol '{symbol}'."
        }, indent=2)
        
    token, tsym = res
    try:
        quote = api.get_quotes(exchange="NSE", token=token)
        if not quote or quote.get("stat") != "Ok":
            return json.dumps({
                "status": "error",
                "message": f"Error fetching quotes from Shoonya: {quote}"
            }, indent=2)
        return json.dumps(quote, indent=2)
    except Exception as e:
        return json.dumps({
            "status": "error",
            "message": f"Error occurred fetching quotes from Shoonya REST API: {e}"
        }, indent=2)


@tool
def get_shoonya_live_tick(symbol: str) -> str:
    """
    Fetch a real-time live tick directly from the Shoonya WebSocket broadcast.
    This connects to the websocket feed, subscribes to the instrument, captures the
    next broadcasted tick (price and volume), and cleanly disconnects.

    Use this tool when you need the absolute latest live traded price or volume broadcast
    from the exchange during active market hours.

    Args:
        symbol: The stock or ETF symbol (e.g., 'GOLDBEES', 'RELIANCE').
    """
    from src.importer.fetchers.shoonya_fetcher import get_shoonya_api
    api = get_shoonya_api()
    if not api:
        return json.dumps({
            "status": "error",
            "message": "Shoonya API is not authenticated or credentials not configured."
        }, indent=2)
        
    res = _resolve_token(api, symbol)
    if not res:
        return json.dumps({
            "status": "error",
            "message": f"Could not find NSE token for symbol '{symbol}'."
        }, indent=2)
        
    token, tsym = res
    
    tick_captured = {}
    tick_event = threading.Event()
    
    def on_feed_update(tick_data):
        if tick_data and (tick_data.get("t") == "tk" or tick_data.get("t") == "tf"):
            tick_captured.update(tick_data)
            tick_event.set()
            
    def on_open():
        api.subscribe([f"NSE|{token}"])
        
    try:
        api.start_websocket(
            order_update_callback=lambda x: None,
            subscribe_callback=on_feed_update,
            socket_open_callback=on_open
        )
        
        # Wait up to 5 seconds for a tick
        success = tick_event.wait(timeout=5.0)
        
        # Clean up connection
        try:
            api.close_websocket()
        except Exception:
            pass
            
        if success:
            # Add status success envelope
            tick_captured["status"] = "success"
            return json.dumps(tick_captured, indent=2)
        else:
            return json.dumps({
                "status": "timeout",
                "message": f"Timeout reached. No live tick broadcasted for {tsym} within 5.0 seconds. (Is the market open and active?)"
            }, indent=2)
            
    except Exception as e:
        # Guarantee cleanup
        try:
            api.close_websocket()
        except Exception:
            pass
        return json.dumps({
            "status": "error",
            "message": f"Error occurred during Shoonya WebSocket live tick capture: {e}"
        }, indent=2)


@tool
def initiate_shoonya_login(_: str = "") -> str:
    """
    Initiate Shoonya authentication by generating the browser authorization URL.
    Returns the URL that the user must open in their browser to log in.
    """
    from config.settings import settings
    user = getattr(settings, "shoonya_user_id", "")
    secret = getattr(settings, "shoonya_api_secret", "")
    
    if not user or not secret:
        return "Error: Shoonya credentials (SHOONYA_USER_ID, SHOONYA_API_SECRET) are not set in your .env file."

    login_url = f"https://api.shoonya.com/OAuthlogin/investor-entry-level/login?api_key={user}&route_to={user}"
    return f"Please open this URL in your browser to authenticate with Shoonya:\n{login_url}\n\nAfter logging in and authorizing, copy the 'code' parameter from the redirect URL and run the complete_shoonya_login(code=...) tool."


@tool
def complete_shoonya_login(code: str) -> str:
    """
    Complete the Shoonya authentication flow by exchanging the browser authorization code
    for session tokens.
    
    Args:
        code: The auth code copied from the redirect URL after browser login.
    """
    from config.settings import settings
    from src.importer.fetchers.shoonya_fetcher import _save_session
    
    user = getattr(settings, "shoonya_user_id", "")
    secret = getattr(settings, "shoonya_api_secret", "")
    
    if not user or not secret:
        return "Error: Shoonya credentials (SHOONYA_USER_ID, SHOONYA_API_SECRET) are not set in your .env file."
        
    try:
        from NorenRestApiPy.NorenApi import NorenApi
        class ShoonyaApiPy(NorenApi):
            def __init__(self):
                NorenApi.__init__(
                    self,
                    host="https://api.shoonya.com/NorenWClientAPI",
                    websocket="wss://api.shoonya.com/NorenWSAPI/",
                )
    except ImportError:
        return "Error: NorenRestApiPy is not installed."

    api = ShoonyaApiPy()
    user_clean = user.replace("_U", "")
    try:
        result = api.getAccessToken(
            authcode=code,
            Secret_Code=secret,
            client_id=user,
            UID=user_clean
        )
        if result is not None:
            asc_tok, usrid, ref_tok, actid = result
            susertoken = getattr(api, '_NorenApi__susertoken', asc_tok)
            _save_session({
                "susertoken": susertoken,
                "access_token": asc_tok,
                "userid": usrid,
                "accountid": actid
            })
            return f"Success! Authenticated successfully as {usrid}. The session token has been cached and saved to ClickHouse."
        else:
            return "Error: OAuth token generation failed. The code might be expired or invalid."
    except Exception as exc:
        return f"Error during OAuth token exchange: {exc}"


SHOONYA_TOOLS = [get_shoonya_quotes, get_shoonya_live_tick, initiate_shoonya_login, complete_shoonya_login]
