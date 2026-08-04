"""
src/importer/fetchers/shoonya_fetcher.py
─────────────────────────────────────────
Fetches daily OHLCV data for NSE-listed symbols via the Shoonya (Finvasia)
brokerage API — a reliable alternative to Yahoo Finance for Indian market data.

Session management
──────────────────
Shoonya requires a daily login with TOTP. The session token is cached to
output/.cache/shoonya_session.json and reused until it expires.  Set
SHOONYA_TOTP_SECRET in .env to enable automated TOTP generation via pyotp.

Symbol format
─────────────
NSE equities and ETFs use the "-EQ" segment suffix: MASPTOP50 → MASPTOP50-EQ
Exchange is always "NSE" for cash-market symbols.

Limitations
───────────
Shoonya covers NSE/BSE/MCX listed symbols only.  Global indices (^GSPC),
commodity futures (GC=F), US ETFs (GLD), and FX pairs (USDINR=X) still
require Yahoo Finance — those are not replaced by this fetcher.
"""
from __future__ import annotations

import json
import logging
import os
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

_SESSION_CACHE = Path("output/.cache/shoonya_session.json")
_SESSION_TTL_HOURS = 20  # Shoonya tokens last ~24h; refresh conservatively


def _load_cached_session() -> dict | None:
    """Return cached session dict if it exists and is not stale."""
    # 1. Try ClickHouse database first
    try:
        from src.db.pool import query_df
        import pandas as pd
        df = query_df(
            """
            SELECT userid, susertoken, access_token, accountid, saved_at
            FROM market_data.shoonya_session FINAL
            LIMIT 1
            """
        )
        if not df.empty:
            row = df.iloc[0]
            saved_at = pd.to_datetime(row["saved_at"]).to_pydatetime()
            if datetime.now() - saved_at <= timedelta(hours=_SESSION_TTL_HOURS):
                session = {
                    "userid": str(row["userid"]),
                    "susertoken": str(row["susertoken"]),
                    "access_token": str(row["access_token"]),
                    "accountid": str(row["accountid"]),
                    "saved_at": saved_at.isoformat()
                }
                # Sync back to local file if missing or stale
                _SESSION_CACHE.parent.mkdir(parents=True, exist_ok=True)
                _SESSION_CACHE.write_text(json.dumps(session))
                log.debug("Shoonya: loaded active session from ClickHouse")
                return session
            else:
                log.debug("Shoonya: session in ClickHouse has expired")
    except Exception as exc:
        log.debug("Shoonya: failed to load session from ClickHouse (%s)", exc)

    # 2. Fallback to local file cache
    try:
        if not _SESSION_CACHE.exists():
            return None
        data = json.loads(_SESSION_CACHE.read_text())
        saved_at = datetime.fromisoformat(data.get("saved_at", "2000-01-01"))
        if datetime.now() - saved_at > timedelta(hours=_SESSION_TTL_HOURS):
            return None
        log.debug("Shoonya: loaded active session from local file cache")
        return data
    except Exception:
        return None


def _save_session(session: dict) -> None:
    # 1. Save to local file cache
    _SESSION_CACHE.parent.mkdir(parents=True, exist_ok=True)
    session["saved_at"] = datetime.now().isoformat()
    _SESSION_CACHE.write_text(json.dumps(session))

    # 2. Save to ClickHouse database
    try:
        from src.db.pool import execute
        execute(
            """
            CREATE TABLE IF NOT EXISTS market_data.shoonya_session (
                userid        String,
                susertoken    String,
                access_token  String,
                accountid     String,
                saved_at      DateTime
            ) ENGINE = ReplacingMergeTree(saved_at)
            ORDER BY userid
            """
        )
        saved_at_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        execute(
            """
            INSERT INTO market_data.shoonya_session (userid, susertoken, access_token, accountid, saved_at)
            VALUES (%(userid)s, %(susertoken)s, %(access_token)s, %(accountid)s, %(saved_at)s)
            """,
            {
                "userid": str(session.get("userid", "")),
                "susertoken": str(session.get("susertoken", "")),
                "access_token": str(session.get("access_token", "")),
                "accountid": str(session.get("accountid", "")),
                "saved_at": saved_at_dt,
            }
        )
        log.info("Shoonya: saved session to ClickHouse")
    except Exception as exc:
        log.warning("Shoonya: failed to save session to ClickHouse (%s)", exc)


def get_shoonya_api():
    """
    Return an authenticated ShoonyaApiPy instance using OAuth flow.

    Tries the cached session first; falls back to prompting for a fresh login
    using credentials from config.settings. Returns None if Shoonya is not
    configured or login fails.
    """
    try:
        from NorenRestApiPy.NorenApi import NorenApi  # type: ignore

        class ShoonyaApiPy(NorenApi):
            def __init__(self):
                NorenApi.__init__(
                    self,
                    host="https://api.shoonya.com/NorenWClientAPI",
                    websocket="wss://api.shoonya.com/NorenWSAPI/",
                )
    except ImportError:
        log.debug("NorenRestApiPy/NorenRestApiOAuth not installed — skipping Shoonya")
        return None

    from config.settings import settings

    user    = getattr(settings, "shoonya_user_id",    "")
    pwd     = getattr(settings, "shoonya_password",   "")
    secret  = getattr(settings, "shoonya_api_secret", "")

    if not all([user, secret]):
        log.debug("Shoonya credentials not configured — skipping")
        return None

    # Strip _U suffix for API requests/session keys (Shoonya expects numeric ID like FN203617)
    user_clean = user.replace("_U", "")

    api = ShoonyaApiPy()

    # Try cached session
    cached = _load_cached_session()
    if cached and cached.get("susertoken") and cached.get("access_token"):
        cached_user = cached.get("userid", user_clean)
        try:
            api.set_session(
                userid=cached_user,
                password=pwd,
                usertoken=cached["susertoken"],
                accesstoken=cached["access_token"]
            )
            api.injectOAuthHeader(cached["access_token"], cached_user, cached_user)
            
            # Verify session is alive
            limits = api.get_limits()
            if limits and limits.get("stat") == "Ok":
                log.debug("Shoonya: reused cached session for %s", cached_user)
                return api
            else:
                log.debug("Shoonya: cached session expired, re-authenticating")
        except Exception as exc:
            log.debug("Shoonya: cached session invalid (%s), re-authenticating", exc)

    # Resolve interactive code prompt
    import sys
    if not sys.stdin.isatty():
        log.warning("Shoonya session expired and terminal is not interactive. Cannot perform OAuth login.")
        return None

    login_url = f"https://api.shoonya.com/OAuthlogin/investor-entry-level/login?api_key={user}&route_to={user}"
    print("\n" + "=" * 60)
    print("SHOONYA OAUTH LOGIN REQUIRED")
    print(f"Please open this URL in your browser:\n{login_url}")
    print("Log in with your password and TOTP, and authorize.")
    print("Copy the 'code' parameter from the final redirect URL (even if it shows a 404 error).")
    print("=" * 60 + "\n")

    try:
        auth_code = input("Enter OAuth Code: ").strip()
    except (KeyboardInterrupt, EOFError):
        log.warning("OAuth login interrupted by user")
        return None

    if not auth_code:
        log.warning("OAuth code cannot be empty")
        return None

    try:
        result = api.getAccessToken(
            authcode=auth_code,
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
            log.info("Shoonya: authenticated successfully via OAuth as %s", usrid)
            return api
        else:
            log.warning("Shoonya OAuth token generation failed")
            return None
    except Exception as exc:
        log.warning("Shoonya OAuth login error: %s", exc)
        return None



def fetch_shoonya_ohlcv(
    symbols: list[tuple[str, str]],  # [(nse_symbol, yahoo_ticker), ...]
    category: str,
    from_date: date,
    to_date: date,
) -> list[dict[str, Any]]:
    """
    Fetch daily OHLCV for NSE-listed symbols via Shoonya get_daily_price_series.

    Accepts the same (nse_symbol, yahoo_ticker) tuple format as yfinance_fetcher
    so it is a transparent drop-in.  Symbols that fail individually are skipped
    and logged; the caller should fall back to yfinance for missing symbols.

    Returns rows with keys: symbol, category, trade_date, open, high, low, close, volume
    """
    api = get_shoonya_api()
    if api is None:
        return []

    start_ts = int(datetime.combine(from_date, datetime.min.time()).timestamp())
    end_ts   = int(datetime.combine(to_date,   datetime.max.time()).timestamp())

    rows: list[dict[str, Any]] = []

    for idx, (nse_sym, _yahoo_sym) in enumerate(symbols, 1):
        pct = (idx / len(symbols)) * 100
        print(f"    [{idx}/{len(symbols)} - {pct:.1f}%] Fetching {nse_sym} via Shoonya...", flush=True)
        tradingsymbol = f"{nse_sym}-EQ"
        try:
            data = api.get_daily_price_series(
                exchange="NSE",
                tradingsymbol=tradingsymbol,
                startdate=start_ts,
                enddate=end_ts,
            )
        except Exception as exc:
            log.warning("Shoonya: get_daily_price_series failed for %s: %s", nse_sym, exc)
            continue

        if not data or (isinstance(data, dict) and data.get("stat") != "Ok"):
            log.debug("Shoonya: no data for %s (response: %s)", nse_sym, str(data)[:80])
            continue

        for bar in data:
            if isinstance(bar, str):
                try:
                    bar = json.loads(bar)
                except Exception:
                    continue
            if not isinstance(bar, dict):
                continue
            try:
                # Parse date — Shoonya returns "DD-MM-YYYY HH:MM:SS" or "DD-MM-YYYY"
                raw_time = bar.get("time", "")
                trade_date = _parse_shoonya_date(raw_time)
                if trade_date is None:
                    continue
                if not (from_date <= trade_date <= to_date):
                    continue

                close = float(bar.get("intc") or bar.get("c") or 0)
                if close <= 0:
                    continue

                rows.append({
                    "symbol":     nse_sym,
                    "category":   category,
                    "trade_date": trade_date,
                    "open":       float(bar.get("into") or bar.get("o") or close),
                    "high":       float(bar.get("inth") or bar.get("h") or close),
                    "low":        float(bar.get("intl") or bar.get("l") or close),
                    "close":      close,
                    "volume":     float(bar.get("intv") or bar.get("v") or bar.get("volume") or 0),
                })
            except (ValueError, TypeError) as exc:
                log.debug("Shoonya: bad bar for %s: %s — %s", nse_sym, bar, exc)
                continue

        # If the daily series is missing recent dates up to to_date/today, try intraday aggregation fallback
        parsed_dates = {r["trade_date"] for r in rows if r["symbol"] == nse_sym}
        check_date = from_date
        max_check = min(to_date, date.today())
        while check_date <= max_check:
            if check_date.weekday() < 5 and check_date not in parsed_dates:
                extra_bar = _fetch_shoonya_intraday_eod(api, nse_sym, category, check_date)
                if extra_bar:
                    rows.append(extra_bar)
                    parsed_dates.add(check_date)
            check_date += timedelta(days=1)

        # Be polite — Shoonya has no documented rate limit but avoid hammering
        time.sleep(0.1)

    # Batch WebSocket fallback for today's price if still missing
    today = date.today()
    if from_date <= today <= to_date:
        missing_today_symbols = []
        for nse_sym, yahoo_sym in symbols:
            symbol_has_today = any(r["symbol"] == nse_sym and r["trade_date"] == today for r in rows)
            if not symbol_has_today:
                missing_today_symbols.append((nse_sym, yahoo_sym))
        
        if missing_today_symbols:
            log.info("Shoonya: %d symbol(s) missing today's bar. Attempting WebSocket touchline fetch...", len(missing_today_symbols))
            ws_rows = _fetch_shoonya_websocket_today_batch(api, missing_today_symbols, category, today)
            rows.extend(ws_rows)

    log.info("Shoonya: fetched %d rows for %s (%s→%s)", len(rows), category, from_date, to_date)
    return rows


def _fetch_shoonya_intraday_eod(api, nse_sym: str, category: str, target_date: date) -> dict[str, Any] | None:
    """Aggregate 1-minute intraday bars from Shoonya into an EOD bar when get_daily_price_series lags."""
    try:
        from src.tools.shoonya_tools import _resolve_token
        token_info = _resolve_token(api, nse_sym)
        if not token_info:
            return None
        token, _tsym = token_info
        exchange = "NSE"
        start_ts = int(datetime.combine(target_date, datetime.min.time()).timestamp())
        end_ts   = int(datetime.combine(target_date, datetime.max.time()).timestamp())
        bars = api.get_time_price_series(exchange=exchange, token=token, starttime=start_ts, endtime=end_ts)
        if not bars or not isinstance(bars, list):
            return None
        valid_bars = [b for b in bars if isinstance(b, dict) and b.get("ssboe") and b.get("into")]
        if not valid_bars:
            return None
        valid_bars.sort(key=lambda x: int(x["ssboe"]))
        open_p  = float(valid_bars[0]["into"])
        high_p  = max(float(b["inth"]) for b in valid_bars)
        low_p   = min(float(b["intl"]) for b in valid_bars)
        close_p = float(valid_bars[-1]["intc"])
        vol     = sum(float(b.get("intv", 0)) for b in valid_bars)
        return {
            "symbol":     nse_sym,
            "category":   category,
            "trade_date": target_date,
            "open":       open_p,
            "high":       high_p,
            "low":        low_p,
            "close":      close_p,
            "volume":     vol,
        }
    except Exception as exc:
        log.debug("Shoonya intraday fallback failed for %s on %s: %s", nse_sym, target_date, exc)
        return None


def _parse_shoonya_date(raw: str) -> date | None:
    """Parse Shoonya date strings: 'DD-MM-YYYY HH:MM:SS', 'DD-MM-YYYY', 'DD-MMM-YYYY', etc."""
    for fmt in ("%d-%m-%Y %H:%M:%S", "%d-%m-%Y", "%d-%b-%Y %H:%M:%S", "%d-%b-%Y"):
        try:
            return datetime.strptime(raw.strip(), fmt).date()
        except ValueError:
            continue
    return None


def _fetch_shoonya_websocket_today_batch(
    api,
    symbols: list[tuple[str, str]],
    category: str,
    today_date: date,
) -> list[dict[str, Any]]:
    """
    Connect to Shoonya WebSocket, subscribe to all symbols in parallel,
    wait for touchline tick data, and format them as EOD bars for today.
    """
    import queue
    import threading
    from src.tools.shoonya_tools import _resolve_token

    token_to_symbol = {}
    tokens = []
    for nse_sym, _yf_sym in symbols:
        token_info = _resolve_token(api, nse_sym)
        if token_info:
            token, _tsym = token_info
            token_to_symbol[token] = nse_sym
            tokens.append(f"NSE|{token}")

    if not tokens:
        return []

    ticks = {}
    ticks_lock = threading.Lock()
    all_done = threading.Event()

    def on_feed(tick_data):
        if not tick_data or tick_data.get("t") not in ("tk", "tf"):
            return
        token = tick_data.get("tk")
        if not token or token not in token_to_symbol:
            return

        lp = tick_data.get("lp")
        o = tick_data.get("o")
        h = tick_data.get("h")
        l = tick_data.get("l")
        v = tick_data.get("v")

        # We need touchline values (last traded price, open, high, low, volume)
        if all(x is not None for x in (lp, o, h, l, v)):
            with ticks_lock:
                ticks[token] = tick_data
                if len(ticks) == len(tokens):
                    all_done.set()

    try:
        api.start_websocket(
            order_update_callback=lambda x: None,
            subscribe_callback=on_feed,
            socket_open_callback=lambda: api.subscribe(tokens),
        )
        # Wait up to 3 seconds for all ticks to arrive
        all_done.wait(timeout=3.0)
    except Exception as exc:
        log.warning("Shoonya: WebSocket batch fetch failed: %s", exc)
    finally:
        try:
            def _close():
                try:
                    api.close_websocket()
                except Exception:
                    pass
            t = threading.Thread(target=_close, daemon=True)
            t.start()
            t.join(timeout=2.0)
        except Exception:
            pass

    # Construct EOD rows from cached ticks
    ws_rows = []
    with ticks_lock:
        for token, tick in ticks.items():
            symbol = token_to_symbol[token]
            try:
                close = float(tick["lp"])
                if close <= 0:
                    continue
                ws_rows.append({
                    "symbol":     symbol,
                    "category":   category,
                    "trade_date": today_date,
                    "open":       float(tick.get("o") or close),
                    "high":       float(tick.get("h") or close),
                    "low":        float(tick.get("l") or close),
                    "close":      close,
                    "volume":     float(tick.get("v") or 0),
                })
            except (ValueError, TypeError, KeyError) as e:
                log.debug("Shoonya: WebSocket parse failed for %s: %s", symbol, e)
                continue

    log.info(
        "Shoonya: WebSocket touchline fetch completed. Retrieved %d rows out of %d requested.",
        len(ws_rows), len(symbols)
    )
    return ws_rows
