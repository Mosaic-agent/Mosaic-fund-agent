"""
src/agents/live_watchlist.py
───────────────────────────────
Resolves the v1 watchlist for the live monitor (src/agents/live_monitor.py)
ONCE at process startup — no dynamic runtime registration in v1 (adding a
symbol requires a restart; deferred to a fast-follow per the approved plan).

Combines, de-duplicated by (exchange, token):
  1. Static indices (NIFTY, NIFTY BANK, INDIA VIX) — hardcoded tokens, no
     searchscrip round-trip needed.
  2. COMEX commodity futures — OPT-IN ONLY via the `comex:` key in the YAML
     config (settings.live_monitor_watchlist_config). Empty/missing → none
     watched. Always Yahoo-sourced (Shoonya has no COMEX feed) regardless of
     which manager is active for NSE symbols.
  3. Current holdings from ClickHouse market_data.user_holdings. Deliberately
     NOT src.tools.zerodha_mcp_tools.fetch_portfolio_holdings() — that's async
     and requires a live Kite/MCP session, unsuitable for a standalone script.
  4. Ad-hoc NSE symbols from the `symbols:` key of the same YAML config file.

Symbols that fail token resolution are logged and skipped, not fatal.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

log = logging.getLogger(__name__)

# Static NSE index tokens (Shoonya) — well-known, stable, no lookup needed.
_INDEX_ENTRIES: list[tuple[str, str, str]] = [
    ("NIFTY", "NSE", "26000"),
    ("NIFTY BANK", "NSE", "26009"),
    ("INDIA VIX", "NSE", "26017"),
]


@dataclass
class WatchlistEntry:
    symbol: str
    exchange: str
    token: str
    source: str  # "index" | "holding" | "config" | "comex"
    yahoo_ticker: str | None = None  # set only for "comex" entries — polled directly, no ".NS" suffix


def _index_entries() -> list[WatchlistEntry]:
    return [
        WatchlistEntry(symbol=s, exchange=e, token=t, source="index")
        for s, e, t in _INDEX_ENTRIES
    ]


def _comex_entries(requested: list[str]) -> list[WatchlistEntry]:
    """
    COMEX commodity futures — always Yahoo-sourced (Shoonya has no COMEX feed),
    reusing the same symbol/ticker catalogue as src.tools.comex_fetcher so the
    live monitor and the pre-market comex snapshot agree on what "COMEX" means.

    Opt-in only: `requested` comes from the `comex:` key in the watchlist YAML
    config. Empty/missing → no COMEX symbols watched at all. Unknown symbols
    are logged and skipped, not fatal.
    """
    if not requested:
        return []
    from src.tools.comex_fetcher import _COMEX_SYMBOLS

    entries = []
    for sym in requested:
        meta = _COMEX_SYMBOLS.get(sym)
        if meta is None:
            log.warning(
                "live_watchlist: unknown COMEX symbol %r in config (valid: %s) — skipping",
                sym, ", ".join(_COMEX_SYMBOLS.keys()),
            )
            continue
        entries.append(WatchlistEntry(
            symbol=sym, exchange="COMEX", token=sym, source="comex",
            yahoo_ticker=meta["yahoo_ticker"],
        ))
    return entries


def _holdings_symbols() -> list[str]:
    try:
        from src.db.pool import query_df
        df = query_df(
            """
            SELECT DISTINCT tradingsymbol
            FROM market_data.user_holdings
            ORDER BY imported_at DESC
            LIMIT 1 BY tradingsymbol
            """
        )
        if df.empty:
            return []
        return [str(s).strip().upper() for s in df["tradingsymbol"].tolist() if s]
    except Exception as exc:
        log.warning("live_watchlist: failed to load holdings from ClickHouse: %s", exc)
        return []


def _load_config(config_path: str | None) -> dict:
    if not config_path:
        return {}
    path = Path(config_path)
    if not path.exists():
        log.debug("live_watchlist: config file %s not found — skipping", path)
        return {}
    try:
        import yaml
        data = yaml.safe_load(path.read_text()) or {}
        # Backward-compat: a bare list (old format, no "symbols:"/"comex:" keys) means NSE symbols only.
        if isinstance(data, list):
            data = {"symbols": data}
        return data
    except Exception as exc:
        log.warning("live_watchlist: failed to parse config file %s: %s", path, exc)
        return {}


def _config_symbols(config: dict) -> list[str]:
    return [str(s).strip().upper() for s in config.get("symbols", []) if s]


def _config_comex_symbols(config: dict) -> list[str]:
    return [str(s).strip().upper() for s in config.get("comex", []) if s]


def resolve_watchlist(api, config_path: str | None = None) -> list[WatchlistEntry]:
    """
    Resolve the full v1 watchlist.

    Parameters
    ----------
    api : Shoonya API instance (already authenticated) **or** ``None``.
          When ``None`` (polling-fallback mode), Shoonya token resolution is
          skipped — the monitor uses symbol names directly against NSE/Yahoo.
          Static index entries are always included because their tokens are
          pre-set constants (no round-trip needed).
    config_path : path to the ad-hoc YAML watchlist config file.
    """
    config = _load_config(config_path)
    entries: list[WatchlistEntry] = list(_index_entries()) + list(_comex_entries(_config_comex_symbols(config)))
    seen = {(e.exchange, e.token) for e in entries}

    def _add_symbols(symbols: list[str], source: str) -> None:
        for symbol in symbols:
            if api is not None:
                # Shoonya websocket path: resolve to a numeric token.
                from src.tools.shoonya_tools import resolve_token
                resolved = resolve_token(api, symbol)
                if resolved is None:
                    log.warning(
                        "live_watchlist: could not resolve token for %s (%s) — skipping",
                        symbol, source,
                    )
                    continue
                token, _tsym = resolved
            else:
                # Polling-fallback path: no Shoonya session; use symbol as token
                # placeholder. NSE/Yahoo fetchers work on symbol name directly.
                token = symbol

            key = ("NSE", token)
            if key in seen:
                continue
            seen.add(key)
            entries.append(WatchlistEntry(symbol=symbol, exchange="NSE", token=token, source=source))

    _add_symbols(_holdings_symbols(), "holding")
    _add_symbols(_config_symbols(config), "config")

    log.info(
        "live_watchlist: resolved %d entries total — %s%s",
        len(entries),
        ", ".join(f"{e.symbol}({e.source})" for e in entries),
        " [polling-fallback mode — no Shoonya tokens]" if api is None else "",
    )
    return entries
