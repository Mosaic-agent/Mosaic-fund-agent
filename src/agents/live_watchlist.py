"""
src/agents/live_watchlist.py
───────────────────────────────
Resolves the v1 watchlist for the live monitor (src/agents/live_monitor.py)
ONCE at process startup — no dynamic runtime registration in v1 (adding a
symbol requires a restart; deferred to a fast-follow per the approved plan).

Combines, de-duplicated by (exchange, token):
  1. Static indices (NIFTY, NIFTY BANK, INDIA VIX) — hardcoded tokens, no
     searchscrip round-trip needed.
  2. Current holdings from ClickHouse market_data.user_holdings. Deliberately
     NOT src.tools.zerodha_mcp_tools.fetch_portfolio_holdings() — that's async
     and requires a live Kite/MCP session, unsuitable for a standalone script.
  3. Ad-hoc symbols from a YAML config file (settings.live_monitor_watchlist_config).

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
    source: str  # "index" | "holding" | "config"


def _index_entries() -> list[WatchlistEntry]:
    return [
        WatchlistEntry(symbol=s, exchange=e, token=t, source="index")
        for s, e, t in _INDEX_ENTRIES
    ]


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


def _config_symbols(config_path: str | None) -> list[str]:
    if not config_path:
        return []
    path = Path(config_path)
    if not path.exists():
        log.debug("live_watchlist: config file %s not found — skipping", path)
        return []
    try:
        import yaml
        data = yaml.safe_load(path.read_text()) or []
        if isinstance(data, dict):
            data = data.get("symbols", [])
        return [str(s).strip().upper() for s in data if s]
    except Exception as exc:
        log.warning("live_watchlist: failed to parse config file %s: %s", path, exc)
        return []


def resolve_watchlist(api, config_path: str | None = None) -> list[WatchlistEntry]:
    """
    Resolve the full v1 watchlist against the given (already-authenticated)
    Shoonya `api` instance. See module docstring for source precedence.
    """
    from src.tools.shoonya_tools import resolve_token

    entries: list[WatchlistEntry] = list(_index_entries())
    seen = {(e.exchange, e.token) for e in entries}

    def _add_symbols(symbols: list[str], source: str) -> None:
        for symbol in symbols:
            resolved = resolve_token(api, symbol)
            if resolved is None:
                log.warning("live_watchlist: could not resolve token for %s (%s) — skipping", symbol, source)
                continue
            token, _tsym = resolved
            key = ("NSE", token)
            if key in seen:
                continue
            seen.add(key)
            entries.append(WatchlistEntry(symbol=symbol, exchange="NSE", token=token, source=source))

    _add_symbols(_holdings_symbols(), "holding")
    _add_symbols(_config_symbols(config_path), "config")

    log.info(
        "live_watchlist: resolved %d entries total (%d static index, %d holding/config)",
        len(entries), len(_INDEX_ENTRIES), len(entries) - len(_INDEX_ENTRIES),
    )
    return entries
