"""
src/tools/security_symbol_resolver.py
───────────────────────────────
Resolve mf_holdings security_name/isin -> a tradeable NSE symbol (the same
symbol convention used in market_data.daily_prices), via Shoonya's
searchscrip. There is no bulk ISIN<->symbol master available anywhere in
this codebase or via the broker APIs in use (checked Kite's instruments dump
and Shoonya's NorenApi — neither exposes one), and searching by ISIN text
does not work against Shoonya's index (verified live: returns None). What
does work is searching by a *cleaned* company name.

Resolution strategy per security_name:
  1. Strip generic corporate suffixes (Limited, Ltd, Company, Corp, ...).
  2. If that fails, progressively drop trailing words ("National Aluminium
     Company" -> "National Aluminium") until a match is found or the name
     is exhausted.
  3. Only accept EQ (equity) instrument matches — skip derivatives/ETFs.

Verified live against a real smallcap security_name sample: "Reliance
Industries" -> RELIANCE, "ICICI Bank" -> ICICIBANK, "National Aluminium" ->
NATIONALUM, "Jubilant Foodworks" -> JUBLFOOD. Full legal names with the un-
abbreviated word "Limited" largely fail against Shoonya's index; the
suffix-stripped short form is what actually matches.

Results are cached in market_data.security_symbol_map (keyed by isin, since
the same company appears under multiple security_name spelling variants
across funds/months — e.g. "Axis Bank Limited" vs "Axis Bank Ltd") so repeat
runs don't re-hit the broker API for names already resolved.
"""
from __future__ import annotations

import logging
import re
from datetime import datetime

import pandas as pd

from src.db.pool import get_pool, query_df, execute

logger = logging.getLogger(__name__)


# Cash/CBLO/T-Bill/net-current-asset lines that show up as "holdings" but
# aren't listed equities — no symbol to resolve them to. Extends
# whale_accumulation_scanner.py's debt-instrument regex with the patterns
# seen in a real smallcap holdings sample ("364 DTB ...", "Cblo",
# "Cash & Other Net Current Assets:").
_NON_EQUITY_RE = re.compile(
    r"^[0-9]+\.[0-9]+%\s|Govt Stock|T-Bill|\(\d{2}/\d{2}/\d{4}\)"
    r"|^\d+\s*DTB\b|Cblo|Treps|Net Current Assets|Net Receivable|Net Payable"
    r"|Corporate Debt Market Development",
    re.IGNORECASE,
)

_CORPORATE_SUFFIXES = [
    " limited", " ltd.", " ltd", " company limited", " co. ltd", " co ltd",
    " corporation", " corp", " pvt. ltd", " pvt ltd", " (partly paid)",
]

_PARTLY_PAID_RE = re.compile(r"\s*\(partly paid.*?\)", re.IGNORECASE)


def is_resolvable_equity(security_name: str) -> bool:
    """False for cash/CBLO/T-Bill/net-current-asset lines — nothing to resolve."""
    if not security_name or not security_name.strip():
        return False
    return not bool(_NON_EQUITY_RE.search(security_name))


def _candidate_names(security_name: str) -> list[str]:
    """Generate search strings from most- to least-specific: full name, then
    with corporate suffixes stripped, then progressively fewer trailing words.
    """
    name = _PARTLY_PAID_RE.sub("", security_name).strip()

    cur = name
    changed = True
    while changed:
        changed = False
        cur_lower = cur.lower()
        for suf in _CORPORATE_SUFFIXES:
            if cur_lower.endswith(suf):
                cur = cur[: -len(suf)].rstrip(" .")
                changed = True
                break

    # Start from the suffix-stripped core name, not the raw legal name —
    # verified live that trying the raw "X Ltd"/"X Limited" form first can
    # return a wrong singleton match ("Infosys Ltd" matches "HCL Infosystems
    # Ltd", a different company, before the stripped "Infosys" form finds
    # the correct INFY). If no suffix was found, cur == name already.
    candidates = [cur]
    words = cur.split()
    for i in range(len(words) - 1, 0, -1):
        candidates.append(" ".join(words[:i]))

    seen = set()
    out = []
    for c in candidates:
        if c and c not in seen:
            seen.add(c)
            out.append(c)
    return out


def _best_match(candidate: str, eq_matches: list[dict]) -> dict | None:
    """Pick the EQ match Shoonya's search actually meant, not just the first
    result in the list. searchscrip ranks loosely (e.g. "BSE" returns
    BSE500IETF ahead of BSE itself), so blindly taking values[0] silently
    returns the wrong stock rather than failing — worse than no match at all.

    Accept only an exact symname match, or a single unambiguous prefix match.
    A candidate with several equally-plausible prefix matches (e.g. "Bharat"
    matching BHARATWIRE/BHARATSE/BHARATRAS/...) is rejected rather than
    guessed at.
    """
    cand_norm = re.sub(r"[^A-Z0-9]", "", candidate.upper())
    if len(cand_norm) < 3:
        return None
    for v in eq_matches:
        if v.get("symname", "").upper() == cand_norm:
            return v
    # cname (the full company name field) often disambiguates better than the
    # compressed symname — e.g. "Infosys" query: symname has no prefix relation
    # to either INFY or HCL-INSYS, but cname "INFOSYS LIMITED" uniquely starts
    # with "INFOSYS" while "HCL INFOSYSTEMS LTD" doesn't.
    cand_upper = candidate.upper().strip()
    cname_matches = [v for v in eq_matches if v.get("cname", "").upper().startswith(cand_upper)]
    if len(cname_matches) == 1:
        return cname_matches[0]
    # Bidirectional prefix: symname is often a truncated/compressed form of the
    # full name (ASHOKLEY for "Ashok Leyland", BHARATFORG for "Bharat Forge"),
    # so check both directions, not just symname-starts-with-candidate.
    prefix_matches = [
        v for v in eq_matches
        if cand_norm.startswith(v.get("symname", "").upper()) or v.get("symname", "").upper().startswith(cand_norm)
    ]
    if len(prefix_matches) == 1:
        return prefix_matches[0]
    # A sufficiently specific query (>=6 chars) that returns exactly one EQ
    # result at all is safe to accept even without a clean prefix relation
    # (handles vowel-dropped compressions like BHARTIARTL for "Bharti Airtel").
    # Only reachable here when the exact/prefix checks above found nothing,
    # so this never overrides a real prefix match — it only covers the
    # single-candidate case those checks can't express.
    if len(eq_matches) == 1 and len(cand_norm) >= 6:
        return eq_matches[0]
    return None


def resolve_security_symbol(api, security_name: str) -> str | None:
    """Try each candidate name against Shoonya's searchscrip; return the
    trading symbol (daily_prices convention, no -EQ suffix) of the first
    unambiguous EQ-instrument match."""
    if not is_resolvable_equity(security_name):
        return None
    for candidate in _candidate_names(security_name):
        try:
            res = api.searchscrip(exchange="NSE", searchtext=candidate)
        except Exception as exc:
            logger.debug("searchscrip error for %r: %s", candidate, exc)
            continue
        if not res or res.get("stat") != "Ok" or not res.get("values"):
            continue
        eq_matches = [v for v in res["values"] if v.get("instname") == "EQ"]
        match = _best_match(candidate, eq_matches)
        if match is None:
            continue
        tsym = match.get("tsym", "")
        return tsym[:-3] if tsym.endswith("-EQ") else tsym
    return None


def _ensure_cache_table() -> None:
    execute("""
        CREATE TABLE IF NOT EXISTS market_data.security_symbol_map (
            isin          String,
            security_name String,
            symbol        Nullable(String),
            resolved_at   DateTime
        ) ENGINE = ReplacingMergeTree(resolved_at)
        ORDER BY isin
    """)


def get_or_resolve_symbols(api, isin_name_pairs: list[tuple[str, str]]) -> dict[str, str | None]:
    """Resolve a batch of (isin, security_name) pairs to symbols, using the
    cache table for anything already resolved and only calling Shoonya for
    ISINs not yet seen. Returns {isin: symbol_or_None}.
    """
    _ensure_cache_table()
    unique_pairs = {isin: name for isin, name in isin_name_pairs if isin}
    if not unique_pairs:
        return {}

    isin_list_sql = ", ".join(f"'{isin}'" for isin in unique_pairs)
    cached = query_df(f"""
        SELECT isin, symbol FROM market_data.security_symbol_map FINAL
        WHERE isin IN ({isin_list_sql})
    """)
    result: dict[str, str | None] = {
        row["isin"]: (row["symbol"] if pd.notna(row["symbol"]) else None)
        for _, row in cached.iterrows()
    } if not cached.empty else {}

    missing = {isin: name for isin, name in unique_pairs.items() if isin not in result}
    if not missing:
        return result

    pool = get_pool()
    now = datetime.utcnow()
    for isin, name in missing.items():
        symbol = resolve_security_symbol(api, name)
        result[isin] = symbol
        pool.execute(
            """
            INSERT INTO market_data.security_symbol_map (isin, security_name, symbol, resolved_at)
            VALUES (%(isin)s, %(security_name)s, %(symbol)s, %(resolved_at)s)
            """,
            {"isin": isin, "security_name": name, "symbol": symbol, "resolved_at": now},
        )
    return result
