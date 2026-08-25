"""
src/db/mf_vector.py
────────────────────
Qdrant integration for mutual fund holdings data.

Two collections
───────────────
mf_holdings
    One point per (fund × security × month).
    Text: "<security_name> (<asset_type>) held by <fund_name> as of <month>:
           <pct_of_nav>% NAV ₹<market_value_cr>Cr ISIN:<isin>"
    Use: "which funds hold HDFC Bank?", "which funds have gold ETF exposure?"
    Tenant index: isin (most queries filter by a specific security)

mf_fund_profiles
    One point per (fund × month) — aggregated portfolio fingerprint.
    Text: "<fund_name> portfolio <month>: equity 52% gold 20% bond 22% cash 6%
           — top: HDFC Bank 6.5%, Infosys 4.2%, GOLDBEES 18.7%"
    Use: "find multi-asset funds similar to DSP_MULTI_ASSET",
         "find funds with high commodity/gold allocation"
    Tenant index: fund_name

Public API (all fire-and-forget)
───────────────────────────────
    vectorize_holdings(rows)           — triggered after any mf_holdings insert
    find_funds_holding_security(query, k)      → list[dict]
    find_similar_fund_profiles(fund_name, as_of_month, k) → list[dict]
    find_funds_by_category(asset_type, query, k)          → list[dict]
"""

from __future__ import annotations

import logging
import threading
import uuid
from collections import defaultdict
from datetime import date, datetime
from typing import Any

import pandas as pd

log = logging.getLogger(__name__)

_HOLDINGS_COLLECTION = "mf_holdings"
_PROFILES_COLLECTION = "mf_fund_profiles"
_EMBED_DIM = 768

# ── Qdrant lazy init ──────────────────────────────────────────────────────────

_qdrant_available = True
try:
    from qdrant_client import QdrantClient
    from qdrant_client.models import (
        Distance,
        FieldCondition,
        Filter,
        KeywordIndexParams,
        KeywordIndexType,
        MatchValue,
        PayloadSchemaType,
        PointStruct,
        Range,
        VectorParams,
    )
except ImportError:
    _qdrant_available = False

_client: Any = None
_client_lock = threading.Lock()
_holdings_ready = False
_profiles_ready = False


def _get_client() -> Any:
    global _client
    if not _qdrant_available:
        return None
    if _client is not None:
        return _client
    with _client_lock:
        if _client is not None:
            return _client
        try:
            import os
            from config.settings import settings
            host = os.environ.get("QDRANT_HOST") or getattr(settings, "qdrant_host", "localhost")
            port = int(os.environ.get("QDRANT_PORT") or getattr(settings, "qdrant_port", 6333))
            grpc_port = int(os.environ.get("QDRANT_GRPC_PORT") or getattr(settings, "qdrant_grpc_port", 6334))
            from src.utils.net_check import is_port_open
            if not is_port_open(host, port):
                log.debug("MFVector: Qdrant unreachable at %s:%s — skipping client init", host, port)
                _client = None
                return None
            _client = QdrantClient(host=host, port=port, grpc_port=grpc_port, prefer_grpc=True, timeout=15.0)
            log.debug("MFVector: Qdrant client at %s:%s", host, port)
        except Exception as e:
            log.debug("MFVector: Qdrant init failed: %s", e)
            _client = None
    return _client


def _ensure_collection(name: str, is_holdings: bool) -> bool:
    global _holdings_ready, _profiles_ready
    ready = _holdings_ready if is_holdings else _profiles_ready
    if ready:
        return True
    client = _get_client()
    if client is None:
        return False
    try:
        existing = {c.name for c in client.get_collections().collections}
        if name not in existing:
            client.create_collection(
                collection_name=name,
                vectors_config=VectorParams(size=_EMBED_DIM, distance=Distance.COSINE),
            )
            log.info("MFVector: created collection '%s'", name)

            if is_holdings:
                # isin is the primary tenant — most queries narrow by a specific security
                index_fields = [
                    ("isin",          KeywordIndexParams(type=KeywordIndexType.KEYWORD, is_tenant=True)),
                    ("fund_name",     PayloadSchemaType.KEYWORD),
                    ("asset_type",    PayloadSchemaType.KEYWORD),
                    ("as_of_timestamp", PayloadSchemaType.FLOAT),
                ]
            else:
                # fund_name is the primary tenant — profile queries always scope to one fund
                index_fields = [
                    ("fund_name",       KeywordIndexParams(type=KeywordIndexType.KEYWORD, is_tenant=True)),
                    ("asset_type_primary", PayloadSchemaType.KEYWORD),
                    ("as_of_timestamp", PayloadSchemaType.FLOAT),
                ]

            for field, schema in index_fields:
                try:
                    client.create_payload_index(
                        collection_name=name, field_name=field, field_schema=schema
                    )
                except Exception:
                    pass

        if is_holdings:
            _holdings_ready = True
        else:
            _profiles_ready = True
        return True
    except Exception as e:
        log.warning("MFVector: ensure_collection('%s') failed: %s", name, e)
        return False


# ── Helpers ───────────────────────────────────────────────────────────────────

def _pid(namespace: str, *parts: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, ":".join([namespace] + list(parts))))


def _to_ts(d: Any) -> float:
    if isinstance(d, datetime):
        return d.timestamp()
    if isinstance(d, date):
        return datetime(d.year, d.month, d.day).timestamp()
    try:
        return datetime.strptime(str(d)[:10], "%Y-%m-%d").timestamp()
    except Exception:
        return 0.0


def _month_str(d: Any) -> str:
    try:
        if isinstance(d, (date, datetime)):
            return d.strftime("%Y-%m")
        return str(d)[:7]
    except Exception:
        return str(d)[:10]


def _embed(texts: list[str]) -> list[list[float]]:
    """Embed texts via Ollama. embed_batch() chunks into safe sub-batches internally."""
    try:
        from src.ml.correlation.news_rag import embed_batch
        return embed_batch(texts)
    except Exception as e:
        log.debug("MFVector: embed_batch failed: %s", e)
        return [[0.0] * _EMBED_DIM for _ in texts]


def _safe_f(v: Any, default: float = 0.0) -> float:
    try:
        return float(v) if v is not None else default
    except Exception:
        return default


# ── Per-holding vectorisation ─────────────────────────────────────────────────

def _holding_text(r: dict) -> str:
    return (
        f"{r.get('security_name', '')} ({r.get('asset_type', 'other')}) "
        f"held by {r.get('fund_name', '')} as of {_month_str(r.get('as_of_month'))}: "
        f"{_safe_f(r.get('pct_of_nav')):.2f}% NAV "
        f"₹{_safe_f(r.get('market_value_cr')):.2f}Cr "
        f"ISIN:{r.get('isin', '')}"
    )


def _do_vectorize_holdings(rows: list[dict]) -> None:
    if not rows:
        return
    client = _get_client()
    if client is None or not _ensure_collection(_HOLDINGS_COLLECTION, is_holdings=True):
        return

    texts = [_holding_text(r) for r in rows]
    vectors = _embed(texts)

    points = [
        PointStruct(
            id=_pid("mf_holding", r.get("scheme_code", ""), r.get("isin", ""), _month_str(r.get("as_of_month"))),
            vector=vectors[i],
            payload={
                "data_type":        "mf_holding",
                "fund_name":        r.get("fund_name", ""),
                "scheme_code":      r.get("scheme_code", ""),
                "isin":             r.get("isin", ""),
                "security_name":    r.get("security_name", ""),
                "asset_type":       r.get("asset_type", "other"),
                "pct_of_nav":       _safe_f(r.get("pct_of_nav")),
                "market_value_cr":  _safe_f(r.get("market_value_cr")),
                "as_of_month":      _month_str(r.get("as_of_month")),
                "as_of_timestamp":  _to_ts(r.get("as_of_month")),
                "text":             texts[i],
            },
        )
        for i, r in enumerate(rows)
    ]

    try:
        # Upsert in chunks of 500 to stay within Qdrant limits
        for start in range(0, len(points), 500):
            client.upsert(collection_name=_HOLDINGS_COLLECTION, points=points[start:start + 500])
        log.info("MFVector: upserted %d holding points", len(points))
    except Exception as e:
        log.warning("MFVector: holdings upsert failed: %s", e)


# ── Per-fund profile vectorisation ────────────────────────────────────────────

def _build_fund_profiles(rows: list[dict]) -> list[dict]:
    """
    Aggregate per-holding rows into one profile dict per (fund_name, as_of_month).
    Returns list of profile dicts.
    """
    groups: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for r in rows:
        key = (r.get("fund_name", ""), _month_str(r.get("as_of_month")))
        groups[key].append(r)

    profiles = []
    for (fund_name, month), holdings in groups.items():
        total_pct = sum(_safe_f(h.get("pct_of_nav")) for h in holdings) or 1.0

        # Asset-type breakdown
        buckets: dict[str, float] = defaultdict(float)
        for h in holdings:
            buckets[h.get("asset_type", "other")] += _safe_f(h.get("pct_of_nav"))

        equity_pct = buckets.get("equity", 0.0)
        gold_pct   = buckets.get("gold",   0.0)
        bond_pct   = buckets.get("bond",   0.0)
        cash_pct   = buckets.get("cash",   0.0)
        other_pct  = buckets.get("other",  0.0)

        # Top 5 holdings by pct_of_nav
        top5 = sorted(holdings, key=lambda h: _safe_f(h.get("pct_of_nav")), reverse=True)[:5]
        top5_names = ", ".join(
            f"{h.get('security_name', '?')} {_safe_f(h.get('pct_of_nav')):.1f}%"
            for h in top5
        )

        # Primary category = largest bucket
        primary = max(buckets, key=buckets.get) if buckets else "other"  # type: ignore[arg-type]

        scheme_code = holdings[0].get("scheme_code", "") if holdings else ""
        as_of_month = holdings[0].get("as_of_month") if holdings else month

        profiles.append({
            "fund_name":        fund_name,
            "scheme_code":      scheme_code,
            "as_of_month":      month,
            "as_of_month_raw":  as_of_month,
            "equity_pct":       round(equity_pct, 2),
            "gold_pct":         round(gold_pct, 2),
            "bond_pct":         round(bond_pct, 2),
            "cash_pct":         round(cash_pct, 2),
            "other_pct":        round(other_pct, 2),
            "total_holdings":   len(holdings),
            "asset_type_primary": primary,
            "top5_text":        top5_names,
        })

    return profiles


def _profile_text(p: dict) -> str:
    return (
        f"{p['fund_name']} portfolio {p['as_of_month']}: "
        f"equity {p['equity_pct']:.1f}% "
        f"gold {p['gold_pct']:.1f}% "
        f"bond {p['bond_pct']:.1f}% "
        f"cash {p['cash_pct']:.1f}% "
        f"other {p['other_pct']:.1f}% "
        f"— top holdings: {p['top5_text']}"
    )


def _do_vectorize_profiles(rows: list[dict]) -> None:
    if not rows:
        return
    client = _get_client()
    if client is None or not _ensure_collection(_PROFILES_COLLECTION, is_holdings=False):
        return

    # Enrich with fund manager metadata from ClickHouse (if available)
    meta_by_code: dict[str, dict] = {}
    try:
        from src.db.pool import get_client as _get_ch_client
        ch = _get_ch_client()
        meta_rows = ch.query(
            "SELECT scheme_code, lead_fund_manager, fund_house, scheme_category, "
            "benchmark_index FROM market_data.mf_scheme_metadata FINAL"
        ).result_rows
        meta_by_code = {
            str(r[0]): {
                "lead_fund_manager": r[1] or "",
                "fund_house": r[2] or "",
                "scheme_category": r[3] or "",
                "benchmark_index": r[4] or "",
            }
            for r in meta_rows
        }
        ch.close()
    except Exception as exc:
        log.debug("MFVector: mf_scheme_metadata lookup failed (non-fatal): %s", exc)

    profiles = _build_fund_profiles(rows)
    if not profiles:
        return

    # Build enriched text for embeddings
    texts = []
    for p in profiles:
        meta = meta_by_code.get(str(p.get("scheme_code", "")), {})
        manager = meta.get("lead_fund_manager", "")
        house = meta.get("fund_house", "")
        base = _profile_text(p)
        if manager or house:
            enriched = (
                f"{p['fund_name']} portfolio {p['as_of_month']}: "
                f"fund_house={house} fund_manager={manager} "
                f"equity {p['equity_pct']:.1f}% "
                f"gold {p['gold_pct']:.1f}% "
                f"bond {p['bond_pct']:.1f}% "
                f"cash {p['cash_pct']:.1f}% "
                f"other {p['other_pct']:.1f}% "
                f"— top holdings: {p['top5_text']}"
            )
            texts.append(enriched)
        else:
            texts.append(base)

    vectors = _embed(texts)

    points = [
        PointStruct(
            id=_pid("mf_profile", p["fund_name"], p["as_of_month"]),
            vector=vectors[i],
            payload={
                "data_type":           "mf_fund_profile",
                "fund_name":           p["fund_name"],
                "scheme_code":         p["scheme_code"],
                "as_of_month":         p["as_of_month"],
                "as_of_timestamp":     _to_ts(p["as_of_month_raw"]),
                "equity_pct":          p["equity_pct"],
                "gold_pct":            p["gold_pct"],
                "bond_pct":            p["bond_pct"],
                "cash_pct":            p["cash_pct"],
                "other_pct":           p["other_pct"],
                "asset_type_primary":  p["asset_type_primary"],
                "total_holdings":      p["total_holdings"],
                "top5_text":           p["top5_text"],
                "text":                texts[i],
                # Fund manager enrichment (empty strings if metadata unavailable)
                "lead_fund_manager":   meta_by_code.get(str(p.get("scheme_code", "")), {}).get("lead_fund_manager", ""),
                "fund_house":          meta_by_code.get(str(p.get("scheme_code", "")), {}).get("fund_house", ""),
                "scheme_category":     meta_by_code.get(str(p.get("scheme_code", "")), {}).get("scheme_category", ""),
                "benchmark_index":     meta_by_code.get(str(p.get("scheme_code", "")), {}).get("benchmark_index", ""),
            },
        )
        for i, p in enumerate(profiles)
    ]

    try:
        client.upsert(collection_name=_PROFILES_COLLECTION, points=points)
        log.info("MFVector: upserted %d fund profile points", len(points))
    except Exception as e:
        log.warning("MFVector: profiles upsert failed: %s", e)


# ── Public write API ──────────────────────────────────────────────────────────

def vectorize_holdings(rows: list[dict]) -> None:
    """
    Vectorize mf_holdings rows into both Qdrant collections (fire-and-forget).
    Safe to call with an empty list — no-ops gracefully.
    """
    if not rows or not _qdrant_available:
        return
    rows_copy = list(rows)
    t1 = threading.Thread(target=_do_vectorize_holdings, args=(rows_copy,), daemon=True)
    t2 = threading.Thread(target=_do_vectorize_profiles, args=(rows_copy,), daemon=True)
    t1.start()
    t2.start()


# ── Public read API ───────────────────────────────────────────────────────────

def find_funds_holding_security(
    query: str,
    k: int = 10,
    asset_type: str = "",
) -> list[dict]:
    """
    Semantic search in `mf_holdings`: which funds hold a security matching *query*?

    Args:
        query:      Free-text — stock name, ISIN, sector, asset type
                    e.g. "HDFC Bank", "gold ETF", "INE040A01034"
        k:          Max results (default 10)
        asset_type: Optional filter — equity | gold | bond | cash | other

    Returns:
        List of dicts with fund_name, security_name, isin, asset_type,
        pct_of_nav, market_value_cr, as_of_month, similarity.
    """
    if not _qdrant_available:
        return []
    client = _get_client()
    if client is None or not _ensure_collection(_HOLDINGS_COLLECTION, is_holdings=True):
        return []

    vecs = _embed([query])
    if all(v == 0.0 for v in vecs[0]):
        return []

    must = []
    if asset_type:
        must.append(FieldCondition(key="asset_type", match=MatchValue(value=asset_type)))

    try:
        hits = client.query_points(
            collection_name=_HOLDINGS_COLLECTION,
            query=vecs[0],
            query_filter=Filter(must=must) if must else None,
            limit=k,
            with_payload=True,
        )
        return [
            {
                "fund_name":       h.payload.get("fund_name", ""),
                "security_name":   h.payload.get("security_name", ""),
                "isin":            h.payload.get("isin", ""),
                "asset_type":      h.payload.get("asset_type", ""),
                "pct_of_nav":      h.payload.get("pct_of_nav", 0.0),
                "market_value_cr": h.payload.get("market_value_cr", 0.0),
                "as_of_month":     h.payload.get("as_of_month", ""),
                "similarity":      float(h.score),
            }
            for h in hits.points
        ]
    except Exception as e:
        log.warning("MFVector: find_funds_holding_security failed: %s", e)
        return []


def find_similar_fund_profiles(
    fund_name: str,
    as_of_month: str = "",
    k: int = 5,
) -> list[dict]:
    """
    Find funds with a similar multi-asset portfolio composition to *fund_name*.

    Queries `mf_fund_profiles` by embedding the target fund's latest profile
    description and returning the closest matches.

    Args:
        fund_name:   Fund name key (e.g. DSP_MULTI_ASSET, ICICI_MULTI_ASSET)
        as_of_month: YYYY-MM — if blank, latest available profile is used
        k:           Number of similar funds to return

    Returns:
        List of dicts with fund_name, equity_pct, gold_pct, bond_pct,
        cash_pct, top5_text, as_of_month, similarity.
    """
    if not _qdrant_available:
        return []
    client = _get_client()
    if client is None or not _ensure_collection(_PROFILES_COLLECTION, is_holdings=False):
        return []

    # Build query text from fund name + optional month context
    query = f"{fund_name} multi-asset portfolio {as_of_month or ''}".strip()
    vecs = _embed([query])
    if all(v == 0.0 for v in vecs[0]):
        return []

    # Exclude the exact fund from results so we get *different* funds
    must_not = [FieldCondition(key="fund_name", match=MatchValue(value=fund_name))]

    try:
        hits = client.query_points(
            collection_name=_PROFILES_COLLECTION,
            query=vecs[0],
            query_filter=Filter(must_not=must_not),
            limit=k,
            with_payload=True,
        )
        return [
            {
                "fund_name":    h.payload.get("fund_name", ""),
                "as_of_month":  h.payload.get("as_of_month", ""),
                "equity_pct":   h.payload.get("equity_pct", 0.0),
                "gold_pct":     h.payload.get("gold_pct", 0.0),
                "bond_pct":     h.payload.get("bond_pct", 0.0),
                "cash_pct":     h.payload.get("cash_pct", 0.0),
                "top5_text":    h.payload.get("top5_text", ""),
                "primary":      h.payload.get("asset_type_primary", ""),
                "similarity":   float(h.score),
                "lead_fund_manager": h.payload.get("lead_fund_manager", ""),
                "fund_house": h.payload.get("fund_house", ""),
                "scheme_category": h.payload.get("scheme_category", ""),
            }
            for h in hits.points
        ]
    except Exception as e:
        log.warning("MFVector: find_similar_fund_profiles failed: %s", e)
        return []


def find_funds_by_category(
    asset_type: str,
    query: str = "",
    k: int = 10,
) -> list[dict]:
    """
    Find fund profiles dominated by a given asset category.

    Args:
        asset_type:  equity | gold | bond | cash | other
                     Used as both a payload filter AND query enrichment.
        query:       Additional free-text context
                     (e.g. "commodity precious metal", "large-cap growth")
        k:           Max results

    Returns:
        List of dicts with fund_name, equity_pct, gold_pct, bond_pct,
        cash_pct, top5_text, as_of_month, similarity.
    """
    if not _qdrant_available:
        return []
    client = _get_client()
    if client is None or not _ensure_collection(_PROFILES_COLLECTION, is_holdings=False):
        return []

    full_query = f"{asset_type} allocation exposure {query}".strip()
    vecs = _embed([full_query])
    if all(v == 0.0 for v in vecs[0]):
        return []

    must = [FieldCondition(key="asset_type_primary", match=MatchValue(value=asset_type))]

    try:
        hits = client.query_points(
            collection_name=_PROFILES_COLLECTION,
            query=vecs[0],
            query_filter=Filter(must=must),
            limit=k,
            with_payload=True,
        )
        return [
            {
                "fund_name":   h.payload.get("fund_name", ""),
                "as_of_month": h.payload.get("as_of_month", ""),
                "equity_pct":  h.payload.get("equity_pct", 0.0),
                "gold_pct":    h.payload.get("gold_pct", 0.0),
                "bond_pct":    h.payload.get("bond_pct", 0.0),
                "cash_pct":    h.payload.get("cash_pct", 0.0),
                "top5_text":   h.payload.get("top5_text", ""),
                "similarity":  float(h.score),
            }
            for h in hits.points
        ]
    except Exception as e:
        log.warning("MFVector: find_funds_by_category failed: %s", e)
        return []
