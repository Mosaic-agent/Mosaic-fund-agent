"""
src/tools/concall_transcripts.py
─────────────────────────────────
Cache-aside pipeline for earnings concall transcript commentary.

ClickHouse (market_data.concall_transcript_index) tracks which (symbol,
period_label) pairs have already been downloaded/chunked/embedded, so a
repeat call for a symbol only fetches whatever quarters are new —
never re-downloads or re-embeds a quarter already on file. The PDF text
itself isn't duplicated in ClickHouse; only the embedded chunks (in Qdrant)
and this index row exist per quarter.

Source: Screener.in's "Concalls" panel → free, BSE-hosted transcript PDFs.
StockScans also tracks concall existence (/api/scans/concall/history) but
gates the actual content (/api/scans/concall/notes) behind a paid
subscription this project's account doesn't have — confirmed by live probe,
not assumed. Screener's own "AI Summary" is the same kind of paywalled
feature; only its "Transcript" link (raw BSE PDF) is free.
"""

from __future__ import annotations

import io
import logging

import requests
from langchain_core.tools import tool

from src.data_importer.tool_fetchers.earnings_scraper import fetch_screener_concall_links
from src.db.concall_vector import has_chunks, search_concall_commentary, store_transcript_chunks

logger = logging.getLogger(__name__)

_PDF_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
}
_PDF_TIMEOUT = 30


def _extract_pdf_text(url: str) -> str:
    import pdfplumber

    try:
        resp = requests.get(url, headers=_PDF_HEADERS, timeout=_PDF_TIMEOUT)
        resp.raise_for_status()
    except Exception as exc:
        logger.warning("Concall transcript PDF download failed (%s): %s", url, exc)
        return ""
    try:
        with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
            return "\n".join(p.extract_text() or "" for p in pdf.pages)
    except Exception as exc:
        logger.warning("Concall transcript PDF parse failed (%s): %s", url, exc)
        return ""


def _already_indexed(symbol: str, period_label: str) -> bool:
    """ClickHouse-first existence check (cheap); falls back to a Qdrant scroll
    only if ClickHouse is unreachable, so a transient DB hiccup doesn't cause
    a silent re-embed of a quarter already stored."""
    try:
        from src.db.pool import query_df
        df = query_df(
            "SELECT count() AS n FROM market_data.concall_transcript_index FINAL "
            f"WHERE symbol = '{symbol}' AND period_label = '{period_label}'"
        )
        if not df.empty:
            return int(df.iloc[0]["n"]) > 0
    except Exception as exc:
        logger.debug("concall_transcript_index check failed, falling back to Qdrant: %s", exc)
    return has_chunks(symbol, period_label)


def refresh_concall_transcripts(symbol: str, max_quarters: int = 4) -> dict:
    """
    Ensure the last `max_quarters` concall transcripts for `symbol` are
    chunked, embedded, and stored in Qdrant. Cache-aside: quarters already
    recorded in concall_transcript_index are skipped entirely (no PDF
    download, no re-embed).

    Returns a summary dict: {symbol, quarters_checked, quarters_fetched,
    quarters_skipped_no_link, chunks_stored}.
    """
    sym = symbol.strip().upper()
    links = fetch_screener_concall_links(sym, max_quarters=max_quarters)

    fetched, skipped_no_link, chunks_stored = 0, 0, 0
    new_index_rows = []

    for row in links:
        period_label = row["period_label"]
        if not period_label:
            continue
        if _already_indexed(sym, period_label):
            continue
        if not row["transcript_url"]:
            skipped_no_link += 1
            continue

        text = _extract_pdf_text(row["transcript_url"])
        if not text.strip():
            continue

        n_chunks = store_transcript_chunks(sym, period_label, row["transcript_url"], text)
        if n_chunks > 0:
            fetched += 1
            chunks_stored += n_chunks
            new_index_rows.append({
                "symbol": sym, "period_label": period_label,
                "pdf_url": row["transcript_url"], "chunk_count": n_chunks,
            })

    if new_index_rows:
        from src.importer.clickhouse import ClickHouseImporter
        ch = ClickHouseImporter()  # defaults to the pool singleton
        try:
            ch.ensure_schema()
            ch.insert_concall_transcript_index(new_index_rows)
        finally:
            ch.close()

    return {
        "symbol": sym,
        "quarters_checked": len(links),
        "quarters_fetched": fetched,
        "quarters_skipped_no_link": skipped_no_link,
        "chunks_stored": chunks_stored,
    }


@tool
def search_concall_transcripts(symbol: str, query: str, max_quarters: int = 4, k: int = 5) -> list[dict]:
    """
    Semantic search over a company's earnings concall transcripts (management
    commentary, Q&A) for a natural-language question.

    Ensures the last `max_quarters` transcripts are cached locally (Qdrant)
    before searching — cheap no-op if already cached.

    Args:
        symbol:       NSE symbol, e.g. "RELIANCE".
        query:        What to look for, e.g. "margin guidance", "capex plans",
                      "management commentary on demand outlook".
        max_quarters: How many recent quarters to ensure are cached (default 4 ≈ 1 year).
        k:            Number of matching transcript excerpts to return.

    Returns:
        List of dicts: {chunk_text, period_label, pdf_url, similarity}, most
        relevant first. Excerpts are verbatim transcript text — never
        LLM-summarized.
    """
    sym = symbol.strip().upper()
    refresh_concall_transcripts(sym, max_quarters=max_quarters)
    return search_concall_commentary(sym, query, k=k)


CONCALL_TRANSCRIPT_TOOLS = [search_concall_transcripts]
