"""
src/scripts/portfolio/stock_dossier/report.py
───────────────────────────────────────────────
Markdown assembly and artifact persistence. Section order and numbering come
from REGISTRY, not a hardcoded list here — reordering or inserting a section
in registry.py is all that's needed to change the report's structure.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from .context import DossierContext
from .registry import REGISTRY
from .utils import get_artifact_dir


def assemble_report(ctx: DossierContext, results: dict[str, str]) -> str:
    header = (
        f"# 🏛️ {ctx.company_name} (`{ctx.symbol}`) — Institutional Research Dossier\n\n"
        f"**As of:** {datetime.now().strftime('%B %d, %Y')} | **CMP:** ₹{ctx.cur_price:,.2f} | "
        f"**Market Cap:** ₹{ctx.mcap_cr:,.2f} Cr\n\n"
        f"**Snapshot:** {ctx.summary_hdr}  \n"
        f"**Price source:** {ctx.price_source}\n\n---\n\n"
    )

    parts = []
    n = 1
    for section in REGISTRY:
        body = results.get(section.key)
        if not body:
            continue
        parts.append(f"## {n}. {section.title}\n\n{body}")
        n += 1

    return header + "\n\n---\n\n".join(parts)


def save_report(symbol: str, report_md: str, artifact_dir: str | None = None) -> str:
    target_dir = Path(artifact_dir) if artifact_dir else get_artifact_dir()
    target_dir.mkdir(parents=True, exist_ok=True)
    target_file = target_dir / f"{symbol.lower()}_institutional_research.md"
    target_file.write_text(report_md, encoding="utf-8")
    return str(target_file)
