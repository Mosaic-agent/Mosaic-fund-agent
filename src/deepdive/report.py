"""
src/deepdive/report.py
───────────────────────
Phase 7: Report assembly.

Reads:
  - output/deepdive/<TICKER>/<DATE>/dataset.json          ← structured data
  - output/deepdive/<TICKER>/<DATE>/sections/<key>.md     ← Gemini-generated narrative (7 files)
  - output/deepdive/cache/<TICKER>/<DATE>/                ← raw cache files (for sources.md)

Writes:
  - output/deepdive/<TICKER>/<DATE>/report.md             ← full research report
  - output/deepdive/<TICKER>/<DATE>/sources.md            ← provenance index

report.md structure
───────────────────
  # {Company} ({TICKER}) — Deep-Dive Research Note
  > Generated: {date}
  ---
  ## Table of Contents
  ---
  ## 1. Core Business        ← sections/core_business.md
  ## 2. Financial Performance ← sections/financials.md
  ## 3. Competitive Landscape ← sections/competitors.md
  ## 4. Investments & Growth  ← sections/investments.md
  ## 5. Execution Quality     ← sections/execution.md
  ## 6. Valuation             ← sections/valuation.md
  ## 7. Talent & Workforce    ← sections/talent.md
  ---
  ## Data Provenance          ← inline summary from sources[]

sources.md structure
────────────────────
  # Sources — {TICKER} {DATE}
  One row per cache file: | File | Type | Size | dataset fields |
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from pathlib import Path

log = logging.getLogger(__name__)

# Maps section key → expected .md filename (order determines report order)
SECTION_ORDER: list[tuple[str, str]] = [
    ("core_business",  "## 1. Core Business"),
    ("financials",     "## 2. Financial Performance"),
    ("competitors",    "## 3. Competitive Landscape"),
    ("investments",    "## 4. Investments & Growth"),
    ("execution",      "## 5. Execution Quality"),
    ("valuation",      "## 6. Valuation"),
    ("talent",         "## 7. Talent & Workforce"),
]

# Type labels for the sources table, matched by file extension / name pattern
_SOURCE_TYPES: list[tuple[str, str]] = [
    ("10-K",    "SEC 10-K Annual Report"),
    ("10-Q",    "SEC 10-Q Quarterly Report"),
    ("8-K",     "SEC 8-K Current Report"),
    ("DEF14A",  "SEC DEF 14A Proxy Statement"),
    ("xbrl_",   "XBRL Financial Data (JSON)"),
    ("section1","10-K Item 1 Business (text)"),
    ("section7","10-K Item 7 MD&A (HTML)"),
    ("exec_comp","Exec Compensation (JSON)"),
    ("workday", "Workday Job Postings (JSON)"),
    ("market",  "Market / Peer Data (JSON)"),
    ("company_meta", "Company Metadata (JSON)"),
    ("filings_index", "Filings Index (JSON)"),
]


def _label_cache_file(filename: str) -> str:
    for prefix, label in _SOURCE_TYPES:
        if filename.startswith(prefix):
            return label
    return "Cache file"


def _dataset_fields_for(filename: str, sources: list[dict]) -> str:
    """Return comma-joined dataset field names that cite this file."""
    fields = [s.get("field", "") for s in sources if Path(s.get("file", "")).name == filename]
    return ", ".join(f for f in fields if f) or "—"


def assemble_report(
    out_dir: Path,
    cache_dir: Path,
    *,
    dataset_path: Path | None = None,
    sections_dir: Path | None = None,
) -> tuple[Path, Path]:
    """
    Assemble report.md and sources.md from dataset.json + sections/*.md.

    Args:
        out_dir:      output/deepdive/<TICKER>/<DATE>/
        cache_dir:    output/deepdive/cache/<TICKER>/<DATE>/
        dataset_path: override path to dataset.json (default: out_dir/dataset.json)
        sections_dir: override path to sections/ dir (default: out_dir/sections/)

    Returns:
        (report_path, sources_path) — paths of the two written files.

    Raises:
        FileNotFoundError: dataset.json missing.
    """
    dataset_path = dataset_path or (out_dir / "dataset.json")
    sections_dir = sections_dir or (out_dir / "sections")

    if not dataset_path.exists():
        raise FileNotFoundError(f"dataset.json not found: {dataset_path}")

    dataset = json.loads(dataset_path.read_text(encoding="utf-8"))
    ticker = dataset.get("ticker", "?")
    company_name = dataset.get("company_name", ticker)
    report_date = dataset.get("report_date", "")
    fiscal_year_end = dataset.get("fiscal_year_end", "")
    sources: list[dict] = dataset.get("sources", [])

    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")

    # ── Collect section markdown ───────────────────────────────────────────────
    section_blocks: list[str] = []
    missing_sections: list[str] = []

    for key, _heading in SECTION_ORDER:
        section_file = sections_dir / f"{key}.md"
        if section_file.exists():
            content = section_file.read_text(encoding="utf-8").strip()
            if content.startswith("<!--"):
                # Failed generation placeholder — include with warning header
                block = f"_{key} section could not be generated — see placeholder below._\n\n{content}"
            else:
                block = content
        else:
            missing_sections.append(key)
            block = f"_{key} section not yet generated._"

        section_blocks.append(block)

    if missing_sections:
        log.warning("report: missing sections: %s", ", ".join(missing_sections))

    # ── Table of Contents ──────────────────────────────────────────────────────
    toc_lines = ["## Table of Contents", ""]
    for _key, heading in SECTION_ORDER:
        # Convert "## 1. Core Business" → "1. [Core Business](#1-core-business)"
        title = heading.lstrip("# ").strip()
        anchor = title.lower().replace(" ", "-").replace(".", "").replace("&", "")
        toc_lines.append(f"- [{title}](#{anchor})")
    toc_lines.append("")

    # ── Header block ──────────────────────────────────────────────────────────
    header_lines = [
        f"# {company_name} ({ticker}) — Deep-Dive Research Note",
        "",
        f"> **Report Date:** {report_date}  ",
        f"> **Fiscal Year End:** {fiscal_year_end}  ",
        f"> **Generated:** {generated_at}  ",
        f"> **Sources:** {len(sources)} dataset fields traced to {len(list(cache_dir.glob('*')))} cache files  ",
        "",
        "---",
        "",
    ]

    # ── Provenance summary ────────────────────────────────────────────────────
    provenance_lines = ["## Data Provenance", ""]
    if sources:
        provenance_lines += [
            "| Field | Source File | Locator |",
            "|-------|-------------|---------|",
        ]
        for s in sources:
            field = s.get("field", "—")
            file_ = Path(s.get("file", "")).name
            locator = s.get("locator", "—")
            provenance_lines.append(f"| `{field}` | `{file_}` | {locator} |")
    else:
        provenance_lines.append("_No source entries recorded in dataset.json._")

    # ── Assemble report.md ────────────────────────────────────────────────────
    parts: list[str] = []
    parts.append("\n".join(header_lines))
    parts.append("\n".join(toc_lines))

    for block in section_blocks:
        parts.append(block)
        parts.append("\n---\n")

    parts.append("\n".join(provenance_lines))

    report_text = "\n\n".join(parts)
    report_path = out_dir / "report.md"
    report_path.write_text(report_text, encoding="utf-8")
    log.info("report: wrote %s (%d chars)", report_path, len(report_text))

    # ── Assemble sources.md ───────────────────────────────────────────────────
    cache_files = sorted(cache_dir.iterdir()) if cache_dir.exists() else []

    sources_lines = [
        f"# Sources — {ticker} {report_date}",
        "",
        f"Cache directory: `{cache_dir}`",
        "",
        "| File | Type | Size | Dataset Fields |",
        "|------|------|------|----------------|",
    ]
    for f in cache_files:
        if not f.is_file():
            continue
        size_kb = f.stat().st_size / 1024
        label = _label_cache_file(f.name)
        fields = _dataset_fields_for(f.name, sources)
        sources_lines.append(f"| `{f.name}` | {label} | {size_kb:.1f} KB | {fields} |")

    sources_lines += [
        "",
        "---",
        "",
        "## Section Files",
        "",
        "| Section | File | Size |",
        "|---------|------|------|",
    ]
    for key, heading in SECTION_ORDER:
        sf = sections_dir / f"{key}.md"
        if sf.exists():
            size_kb = sf.stat().st_size / 1024
            sources_lines.append(f"| {heading.lstrip('# ')} | `sections/{key}.md` | {size_kb:.1f} KB |")
        else:
            sources_lines.append(f"| {heading.lstrip('# ')} | _not generated_ | — |")

    sources_text = "\n".join(sources_lines)
    sources_path = out_dir / "sources.md"
    sources_path.write_text(sources_text, encoding="utf-8")
    log.info("report: wrote %s (%d lines)", sources_path, sources_text.count("\n"))

    return report_path, sources_path
