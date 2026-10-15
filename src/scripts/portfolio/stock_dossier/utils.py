"""
src/scripts/portfolio/stock_dossier/utils.py
─────────────────────────────────────────────
Shared helpers for the consolidated `stock-research` dossier: artifact-dir
resolution (ported verbatim from the deleted run_stock_quant_workflow.py,
which fixed a real bug — reports silently lost on container teardown) and
AMC display-name canonicalization (ported from the deleted
stock_quant_deepdive_cli.py's `_detect_amc`).
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent.parent


def get_artifact_dir() -> Path:
    """Resolve the report output directory.

    Order: ANTIGRAVITY_ARTIFACT_DIR env -> /app/output (bind-mounted into the
    mosaic container) -> <repo>/output.

    Only src/ and output/ are bind-mounted in the container, so a path outside
    those (e.g. a hardcoded home-directory path) is created *inside* the
    container and silently lost on teardown while the caller still sees
    "SUCCESS" and a host-looking path.
    """
    env_dir = os.getenv("ANTIGRAVITY_ARTIFACT_DIR")
    if env_dir and os.path.exists(env_dir):
        return Path(env_dir)
    for cand in (Path("/app/output"), ROOT_DIR / "output"):
        if cand.exists():
            return cand
    fallback = ROOT_DIR / "output"
    fallback.mkdir(parents=True, exist_ok=True)
    return fallback


def make_markdown_table(df: pd.DataFrame) -> str:
    headers = list(df.columns)
    lines = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    for _, row in df.iterrows():
        cells = []
        for val in row:
            if isinstance(val, float):
                cells.append(f"{val:.2f}")
            elif val is None:
                cells.append("N/A")
            else:
                cells.append(str(val))
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


_AMC_PREFIXES = [
    ("DSP", "DSP"),
    ("NIPPON", "Nippon"),
    ("RELIANCE", "Nippon"),
    ("HDFC", "HDFC"),
    ("ICICI", "ICICI Pru"),
    ("KOTAK", "Kotak"),
    ("QUANT", "Quant"),
    ("BAJAJ", "Bajaj"),
    ("SBI", "SBI"),
    ("AXIS", "Axis"),
    ("MOTILAL", "Motilal"),
    ("ABAKKUS", "Abakkus"),
    ("ABACUS", "Abakkus"),
    ("HELIOS", "Helios"),
    ("INVESCO", "Invesco"),
    ("CANARA", "Canara Robeco"),
    ("MIRAE", "Mirae"),
]


def detect_amc(fund_name: str) -> str:
    """Map a raw mf_holdings.fund_name to a short AMC display label."""
    fn = fund_name.upper()
    for prefix, label in _AMC_PREFIXES:
        if fn.startswith(prefix) or prefix in fn:
            return label
    return "Other"
