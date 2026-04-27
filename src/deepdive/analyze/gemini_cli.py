"""
src/deepdive/analyze/gemini_cli.py
───────────────────────────────────
Phase 6: Prompt assembly for Gemini CLI.

This module does NOT invoke the Gemini CLI as a subprocess.  Instead the entire
`python src/main.py deepdive <TICKER>` command is meant to be run directly by
Gemini CLI as a shell tool.  Phase 6 assembles the full prompt for each section
(static rules + dataset JSON + task template) and prints them to stdout.  Gemini
CLI reads that output and generates the narrative in its own response.

Public API:
    assemble_prompt(section_key, dataset_json, prompts_dir) -> str
        Build the complete prompt string for one section.

    SECTION_KEYS  — ordered list of the 7 section identifiers
    PROMPTS_DIR   — default Path to the bundled prompt templates
"""

from __future__ import annotations

import logging
from pathlib import Path

log = logging.getLogger(__name__)

# Default prompts directory — alongside this file in analyze/prompts/
PROMPTS_DIR: Path = Path(__file__).parent / "prompts"

# Report section keys in document order
SECTION_KEYS: list[str] = [
    "core_business",
    "financials",
    "competitors",
    "investments",
    "execution",
    "valuation",
    "talent",
]

# ── Anti-hallucination + formatting rules injected into every prompt ──────────

_STATIC_RULES = """\
You are a senior equity analyst writing a structured research note.
You will receive a JSON dataset object and a specific analytical task.

RULES — follow strictly:
1. CITATIONS: Every number, percentage, or metric you state must be immediately
   followed by a source tag: [src: <field.path>] using dot-notation that exactly
   matches a key in the provided JSON.
   Example: "Revenue grew to $7,206M [src: financials[2].revenue_usd_m] in FY2026."
2. MISSING DATA: If a metric is absent or null in the dataset, write "not disclosed"
   — never estimate, interpolate, or invent a figure.
3. SCOPE: Do not reference any events, filings, or data outside the provided JSON.
   Do not mention today's date or make market predictions.
4. FORMAT: Output pure Markdown starting directly with the section heading (## N. Title).
   No preamble ("Here is the analysis…"), no "In conclusion" filler.
5. LENGTH: 350–600 words per section. Dense, analyst-register prose.
6. UNITS: Figures are in USD millions unless explicitly stated otherwise in the JSON.
""".strip()


# ── Prompt assembly ───────────────────────────────────────────────────────────

def assemble_prompt(
    section_key: str,
    dataset_json: str,
    prompts_dir: Path,
) -> str:
    """
    Build the complete prompt string for one report section.

    Concatenates:
        static rules  +  dataset JSON  +  section task template

    Args:
        section_key:  One of SECTION_KEYS (e.g. "financials").
        dataset_json: Serialised CompanyDataset JSON string.
        prompts_dir:  Directory containing <section_key>.txt template files.

    Returns:
        Single string ready to be printed to stdout or passed to an LLM.

    Raises:
        FileNotFoundError: Prompt template file is missing.
    """
    prompt_file = prompts_dir / f"{section_key}.txt"
    if not prompt_file.exists():
        raise FileNotFoundError(
            f"Prompt template not found: {prompt_file}. "
            f"Expected one of: {sorted(f.name for f in prompts_dir.glob('*.txt'))}"
        )
    task = prompt_file.read_text(encoding="utf-8")

    return (
        f"{_STATIC_RULES}\n\n"
        f"---\n\n"
        f"DATASET (JSON):\n{dataset_json}\n\n"
        f"---\n\n"
        f"TASK:\n{task}"
    )
