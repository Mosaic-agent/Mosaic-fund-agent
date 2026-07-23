"""
src/workflows/state.py
───────────────────────
Shared TypedDict ancestor for every StateGraph workflow.

Each workflow keeps its own fields (fetch results, control flags, output); this
adds one common, optional field — `datasets` — an auditable record of what a
workflow fetched via `_par_datasets()` (see `.base`), alongside whatever named
fields the workflow's own synthesis prompts read directly.
"""
from __future__ import annotations

from typing import TypedDict

from .context_manager import DatasetRef


class MosaicState(TypedDict, total=False):
    """Common optional fields shared across workflow TypedDicts.

    `total=False`: subclasses aren't required to populate fields they don't
    use (e.g. `portfolio_analysis` has no `question`; `multi_fund_consensus`
    takes a `period` instead). This is a typing ancestor only — TypedDict has
    no runtime enforcement, so adding it doesn't change existing behavior.
    """

    question: str
    plan: list[str]
    plan_id: str
    datasets: dict[str, DatasetRef]
