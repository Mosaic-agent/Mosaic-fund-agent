"""
src/scripts/portfolio/stock_dossier/base.py
───────────────────────────────────────────
Strategy interface for one dossier report segment — same pattern as
`SignalSource` (src/agents/signal_sources.py) and `Fetcher`
(src/data_importer/fetchers/adapters.py): a small ABC plus a registry list the
orchestrator loops over. Add a new capability by subclassing `DossierSection`
in sections/<name>.py and appending an instance to REGISTRY in registry.py —
no other file needs to change.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from .context import DossierContext


class DossierSection(ABC):
    key: str
    title: str

    def enabled(self, ctx: DossierContext) -> bool:
        """Override to gate a section behind context state (e.g. the live
        order-book section only runs when --live was passed)."""
        return True

    @abstractmethod
    def fetch(self, ctx: DossierContext) -> str:
        """Return this section's markdown body (no '## N. Title' heading —
        report.py synthesises and numbers that from `title` and registry
        position, so sections stay reorderable without renumbering)."""
        raise NotImplementedError

    def run(self, ctx: DossierContext) -> str:
        """Template-method wrapper: isolates one section's failure from the
        rest of the report instead of letting it abort the whole dossier."""
        try:
            return self.fetch(ctx)
        except Exception as exc:
            return f"*{self.key} section failed: {exc}*"
