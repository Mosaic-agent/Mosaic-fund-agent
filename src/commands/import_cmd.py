from dataclasses import dataclass, field
from typing import Any, Optional
from src.commands.base import Command
from src.importer.clickhouse import ClickHouseImporter

@dataclass
class ImportDataCommand(Command):
    categories: list[str]
    lookback_days: int = 3650
    full_reimport: bool = False
    dry_run: bool = False
    _snapshot: Optional[list[dict[str, Any]]] = field(default=None, repr=False)

    def execute(self) -> dict[str, Any]:
        importer = ClickHouseImporter()
        # snapshot current state (watermarks) before mutating
        self._snapshot = importer.snapshot(self.lookback_days)
        return importer.run(
            categories=self.categories,
            lookback_days=self.lookback_days,
            full_reimport=self.full_reimport,
            dry_run=self.dry_run,
        )

    def undo(self) -> None:
        if self._snapshot is None:
            raise RuntimeError("No snapshot — execute() was never called")
        ClickHouseImporter().restore(self._snapshot)
