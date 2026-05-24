from dataclasses import dataclass, field
from typing import Any, Optional
from src.commands.base import Command
from src.importer.clickhouse import ClickHouseImporter

def _make_importer() -> ClickHouseImporter:
    from config.settings import settings
    return ClickHouseImporter(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        database=settings.clickhouse_database,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
    )

@dataclass
class ImportDataCommand(Command):
    categories: list[str]
    lookback_days: int = 3650
    full_reimport: bool = False
    dry_run: bool = False
    _snapshot: Optional[list[dict[str, Any]]] = field(default=None, repr=False)

    def execute(self) -> dict[str, Any]:
        importer = _make_importer()
        importer.ensure_schema()
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
        _make_importer().restore(self._snapshot)
