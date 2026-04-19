import pytest
from unittest.mock import MagicMock, patch
from src.commands.import_cmd import ImportDataCommand

@patch("src.commands.import_cmd.ClickHouseImporter")
def test_import_command_execute_snapshots(mock_importer_class):
    mock_importer = mock_importer_class.return_value
    mock_importer.snapshot.return_value = [{"source": "test", "symbol": "TEST", "last_date": "2023-01-01"}]
    mock_importer.run.return_value = {"status": "success"}

    cmd = ImportDataCommand(categories=["test"])
    result = cmd.execute()

    assert result == {"status": "success"}
    assert cmd._snapshot == [{"source": "test", "symbol": "TEST", "last_date": "2023-01-01"}]
    mock_importer.snapshot.assert_called_once()
    mock_importer.run.assert_called_once_with(
        categories=["test"], lookback_days=3650, full_reimport=False, dry_run=False
    )

@patch("src.commands.import_cmd.ClickHouseImporter")
def test_import_command_undo_restores(mock_importer_class):
    mock_importer = mock_importer_class.return_value
    snapshot = [{"source": "test", "symbol": "TEST", "last_date": "2023-01-01"}]
    
    cmd = ImportDataCommand(categories=["test"])
    cmd._snapshot = snapshot
    
    cmd.undo()
    
    mock_importer.restore.assert_called_once_with(snapshot)

def test_import_command_undo_fails_without_snapshot():
    cmd = ImportDataCommand(categories=["test"])
    with pytest.raises(RuntimeError, match="No snapshot"):
        cmd.undo()
