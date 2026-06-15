"""
tests/test_chat_persistence.py
──────────────────────────────
Unit tests verifying SqliteSaver usage and CLI command parameters for chat persistence.
"""
import unittest
from unittest.mock import patch, MagicMock
import os

from src.commands.chat_cmd import run_chat_loop
from typer.testing import CliRunner
from src.main import app

class TestChatPersistence(unittest.TestCase):
    @patch("langgraph.checkpoint.sqlite.SqliteSaver.from_conn_string")
    @patch("src.commands.chat_cmd._run_chat_loop_inner")
    def test_run_chat_loop_uses_sqlite_saver(self, mock_inner, mock_sqlite_from_conn):
        # Set up mock checkpointer context manager
        mock_checkpointer = MagicMock()
        mock_cm = MagicMock()
        mock_cm.__enter__.return_value = mock_checkpointer
        mock_sqlite_from_conn.return_value = mock_cm

        mock_console = MagicMock()
        run_chat_loop(console=mock_console, thread_id="test-thread-123")

        # Check that from_conn_string was called with "output/checkpoints.db"
        mock_sqlite_from_conn.assert_called_once_with("output/checkpoints.db")
        # Check that context manager entered
        mock_cm.__enter__.assert_called_once()
        # Check that inner function was called with the checkpointer and thread_id
        mock_inner.assert_called_once_with(console=mock_console, checkpointer=mock_checkpointer, thread_id="test-thread-123")
        # Check that context manager exited
        mock_cm.__exit__.assert_called_once()

    @patch("src.commands.chat_cmd.run_chat_loop")
    def test_cli_chat_command_parameters(self, mock_run_chat_loop):
        runner = CliRunner()
        # Run standard chat with no thread ID
        with patch("src.main._check_config", return_value=True), \
             patch("src.main.console", None):
            result = runner.invoke(app, ["chat"])
            self.assertEqual(result.exit_code, 0)
            mock_run_chat_loop.assert_called_with(None, thread_id=None)

        # Run chat with thread ID option
        with patch("src.main._check_config", return_value=True), \
             patch("src.main.console", None):
            result = runner.invoke(app, ["chat", "--thread-id", "resume-id"])
            self.assertEqual(result.exit_code, 0)
            mock_run_chat_loop.assert_called_with(None, thread_id="resume-id")

        # Run chat with -t short flag option
        with patch("src.main._check_config", return_value=True), \
             patch("src.main.console", None):
            result = runner.invoke(app, ["chat", "-t", "resume-id-short"])
            self.assertEqual(result.exit_code, 0)
            mock_run_chat_loop.assert_called_with(None, thread_id="resume-id-short")

if __name__ == "__main__":
    unittest.main()
