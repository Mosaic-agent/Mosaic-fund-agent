import sys
import unittest
from unittest.mock import patch, MagicMock
import subprocess

from src.tools.code_tools import (
    _parse_missing_module,
    _run_subprocess,
    install_python_dependency,
)

class TestCodeToolsDependencyHandler(unittest.TestCase):
    def test_parse_missing_module_modulenotfound(self):
        output = "Traceback (most recent call last):\n  File \"<stdin>\", line 1, in <module>\nModuleNotFoundError: No module named 'some_fake_package'"
        self.assertEqual(_parse_missing_module(output), "some_fake_package")

        output_double_quotes = 'ModuleNotFoundError: No module named "another_package"'
        self.assertEqual(_parse_missing_module(output_double_quotes), "another_package")

    def test_parse_missing_module_importerror(self):
        output = "ImportError: No module named some_old_module"
        self.assertEqual(_parse_missing_module(output), "some_old_module")

    @patch("src.tools.code_tools._add_to_requirements")
    @patch("subprocess.run")
    def test_install_python_dependency_success(self, mock_run, mock_add_to_reqs):
        mock_res = MagicMock()
        mock_res.returncode = 0
        mock_run.return_value = mock_res

        result = install_python_dependency.invoke({"package_name": "some_fake_package"})
        self.assertIn("Successfully installed some_fake_package", result)
        mock_run.assert_called_once_with(
            [sys.executable, "-m", "pip", "install", "some_fake_package"],
            capture_output=True,
            text=True,
            timeout=120
        )
        mock_add_to_reqs.assert_called_once_with("some_fake_package")

    @patch("subprocess.run")
    def test_install_python_dependency_failure(self, mock_run):
        mock_res = MagicMock()
        mock_res.returncode = 1
        mock_res.stdout = "Failure stdout"
        mock_res.stderr = "Failure stderr"
        mock_run.return_value = mock_res

        result = install_python_dependency.invoke({"package_name": "some_fake_package"})
        self.assertIn("Failed to install some_fake_package", result)

    @patch("subprocess.run")
    @patch("src.tools.code_tools._auto_install_pkg")
    def test_run_subprocess_auto_installs_and_retries(self, mock_auto_install, mock_run):
        # First call returns ModuleNotFoundError
        first_res = MagicMock()
        first_res.stdout = ""
        first_res.stderr = "ModuleNotFoundError: No module named 'missing_lib'"
        
        # Second call succeeds
        second_res = MagicMock()
        second_res.stdout = "Successful Output"
        second_res.stderr = ""
        
        mock_run.side_effect = [first_res, second_res]
        mock_auto_install.return_value = True

        result = _run_subprocess(["python", "some_script.py"])
        
        self.assertIn("Successful Output", result)
        self.assertIn("Detected missing module 'missing_lib'", result)
        self.assertIn("Successfully installed 'missing_lib'", result)
        self.assertEqual(mock_run.call_count, 2)
        mock_auto_install.assert_called_once_with("missing_lib")

    @patch("subprocess.run")
    def test_run_subprocess_timeout_retries_and_increases_timeout(self, mock_run):
        # Mock subprocess.run raising TimeoutExpired twice, then returning success
        mock_run.side_effect = [
            subprocess.TimeoutExpired(cmd=["python"], timeout=10),
            subprocess.TimeoutExpired(cmd=["python"], timeout=20),
            MagicMock(stdout="Finished", stderr="")
        ]

        result = _run_subprocess(["python", "some_script.py"], timeout=10)
        
        self.assertIn("Finished", result)
        self.assertIn("Command timed out after 10s. Retrying with timeout=20s...", result)
        self.assertIn("Command timed out after 20s. Retrying with timeout=40s...", result)
        self.assertEqual(mock_run.call_count, 3)

if __name__ == "__main__":
    unittest.main()
