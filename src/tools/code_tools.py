"""
src/tools/code_tools.py
───────────────────────
LangChain tools for Python code execution, file I/O, and project navigation.
Used exclusively by CodeSubAgent — not included in ALL_TOOLS for the main agent.

Tools
-----
execute_python_snippet  — run a Python snippet with project context injected
write_project_file      — write a file to src/scripts/ or output/
read_project_file       — read any project file (first 8 000 chars)
list_project_scripts    — tree of scripts under src/scripts/ and src/tools/
run_existing_script     — run an existing script by relative path
search_project_code     — grep for a pattern across src/*.py
"""

from __future__ import annotations

import os
import sys
import re
import shlex
import subprocess
import tempfile

from langchain_core.tools import tool

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

_PREAMBLE = f"""\
import sys, os
sys.path.insert(0, {repr(PROJECT_ROOT)})
os.chdir({repr(PROJECT_ROOT)})
import warnings; warnings.filterwarnings("ignore")
try:
    import pandas as pd
    import numpy as np
except ImportError:
    pass
try:
    from src.db.pool import get_pool, query_df
    from config.settings import settings
except Exception:
    pass
"""

_MODULE_TO_PKG = {
    "bs4": "beautifulsoup4",
    "sklearn": "scikit-learn",
    "yaml": "pyyaml",
    "PIL": "pillow",
    "fitz": "pymupdf",
    "pgvector": "pgvector",
    "clickhouse_connect": "clickhouse-connect",
    "google": "google-api-python-client",
    "google.genai": "google-genai",
    "yfinance": "yfinance",
    "plotext": "plotext",
    "arch": "arch",
    "lightgbm": "lightgbm",
}


def _parse_missing_module(output: str) -> str | None:
    """Parse ModuleNotFoundError or ImportError from process output."""
    # Match pattern: ModuleNotFoundError: No module named 'module_name'
    match = re.search(r"ModuleNotFoundError:\s*No\s*module\s*named\s*['\"]([^'\"]+)['\"]", output)
    if match:
        return match.group(1).split(".")[0]
    # Match pattern: ImportError: No module named module_name
    match = re.search(r"ImportError:\s*No\s*module\s*named\s*([^\s]+)", output)
    if match:
        return match.group(1).split(".")[0]
    return None


def _add_to_requirements(pkg_name: str) -> None:
    """Append a successfully installed package to requirements.txt if not present."""
    req_path = os.path.join(PROJECT_ROOT, "requirements.txt")
    if os.path.isfile(req_path):
        try:
            with open(req_path, "r", encoding="utf-8") as f:
                content = f.read()
            if pkg_name not in content:
                with open(req_path, "a", encoding="utf-8") as f:
                    f.write(f"\n{pkg_name}                      # Added automatically by CodeSubAgent auto-dependency installer\n")
        except Exception:
            pass


def _auto_install_pkg(module_name: str) -> bool:
    """Attempt to install a package matching the module name."""
    pkg_name = _MODULE_TO_PKG.get(module_name, module_name)
    try:
        res = subprocess.run(
            [sys.executable, "-m", "pip", "install", pkg_name],
            capture_output=True,
            text=True,
            timeout=90
        )
        if res.returncode == 0:
            _add_to_requirements(pkg_name)
            return True
        return False
    except Exception:
        return False


@tool
def install_python_dependency(package_name: str) -> str:
    """
    Install a Python package/dependency using pip.
    Use this if a script fails with ImportError or ModuleNotFoundError, or if you need
    a specific library for your analysis.

    Example
    -------
    install_python_dependency("openpyxl")
    """
    pkg_name = _MODULE_TO_PKG.get(package_name, package_name)
    try:
        res = subprocess.run(
            [sys.executable, "-m", "pip", "install", pkg_name],
            capture_output=True,
            text=True,
            timeout=120
        )
        if res.returncode == 0:
            _add_to_requirements(pkg_name)
            return f"Successfully installed {pkg_name}."
        else:
            return f"Failed to install {pkg_name}. Stdout: {res.stdout} Stderr: {res.stderr}"
    except Exception as exc:
        return f"Error installing package {pkg_name}: {exc}"


def _run_subprocess(cmd: list[str], *, timeout: int = 60, retry_on_timeout: bool = True) -> str:
    """Run a command from PROJECT_ROOT and return combined stdout+stderr."""
    env = os.environ.copy()
    env["ALLOW_LOCAL_RUN"] = "1"
    env["PYTHONPATH"] = PROJECT_ROOT + os.pathsep + env.get("PYTHONPATH", "")
    
    current_timeout = timeout
    attempts = 0
    max_attempts = 3
    logs = []
    
    while attempts < max_attempts:
        attempts += 1
        try:
            res = subprocess.run(
                cmd,
                cwd=PROJECT_ROOT,
                capture_output=True,
                text=True,
                env=env,
                timeout=current_timeout,
            )
            out = res.stdout.strip()
            err = res.stderr.strip()
            
            combined = out + "\n" + err
            missing_module = _parse_missing_module(combined)
            if missing_module:
                pkg_name = _MODULE_TO_PKG.get(missing_module, missing_module)
                logs.append(f"[Auto-Installer] Detected missing module '{missing_module}'. Attempting to install '{pkg_name}'...")
                if _auto_install_pkg(missing_module):
                    logs.append(f"[Auto-Installer] Successfully installed '{pkg_name}'. Retrying execution...")
                    continue
                else:
                    logs.append(f"[Auto-Installer] Failed to install '{pkg_name}'.")
            
            if err:
                out = (out + "\n--- STDERR ---\n" + err).strip()
            
            if logs:
                prefix = "\n".join(logs)
                return f"{prefix}\n\n{out or '(no output)'}"
            return out or "(no output)"
            
        except subprocess.TimeoutExpired:
            if retry_on_timeout and attempts < max_attempts:
                new_timeout = current_timeout * 2
                logs.append(f"[Timeout Handler] Command timed out after {current_timeout}s. Retrying with timeout={new_timeout}s...")
                current_timeout = new_timeout
                continue
            
            prefix = "\n".join(logs)
            err_msg = f"Error: execution timed out after {current_timeout}s."
            if prefix:
                return f"{prefix}\n\n{err_msg}"
            return err_msg
            
        except Exception as exc:
            prefix = "\n".join(logs)
            err_msg = f"Error: {exc}"
            if prefix:
                return f"{prefix}\n\n{err_msg}"
            return err_msg
            
    prefix = "\n".join(logs)
    err_msg = "Error: maximum execution attempts reached."
    if prefix:
        return f"{prefix}\n\n{err_msg}"
    return err_msg


@tool
def execute_python_snippet(code: str) -> str:
    """
    Execute a Python code snippet with full project context pre-injected.

    Available out-of-the-box: pandas, numpy, get_pool(), query_df(), settings.
    Use `query_df(sql)` to query ClickHouse directly.  Always add FINAL to
    ReplacingMergeTree tables.  Print results — return value is ignored.

    Example
    -------
    df = query_df("SELECT symbol, close FROM market_data.daily_prices FINAL LIMIT 5")
    print(df.to_markdown(index=False))
    """
    full_code = _PREAMBLE + "\n" + code
    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False, prefix="mosaic_code_") as f:
        f.write(full_code)
        tmp_path = f.name
    try:
        return _run_subprocess([sys.executable, tmp_path], timeout=90)
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


@tool
def write_project_file(filepath: str, content: str) -> str:
    """
    Write a Python script or text file to the project.

    filepath must be relative to the project root and must start with
    src/scripts/ or output/.  Parent directories are created automatically.

    Example
    -------
    write_project_file("src/scripts/etf/my_analysis.py", "import pandas as pd\\n...")
    """
    safe_prefixes = ("src/scripts/", "output/")
    if not any(filepath.startswith(p) for p in safe_prefixes):
        return (
            f"Error: writes are restricted to {safe_prefixes}. "
            f"Got: {filepath!r}"
        )
    abs_path = os.path.join(PROJECT_ROOT, filepath)
    os.makedirs(os.path.dirname(abs_path), exist_ok=True)
    with open(abs_path, "w", encoding="utf-8") as fh:
        fh.write(content)
    return f"Wrote {len(content)} chars → {filepath}"


@tool
def read_project_file(filepath: str) -> str:
    """
    Read a file from the project (relative to project root).

    Returns the first 8 000 characters.  Use list_project_scripts() first
    to discover available files.

    Example
    -------
    read_project_file("src/tools/skills_tools.py")
    """
    abs_path = os.path.join(PROJECT_ROOT, filepath)
    if not os.path.isfile(abs_path):
        return f"File not found: {filepath}"
    try:
        with open(abs_path, "r", encoding="utf-8", errors="replace") as fh:
            content = fh.read(8000)
        if os.path.getsize(abs_path) > 8000:
            content += f"\n... [truncated — full file is {os.path.getsize(abs_path)} bytes]"
        return content
    except Exception as exc:
        return f"Error reading {filepath}: {exc}"


@tool
def list_project_scripts() -> str:
    """
    List all Python files under src/scripts/ and src/tools/.

    Returns a sorted newline-delimited list of relative paths.
    """
    import pathlib
    lines: list[str] = []
    for base in ("src/scripts", "src/tools"):
        base_path = pathlib.Path(PROJECT_ROOT) / base
        if not base_path.exists():
            continue
        for p in sorted(base_path.rglob("*.py")):
            lines.append(str(p.relative_to(PROJECT_ROOT)))
    return "\n".join(lines) if lines else "No Python files found."


@tool
def run_existing_script(script_path: str, extra_args: str = "") -> str:
    """
    Run an existing Python script from the project by its relative path.

    script_path must start with src/ or output/.
    extra_args is an optional whitespace-separated string of CLI arguments.

    Example
    -------
    run_existing_script("src/scripts/market/whale_tracker.py")
    run_existing_script("src/main.py", "signals --save")
    """
    if not (script_path.startswith("src/") or script_path.startswith("output/")):
        return "Error: script_path must start with src/ or output/"
    abs_path = os.path.join(PROJECT_ROOT, script_path)
    if not os.path.isfile(abs_path):
        return f"Script not found: {script_path}"
    cmd = [sys.executable, script_path]
    if extra_args.strip():
        cmd += shlex.split(extra_args)
    return _run_subprocess(cmd, timeout=120)


@tool
def search_project_code(pattern: str) -> str:
    """
    Search for a text pattern across all Python files under src/.

    Returns matching lines with filename:lineno prefix (up to 50 results).

    Example
    -------
    search_project_code("class SignalSource")
    search_project_code("def run_goldbees")
    """
    try:
        res = subprocess.run(
            ["grep", "-rn", "--include=*.py", pattern, "src/"],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=15,
        )
        out = res.stdout.strip()
        if not out:
            return f"No matches for pattern: {pattern!r}"
        lines = out.splitlines()
        result = "\n".join(lines[:50])
        if len(lines) > 50:
            result += f"\n... ({len(lines) - 50} more matches — refine your pattern)"
        return result
    except Exception as exc:
        return f"Error: {exc}"


CODE_TOOLS = [
    execute_python_snippet,
    write_project_file,
    read_project_file,
    list_project_scripts,
    run_existing_script,
    search_project_code,
    install_python_dependency,
]
