"""
src/tools/_subprocess.py
────────────────────────
Shared subprocess helpers used by skills_tools.py and runners.py.
No project imports — safe to import from any tool module without circular deps.
"""

from __future__ import annotations

import os
import re
import sys
import subprocess

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

_ANSI_STRIP_RE = re.compile(r"\x1b\[[0-9;]*[a-zA-Z]|\x1b[()][A-Z0-9=><]|\x1b[ABCDEF78]")


def _clean_terminal_output(text: str) -> str:
    """Strip box-drawing chars and excessive blank lines for LLM readability."""
    replacements = {
        "█": "#", "░": ".", "▒": ".", "▓": "#",
        "■": "*", "▲": "^", "▼": "v",
    }
    lines = []
    for line in text.splitlines():
        for char, replacement in replacements.items():
            line = line.replace(char, replacement)
        cleaned_chars = []
        for char in line:
            val = ord(char)
            if 0x2500 <= val <= 0x257F:
                cleaned_chars.append("-" if char in "─━═┄┅┈┉╌╍" else "")
            else:
                cleaned_chars.append(char)
        line = "".join(cleaned_chars).strip()
        if line and not all(c in "-_ " for c in line):
            lines.append(line)
    return "\n".join(lines)


def _run_cmd(args: list[str]) -> str:
    """Run a command via subprocess from PROJECT_ROOT with the venv Python."""
    env = os.environ.copy()
    env["ALLOW_LOCAL_RUN"] = "1"
    env["PYTHONPATH"] = PROJECT_ROOT + os.pathsep + env.get("PYTHONPATH", "")
    cmd = [sys.executable] + args
    try:
        res = subprocess.run(
            cmd, cwd=PROJECT_ROOT, capture_output=True, text=True, env=env, timeout=300,
        )
        output = res.stdout
        if res.stderr:
            output += "\n--- STDERR ---\n" + res.stderr
        return _clean_terminal_output(output)
    except Exception as e:
        return f"Error executing command {' '.join(cmd)}: {e}"


def _run_cmd_streaming(args: list[str]) -> str:
    """Like _run_cmd but prints each line live. Used for long-running imports."""
    env = os.environ.copy()
    env.update({"ALLOW_LOCAL_RUN": "1", "NO_COLOR": "1", "TERM": "dumb"})
    env["PYTHONPATH"] = PROJECT_ROOT + os.pathsep + env.get("PYTHONPATH", "")
    cmd = [sys.executable] + args
    collected: list[str] = []
    try:
        proc = subprocess.Popen(
            cmd, cwd=PROJECT_ROOT,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, env=env, bufsize=1,
        )
        for raw_line in iter(proc.stdout.readline, ""):
            stripped = _ANSI_STRIP_RE.sub("", raw_line).rstrip()
            cleaned = _clean_terminal_output(stripped)
            if cleaned:
                sys.stdout.write(f"  {cleaned}\n")
                sys.stdout.flush()
                collected.append(cleaned)
        proc.wait()
        rc = proc.returncode
    except Exception as exc:
        return f"Import error: {exc}\n" + "\n".join(collected[:40])
    if len(collected) > 40:
        truncated_count = len(collected) - 30
        summary_lines = (
            collected[:10]
            + [f"\n... [{truncated_count} lines of progress logs printed to console but omitted from LLM context] ...\n"]
            + collected[-20:]
        )
        result = "\n".join(summary_lines)
    else:
        result = "\n".join(collected) if collected else "Import completed (no output)."
    if rc != 0:
        result += f"\n[Process exited with code {rc}]"
    return result


def _summarize_whale_tracker_output(output: str) -> str:
    """Parse raw whale tracker output → concise Markdown accumulation/trim tables."""
    accumulations: list[tuple] = []
    trims: list[tuple] = []
    current_fund = ""
    for line in output.splitlines():
        line_str = line.strip()
        if "Multi Asset" in line_str:
            current_fund = line_str.split("(")[0].strip()
        if "%" in line_str and ("+" in line_str or "-" in line_str):
            parts = line_str.split()
            if len(parts) >= 5:
                change_str = parts[-1].rstrip("%")
                try:
                    change_val = float(change_str)
                    pct_indices = [i for i, p in enumerate(parts) if "%" in p]
                    if len(pct_indices) >= 2:
                        prev_pct_idx = pct_indices[-2]
                        theme_idx = 1 if parts[0].startswith(("🥇", "🥈", "⚛️", "🛢️", "🏗️")) else 0
                        theme    = " ".join(parts[:theme_idx + 1])
                        security = " ".join(parts[theme_idx + 1:prev_pct_idx])
                        if change_val > 0.05:
                            accumulations.append((change_val, security, theme, current_fund))
                        elif change_val < -0.05:
                            trims.append((change_val, security, theme, current_fund))
                except ValueError:
                    continue
    accumulations.sort(key=lambda x: x[0], reverse=True)
    trims.sort(key=lambda x: x[0])

    summary = "\n\n### 🐋 Whale Tracker Concise Summary\n\n"
    summary += "#### Top Accumulations (Increasing Weight)\n\n"
    summary += "| Fund | Theme | Security | Change |\n| :--- | :--- | :--- | ---: |\n"
    for change_val, security, theme, fund in (accumulations[:5] or [("", "", "", "None detected")]):
        summary += f"| {fund} | {theme} | {security} | {change_val:+.2f}% |\n" if fund != "None detected" else "| None detected | | | |\n"

    summary += "\n#### Top Trims (Reducing Weight)\n\n"
    summary += "| Fund | Theme | Security | Change |\n| :--- | :--- | :--- | ---: |\n"
    for change_val, security, theme, fund in (trims[:5] or [("", "", "", "None detected")]):
        summary += f"| {fund} | {theme} | {security} | {change_val:+.2f}% |\n" if fund != "None detected" else "| None detected | | | |\n"

    if "Unified Macro Theme Allocations" in output:
        sub = output.split("Unified Macro Theme Allocations")[1]
        if "High-Conviction Equity Cross-Ownership" in sub:
            sub = sub.split("High-Conviction Equity Cross-Ownership")[0]
        themes, latest_weights, flow_changes = [], [], []
        for line in sub.splitlines():
            if "%" in line:
                line_clean = line
                for emoji in ["🥈", "🥇", "⚛️", "🛢️", "🏗️"]:
                    line_clean = line_clean.replace(emoji, "")
                parts = line_clean.split()
                if len(parts) >= 4 and parts[0] in ["Silver", "Gold", "Nuclear/Grid", "Energy", "Infra"]:
                    try:
                        latest_weights.append(float(parts[-2].replace("%", "")))
                        flow_changes.append(float(parts[-1].replace("%", "")))
                        themes.append(parts[0])
                    except ValueError:
                        pass
        if themes:
            try:
                import plotext as plt
                ansi_re = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
                plt.clear_figure()
                plt.bar(themes, latest_weights, orientation="horizontal")
                plt.title("Combined Latest Weights by Sector/Theme (%)")
                plt.plot_size(70, 15)
                summary += f"\n\n#### Combined Latest Weights by Sector/Theme (%)\n```text\n{ansi_re.sub('', plt.build())}\n```\n"
                plt.clear_figure()
                plt.bar(themes, flow_changes, orientation="horizontal")
                plt.title("Net Flow Change by Sector/Theme (%)")
                plt.plot_size(70, 15)
                summary += f"\n\n#### Net Flow Change by Sector/Theme (%)\n```text\n{ansi_re.sub('', plt.build())}\n```\n"
            except Exception as e:
                summary += f"\n\n*(Note: Could not generate ASCII charts: {e})*\n"
    return summary
