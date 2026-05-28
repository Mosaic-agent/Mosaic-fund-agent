"""
src/utils/markdown_renderer.py
──────────────────────────────
Helper to render markdown nicely in the terminal, including beautifully
formatted tables using rich.table.Table instead of raw markdown pipes.
"""
from __future__ import annotations

import re
from typing import Any

from rich.console import Group
from rich.markdown import Markdown
from rich.table import Table
from rich import box


def parse_markdown_table(table_text: str) -> Table | None:
    """Parse a markdown table string into a styled rich.table.Table."""
    lines = [l.strip() for l in table_text.strip().split("\n") if l.strip()]
    if len(lines) < 2:
        return None

    def split_row(row_str: str) -> list[str]:
        r = row_str.strip()
        if r.startswith("|"):
            r = r[1:]
        if r.endswith("|"):
            r = r[:-1]
        return [cell.strip() for cell in r.split("|")]

    headers = split_row(lines[0])
    if not headers:
        return None

    # Check if second line is a markdown table separator (e.g. |---|---|)
    sep_cells = split_row(lines[1])
    if not sep_cells or not all(re.match(r"^:?-+:?$", c) for c in sep_cells):
        return None

    has_headers = not all(h == "" for h in headers)
    table = Table(
        box=box.ROUNDED,
        show_header=has_headers,
        header_style="bold cyan",
        border_style="dim",
        expand=False,
    )
    for h in headers:
        table.add_column(h)

    for line in lines[2:]:
        cells = split_row(line)
        if len(cells) < len(headers):
            cells += [""] * (len(headers) - len(cells))
        elif len(cells) > len(headers):
            cells = cells[:len(headers)]
        
        # Style rows with emojis or checks if present
        styled_cells = []
        for c in cells:
            if "✅" in c:
                styled_cells.append(f"[green]{c}[/green]")
            elif "⚠️" in c:
                styled_cells.append(f"[yellow]{c}[/yellow]")
            elif "🔴" in c:
                styled_cells.append(f"[red]{c}[/red]")
            else:
                styled_cells.append(c)
        table.add_row(*styled_cells)

    return table


def render_markdown_to_group(text: str) -> Group:
    """
    Parse markdown text and return a Group of renderables where tables
    are beautifully formatted tables and the rest is standard markdown.
    """
    lines = text.split("\n")
    renderables: list[Any] = []
    current_block: list[str] = []
    in_table = False

    def flush_block():
        nonlocal current_block, in_table
        if not current_block:
            return
        block_text = "\n".join(current_block)
        if in_table:
            table = parse_markdown_table(block_text)
            if table:
                renderables.append(table)
            else:
                renderables.append(Markdown(block_text))
        else:
            renderables.append(Markdown(block_text))
        current_block = []

    for line in lines:
        is_table_line = line.strip().startswith("|")
        if is_table_line:
            if not in_table:
                flush_block()
                in_table = True
            current_block.append(line)
        else:
            if in_table:
                flush_block()
                in_table = False
            current_block.append(line)

    flush_block()
    return Group(*renderables)
