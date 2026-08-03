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


def looks_like_markdown_table(text: str) -> bool:
    """True if `text` contains a real markdown table: a header row followed
    by a `|---|---|`-style separator row. A stray `|` or `-` elsewhere in
    plain text (log lines, file paths, JSON) does not count."""
    lines = text.split("\n")
    for i in range(len(lines) - 1):
        if "|" not in lines[i]:
            continue
        next_stripped = lines[i + 1].strip()
        if not next_stripped or "|" not in next_stripped and not next_stripped.startswith("-"):
            continue
        cells = [c.strip() for c in next_stripped.strip("|").split("|")]
        if cells and all(re.match(r"^:?-+:?$", c) for c in cells):
            return True
    return False


def render_markdown_to_group(text: str) -> Group:
    """
    Parse markdown text and return a Group of renderables where tables
    are beautifully formatted tables and the rest is standard markdown.
    """
    lines = text.split("\n")
    renderables: list[Any] = []
    
    in_table = False
    table_lines: list[str] = []
    text_lines: list[str] = []
    
    def flush_text():
        if text_lines:
            txt = "\n".join(text_lines).strip()
            if txt:
                # Check if it's a chart (contains chart tick or frame characters)
                if any(c in txt for c in ("┤", "┼", "─", "└", "┐", "┘", "┌", "├", "┬", "┴", "╮", "╰", "╭")):
                    from rich.text import Text as RichText
                    chart_text = RichText.from_ansi(txt)
                    chart_text.no_wrap = True
                    renderables.append(chart_text)
                else:
                    renderables.append(Markdown(txt))
            text_lines.clear()

    def flush_table():
        if table_lines:
            tbl_txt = "\n".join(table_lines)
            table = parse_markdown_table(tbl_txt)
            if table:
                # Give the table the same breathing room a Markdown
                # paragraph gets, so it doesn't visually collide with
                # the prose immediately above/below it.
                if renderables:
                    renderables.append("")
                renderables.append(table)
                renderables.append("")
            else:
                # If parsing failed, render it as normal markdown text
                text_lines.extend(table_lines)
                flush_text()
            table_lines.clear()

    i = 0
    while i < len(lines):
        line = lines[i]
        stripped = line.strip()
        
        # A markdown table row must have '|'
        is_table_row = "|" in line
        
        # Check if we can start a table
        # We need a header row with '|' and the next row must be a separator row (e.g. |---|)
        if not in_table and is_table_row and (i + 1 < len(lines)):
            next_stripped = lines[i+1].strip()
            # Split and clean next row
            if next_stripped.startswith("|") or next_stripped.endswith("|") or "|" in next_stripped:
                cells = [c.strip() for c in next_stripped.strip("|").split("|")]
                if cells and all(re.match(r"^:?-+:?$", c) for c in cells):
                    # Found a table start!
                    flush_text()
                    in_table = True
        
        if in_table:
            if is_table_row:
                table_lines.append(line)
            else:
                # End of table
                flush_table()
                in_table = False
                text_lines.append(line)
        else:
            text_lines.append(line)
        i += 1
        
    flush_table()
    flush_text()

    return Group(*renderables)

