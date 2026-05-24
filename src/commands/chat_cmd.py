"""
src/commands/chat_cmd.py
────────────────────────
Interactive REPL chat loop for the Mosaic-fund-agent.

Invoked by:
    python src/main.py chat          (explicit)
    ./mosaic.sh                      (default when no args given)

Features
--------
- Infinite prompt loop; exits on 'quit' / Ctrl-C
- In-session conversation memory via LangGraph MemorySaver (cleared on exit)
- Intent-based sub-agent auto-routing (deepdive / signal / macro / main)
- Slash commands for direct dispatch and utility actions
- Rich spinner while waiting; Markdown-rendered responses
"""
from __future__ import annotations

import logging
import os
import uuid
from typing import Any

# Enable readline history and in-line editing for input() on Linux/macOS.
# After this import, ↑/↓ navigate history, ←/→ move cursor, Ctrl-R reverse-search.
try:
    import readline as _readline
    _HISTORY_FILE = os.path.expanduser("~/.mosaic_chat_history")
    try:
        _readline.read_history_file(_HISTORY_FILE)
    except FileNotFoundError:
        pass
    _readline.set_history_length(500)
    import atexit as _atexit
    _atexit.register(_readline.write_history_file, _HISTORY_FILE)
except ImportError:
    pass  # Windows — graceful no-op

from rich.console import Console
from rich.markdown import Markdown
from rich.panel import Panel

logger = logging.getLogger(__name__)

# ── Banner & help ──────────────────────────────────────────────────────────────

_BANNER = """[bold blue]
╔══════════════════════════════════════════════════════════╗
║        Mosaic-fund-agent  •  Interactive Chat            ║
║    Indian Equity & US Research Intelligence              ║
╚══════════════════════════════════════════════════════════╝
[/bold blue]
Type your question, or use a slash command:

  [cyan]/analyze [--max N][/cyan]   — full Zerodha portfolio analysis
  [cyan]/signals[/cyan]             — ETF composite signal dashboard
  [cyan]/deepdive TICKER[/cyan]     — US stock SEC deep-dive (e.g. /deepdive ADSK)
  [cyan]/macro[/cyan]              — macro events + COMEX + FII/DII scan
  [cyan]/cache[/cyan]              — show LLM cache stats  ([cyan]/cache clear[/cyan] to wipe)
  [cyan]/clear[/cyan]              — reset conversation memory (fresh thread)
  [cyan]/help[/cyan]               — show this help text
  [dim]quit / exit / Ctrl-C[/dim]  — exit

Auto-routing (no slash needed):
  "deep-dives adsk"  →  DeepDive sub-agent
  "goldbees signal"  →  Signal sub-agent
  "comex gold"       →  Macro sub-agent
"""

_HELP_MD = """
## Mosaic Chat — Quick Reference

### Slash Commands

| Command | Action |
|---|---|
| `/analyze [--max N]` | Full Zerodha portfolio analysis (use --max 3 for quick test) |
| `/signals` | ETF composite signal aggregator (all 18 ETFs) |
| `/deepdive TICKER` | US stock SEC 10-K deep-dive (e.g. `/deepdive ADSK`) |
| `/macro` | Live macro events + COMEX + FII/DII institutional flows |
| `/cache` | Show LLM cache stats; `/cache clear` wipes cached responses |
| `/clear` | Reset session memory — next question starts a fresh thread |
| `/help` | This help text |
| `quit` / `exit` / `q` | Exit the chat |

### Auto-Routing Keywords

| Keywords | Routes to |
|---|---|
| `deep-dive`, `10-K`, `SEC`, `ADSK`, `AAPL`, `MSFT` … | DeepDive sub-agent |
| `signal`, `GOLDBEES`, `Kelly`, `iNAV`, `risk governor` … | Signal sub-agent |
| `COMEX`, `macro`, `FII`, `DII`, `gold price`, `crude` … | Macro sub-agent |
| Everything else | Main portfolio agent |

### Tips
- Use **`/analyze --max 3`** for a quick 3-holding test run.
- Sub-agents share the same LLM but have focused tool sets and system prompts.
- Memory is **in-session only** — it resets when the container exits.
"""


# ── Slash command dispatcher ───────────────────────────────────────────────────

def _dispatch_slash(
    raw: str,
    console: Console,
    agent: Any,       # MosaicFundAgent
    thread_id: str,
) -> tuple[str, str]:
    """
    Parse and handle a slash command.

    Returns
    -------
    (answer, new_thread_id)
    If answer is empty string the handler already printed its own output.
    """
    parts     = raw.lstrip("/").split()
    name      = parts[0].lower() if parts else ""

    # ── /help ──────────────────────────────────────────────────────────────
    if name == "help":
        console.print(Panel(Markdown(_HELP_MD), border_style="blue", title="[bold]Help[/bold]"))
        return "", thread_id

    # ── /clear ─────────────────────────────────────────────────────────────
    if name == "clear":
        new_id = str(uuid.uuid4())
        console.print("[yellow]Memory cleared — new conversation thread started.[/yellow]")
        return "", new_id

    # ── /analyze [--max N] ─────────────────────────────────────────────────
    if name == "analyze":
        import os
        max_n = 0
        p = parts[1:]
        while p:
            if p[0] == "--max" and len(p) > 1:
                try:
                    max_n = int(p[1])
                except ValueError:
                    pass
                p = p[2:]
            else:
                p = p[1:]
        if max_n > 0:
            os.environ["MAX_HOLDINGS_PER_RUN"] = str(max_n)
        with console.status("[yellow]Running full portfolio analysis…[/yellow]", spinner="dots"):
            try:
                report = agent.run_full_analysis(console=console)
            except Exception as exc:
                console.print(f"[bold red]✗ Analysis failed:[/bold red] {exc}")
                return "", thread_id
        if report:
            from src.formatters.output import print_report_to_console
            print_report_to_console(report, console=console)
        return "", thread_id

    # ── /signals ───────────────────────────────────────────────────────────
    if name == "signals":
        return agent.chat("Run the daily ETF composite signal aggregator and show results", thread_id=thread_id), thread_id

    # ── /deepdive TICKER ───────────────────────────────────────────────────
    if name == "deepdive":
        ticker = parts[1].upper() if len(parts) > 1 else ""
        if not ticker:
            return "Usage: `/deepdive TICKER`  — e.g. `/deepdive ADSK`", thread_id
        return agent.chat(f"deep-dive {ticker}", thread_id=thread_id), thread_id

    # ── /macro ─────────────────────────────────────────────────────────────
    if name == "macro":
        return agent.chat("Run the macro scanner and show COMEX signals plus FII/DII flows", thread_id=thread_id), thread_id

    # ── /cache [clear] ─────────────────────────────────────────────────────
    if name == "cache":
        from src.utils.llm_cache import get_cache
        cache = get_cache()
        if cache is None:
            return "LLM cache is **disabled** (set `LLM_CACHE_ENABLED=true` in .env to enable).", thread_id
        if len(parts) > 1 and parts[1].lower() == "clear":
            cache.clear()
            return "LLM cache cleared.", thread_id
        s = cache.stats()
        return (
            f"**LLM Cache** (`output/.cache/llm_cache.db`)\n\n"
            f"| Stat | Value |\n|---|---|\n"
            f"| Live entries | {s['live_entries']} |\n"
            f"| Total entries | {s['total_entries']} |\n"
            f"| DB size | {s['db_size_kb']} kB |\n\n"
            f"Use `/cache clear` to wipe all cached responses."
        ), thread_id

    # Unknown
    return f"Unknown command: `/{name}` — type `/help` for the full list.", thread_id


# ── Main loop ─────────────────────────────────────────────────────────────────

def run_chat_loop(console: Console | None = None) -> None:
    """
    Start the interactive REPL.

    Runs until the user types 'quit' / 'exit' / 'q' or presses Ctrl-C.
    """
    if console is None:
        console = Console()

    # Build agent with in-session memory
    from langgraph.checkpoint.memory import MemorySaver
    from src.agents.mosaic_fund_agent import MosaicFundAgent

    console.print(_BANNER)

    with console.status("[yellow]Loading agent…[/yellow]", spinner="dots"):
        agent     = MosaicFundAgent(checkpointer=MemorySaver())
        thread_id = str(uuid.uuid4())

    console.print("[dim]Agent ready.  Type your first question.[/dim]\n")

    while True:
        # ── Read input ─────────────────────────────────────────────────────
        try:
            raw = input("You: ").strip()
        except (EOFError, KeyboardInterrupt):
            console.print("\n[dim]Goodbye.[/dim]")
            break

        if not raw:
            continue

        if raw.lower() in ("quit", "exit", "bye", "q"):
            console.print("[dim]Goodbye.[/dim]")
            break

        # ── Slash commands ─────────────────────────────────────────────────
        if raw.startswith("/"):
            answer, thread_id = _dispatch_slash(raw, console, agent, thread_id)
            if answer:
                console.print(Panel(Markdown(answer), border_style="cyan"))
            continue

        # ── Normal chat turn ───────────────────────────────────────────────
        try:
            import os
            if os.getenv("VERBOSE") == "1":
                # Skip spinner so callback handler can print tool calls live
                answer = agent.chat(raw, thread_id=thread_id)
            else:
                with console.status("[yellow]Thinking…[/yellow]", spinner="dots"):
                    answer = agent.chat(raw, thread_id=thread_id)
            console.print(Panel(Markdown(answer), border_style="green"))
        except KeyboardInterrupt:
            console.print("\n[yellow]Interrupted.[/yellow]")
        except Exception as exc:
            console.print(f"[bold red]✗ Error:[/bold red] {exc}")
            logger.exception("chat turn failed")
