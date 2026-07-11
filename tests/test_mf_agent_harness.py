#!/usr/bin/env python3
"""
tests/test_mf_agent_harness.py
──────────────────────────────
Test harness and interactive REPL for the Mutual Fund (MF) sub-agent.
Allows testing MF holdings, consensus, rotations, and NAV performance in isolation.

Usage:
  # Run a single query directly
  ALLOW_LOCAL_RUN=1 python tests/test_mf_agent_harness.py "what pattern do you see across multi asset funds"

  # Run in interactive REPL mode
  ALLOW_LOCAL_RUN=1 python tests/test_mf_agent_harness.py
"""

from __future__ import annotations

import sys
import os
import time
import argparse
from typing import Optional

# Ensure project root is in path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Set local run bypass flag automatically for the harness context
os.environ["ALLOW_LOCAL_RUN"] = "1"

# Suppress warnings for clean output
import warnings
warnings.filterwarnings("ignore")

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import box

console = Console()


def run_mf_agent_query(question: str, verbose: bool = True) -> str:
    """Run a query against the MFSubAgent in isolation with callback tracing."""
    from src.agents.sub_agents.mf import MFSubAgent
    from src.agents.tracer import TracingCallbackHandler
    from src.agents.budget import BudgetCallbackHandler
    from src.agents.mosaic_fund_agent import RichConsoleCallbackHandler

    # Resolve agent and prepare callbacks
    agent = MFSubAgent()
    
    tracer = TracingCallbackHandler(agent="mf")
    budget = BudgetCallbackHandler()
    callbacks = [tracer, budget]

    if verbose:
        # RichConsoleCallbackHandler outputs live step-by-step tool executions
        callbacks.append(RichConsoleCallbackHandler(agent_name="mf"))

    start_time = time.monotonic()
    
    console.print(Panel(
        f"[bold]Input Query:[/bold] {question}",
        title="[bold cyan]MF Agent Invocation[/bold cyan]",
        border_style="cyan"
    ))

    try:
        response = agent.run(question, callbacks=callbacks)
    except Exception as exc:
        console.print(f"[bold red]✗ Agent execution failed:[/bold red] {exc}")
        import traceback
        traceback.print_exc()
        return f"Error: {exc}"

    elapsed = time.monotonic() - start_time
    
    # Render response
    from src.utils.markdown_renderer import render_markdown_to_group
    console.print(Panel(
        render_markdown_to_group(response),
        title=f"[bold green]MF Agent Response[/bold green] (took {elapsed:.2f}s)",
        border_style="green"
    ))
    
    return response


def run_interactive_repl():
    """Run an interactive shell loop for the MF sub-agent."""
    console.print(Panel(
        "[bold green]Mosaic Mutual Fund Agent Isolated Test Harness (REPL)[/bold green]\n"
        "[dim]Enter your query about mutual fund holdings, NAVs, or rotation consensus.\n"
        "Type 'exit', 'quit', or 'q' to end the session.[/dim]",
        border_style="bold green",
        box=box.ROUNDED
    ))

    while True:
        try:
            query = console.input("\n[bold yellow]mf-agent> [/bold yellow]").strip()
            if not query:
                continue
            if query.lower() in ("exit", "quit", "q"):
                console.print("[bold green]Goodbye![/bold green]")
                break
                
            run_mf_agent_query(query, verbose=True)
            
        except KeyboardInterrupt:
            console.print("\n[bold green]Goodbye![/bold green]")
            break
        except Exception as exc:
            console.print(f"[bold red]Error in REPL loop:[/bold red] {exc}")


def main():
    parser = argparse.ArgumentParser(description="Mutual Fund sub-agent isolated test harness.")
    parser.add_argument(
        "query", 
        nargs="?", 
        type=str, 
        help="The query/question to send to the MF agent. If omitted, starts interactive REPL."
    )
    parser.add_argument(
        "--no-verbose", 
        action="store_true", 
        help="Disable step-by-step tool invocation logs."
    )
    args = parser.parse_args()

    if args.query:
        run_mf_agent_query(args.query, verbose=not args.no_verbose)
    else:
        run_interactive_repl()


if __name__ == "__main__":
    main()
