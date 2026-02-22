"""
src/main.py
───────────
On-demand CLI entry point for Portfolio Insight.

Usage:
    python src/main.py analyze          # Full portfolio analysis + JSON report
    python src/main.py ask "question"   # Ask agent a free-form question
    python src/main.py config           # Show current configuration (non-sensitive only)

Run `python src/main.py --help` for all options.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import box

# Ensure project root is on sys.path when running as script
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import settings

app = typer.Typer(
    name="portfolio-insight",
    help="🇮🇳 Zerodha Portfolio Intelligence Agent – powered by LangChain + Kite MCP",
    add_completion=False,
)
console = Console()


def _setup_logging() -> None:
    """Configure logging based on LOG_LEVEL from config."""
    level = getattr(logging, settings.log_level.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # Suppress noisy third-party loggers
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("yfinance").setLevel(logging.WARNING)


def _check_config() -> bool:
    """Validate sensitive config fields and warn if missing. Returns True if OK."""
    warnings = settings.validate_sensitive_fields()
    if warnings:
        console.print("\n[bold yellow]⚠ Configuration Warnings:[/bold yellow]")
        for w in warnings:
            console.print(f"  [yellow]• {w}[/yellow]")
        if not settings.openai_api_key and not settings.anthropic_api_key:
            console.print(
                "\n[bold red]✗ Cannot run analysis without an LLM API key.[/bold red]\n"
                "  Copy [bold].env.example → .env[/bold] and fill in your API keys.\n"
            )
            return False
    return True


# ── Commands ──────────────────────────────────────────────────────────────────

@app.command()
def analyze(
    max_holdings: int = typer.Option(
        0,
        "--max",
        "-m",
        help="Limit analysis to top N holdings (0 = all). Useful for testing.",
    ),
    output_json: bool = typer.Option(
        True,
        "--json/--no-json",
        help="Save a JSON report to the output directory.",
    ),
    quiet: bool = typer.Option(
        False,
        "--quiet",
        "-q",
        help="Skip terminal report display; only save JSON.",
    ),
    demo: bool = typer.Option(
        False,
        "--demo",
        help=(
            "Run in demo mode: use sample NSE holdings and rule-based scoring. "
            "No Zerodha login or LLM API keys required."
        ),
    ),
) -> None:
    """
    Run full portfolio intelligence analysis on your Zerodha holdings.

    Fetches holdings from Kite MCP, enriches with Yahoo Finance data,
    recent news (NewsAPI), and quarterly results (Screener.in),
    then generates an AI-powered report with risk scores and insights.

    Use --demo to test the full pipeline without any API keys.
    """
    _setup_logging()

    console.print(
        Panel(
            "[bold]Zerodha Portfolio Intelligence Agent[/bold]\n"
            "[dim]Indian Equity Market Analysis | NSE & BSE[/dim]"
            + ("\n[yellow bold]── DEMO MODE ──[/yellow bold]" if demo else ""),
            border_style="blue" if not demo else "yellow",
        )
    )

    if demo:
        console.print(
            "[yellow]ℹ  Demo mode:[/yellow] Using sample holdings "
            "(RELIANCE, TCS, HDFCBANK, INFY, NIFTYBEES, GOLDBEES).\n"
            "   Real Yahoo Finance + Screener.in data will be fetched.\n"
            "   LLM scoring replaced by rule-based algorithm.\n"
        )
    elif not _check_config():
        raise typer.Exit(code=1)

    # Override max holdings if specified via CLI
    if max_holdings > 0:
        import os
        os.environ["MAX_HOLDINGS_PER_RUN"] = str(max_holdings)
        # Re-read settings to pick up the override
        settings.__dict__["max_holdings_per_run"] = max_holdings

    # Import agent lazily to avoid import errors when just checking config
    from src.agents.portfolio_agent import PortfolioAgent
    from src.formatters.output import print_report_to_console, save_json_report

    agent = PortfolioAgent(demo_mode=demo)

    try:
        report = agent.run_full_analysis(console=console)
    except Exception as exc:
        console.print(f"\n[bold red]✗ Analysis failed:[/bold red] {exc}")
        logging.exception("Full analysis failed")
        raise typer.Exit(code=1)

    if not report:
        console.print(
            "\n[bold red]✗ No report generated.[/bold red] "
            + ("Check your Kite authentication." if not demo else "Unexpected error in demo mode.")
        )
        raise typer.Exit(code=1)

    # Save JSON report
    if output_json:
        console.print("\n[bold cyan]Step 4/4:[/bold cyan] Saving report...")
        filepath = save_json_report(report)
        console.print(f"[green]✓ JSON report saved:[/green] {filepath}")

    # Print to terminal
    if not quiet:
        print_report_to_console(report, console=console)


@app.command()
def ask(
    question: str = typer.Argument(..., help="Natural language question about your portfolio."),
) -> None:
    """
    Ask the portfolio agent a free-form question using ReAct reasoning.

    Examples:
      python src/main.py ask "Which stock has the highest risk in my portfolio?"
      python src/main.py ask "What is the sector concentration of my holdings?"
    """
    _setup_logging()

    if not _check_config():
        raise typer.Exit(code=1)

    from src.agents.portfolio_agent import PortfolioAgent

    console.print(f"\n[bold]Question:[/bold] {question}\n")
    agent = PortfolioAgent()

    try:
        answer = agent.ask(question)
        console.print(Panel(answer, title="[bold green]Agent Response[/bold green]", border_style="green"))
    except Exception as exc:
        console.print(f"[bold red]✗ Error:[/bold red] {exc}")
        raise typer.Exit(code=1)


@app.command()
def config() -> None:
    """
    Display current non-sensitive configuration settings.

    Sensitive fields (API keys) are masked. Use this to verify
    your .env setup before running analysis.
    """
    _setup_logging()

    table = Table(
        title="Portfolio Insight Configuration",
        box=box.ROUNDED,
        show_header=True,
        header_style="bold magenta",
    )
    table.add_column("Setting", style="bold", min_width=30)
    table.add_column("Value", min_width=40)
    table.add_column("Sensitive?", justify="center", min_width=10)

    def masked(val: str) -> str:
        if val and len(val) > 4:
            return val[:4] + "****" + val[-2:]
        return "NOT SET" if not val else "****"

    rows = [
        # [NON-SENSITIVE] settings shown as-is
        ("LLM Provider", settings.llm_provider if not settings.llm_base_url else "local (OpenAI-compatible)", "No"),
        ("LLM Model", settings.llm_model, "No"),
        ("LLM Base URL", settings.llm_base_url if settings.llm_base_url else "(cloud default)", "No"),
        ("Kite MCP URL", settings.kite_mcp_url, "No"),
        ("Kite MCP Timeout (s)", str(settings.kite_mcp_timeout), "No"),
        ("News Articles/Stock", str(settings.news_articles_per_stock), "No"),
        ("News Lookback (days)", str(settings.news_lookback_days), "No"),
        ("Max Holdings/Run", str(settings.max_holdings_per_run) or "Unlimited", "No"),
        ("Scrape Delay (s)", str(settings.scrape_delay_seconds), "No"),
        ("Output Directory", settings.output_dir, "No"),
        ("Log Level", settings.log_level, "No"),
        ("NSE Yahoo Suffix", settings.nse_suffix, "No"),
        ("BSE Yahoo Suffix", settings.bse_suffix, "No"),
        ("Market Timezone", settings.market_timezone, "No"),
        # [SENSITIVE] settings are masked
        ("OpenAI API Key", masked(settings.openai_api_key), "⚠ YES"),
        ("Anthropic API Key", masked(settings.anthropic_api_key), "⚠ YES"),
        ("NewsAPI Key", masked(settings.newsapi_key), "⚠ YES"),
        ("Kite API Key", masked(settings.kite_api_key), "⚠ YES"),
        ("Kite API Secret", masked(settings.kite_api_secret), "⚠ YES"),
    ]

    for name, value, sensitive in rows:
        style = "yellow" if "YES" in sensitive else "white"
        table.add_row(name, f"[{style}]{value}[/{style}]", sensitive)

    console.print()
    console.print(table)

    warnings = settings.validate_sensitive_fields()
    if warnings:
        console.print("\n[bold yellow]Configuration Warnings:[/bold yellow]")
        for w in warnings:
            console.print(f"  [yellow]• {w}[/yellow]")
    else:
        console.print("\n[bold green]✓ All required configuration fields are set.[/bold green]")
    console.print()


# ── Entry Point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app()
