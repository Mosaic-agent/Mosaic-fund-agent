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
    no_dashboard: bool = typer.Option(
        False,
        "--no-dashboard",
        help="Skip auto-generating the HTML dashboard after analysis.",
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

    # Auto-generate HTML dashboard
    if not no_dashboard:
        try:
            from src.agents.visualization_agent import VisualizationAgent
            with console.status("[cyan]Generating HTML dashboard…[/cyan]"):
                viz = VisualizationAgent(output_dir=settings.output_dir)
                dash_path = viz.generate(report)
            console.print(f"[green]✓ Dashboard ready:[/green] {dash_path}")
            console.print(
                "[dim]  Open in browser: [/dim]"
                f"[link=file://{dash_path}]{dash_path}[/link]"
            )
        except Exception as exc:
            console.print(f"[yellow]⚠ Dashboard generation failed (non-fatal):[/yellow] {exc}")
            logging.exception("Dashboard generation failed")


@app.command()
def dashboard(
    no_open: bool = typer.Option(
        False,
        "--no-open",
        help="Generate the dashboard but do not open it in the browser.",
    ),
) -> None:
    """
    Generate an interactive HTML dashboard from the latest portfolio report.

    Reads the most recent JSON report from the output directory and renders
    a self-contained React dashboard (no build step required).  The file is
    saved to ./output/dashboard.html and opened in your default browser
    unless --no-open is passed.

    Example:
      python src/main.py dashboard
      python src/main.py dashboard --no-open
    """
    _setup_logging()

    from src.utils.report_loader import load_latest_report
    from src.agents.visualization_agent import VisualizationAgent

    report = load_latest_report(output_dir=settings.output_dir)
    if not report:
        console.print(
            "[bold red]✗ No portfolio report found.[/bold red]\n"
            "  Run [bold]python src/main.py analyze[/bold] first to generate one."
        )
        raise typer.Exit(code=1)

    with console.status("[cyan]Rendering dashboard…[/cyan]"):
        try:
            viz = VisualizationAgent(output_dir=settings.output_dir)
            dash_path = viz.generate(report)
        except Exception as exc:
            console.print(f"[bold red]✗ Dashboard generation failed:[/bold red] {exc}")
            logging.exception("Dashboard generation failed")
            raise typer.Exit(code=1)

    console.print(
        Panel(
            f"[green]✓ Dashboard generated:[/green] {dash_path}",
            title="[bold]Portfolio Dashboard[/bold]",
            border_style="green",
        )
    )

    if not no_open:
        VisualizationAgent.open_in_browser(dash_path)
        console.print("[dim]Opening in browser…[/dim]")


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
        ("LLM Context Window (tokens)", str(settings.llm_context_window), "No"),
        ("LLM Token Budget (output)", str(settings.llm_token_budget), "No"),
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


@app.command()
def news(
    symbol: str = typer.Argument(..., help="NSE/BSE trading symbol e.g. RELIANCE, TCS"),
    company: str = typer.Option(
        "",
        "--company",
        "-c",
        help="Optional full company name for richer news queries.",
    ),
) -> None:
    """
    Multi-source news sentiment analysis for a stock symbol.

    Fetches articles from NewsAPI.org (premium Indian publications) AND
    Google News (GNews RSS), deduplicates, scores sentiment, and displays
    a rich collated report powered by the Deep Agents framework.

    Examples:
      python src/main.py news RELIANCE
      python src/main.py news RELIANCE --company "Reliance Industries"
      python src/main.py news TCS -c "Tata Consultancy Services"
    """
    from rich.columns import Columns
    from rich.text import Text

    _setup_logging()

    symbol_upper = symbol.strip().upper()
    display_name = f"{symbol_upper} — {company}" if company else symbol_upper

    console.print(
        Panel(
            f"[bold]News Sentiment Analysis[/bold]  [cyan]{display_name}[/cyan]\n"
            "[dim]Sources: NewsAPI.org (premium) + Google News (GNews) · Deep Agents[/dim]",
            border_style="cyan",
        )
    )

    from src.agents.news_sentiment_agent import NewsSentimentAgent

    agent = NewsSentimentAgent()

    with console.status(f"[cyan]Fetching news for {symbol_upper} from both sources…[/cyan]"):
        try:
            report = agent.run(symbol_upper, company)
        except Exception as exc:
            console.print(f"[bold red]✗ News analysis failed:[/bold red] {exc}")
            raise typer.Exit(code=1)

    if not report or report.get("total_articles", 0) == 0:
        console.print(
            f"[yellow]⚠ No news articles found for {symbol_upper}.[/yellow]\n"
            "  Check your NEWSAPI_KEY in .env and your network connection."
        )
        raise typer.Exit(code=0)

    # ── Sentiment banner ──────────────────────────────────────────────────────
    overall = report.get("overall_sentiment", "NEUTRAL")
    score = report.get("sentiment_score", 0.0)
    score_bar = "█" * int(abs(score) * 10)
    sentiment_color = {"POSITIVE": "green", "NEGATIVE": "red", "NEUTRAL": "yellow"}.get(overall, "white")

    console.print(
        Panel(
            f"[bold {sentiment_color}]{overall}[/bold {sentiment_color}]  "
            f"Score: [{sentiment_color}]{score:+.3f}[/{sentiment_color}]  "
            f"[dim]{score_bar or '─'}[/dim]\n"
            f"[dim]Total articles: {report.get('total_articles')}  │  "
            f"NewsAPI: {report.get('newsapi_count', 0)}  │  "
            f"GNews: {report.get('gnews_count', 0)}  │  "
            f"Deduped: {report.get('deduplicated_count', 0)}[/dim]",
            title=f"[bold]Overall Sentiment — {symbol_upper}[/bold]",
            border_style=sentiment_color,
        )
    )

    # ── Breakdown table ───────────────────────────────────────────────────────
    breakdown = report.get("sentiment_breakdown", {})
    bd_table = Table(box=box.SIMPLE, show_header=True, header_style="bold")
    bd_table.add_column("Sentiment", min_width=12)
    bd_table.add_column("Count", justify="right", min_width=8)
    bd_table.add_column("Share", justify="right", min_width=8)
    bd_table.add_row(
        "[green]Positive[/green]",
        str(report.get("positive_count", 0)),
        f"[green]{breakdown.get('positive_pct', 0):.1f}%[/green]",
    )
    bd_table.add_row(
        "[red]Negative[/red]",
        str(report.get("negative_count", 0)),
        f"[red]{breakdown.get('negative_pct', 0):.1f}%[/red]",
    )
    bd_table.add_row(
        "[yellow]Neutral[/yellow]",
        str(report.get("neutral_count", 0)),
        f"[yellow]{breakdown.get('neutral_pct', 0):.1f}%[/yellow]",
    )
    console.print(bd_table)

    # ── Top headlines ─────────────────────────────────────────────────────────
    pos_heads = report.get("top_positive_headlines", [])
    neg_heads = report.get("top_negative_headlines", [])

    if pos_heads:
        console.print(Panel(
            "\n".join(f"  [green]▲[/green] {h}" for h in pos_heads),
            title="[bold green]Top Positive Headlines[/bold green]",
            border_style="green",
        ))
    if neg_heads:
        console.print(Panel(
            "\n".join(f"  [red]▼[/red] {h}" for h in neg_heads),
            title="[bold red]Top Negative Headlines[/bold red]",
            border_style="red",
        ))

    # ── Full article list ─────────────────────────────────────────────────────
    articles = report.get("articles", [])
    if articles:
        art_table = Table(
            title=f"All Articles ({len(articles)})",
            box=box.ROUNDED,
            show_header=True,
            header_style="bold magenta",
        )
        art_table.add_column("#", justify="right", min_width=3)
        art_table.add_column("Src", min_width=7)
        art_table.add_column("Sentiment", min_width=9, justify="center")
        art_table.add_column("Source / Publisher", min_width=20)
        art_table.add_column("Headline", min_width=50)
        art_table.add_column("Published", min_width=12)

        sent_color_map = {"POSITIVE": "green", "NEGATIVE": "red", "NEUTRAL": "yellow"}
        for idx, art in enumerate(articles, 1):
            sc = sent_color_map.get(art.get("sentiment", "NEUTRAL"), "white")
            tag = art.get("_source_tag", "")
            tag_color = "blue" if tag == "NewsAPI" else "cyan"
            art_table.add_row(
                str(idx),
                f"[{tag_color}]{tag}[/{tag_color}]",
                f"[{sc}]{art.get('sentiment', '—')}[/{sc}]",
                art.get("source", "")[:22],
                art.get("title", "")[:60],
                str(art.get("published_at", ""))[:16],
            )
        console.print(art_table)

    console.rule("[dim]End of News Report[/dim]")


@app.command()
def comex() -> None:
    """
    Run COMEX commodity pre-market signal analysis.

    Fetches live spot prices from gold-api.com for Gold (XAU), Silver (XAG),
    Platinum (XPT), Palladium (XPD), and Copper (HG), compares against
    previous-day Yahoo Finance futures closes, and classifies each as
    STRONG BULLISH / BULLISH / NEUTRAL / BEARISH / STRONG BEARISH.

    Identifies which Indian NSE ETFs and stocks are directly affected.
    Powered by the Deep Agents framework (create_deep_agent).

    Requires GOLD_API_KEY in .env.
    """
    _setup_logging()

    console.print(
        Panel(
            "[bold]🌍 COMEX Pre-Market Signal Analysis[/bold]\n"
            "[dim]Sources: gold-api.com (live) + Yahoo Finance (prev close) · Deep Agents[/dim]",
            border_style="yellow",
        )
    )

    from src.agents.comex_agent import ComexAgent

    with console.status("[yellow]Fetching COMEX live prices…[/yellow]"):
        try:
            report = ComexAgent().run()
        except Exception as exc:
            console.print(f"[bold red]✗ COMEX analysis failed:[/bold red] {exc}")
            raise typer.Exit(code=1)

    if report.get("error"):
        console.print(
            f"[yellow]⚠ {report['error']}[/yellow]\n"
            "  Set GOLD_API_KEY in .env — get a free key at https://gold-api.com/"
        )
        raise typer.Exit(code=0)

    # ── Overall banner ───────────────────────────────────────────────────────
    overall   = report.get("overall_signal", "UNKNOWN")
    summary   = report.get("summary", "")
    run_time  = report.get("run_time_ist", "")
    pre_mkt   = report.get("pre_market", False)

    sig_color = {
        "STRONG BULLISH": "bright_green",
        "BULLISH":        "green",
        "NEUTRAL":        "yellow",
        "BEARISH":        "red",
        "STRONG BEARISH": "bright_red",
    }.get(overall, "white")
    sig_icon  = {
        "STRONG BULLISH": "⬆⬆",
        "BULLISH":        "↑",
        "NEUTRAL":        "→",
        "BEARISH":        "↓",
        "STRONG BEARISH": "⬇⬇",
    }.get(overall, "?")
    pre_note = "  [italic dim](pre-market — NSE not yet open)[/italic dim]" if pre_mkt else ""

    console.print(
        Panel(
            f"[bold {sig_color}]{sig_icon} {overall}[/bold {sig_color}]{pre_note}\n"
            f"[dim]{summary}[/dim]"
            + (f"\n[dim]Run time: {run_time}[/dim]" if run_time else ""),
            title="[bold]🌍 Overall Signal[/bold]",
            border_style=sig_color,
        )
    )

    # ── Commodity table ────────────────────────────────────────────────────
    commodities = report.get("commodities", {})
    if commodities:
        c_table = Table(
            title="Commodity Signals",
            box=box.ROUNDED,
            show_header=True,
            header_style="bold magenta",
        )
        c_table.add_column("", min_width=3, justify="center")   # emoji
        c_table.add_column("Symbol", min_width=6)
        c_table.add_column("Name", min_width=10)
        c_table.add_column("Signal", min_width=15, justify="center")
        c_table.add_column("Change", min_width=9, justify="right")
        c_table.add_column("Live Price", min_width=14, justify="right")
        c_table.add_column("Prev Close", min_width=14, justify="right")
        c_table.add_column("NSE ETFs / Stocks", min_width=30)

        icon_map = {
            "STRONG BULLISH": ("⬆⬆", "bright_green"),
            "BULLISH":        ("↑",   "green"),
            "NEUTRAL":        ("→",   "yellow"),
            "BEARISH":        ("↓",   "red"),
            "STRONG BEARISH": ("⬇⬇", "bright_red"),
        }

        for sym, c in commodities.items():
            sig    = c.get("signal", "UNKNOWN")
            icon_s, clr = icon_map.get(sig, ("?", "white"))
            chg    = c.get("change_pct")
            live   = c.get("live_price")
            prev   = c.get("prev_close")
            etfs   = c.get("nse_etfs", [])
            unit   = c.get("unit", "")
            chg_str  = f"[{clr}]{chg:+.3f}%[/{clr}]" if chg is not None else "[dim]N/A[/dim]"
            live_str = f"${live:,.2f}" if live is not None else "N/A"
            prev_str = f"${prev:,.2f}" if prev is not None else "N/A"
            etf_str  = ", ".join(etfs) if etfs else "[dim]—[/dim]"
            c_table.add_row(
                c.get("emoji", ""),
                f"[bold]{sym}[/bold]",
                c.get("name", sym),
                f"[{clr}]{icon_s} {sig}[/{clr}]",
                chg_str,
                live_str,
                prev_str,
                etf_str,
            )
        console.print(c_table)

    console.rule("[dim]End of COMEX Report[/dim]")


# ── Entry Point ───────────────────────────────────────────────────────────────

@app.command()
def ui(
    port: int = typer.Option(8501, "--port", "-p", help="Port to serve the Streamlit UI on."),
    host: str = typer.Option("localhost", "--host", help="Address to bind to."),
) -> None:
    """
    Launch the Mosaic Data Hub web UI (Streamlit).

    Opens a browser at http://<host>:<port> with three tabs:
      📥 Import Data — trigger historical data imports
      🔍 SQL Query   — run SQL against ClickHouse
      📊 Explorer    — interactive charts (Gold, GOLDBEES, iNAV)
    """
    import subprocess

    ui_path = str(Path(__file__).resolve().parent / "ui" / "app.py")
    cmd = [
        sys.executable, "-m", "streamlit", "run", ui_path,
        f"--server.port={port}",
        f"--server.address={host}",
        "--server.headless=false",
    ]
    console.print(
        Panel(
            f"[bold]🌐 Mosaic Data Hub[/bold]\n"
            f"[dim]Opening at [link=http://{host}:{port}]http://{host}:{port}[/link][/dim]",
            border_style="cyan",
        )
    )
    subprocess.run(cmd)


@app.command(name="import")
def import_data(
    category: str = typer.Option(
        "all",
        "--category",
        "-c",
        help=(
            "Comma-separated categories to import: "
            "stocks, etfs, commodities, indices, mf, all. "
            "Default: all."
        ),
    ),
    lookback_days: int = typer.Option(
        730,
        "--lookback",
        "-l",
        help="Days of history on first run (default 730 = ~2 years).",
    ),
    full_reimport: bool = typer.Option(
        False,
        "--full",
        help="Ignore watermarks and re-fetch the full lookback window.",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Fetch data but do NOT write to ClickHouse. Prints row counts only.",
    ),
) -> None:
    """
    Import historical market data (stocks, ETFs, MF NAV, commodities, indices)
    into ClickHouse.

    First run: fetches the full lookback window (default 2 years).
    Subsequent runs: delta-sync only — fetches from last watermark to today.

    \b
    Examples:
      mosaic import                          # full sync, all categories
      mosaic import --category stocks,etfs   # only stocks and ETFs
      mosaic import --category mf            # only mutual fund NAV
      mosaic import --dry-run                # preview without writing
      mosaic import --full --lookback 365    # re-import last 1 year
    """
    _setup_logging()

    categories = [c.strip().lower() for c in category.split(",") if c.strip()]

    console.print(
        Panel(
            f"[bold]📥 Historical Data Importer[/bold]\n"
            f"[dim]Categories: {', '.join(categories)} · "
            f"Lookback: {lookback_days}d · "
            f"{'Full re-import' if full_reimport else 'Delta sync'}"
            f"{' · DRY RUN' if dry_run else ''}[/dim]",
            border_style="cyan",
        )
    )

    from src.importer.cli import run_import

    run_import(
        categories=categories,
        lookback_days=lookback_days,
        full_reimport=full_reimport,
        dry_run=dry_run,
        console=console,
        clickhouse_host=settings.clickhouse_host,
        clickhouse_port=settings.clickhouse_port,
        clickhouse_database=settings.clickhouse_database,
        clickhouse_user=settings.clickhouse_user,
        clickhouse_password=settings.clickhouse_password,
    )


if __name__ == "__main__":
    app()
