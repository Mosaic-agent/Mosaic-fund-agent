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
import warnings
warnings.filterwarnings("ignore", category=UserWarning)

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import box

# Ensure project root is on sys.path when running as script
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Guard: Ensure script is running inside the Docker container to prevent local package import errors
import os
if not os.environ.get('RUNNING_IN_DOCKER') and not os.path.exists('/.dockerenv') and os.environ.get('ALLOW_LOCAL_RUN') != '1':
    print("=================================================================", file=sys.stderr)
    print(" ERROR: This command must be run inside the Docker container", file=sys.stderr)
    print(" to ensure that all required dependencies are loaded correctly.", file=sys.stderr)
    print("-----------------------------------------------------------------", file=sys.stderr)
    print(" Please use the wrapper script instead:", file=sys.stderr)
    print(f"   ./mosaic.sh {' '.join(sys.argv[1:])}", file=sys.stderr)
    print(" (On Windows, use 'mosaic.bat' instead)", file=sys.stderr)
    print("-----------------------------------------------------------------", file=sys.stderr)
    print(" If you are a developer and want to bypass this check, set:", file=sys.stderr)
    print("   export ALLOW_LOCAL_RUN=1  (or set it in your environment)", file=sys.stderr)
    print("=================================================================", file=sys.stderr)
    sys.exit(1)

from config.settings import settings

app = typer.Typer(
    name="portfolio-insight",
    help="🇮🇳 Mosaic-fund-agent – powered by LangChain + Kite MCP",
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


def _check_config(require_llm: bool = True) -> bool:
    """Validate sensitive config fields and warn if missing. Returns True if OK."""
    warnings = settings.validate_sensitive_fields()
    if warnings:
        console.print("\n[bold yellow]⚠ Configuration Warnings:[/bold yellow]")
        for w in warnings:
            console.print(f"  [yellow]• {w}[/yellow]")
        if require_llm and not settings.openai_api_key and not settings.anthropic_api_key:
            console.print(
                "\n[bold red]✗ Cannot run analysis without an LLM API key.[/bold red]\n"
                "  Copy [bold].env.example → .env[/bold] and fill in your API keys.\n"
            )
            return False
    return True


# ── Global Callback ─────────────────────────────────────────────────────────────
@app.callback()
def main(
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show live LangChain reasoning and tool calls."),
):
    if verbose:
        import os
        os.environ["VERBOSE"] = "1"

    # Initialize the global SQLite LLM response cache
    try:
        from src.utils.llm_cache import setup_llm_cache
        setup_llm_cache()
    except Exception as exc:
        print(f"Warning: could not setup LLM cache: {exc}", file=sys.stderr)

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
) -> None:
    """
    Run full portfolio intelligence analysis on your Zerodha holdings.

    Fetches holdings from Kite MCP, enriches with Yahoo Finance data,
    recent news (NewsAPI), and quarterly results (Screener.in),
    then generates an AI-powered report with risk scores and insights.
    """
    _setup_logging()

    console.print(
        Panel(
            "[bold]Mosaic-fund-agent[/bold]\n"
            "[dim]Indian Equity Market Analysis | NSE & BSE[/dim]",
            border_style="blue",
        )
    )

    if not _check_config():
        raise typer.Exit(code=1)

    # Override max holdings if specified via CLI
    if max_holdings > 0:
        import os
        os.environ["MAX_HOLDINGS_PER_RUN"] = str(max_holdings)
        # Re-read settings to pick up the override
        settings.__dict__["max_holdings_per_run"] = max_holdings

    # Import agent lazily to avoid import errors when just checking config
    from src.agents.mosaic_fund_agent import MosaicFundAgent
    from src.formatters.output import print_report_to_console, save_json_report

    agent = MosaicFundAgent()

    try:
        report = agent.run_full_analysis(console=console)
    except Exception as exc:
        console.print(f"\n[bold red]✗ Analysis failed:[/bold red] {exc}")
        logging.exception("Full analysis failed")
        raise typer.Exit(code=1)

    if not report:
        console.print(
            "\n[bold red]✗ No report generated.[/bold red] "
            "Check your Kite authentication."
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
    caveman: str = typer.Option(
        None,
        "--caveman",
        "-c",
        help="Enable caveman mode with specific intensity (lite, full, ultra, wenyan, off).",
    ),
) -> None:
    """
    Ask the Mosaic-fund-agent a free-form question using ReAct reasoning.
    (Note: This uses the most recently generated portfolio report context)

    Examples:
      python src/main.py ask "Which stock has the highest risk in my portfolio?"
      python src/main.py ask "What is the sector concentration of my holdings?"
    """
    _setup_logging()

    if not _check_config():
        raise typer.Exit(code=1)

    if caveman:
        import os
        os.environ["CAVEMAN_LEVEL"] = caveman

    from src.agents.mosaic_fund_agent import MosaicFundAgent

    console.print(f"\n[bold]Question:[/bold] {question}\n")
    agent = MosaicFundAgent()

    try:
        answer = agent.ask(question)
        from src.utils.markdown_renderer import render_markdown_to_group
        console.print(Panel(render_markdown_to_group(answer), title="[bold green]Agent Response[/bold green]", border_style="green"))
    except Exception as exc:
        console.print(f"[bold red]✗ Error:[/bold red] {exc}")
        raise typer.Exit(code=1)


@app.command()
def chat(
    thread_id: str = typer.Option(
        None,
        "--thread-id",
        "-t",
        help="The conversation thread ID to resume (resumes memory from persistent DB)",
    )
) -> None:
    """
    Start an interactive multi-turn chat session with the Mosaic-fund-agent.

    Features:
      - Infinite prompt loop (exit with 'quit' or Ctrl-C)
      - Persistent conversation memory via SqliteSaver (saved in output/checkpoints.db)
      - Intent-based sub-agent routing: deepdive / signals / macro / main
      - Slash commands: /analyze, /signals, /deepdive TICKER, /macro, /clear, /help

    Examples:
      ./mosaic.sh                     — default: starts this chat
      ./mosaic.sh chat                — explicit
      ./mosaic.sh chat -t THREAD_ID   — resume an exited chat session
      /analyze --max 3               — run portfolio analysis inside chat
      /deepdive ADSK                 — US stock SEC deep-dive
      deep-dives adsk                — auto-routes to DeepDive sub-agent
    """
    _setup_logging()
    if not _check_config():
        raise typer.Exit(code=1)
    from src.commands.chat_cmd import run_chat_loop
    run_chat_loop(console, thread_id=thread_id)


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
        # Deep-Dive [NON-SENSITIVE]
        ("Gemini CLI Path", settings.gemini_cli_path, "No"),
        # [SENSITIVE] settings are masked
        ("OpenAI API Key", masked(settings.openai_api_key), "⚠ YES"),
        ("Anthropic API Key", masked(settings.anthropic_api_key), "⚠ YES"),
        ("OpenRouter API Key", masked(settings.openrouter_api_key), "⚠ YES"),
        ("NewsAPI Key", masked(settings.newsapi_key), "⚠ YES"),
        ("Kite API Key", masked(settings.kite_api_key), "⚠ YES"),
        ("Kite API Secret", masked(settings.kite_api_secret), "⚠ YES"),
        ("sec-api.io Key", masked(settings.sec_api_key), "⚠ YES"),
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

    if not _check_config():
        raise typer.Exit(code=1)

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


@app.command(name="premium-alerts")
def premium_alerts(
    lookback: int = typer.Option(
        30,
        "--lookback",
        "-l",
        help="Days of iNAV history used to compute mean/std (default 30).",
    ),
    z_threshold: float = typer.Option(
        -1.5,
        "--z-threshold",
        "-z",
        help="Z-score at or below which SCREAMING BUY fires (default -1.5).",
    ),
    symbols: str = typer.Option(
        "",
        "--symbols",
        "-s",
        help="Comma-separated NSE symbols to scan. Default: all international ETFs.",
    ),
    min_snapshots: int = typer.Option(
        5,
        "--min-snapshots",
        help="Minimum hourly snapshots required to compute a meaningful Z-score (default 5).",
    ),
) -> None:
    """
    Scarcity Premium Alerts for international ETFs (MAFANG, HNGSNGBEES, …).

    The RBI $7B overseas investment cap creates a structural premium on
    international ETFs that rarely reverts to zero.  This command trades the
    *volatility* of that premium: when it dips well below its 30-day mean,
    it signals a likely snap-back and a favourable entry point.

    Signal thresholds:
      Z ≤ -1.5   →  🟢 SCREAMING BUY
      Z ≤ -1.0   →  🟡 GOOD ENTRY
      otherwise  →  🔴 NO ACTION

    \\b
    Examples:
      python src/main.py premium-alerts
      python src/main.py premium-alerts --lookback 14 --z-threshold -1.0
      python src/main.py premium-alerts --symbols MAFANG,HNGSNGBEES
    """
    _setup_logging()

    from src.tools.premium_alerts import check_premium_alerts, INTL_ETF_SYMBOLS

    sym_list = (
        [s.strip().upper() for s in symbols.split(",") if s.strip()]
        if symbols
        else INTL_ETF_SYMBOLS
    )

    console.print(
        Panel(
            "[bold]🌍 International ETF — Scarcity Premium Alerts[/bold]\n"
            "[dim]RBI $7B overseas cap creates structural premiums. "
            "Trade the volatility of the premium — not the level itself.[/dim]",
            border_style="cyan",
        )
    )

    try:
        from src.db.pool import get_pool as _get_ch_pool
        ch = _get_ch_pool().get_client()  # unmanaged client; closed by check_premium_alerts
    except Exception as exc:
        console.print(f"[bold red]✗ ClickHouse connection failed:[/bold red] {exc}")
        raise typer.Exit(code=1)

    with console.status(f"[cyan]Computing premium Z-scores for {', '.join(sym_list)}…[/cyan]"):
        try:
            results = check_premium_alerts(
                ch_client=ch,
                symbols=sym_list,
                lookback_days=lookback,
                z_threshold=z_threshold,
                good_entry_threshold=z_threshold + 0.5,
                min_snapshots=min_snapshots,
            )
        except Exception as exc:
            console.print(f"[bold red]✗ Alert computation failed:[/bold red] {exc}")
            raise typer.Exit(code=1)
        finally:
            ch.close()

    if not results:
        console.print("[yellow]⚠ No results returned — check that iNAV snapshots exist in ClickHouse.[/yellow]")
        console.print("  Run: [bold]python src/main.py import --category inav[/bold]")
        raise typer.Exit(code=0)

    # ── Results table ─────────────────────────────────────────────────────────
    tbl = Table(
        title=f"Premium Z-Score Report  (lookback {lookback}d · Z threshold {z_threshold:+.1f})",
        box=box.ROUNDED,
        show_header=True,
        header_style="bold magenta",
    )
    tbl.add_column("Symbol",             min_width=14, style="bold")
    tbl.add_column("Latest Prem (%)",    min_width=16, justify="right")
    tbl.add_column(f"{lookback}d Avg (%)",  min_width=14, justify="right")
    tbl.add_column("Std Dev",            min_width=9,  justify="right")
    tbl.add_column("Z-Score",            min_width=9,  justify="right")
    tbl.add_column("Snapshots",          min_width=11, justify="right")
    tbl.add_column("Action",             min_width=20)

    n_buy = n_entry = n_noaction = n_bad = 0

    for r in results:
        latest = f"{r['latest_premium']:+.3f}%" if r["latest_premium"] is not None else "[dim]—[/dim]"
        avg    = f"{r['mean_premium']:+.3f}%"   if r["mean_premium"]   is not None else "[dim]—[/dim]"
        std    = f"{r['std_premium']:.4f}"       if r["std_premium"]    is not None else "[dim]—[/dim]"
        zscore = f"{r['z_score']:+.3f}"          if r["z_score"]        is not None else "[dim]—[/dim]"
        n_out  = r.get("n_outliers_removed", 0)
        snaps  = f"{r['n_snapshots']}" + (f" [dim](-{n_out}✂)[/dim]" if n_out else "")
        style  = r["action_style"]
        action = f"[{style}]{r['action']}[/{style}]"

        if r["error"]:
            action = f"[dim]{r['action']}[/dim]\n[dim italic]{r['error']}[/dim italic]"

        tbl.add_row(r["symbol"], latest, avg, std, zscore, snaps, action)

        if "SCREAMING" in r["action"]:  n_buy     += 1
        elif "ENTRY"   in r["action"]:  n_entry   += 1
        elif "NO ACTION" in r["action"]: n_noaction += 1
        else:                           n_bad     += 1

    console.print(tbl)

    # ── Summary footer ────────────────────────────────────────────────────────
    console.print()
    summary_parts = []
    if n_buy:
        summary_parts.append(f"[bold green]{n_buy} SCREAMING BUY[/bold green]")
    if n_entry:
        summary_parts.append(f"[bold yellow]{n_entry} GOOD ENTRY[/bold yellow]")
    if n_noaction:
        summary_parts.append(f"[red]{n_noaction} NO ACTION[/red]")
    if n_bad:
        summary_parts.append(f"[dim]{n_bad} insufficient/error[/dim]")

    console.print("  Signals: " + "  ·  ".join(summary_parts) if summary_parts else "")
    console.print(
        "[dim]  Strategy: RBI cap → structural premium. "
        "Buy when premium dips below its mean (low Z), "
        "not when it is high.[/dim]"
    )
    console.rule("[dim]End of Premium Alerts[/dim]")


@app.command()
def deepdive(
    ticker: str = typer.Argument(..., help="US ticker symbol, e.g. ADSK"),
    date: str = typer.Option(None, "--date", "-d", help="Report date YYYY-MM-DD (default: today)"),
    skip_fetch: bool = typer.Option(False, "--skip-fetch", help="Use cached files only; skip all network calls"),
    section: str = typer.Option(
        None,
        "--section",
        "-s",
        help="Regenerate one section: core_business|financials|competitors|investments|execution|valuation|talent",
    ),
) -> None:
    """
    Run a company deep-dive for a US-listed stock.

    Fetches SEC filings (via sec-api.io), XBRL financials, Workday job postings,
    and peer market data, then generates a 7-section markdown research report
    with source citations. Narrative is produced by the claude CLI binary
    (no Anthropic SDK or API key needed in .env).

    \b
    Examples:
      python src/main.py deepdive ADSK
      python src/main.py deepdive ADSK --skip-fetch
      python src/main.py deepdive ADSK --section valuation
    """
    _setup_logging()
    if not _check_config(require_llm=False):
        raise typer.Exit(code=1)
    from src.deepdive.runner import run_deepdive
    run_deepdive(ticker=ticker.upper(), date=date, skip_fetch=skip_fetch, section=section)


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
            "stocks, etfs, commodities, indices, mf, inav, nse_eod, "
            "cot, cb_reserves, etf_aum, mf_holdings, fii_dii, "
            "earnings, insider, valuation, "
            "world_bank, imf_weo, tijori_macro, "
            "icici, nippon, icici-index, dsp, all. "
            "Default: all."
        ),
    ),
    lookback_days: int = typer.Option(
        3650,
        "--lookback",
        "-l",
        help="Days of history on first run (default 3650 = ~10 years).",
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
    data_source: str = typer.Option(
        "",
        "--source",
        help="Price source for stock/ETF imports: shoonya, nse, or yfinance.",
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
      mosaic import --category stocks --source nse
      mosaic import --category mf            # only mutual fund NAV
      mosaic import --dry-run                # preview without writing
      mosaic import --full --lookback 365    # re-import last 1 year
    """
    _setup_logging()

    categories = [c.strip().lower() for c in category.split(",") if c.strip()]
    imports_market_prices = "all" in categories or bool({"stocks", "etfs"} & set(categories))
    reused_saved_source = False
    if imports_market_prices:
        from src.importer.source_preference import resolve_data_source

        try:
            data_source, reused_saved_source = resolve_data_source(data_source)
        except ValueError as exc:
            raise typer.BadParameter(str(exc)) from exc

    if imports_market_prices and not data_source:
        console.print(
            "\n[bold]Select the stock/ETF data source:[/bold]\n"
            "  1. Shoonya\n"
            "  2. NSE\n"
            "  3. yfinance"
        )
        source_choice = typer.prompt("Data source", type=int, default=1)
        source_options = {1: "shoonya", 2: "nse", 3: "yfinance"}
        if source_choice not in source_options:
            raise typer.BadParameter("Choose 1 (Shoonya), 2 (NSE), or 3 (yfinance).")
        data_source, _ = resolve_data_source(source_options[source_choice])
    elif reused_saved_source:
        console.print(
            f"[dim]Using saved data source: {data_source} "
            "(valid for 24 hours from selection).[/dim]"
        )

    console.print(
        Panel(
            f"[bold]📥 Historical Data Importer[/bold]\n"
            f"[dim]Categories: {', '.join(categories)} · "
            f"Lookback: {lookback_days}d · "
            f"{'Full re-import' if full_reimport else 'Delta sync'}"
            f"{f' · Source: {data_source}' if data_source else ''}"
            f"{' · DRY RUN' if dry_run else ''}[/dim]",
            border_style="cyan",
        )
    )

    from src.commands.base import CommandRunner
    from src.commands.import_cmd import ImportDataCommand

    cmd = ImportDataCommand(
        categories=categories,
        lookback_days=lookback_days,
        full_reimport=full_reimport,
        dry_run=dry_run,
        data_source=data_source,
    )
    runner = CommandRunner()
    
    try:
        runner.run(cmd)
    except Exception as exc:
        console.print(f"[bold red]✗ Import failed:[/bold red] {exc}")
        raise SystemExit(1)


@app.command(name="macro")
def macro_scan(
    max_per_theme: int = typer.Option(
        4, "--max", "-m", help="Max articles per macro theme (default 4).",
    ),
    save: bool = typer.Option(
        False, "--save", "-s", help="Save results to ClickHouse DB.",
    ),
) -> None:
    """
    Scan live macro & geopolitical events and map them to ETF impact.

    Detects 8 themes: War/Geopolitics, Fed/RBI Policy, Crude Oil, Currency,
    Trade War, India Macro, Gold/Commodity, Global Risk-Off.

    No API key required — uses Google News RSS + Yahoo Finance.

    \b
    Examples:
      mosaic macro              # full macro scan
      mosaic macro --max 6      # 6 articles per theme
      mosaic macro --save       # scan + persist to ClickHouse
    """
    _setup_logging()

    from rich.live import Live
    from rich.spinner import Spinner
    from rich.text import Text
    from src.tools.macro_event_scanner import scan_macro_events, print_macro_report

    status = {"msg": "Initialising macro scanner…"}

    def _cb(msg: str) -> None:
        status["msg"] = msg

    def _render():
        return Text.assemble(Spinner("dots").render(0), " ", status["msg"])

    with Live(_render(), refresh_per_second=8, console=console) as live:
        def _live_cb(msg: str) -> None:
            status["msg"] = msg
            live.update(Text.assemble("⠿ ", ("  " + msg, "cyan")))

        report = scan_macro_events(max_per_theme=max_per_theme, progress_cb=_live_cb)

    print_macro_report(report, max_per_theme=max_per_theme)

    if save:
        from src.importer.clickhouse import ClickHouseImporter
        from src.tools.macro_event_scanner import save_macro_events_to_db
        ch = ClickHouseImporter(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            username=settings.clickhouse_user,
            password=settings.clickhouse_password,
            database=settings.clickhouse_database,
        )
        ch.ensure_schema()
        n = save_macro_events_to_db(report, ch)
        console.print(f"[green]✓ Saved {n} macro events to DB.[/green]")


@app.command(name="macro-themes")
def cmd_macro_themes(
    max: int = typer.Option(4, "--max", "-m", help="Headlines per theme"),
    json_out: bool = typer.Option(False, "--json", help="JSON output"),
):
    """Long/Short macro theme agent — news + quant overlay."""
    _setup_logging()
    if not _check_config():
        raise typer.Exit(code=1)

    from rich.live import Live
    from rich.text import Text
    from src.scripts.market.macro_theme_agent import run_macro_theme_agent, print_macro_theme_report

    with Live("", refresh_per_second=8, console=console) as live:
        def _cb(msg: str) -> None:
            live.update(Text.assemble("⠿ ", ("  " + msg, "cyan")))

        report = run_macro_theme_agent(max_per_theme=max, progress_cb=_cb)

    if json_out:
        import json
        from dataclasses import asdict
        typer.echo(json.dumps(asdict(report), indent=2))
    else:
        print_macro_theme_report(report)


@app.command(name="etf-news")
def etf_news(
    category: str = typer.Option(
        "",
        "--category",
        "-c",
        help=(
            "Comma-separated ETF categories to scan. "
            "Options: 'Gold ETFs', 'Nifty ETFs', 'Bank ETFs', 'IT ETFs', "
            "'PSU ETFs', 'Mid/Small Cap ETFs', 'Pharma ETFs', "
            "'International ETFs', 'Debt / Liquid ETFs', 'Auto ETFs'. "
            "Default: all categories."
        ),
    ),
    max_per_topic: int = typer.Option(
        4,
        "--max",
        "-m",
        help="Max articles per search topic (default 4).",
    ),
    save: bool = typer.Option(
        False, "--save", "-s", help="Save results to ClickHouse DB.",
    ),
) -> None:
    """
    Fetch free news that can impact Indian ETFs.

    Uses Google News RSS + Yahoo Finance — no API key required.
    Each article is tagged with the ETFs it affects and a sentiment score.

    \b
    Examples:
      mosaic etf-news                          # scan all ETF categories
      mosaic etf-news --category "Gold ETFs"   # gold ETFs only
      mosaic etf-news --category "Gold ETFs,Bank ETFs"
      mosaic etf-news --max 6                  # 6 articles per topic
      mosaic etf-news --save                   # scan + persist to ClickHouse
    """
    _setup_logging()

    categories = [c.strip() for c in category.split(",") if c.strip()] or None

    console.print(
        Panel(
            "[bold cyan]📰 ETF-Impact News Scanner[/bold cyan]\n"
            f"[dim]Categories: {', '.join(categories) if categories else 'All'}  •  "
            f"Max {max_per_topic} articles/topic  •  "
            "Sources: Google News RSS + Yahoo Finance + NewsAPI (Indian fin. press)[/dim]",
            border_style="cyan",
        )
    )

    from src.tools.etf_news_scanner import scan_etf_news, print_etf_news_report

    report = scan_etf_news(categories=categories, max_per_topic=max_per_topic)
    print_etf_news_report(report)

    if save:
        from src.importer.clickhouse import ClickHouseImporter
        from src.tools.etf_news_scanner import save_etf_news_to_db
        ch = ClickHouseImporter(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            username=settings.clickhouse_user,
            password=settings.clickhouse_password,
            database=settings.clickhouse_database,
        )
        ch.ensure_schema()
        n = save_etf_news_to_db(report, ch)
        console.print(f"[green]✓ Saved {n} ETF news articles to DB.[/green]")


@app.command(name="risk")
def risk_cmd(
    symbol: str = typer.Option(
        "GOLDBEES", "--symbol", "-s",
        help="ETF symbol to size (currently GOLDBEES only — predictor scope).",
    ),
    save: bool = typer.Option(
        False, "--save",
        help="Persist today's weight decisions to ClickHouse weight_checkpoints table.",
    ),
    evaluate: bool = typer.Option(
        False, "--evaluate", "-e",
        help="Show realised performance by method from stored checkpoints.",
    ),
    since_days: int = typer.Option(
        90, "--since-days",
        help="Lookback window in days for --evaluate (default 90).",
    ),
    blend: float = typer.Option(
        0.5, "--blend",
        help="Kelly blend fraction: 0=pure RG, 1=pure Kelly, 0.5=50/50 (default).",
    ),
) -> None:
    """
    Adaptive Kelly position sizing — blends LightGBM forecast with Risk Governor.

    Shows recommended weights for four methods side-by-side:
      rg         Inverse-vol + regime + trend filter (existing Risk Governor)
      kelly      Pure Kelly from LightGBM expected return + quantile vol
      blended    Convex blend of RG and Kelly (configurable --blend ratio)

    \\b
    Examples:
      mosaic risk                            # show today's weights for GOLDBEES
      mosaic risk --save                     # save today's decisions to ClickHouse
      mosaic risk --evaluate                 # realised performance over last 90 days
      mosaic risk --evaluate --since-days 180
    """
    import math
    from datetime import date as dt_date
    import yfinance as yf
    import numpy as np

    _setup_logging()

    console.print(Panel(
        f"[bold cyan]⚖️  Adaptive Kelly — {symbol}[/bold cyan]\n"
        "[dim]Risk Governor (RG) + LightGBM Kelly blend[/dim]",
        border_style="cyan",
    ))

    # ── Evaluate mode ─────────────────────────────────────────────────────────
    if evaluate:
        from src.tools.weight_checkpoint import evaluate_methods
        console.print(f"\n[bold]Realised performance by method (last {since_days} days)[/bold]")
        df = evaluate_methods(symbol=symbol, since_days=since_days)
        if df.empty:
            console.print("[yellow]No checkpoint data found. Run `mosaic risk --save` first.[/yellow]")
            return
        tbl = Table(box=box.SIMPLE_HEAD, show_header=True)
        for col in ["method", "n", "total_return_pct", "ann_return_pct",
                    "ann_vol_pct", "sharpe", "hit_ratio_pct", "avg_weight_pct"]:
            tbl.add_column(col, justify="right" if col != "method" else "left")
        for _, row in df.iterrows():
            sharpe_color = "green" if (row.get("sharpe") or 0) > 0 else "red"
            tbl.add_row(
                row["method"],
                str(int(row["n"])),
                f"{row['total_return_pct']:.1f}%",
                f"{row['ann_return_pct']:.1f}%",
                f"{row['ann_vol_pct']:.1f}%",
                f"[{sharpe_color}]{row['sharpe']:.2f}[/{sharpe_color}]",
                f"{row['hit_ratio_pct']:.0f}%",
                f"{row['avg_weight_pct']:.0f}%",
            )
        console.print(tbl)
        return

    # ── Fetch live inputs ─────────────────────────────────────────────────────
    from config.settings import settings
    from src.tools.risk_governor import compute_position_weight, vol_target_for

    today = dt_date.today()

    # 1. Latest ML prediction from ClickHouse
    from src.db.pool import get_pool as _get_ch_pool
    _ch_pool = _get_ch_pool()
    pred_df = _ch_pool.query_df("""
        SELECT expected_return_pct, confidence_low, confidence_high,
               cv_r2_mean, regime_signal, horizon_days
        FROM market_data.ml_predictions FINAL
        ORDER BY as_of DESC LIMIT 1
    """)

    if pred_df.empty:
        console.print("[red]No ML predictions found. Run `python src/main.py signals` or the trend predictor first.[/red]")
        return

    pred = pred_df.iloc[0]
    exp_ret   = float(pred["expected_return_pct"])
    conf_low  = float(pred["confidence_low"])
    conf_high = float(pred["confidence_high"])
    cv_r2     = float(pred["cv_r2_mean"])
    ml_regime = str(pred["regime_signal"])
    horizon   = int(pred["horizon_days"])

    # 2. Latest GARCH vol + regime from anomaly pipeline
    price_df = _ch_pool.query_df(f"""
        SELECT trade_date,
               toFloat64(argMax(close, imported_at)) AS close
        FROM market_data.daily_prices
        WHERE symbol = '{symbol}' AND category = 'etfs'
        GROUP BY trade_date ORDER BY trade_date ASC
    """)

    garch_vol_pct = vol_target_for(symbol)  # fallback if GARCH fails
    regime = "✅ Normal"
    price_below_ema50 = False

    if not price_df.empty:
        try:
            import pandas as _pd
            from src.ml.anomaly import run_composite_anomaly
            price_df["trade_date"] = _pd.to_datetime(price_df["trade_date"])
            price_df_full = _pd.DataFrame({
                "trade_date": price_df["trade_date"],
                "open": price_df["close"], "high": price_df["close"],
                "low": price_df["close"],  "close": price_df["close"],
                "volume": 0,
            })
            df_res, _, _ = run_composite_anomaly(price_df_full)
            last = df_res.dropna(subset=["garch_vol"]).iloc[-1]
            garch_vol_pct = float(last["garch_vol"])
            regime = str(last["regime"])
            close_series = price_df["close"]
            ema50 = close_series.ewm(span=50, adjust=False).mean()
            price_below_ema50 = bool(close_series.iloc[-1] < ema50.iloc[-1])
        except Exception as exc:
            logger.warning("GARCH computation failed, using vol target: %s", exc)

    # ── Compute all method weights ─────────────────────────────────────────────
    from src.tools.adaptive_kelly import compute_kelly_weight, compute_blended_weight

    vol_target = vol_target_for(symbol)
    rg_dec = compute_position_weight(
        garch_annual_vol_pct=garch_vol_pct,
        regime=regime,
        vol_target_pct=vol_target,
        price_below_ema50=price_below_ema50,
    )
    kelly_dec = compute_kelly_weight(
        expected_return_pct=exp_ret,
        confidence_low_pct=conf_low,
        confidence_high_pct=conf_high,
        horizon_days=horizon,
        cv_r2=cv_r2,
        garch_annual_vol_pct=garch_vol_pct,
    )
    blended_w   = compute_blended_weight(rg_dec.final_weight, kelly_dec.final_weight, blend)
    blended_30  = compute_blended_weight(rg_dec.final_weight, kelly_dec.final_weight, 0.3)

    # ── Display ───────────────────────────────────────────────────────────────
    console.print(f"\n[dim]As of {today}  |  GARCH vol: {garch_vol_pct:.1f}%  |  "
                  f"Regime: {regime}  |  EMA50: {'below ⬇' if price_below_ema50 else 'above ⬆'}[/dim]")
    console.print(f"[dim]ML expected return: {exp_ret:+.2f}%  |  "
                  f"Conf band: [{conf_low:.2f}%, {conf_high:.2f}%]  |  CV R²: {cv_r2:.3f}[/dim]\n")

    tbl = Table(box=box.SIMPLE_HEAD, show_header=True)
    tbl.add_column("Method",   style="bold")
    tbl.add_column("Weight",   justify="right")
    tbl.add_column("Tier",     justify="center")
    tbl.add_column("Notes")

    def _tier_color(w: float) -> str:
        if w >= 0.85: return "green"
        if w >= 0.65: return "yellow"
        if w >= 0.40: return "dark_orange"
        return "red"

    rows_data = [
        ("rg",          rg_dec.final_weight,  rg_dec.tier,
         f"inverse-vol × regime × trend"),
        ("kelly",       kelly_dec.final_weight, "—",
         f"μ/σ²  raw={kelly_dec.raw_kelly:.1f}×  haircut={kelly_dec.confidence_haircut:.0%}"),
        (f"blended_{int(blend*100)}", blended_w, "—",
         f"{int((1-blend)*100)}% RG + {int(blend*100)}% Kelly"),
        ("blended_30",  blended_30, "—",
         "70% RG + 30% Kelly (conservative)"),
    ]
    for method, w, tier, notes in rows_data:
        c_ = _tier_color(w)
        tbl.add_row(method, f"[{c_}]{w:.0%}[/{c_}]", tier, notes)

    console.print(tbl)

    # ── Save checkpoints ──────────────────────────────────────────────────────
    if save:
        from src.tools.weight_checkpoint import save_checkpoints
        checkpoint_rows = [
            {
                "as_of": today, "symbol": symbol, "method": "rg",
                "recommended_weight": rg_dec.final_weight,
                "garch_vol_pct": garch_vol_pct, "regime": regime,
                "price_below_ema50": int(price_below_ema50),
                "horizon_days": horizon,
                "rationale": f"vol={garch_vol_pct:.1f}% regime_mult={rg_dec.regime_mult:.0%} trend={rg_dec.trend_mult:.0%}",
            },
            {
                "as_of": today, "symbol": symbol, "method": "kelly",
                "recommended_weight": kelly_dec.final_weight,
                "expected_return_pct": exp_ret, "expected_vol_pct": kelly_dec.implied_vol_pct,
                "garch_vol_pct": garch_vol_pct, "regime": regime,
                "price_below_ema50": int(price_below_ema50),
                "cv_r2": cv_r2, "horizon_days": horizon,
                "rationale": f"raw_kelly={kelly_dec.raw_kelly:.2f} frac={kelly_dec.fractional_kelly:.2f} haircut={kelly_dec.confidence_haircut:.0%}",
            },
            {
                "as_of": today, "symbol": symbol, "method": f"blended_{int(blend*100)}",
                "recommended_weight": blended_w,
                "expected_return_pct": exp_ret, "expected_vol_pct": kelly_dec.implied_vol_pct,
                "garch_vol_pct": garch_vol_pct, "regime": regime,
                "price_below_ema50": int(price_below_ema50),
                "cv_r2": cv_r2, "horizon_days": horizon,
                "rationale": f"rg={rg_dec.final_weight:.0%} kelly={kelly_dec.final_weight:.0%} blend={blend:.0%}",
            },
            {
                "as_of": today, "symbol": symbol, "method": "blended_30",
                "recommended_weight": blended_30,
                "expected_return_pct": exp_ret, "expected_vol_pct": kelly_dec.implied_vol_pct,
                "garch_vol_pct": garch_vol_pct, "regime": regime,
                "price_below_ema50": int(price_below_ema50),
                "cv_r2": cv_r2, "horizon_days": horizon,
                "rationale": f"rg={rg_dec.final_weight:.0%} kelly={kelly_dec.final_weight:.0%} blend=30%",
            },
        ]
        n = save_checkpoints(checkpoint_rows)
        console.print(f"\n[green]✓ Saved {n} checkpoint rows to market_data.weight_checkpoints[/green]")


@app.command(name="signals")
def signals_cmd(
    save: bool = typer.Option(
        False, "--save", "-s", help="Save composite scores to ClickHouse DB.",
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Show per-source debug info.",
    ),
) -> None:
    """
    Run the Signal Aggregator — combine macro, news, valuation, flow,
    ML, and anomaly signals into a unified per-ETF composite score.

    Each ETF gets a 0–100 score with an action: BUY / ACCUMULATE / HOLD / TRIM / AVOID.

    \b
    Examples:
      mosaic signals                # compute and display
      mosaic signals --save         # compute + persist to ClickHouse
      mosaic signals --save -v      # verbose logging
    """
    _setup_logging()

    from src.agents.signal_aggregator import run_signal_aggregation, print_signal_report

    report = run_signal_aggregation(save=save, verbose=verbose)
    print_signal_report(report)

    if save:
        console.print(f"[green]✓ Signal composite saved to DB for {len(report.signals)} ETFs.[/green]")


@app.command(name="drift-monitor")
def drift_monitor_cmd(
    retrain: bool = typer.Option(
        True, "--retrain/--no-retrain", help="Auto-trigger retraining if drift is detected."
    ),
    lookback: int = typer.Option(
        90, "--lookback", "-l", help="Number of predictions to evaluate."
    ),
) -> None:
    """
    Monitor GOLDBEES ML prediction model drift.

    Queries predictions, maps them to future realized returns, computes
    hit ratios and AUC over time, and triggers retraining if skill degrades.
    """
    _setup_logging()

    from src.ml.drift_monitor import run_drift_monitor

    run_drift_monitor(lookback_days=lookback, auto_retrain=retrain)


@app.command(name="telemetry")
def telemetry_cmd(
    live: bool = typer.Option(False, "--live", "-l", help="Run in live update mode (refresh every 2s)."),
    prompt: str = typer.Option(None, "--prompt", "-p", help="Submit a test prompt to the LLM to verify caching and latency.")
) -> None:
    """
    Display live system telemetry dashboard.

    Shows host RAM, local Ollama model engine/port/memory, active
    Docker containers (CPU/Memory usage), ClickHouse table sizes,
    and semantic LLM cache stats.
    """
    _setup_logging()
    from src.scripts.portfolio.system_telemetry import render_dashboard
    render_dashboard(live=live, prompt=prompt)


@app.command(name="correlate")
def correlate_cmd(
    symbol: str = typer.Option("MSUMI", "--symbol", "-s", help="NSE stock ticker symbol to analyze."),
    lookback: int = typer.Option(365, "--lookback", "-l", help="Lookback window in days for anomaly history."),
) -> None:
    """
    Map stock anomalies to company filings and global macro trigger events.
    """
    _setup_logging()
    from src.tools.market.correlation_tools import find_anomaly_correlations
    result = find_anomaly_correlations.func(symbol=symbol, lookback_days=lookback)
    typer.echo(result)


@app.command()
def research(
    question: str = typer.Argument(..., help="Free-text question, e.g. 'research ADANIENT'"),
) -> None:
    """Deep equity research via LangGraph StateGraph workflow (80% fewer tokens than agent)."""
    _setup_logging()
    from rich.console import Console
    from rich.markdown import Markdown
    _console = Console()
    from src.workflows.autonomous_research import run
    report = run(question)
    _console.print(Markdown(report))


@app.command(name="portfolio-wf")
def portfolio_wf() -> None:
    """Portfolio analysis with adversarial verification from ClickHouse holdings."""
    _setup_logging()
    from rich.console import Console
    from rich.markdown import Markdown
    _console = Console()
    from src.workflows.portfolio_analysis import run
    report = run()
    _console.print(Markdown(report))

@app.command(name="crossover")
def crossover(
    symbol: str = typer.Option("GOLDBEES", "--symbol", "-s", help="Symbol to backtest"),
    fast: int = typer.Option(50, "--fast", "-f", help="Fast MA period"),
    slow: int = typer.Option(200, "--slow", "-l", help="Slow MA period"),
    ma_type: str = typer.Option("sma", "--type", "-t", help="MA type (sma, ema)"),
    plot: bool = typer.Option(True, "--plot", "-p", help="Generate and save chart"),
) -> None:
    """Run a Moving Average Crossover backtest for a stock or ETF."""
    _setup_logging()
    from src.scripts.market.ma_crossover_backtest import run_crossover_backtest, print_cli_report
    metrics = run_crossover_backtest(symbol, fast, slow, ma_type, plot)
    print_cli_report(metrics)


@app.command(name="scan-setups")
def scan_setups() -> None:
    """Scan all 18 tracked ETFs for volume-volatility setups."""
    _setup_logging()
    from rich.console import Console
    from rich.markdown import Markdown
    from src.tools.etf_setup_scanner import scan_etf_setups
    
    _console = Console()
    report = scan_etf_setups.func()
    _console.print(Markdown(report))


@app.command(name="scan-trends")
def scan_trends() -> None:
    """Scan all 18 tracked ETFs for short, medium, and long term trend status."""
    _setup_logging()
    from rich.console import Console
    from rich.markdown import Markdown
    from src.tools.etf_setup_scanner import scan_etf_trends
    
    _console = Console()
    report = scan_etf_trends.func()
    _console.print(Markdown(report))


if __name__ == "__main__":
    app()


