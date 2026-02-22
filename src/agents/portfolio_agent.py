"""
src/agents/portfolio_agent.py
──────────────────────────────
LangChain ReAct agent that orchestrates the full portfolio intelligence workflow.

Architecture:
  User triggers CLI
    ↓
  PortfolioAgent.run()
    ↓
  Step 1: Authenticate with Kite MCP → get login URL
  Step 2: Fetch all holdings via Kite MCP
  Step 3: For each holding → parallel enrichment (Yahoo, News, Earnings)
  Step 4: Per-asset LLM summarization
  Step 5: Portfolio-level aggregation and LLM analysis
  Step 6: Format and return structured JSON report

The agent uses LangChain's AgentExecutor with all registered tools so
it can autonomously decide which tools to call and in what order.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from langgraph.prebuilt import create_react_agent

from config.settings import settings
from src.analyzers.asset_analyzer import analyze_holding
from src.analyzers.portfolio_analyzer import build_portfolio_report
from src.clients.mcp_client import KiteMCPClient
from src.models.portfolio import Holding, Portfolio
from src.tools.earnings_scraper import EARNINGS_TOOLS
from src.tools.news_search import NEWS_TOOLS
from src.tools.summarization import SUMMARIZATION_TOOLS
from src.tools.yahoo_finance import YAHOO_TOOLS
from src.tools.zerodha_mcp_tools import ZERODHA_TOOLS, _parse_holdings
from src.utils.demo_data import get_demo_holdings

logger = logging.getLogger(__name__)

# All tools available to the agent
ALL_TOOLS = ZERODHA_TOOLS + YAHOO_TOOLS + NEWS_TOOLS + EARNINGS_TOOLS + SUMMARIZATION_TOOLS


# System prompt for the LangGraph ReAct agent
AGENT_SYSTEM_PROMPT = (
    "You are a Financial Portfolio Intelligence Agent for Indian equity markets (NSE/BSE). "
    "You have access to tools to fetch portfolio data, market information, news, and financial results. "
    "Your goal is to provide comprehensive, accurate investment insights on the user's Zerodha portfolio. "
    "Always reason step by step and use the available tools to gather data before answering."
)


# ── Portfolio Agent ────────────────────────────────────────────────────────────

class PortfolioAgent:
    """
    Orchestrates the full portfolio intelligence workflow.

    This agent works in a direct orchestration mode (not purely ReAct)
    for the main workflow, using the ReAct agent for ad-hoc queries.
    """

    def __init__(self, demo_mode: bool = False) -> None:
        self._demo_mode = demo_mode
        # Always attempt to build the LLM — even in demo mode.
        # demo_mode only controls the data source (sample holdings vs Kite).
        # LLM scoring is used whenever an LLM is successfully initialised.
        try:
            self._llm = self._build_llm()
            self._agent = self._build_agent()
        except Exception as exc:
            logger.warning(
                "LLM not available (%s) — falling back to rule-based scoring.", exc
            )
            self._llm = None
            self._agent = None

    @property
    def _use_llm_scoring(self) -> bool:
        """True when an LLM is configured and ready; False → rule-based fallback."""
        return self._llm is not None

    def _build_llm(self) -> Any:
        """
        Build the LLM instance from config.

        Supports three modes:
          1. Local model (LLM_BASE_URL set) — any OpenAI-compatible server
             e.g. Ollama (http://localhost:11434/v1) or LM Studio (http://localhost:1234/v1)
             Set LLM_MODEL to the model name your server expects, e.g. deepseek-r1:7b
          2. OpenAI cloud  — LLM_PROVIDER=openai  (default)
          3. Anthropic cloud — LLM_PROVIDER=anthropic

        [SENSITIVE] API keys are loaded from config/settings.py → .env
        """
        provider = settings.llm_provider.lower()

        # ── Local / custom OpenAI-compatible endpoint (Ollama, LM Studio, etc.) ──
        if settings.llm_base_url:
            from langchain_openai import ChatOpenAI
            logger.info(
                "Using local LLM: model=%s  base_url=%s",
                settings.llm_model,
                settings.llm_base_url,
            )
            return ChatOpenAI(
                model=settings.llm_model,
                base_url=settings.llm_base_url,
                # Local servers don't need a real key; use a placeholder if empty
                api_key=settings.openai_api_key or "local",
                temperature=0,
                max_tokens=settings.llm_token_budget,
            )

        # ── Anthropic cloud ────────────────────────────────────────────────────
        if provider == "anthropic":
            from langchain_anthropic import ChatAnthropic
            # [SENSITIVE] anthropic_api_key from .env
            return ChatAnthropic(
                model=settings.llm_model,
                api_key=settings.anthropic_api_key,
                temperature=0,
                max_tokens=settings.llm_token_budget,
            )

        # ── OpenAI cloud (default) ─────────────────────────────────────────────
        from langchain_openai import ChatOpenAI
        # [SENSITIVE] openai_api_key from .env
        return ChatOpenAI(
            model=settings.llm_model,
            api_key=settings.openai_api_key,
            temperature=0,
            max_tokens=settings.llm_token_budget,
        )

    def _build_agent(self) -> Any:
        """Build the LangGraph ReAct agent with all tools."""
        return create_react_agent(
            model=self._llm,
            tools=ALL_TOOLS,
            prompt=AGENT_SYSTEM_PROMPT,
        )

    # ── Public API ────────────────────────────────────────────────────────────

    def run_full_analysis(self, console: Any = None) -> dict[str, Any]:
        """
        Execute the full portfolio intelligence workflow on-demand.

        Steps:
          1. Fetch holdings from Zerodha Kite MCP (or demo data if demo_mode)
          2. Enrich each holding with Yahoo Finance, News, Earnings data
          3. Generate per-asset insights (LLM or rule-based in demo_mode)
          4. Aggregate into portfolio-level report

        Args:
            console: Rich Console instance for progress output (optional).

        Returns:
            Final portfolio report as a dict matching the specified JSON schema.
        """
        from rich.console import Console
        from rich.progress import Progress, SpinnerColumn, TextColumn

        if console is None:
            console = Console()

        # ── Step 1: Fetch Holdings ────────────────────────────────────────────
        if self._demo_mode:
            console.print(
                "\n[bold cyan]Step 1/4:[/bold cyan] "
                "[yellow]DEMO MODE[/yellow] — Using sample NSE portfolio "
                "(RELIANCE, TCS, HDFCBANK, INFY, NIFTYBEES, GOLDBEES)"
            )
            if self._use_llm_scoring:
                console.print(
                    "[dim]  LLM scoring active — "
                    f"{settings.llm_model} @ {settings.llm_base_url or 'cloud'}[/dim]"
                )
            else:
                console.print("[dim]  No LLM configured — using rule-based scoring.[/dim]")
            holdings = get_demo_holdings()
        else:
            console.print(
                "\n[bold cyan]Step 1/4:[/bold cyan] "
                "Fetching portfolio from Zerodha Kite MCP..."
            )
            holdings = self._fetch_holdings_sync()

        if not holdings:
            console.print("[bold red]✗ No holdings found. Ensure you are authenticated with Kite.[/bold red]")
            return {}

        # Apply max holdings cap from config
        max_h = settings.max_holdings_per_run
        if max_h and max_h > 0:
            holdings = holdings[:max_h]
            console.print(f"[yellow]⚠ Capped to {max_h} holdings (MAX_HOLDINGS_PER_RUN setting)[/yellow]")

        console.print(f"[green]✓ Found {len(holdings)} holdings[/green]")

        # Build Portfolio object for totals
        portfolio = Portfolio(holdings=holdings)

        # ── Step 2 & 3: Enrich Each Holding ──────────────────────────────────
        console.print(f"\n[bold cyan]Step 2/4:[/bold cyan] Enriching {len(holdings)} holdings with market data...")

        analyses = []
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Analyzing holdings...", total=len(holdings))
            for holding in holdings:
                progress.update(task, description=f"Analyzing [bold]{holding.tradingsymbol}[/bold]...")
                try:
                    analysis = analyze_holding(holding, use_llm_scoring=self._use_llm_scoring)
                    analyses.append(analysis)
                    console.print(
                        f"  [green]✓[/green] {holding.tradingsymbol} "
                        f"| Sentiment: {analysis.sentiment_score:+.2f} "
                        f"| Risk: {analysis.risk_score:.0f}/10"
                    )
                except Exception as exc:
                    logger.error("Analysis failed for %s: %s", holding.tradingsymbol, exc)
                    console.print(f"  [red]✗[/red] {holding.tradingsymbol} – analysis failed: {exc}")
                finally:
                    progress.advance(task)

        console.print(f"[green]✓ Enriched {len(analyses)} holdings[/green]")

        # ── Step 4: Portfolio-Level Report ────────────────────────────────────
        console.print("\n[bold cyan]Step 3/4:[/bold cyan] Generating portfolio-level intelligence...")

        report = build_portfolio_report(portfolio, analyses, use_llm_scoring=self._use_llm_scoring)

        console.print("[green]✓ Portfolio analysis complete[/green]")

        # ── Step 4b: COMEX pre-market signals (ComexAgent) ─────────────────────
        console.print("\n[bold cyan]COMEX:[/bold cyan] Fetching commodity pre-market signals...")
        try:
            from src.agents.comex_agent import ComexAgent
            comex = ComexAgent().run()
            console.print(
                f"[green]✓ COMEX signals:[/green] {comex.get('overall_signal', '—')}  "
                f"({comex.get('summary', '')})"
            )
        except Exception as _exc:
            logger.debug("COMEX fetch failed: %s", _exc)
            comex = {"error": str(_exc)}

        # Convert to dict and attach COMEX
        report_dict = report.model_dump()
        report_dict["comex_signals"] = comex
        return report_dict

    def ask(self, question: str) -> str:
        """
        Ask the agent a free-form question about the portfolio using ReAct reasoning.

        Args:
            question: Natural language question e.g. "Which of my stocks has the highest risk?"

        Returns:
            Agent's text response.
        """
        if self._agent is None:
            return (
                "Agent not available in demo mode. "
                "Run 'analyze --demo' for a full demo analysis."
            )

        from langchain_core.messages import HumanMessage, SystemMessage
        from src.utils.report_loader import load_latest_report, _compact_context

        try:
            messages = []

            # Inject the most recent analyze report so the agent can answer
            # questions without re-fetching everything from scratch.
            last_report = load_latest_report(settings.output_dir)
            if last_report:
                context = _compact_context(last_report)
                messages.append(
                    SystemMessage(
                        content=(
                            "The user's most recent portfolio analysis is shown below. "
                            "Use it as context when answering — call tools only if you need "
                            "fresher or more detailed data than what is provided here.\n\n"
                            f"--- LAST PORTFOLIO REPORT ---\n{context}\n--- END REPORT ---"
                        )
                    )
                )

            messages.append(HumanMessage(content=question))

            result = self._agent.invoke({"messages": messages})
            msgs = result.get("messages", [])
            return msgs[-1].content if msgs else "No answer generated."
        except Exception as exc:
            logger.error("Agent query failed: %s", exc)
            return f"Error: {exc}"

    # ── Private Helpers ───────────────────────────────────────────────────────

    def _fetch_holdings_sync(self) -> list[Holding]:
        """Synchronously fetch holdings from Kite MCP."""
        return asyncio.run(self._fetch_holdings_async())

    async def _fetch_holdings_async(self) -> list[Holding]:
        """Async holdings fetch with automatic login prompt if needed."""
        async with KiteMCPClient() as client:
            try:
                raw = await client.get_holdings()
                return _parse_holdings(raw)
            except Exception as exc:
                # If unauthorized, trigger login flow
                if "401" in str(exc) or "unauthorized" in str(exc).lower() or "login" in str(exc).lower():
                    logger.info("Not authenticated – initiating Kite login flow")
                    login_url = await client.login()
                    print(
                        f"\n[AUTH REQUIRED] Please open this URL in your browser to login:\n"
                        f"{login_url}\n"
                        f"Press ENTER after completing authentication..."
                    )
                    input()
                    raw = await client.get_holdings()
                    return _parse_holdings(raw)
                raise
