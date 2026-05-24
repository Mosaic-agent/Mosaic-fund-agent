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
from src.tools.skills_tools import SKILLS_TOOLS
from src.tools.newsapi_search import get_newsapi_stock_news
from langchain_core.callbacks import BaseCallbackHandler
from rich.console import Console
from rich.panel import Panel
from rich.markdown import Markdown

logger = logging.getLogger(__name__)

# All tools available to the agent
ALL_TOOLS = ZERODHA_TOOLS + YAHOO_TOOLS + NEWS_TOOLS + [get_newsapi_stock_news] + EARNINGS_TOOLS + SUMMARIZATION_TOOLS + SKILLS_TOOLS


class RichConsoleCallbackHandler(BaseCallbackHandler):
    """Callback handler to print intermediate LLM steps and tool calls beautifully in the console."""

    def __init__(self) -> None:
        self.console = Console()

    def on_llm_start(self, serialized: dict[str, Any], prompts: list[str], **kwargs: Any) -> None:
        self.console.print("\n[bold cyan]🤖 Thinking...[/bold cyan]")

    def on_tool_start(self, serialized: dict[str, Any], input_str: str, **kwargs: Any) -> None:
        tool_name = serialized.get("name", "Unknown Tool")
        self.console.print(Panel(
            f"[bold yellow]🔧 Calling Tool:[/bold yellow] [green]{tool_name}[/green]\n"
            f"[dim]Arguments: {input_str.strip()}[/dim]",
            border_style="yellow",
            title="Tool Call",
        ))

    def on_tool_end(self, output: Any, **kwargs: Any) -> None:
        if hasattr(output, "content"):
            output_str = str(output.content).strip()
        else:
            output_str = str(output).strip()
        # Check if output is a markdown table or has markdown elements
        if "|" in output_str and "-" in output_str:
            self.console.print(Panel(
                Markdown(output_str),
                border_style="green",
                title="Tool Output",
            ))
        else:
            self.console.print(Panel(
                output_str,
                border_style="green",
                title="Tool Output",
            ))

    def on_tool_error(self, error: BaseException, **kwargs: Any) -> None:
        self.console.print(f"[bold red]✗ Tool Error:[/bold red] {error}")


# System prompt for the LangGraph ReAct agent
AGENT_SYSTEM_PROMPT = (
    "You are the Mosaic-fund-agent, a quantitative investment platform and agent for Indian equity markets (NSE/BSE). "
    "You have access to tools to fetch portfolio data, market information, news, financial results, "
    "and run core platform scripts. "
    "Guidance on using specific tools/data sources:\n"
    "  • Mutual Funds (MF): Use `query_clickhouse_db` to query `market_data.mf_holdings` and `market_data.mf_nav` directly, or use `run_fund_mom_returns` to fetch NAV returns.\n"
    "  • Yahoo Finance: Use `get_yahoo_finance_data` and `get_price_momentum` to fetch live prices, PE/PB ratios, dividend yield, and 52-week ranges.\n"
    "  • Screener.in / Quarterly Results: Use `get_quarterly_results` to fetch quarterly revenue, net profit, EPS, and YoY growth percentages.\n"
    "  • News API / Google News: Use `get_stock_news` (Google News RSS) and `get_newsapi_stock_news` (NewsAPI.org) to fetch recent financial news and infer sentiment.\n"
    "Your goal is to provide comprehensive, accurate investment insights on the user's Zerodha portfolio. "
    "Always reason step by step and use the available tools to gather data before answering. "
    "When presenting structured data, weight shifts, signals, returns, or tabular results from any tool, "
    "ALWAYS format the output in a clean, readable Markdown table rather than using lists or bullet points."
)


# ── Portfolio Agent ────────────────────────────────────────────────────────────

class PortfolioAgent:
    """
    Orchestrates the full portfolio intelligence workflow.

    This agent works in a direct orchestration mode (not purely ReAct)
    for the main workflow, using the ReAct agent for ad-hoc queries.
    """

    def __init__(self) -> None:
        try:
            self._llm = self._build_llm()
            self._agent = self._build_agent()
        except Exception as exc:
            logger.warning("LLM not available (%s).", exc)
            self._llm = None
            self._agent = None

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
          1. Fetch holdings from Zerodha Kite MCP
          2. Enrich each holding with Yahoo Finance, News, Earnings data
          3. Generate per-asset LLM insights
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
                    analysis = analyze_holding(holding)
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

        # ── Step 3: COMEX pre-market signals ────────────────────────────────────
        console.print("\n[bold cyan]Step 3/4:[/bold cyan] Fetching COMEX commodity pre-market signals...")
        try:
            from src.agents.comex_agent import ComexAgent
            comex: dict = ComexAgent().run()
            console.print(
                f"[green]✓ COMEX:[/green] {comex.get('overall_signal', '—')}  "
                f"({comex.get('summary', '')})"
            )
        except Exception as _exc:
            logger.debug("COMEX fetch failed: %s", _exc)
            comex = {"error": str(_exc)}
            console.print("[yellow]⚠ COMEX unavailable — scoring without commodity signals[/yellow]")

        # ── Fetch FII/DII institutional flow context ──────────────────────────
        fii_dii_ctx: dict = {"summary_str": "FII/DII flow data unavailable.", "rows": []}
        try:
            from src.tools.market_context import get_fii_dii_context
            fii_dii_ctx = get_fii_dii_context(days=5)
            if fii_dii_ctx.get("rows"):
                console.print(
                    f"[green]✓ FII/DII:[/green] "
                    f"{len(fii_dii_ctx['rows'])} days of flow data loaded"
                )
        except Exception as _exc:
            logger.debug("FII/DII context fetch failed: %s", _exc)

        # ── Step 4: Portfolio-Level Report (with COMEX + institutional context) ─
        console.print("\n[bold cyan]Step 4/4:[/bold cyan] Generating portfolio-level intelligence...")

        report = build_portfolio_report(
            portfolio, analyses,
            comex_signals=comex,
            institutional_flows=fii_dii_ctx,
        )

        console.print("[green]✓ Portfolio analysis complete[/green]")

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
            return "Agent not available — LLM is not configured. Set LLM_PROVIDER and API key in .env."

        from langchain_core.messages import HumanMessage
        import os

        try:
            messages = [HumanMessage(content=question)]
            config = {}
            if os.getenv("VERBOSE") == "1":
                config["callbacks"] = [RichConsoleCallbackHandler()]

            result = self._agent.invoke({"messages": messages}, config=config)
            msgs = result.get("messages", [])
            return msgs[-1].content if msgs else "No answer generated."
        except Exception as exc:
            err_msg = str(exc).lower()
            if "tool" in err_msg or "400" in err_msg or "invalid_request" in err_msg:
                logger.warning("Model does not support tools. Falling back to direct LLM Q&A.")
                if self._llm is not None:
                    try:
                        res = self._llm.invoke(messages)
                        return str(res.content)
                    except Exception as fallback_exc:
                        logger.error("Fallback LLM query failed: %s", fallback_exc)
                        return f"Error: {fallback_exc}"
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
