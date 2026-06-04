"""
src/agents/mosaic_fund_agent.py
──────────────────────────────
LangChain ReAct agent that orchestrates the full portfolio intelligence workflow.

Architecture:
  User triggers CLI
    ↓
  MosaicFundAgent.run()
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
import concurrent.futures
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
from src.tools.chart_tools import CHART_TOOLS
from src.tools.shoonya_tools import SHOONYA_TOOLS
from langchain_core.callbacks import BaseCallbackHandler
from rich.console import Console
from rich.panel import Panel
from rich.markdown import Markdown

logger = logging.getLogger(__name__)

# All tools available to the agent
ALL_TOOLS = ZERODHA_TOOLS + YAHOO_TOOLS + NEWS_TOOLS + [get_newsapi_stock_news] + EARNINGS_TOOLS + SUMMARIZATION_TOOLS + SKILLS_TOOLS + CHART_TOOLS + SHOONYA_TOOLS


def _make_daemon_thread() -> None:
    import threading
    try:
        threading.current_thread().daemon = True
    except RuntimeError:
        pass  # In Python 3.13+, daemon status cannot be set on active threads; ignore it.


class RichConsoleCallbackHandler(BaseCallbackHandler):
    """Callback handler to print intermediate LLM steps and tool calls beautifully in the console."""

    def __init__(self) -> None:
        self.console = Console()
        self._llm_start_time = None

    def on_llm_start(self, serialized: dict[str, Any], prompts: list[str], **kwargs: Any) -> None:
        import time
        self.console.print("\n[bold cyan]🤖 Thinking...[/bold cyan]")
        self._llm_start_time = time.time()

    def on_llm_end(self, response: Any, **kwargs: Any) -> None:
        try:
            import re
            import time
            import json
            from pathlib import Path
            if not getattr(self, "_llm_start_time", None):
                return
            elapsed = time.time() - self._llm_start_time
            token_usage = {}
            model_name = settings.llm_model

            # Extract and display native thinking content (qwen3 / deepseek-r1 via Ollama think=true)
            if settings.llm_think and response and hasattr(response, "generations") and response.generations:
                for gen_list in response.generations:
                    for gen in gen_list:
                        think_text = None
                        # Check additional_kwargs for a dedicated thinking field
                        msg = getattr(gen, "message", None)
                        if msg:
                            ak = getattr(msg, "additional_kwargs", {}) or {}
                            think_text = ak.get("thinking") or ak.get("reasoning_content")
                        # Fall back to extracting <think>...</think> from content text
                        if not think_text:
                            raw = getattr(gen, "text", "") or ""
                            m = re.search(r"<think>(.*?)</think>", raw, re.DOTALL)
                            if m:
                                think_text = m.group(1).strip()
                        if think_text:
                            self.console.print(Panel(
                                think_text,
                                title="[bold magenta]💭 Reasoning[/bold magenta]",
                                border_style="magenta",
                                style="dim",
                            ))
                        break

            if response and hasattr(response, "llm_output") and response.llm_output:
                token_usage = response.llm_output.get("token_usage", {})
                model_name = response.llm_output.get("model_name", model_name)

            if not token_usage and response and hasattr(response, "generations") and response.generations:
                for gen_list in response.generations:
                    for gen in gen_list:
                        if hasattr(gen, "generation_info") and gen.generation_info:
                            token_usage = gen.generation_info.get("token_usage", {})
                            model_name = gen.generation_info.get("model_name", model_name)
                            break
            
            if token_usage:
                completion_tokens = token_usage.get("completion_tokens", 0)
                prompt_tokens = token_usage.get("prompt_tokens", 0)
                total_tokens = token_usage.get("total_tokens", 0)
                token_speed = completion_tokens / elapsed if elapsed > 0 else 0
                
                stats = {
                    "completion_tokens": completion_tokens,
                    "prompt_tokens": prompt_tokens,
                    "total_tokens": total_tokens,
                    "elapsed_seconds": round(elapsed, 3),
                    "token_speed": round(token_speed, 1),
                    "model_name": model_name,
                    "timestamp": time.time()
                }
                
                cache_dir = Path(settings.output_dir) / ".cache"
                cache_dir.mkdir(parents=True, exist_ok=True)
                with open(cache_dir / "last_query_telemetry.json", "w") as f:
                    json.dump(stats, f)
        except Exception:
            pass

    def on_tool_start(self, serialized: dict[str, Any], input_str: str, **kwargs: Any) -> None:
        tool_name = serialized.get("name", "Unknown Tool")
        self.console.print(Panel(
            f"[bold yellow]🔧 Calling Tool:[/bold yellow] [green]{tool_name}[/green]\n"
            f"[dim]Arguments: {input_str.strip()}[/dim]",
            border_style="yellow",
            title="Tool Call",
        ))

    def on_tool_end(self, output: Any, **kwargs: Any) -> None:
        from rich.text import Text
        if hasattr(output, "content"):
            output_str = str(output.content).strip()
        else:
            output_str = str(output).strip()

        # Detect ASCII chart output — contains box-drawing chars from plotext.
        # Render with Text.from_ansi + no_wrap so lines stay intact.
        _CHART_CHARS = ("┤", "┼", "─", "└", "┐", "┘", "┌", "├", "┬", "┴", "╮", "╰", "╭")
        is_chart = any(c in output_str for c in _CHART_CHARS)

        if is_chart:
            _text = Text.from_ansi(output_str)
            _text.no_wrap = True
            self.console.print(Panel(
                _text,
                border_style="green",
                title="Tool Output",
                expand=False,
            ))
        elif "|" in output_str and "-" in output_str:
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
    "  • Importing Stocks: If the user names a SPECIFIC symbol (e.g. 'import ADVENZYMES', 'refresh GOLDBEES'), call `import_symbol_data(symbol)` — never `run_data_engineering_importer` for a single symbol. Only use `run_data_engineering_importer(category='stocks')` when the user asks to import ALL stocks generically without naming one. When the user specifies a particular year (e.g. '2019'), date, or month range, parse the dates and pass them as `start_date` (format YYYY-MM-DD) and `end_date` (format YYYY-MM-DD) parameters to `import_symbol_data` and `plot_price_chart` (e.g. for year 2019, `start_date='2019-01-01'` and `end_date='2019-12-31'`).\n"
    "  • Yahoo Finance: Use `get_yahoo_finance_data` and `get_price_momentum` to fetch live prices, PE/PB ratios, dividend yield, and 52-week ranges. Supports US listed stocks by passing 'US' as the exchange (e.g. `ADSK:US`, `AAPL:US`).\n"
    "  • Screener.in / Quarterly Results: Use `get_quarterly_results` to fetch quarterly revenue (in $ Millions for US stocks), net profit, EPS, and YoY growth percentages. Supports US listed stocks by passing 'US' as the exchange (e.g. `ADSK:US` or `AAPL:US`), which falls back to Yahoo Finance.\n"
    "  • News API / Google News: Use `get_stock_news` (Google News RSS) and `get_newsapi_stock_news` (NewsAPI.org) to fetch recent financial news and infer sentiment.\n"
    "  • US Company Deep-Dive / SEC Filings: Use `run_deepdive_analysis` to fetch SEC filings (10-K, 10-Q, etc.) and generate a multi-section research report for US stocks (like ADSK, AAPL). These reports are automatically persisted to ClickHouse and will be used to enhance your insights during `analyze` if they exist. You can also use the `query_clickhouse_db` tool to read raw data from `deepdive_*` tables or the `view_file` tool to read the final report.md file.\n"
    "  • Visualisation / Charts: Use the appropriate `plot_*` tools (like `plot_price_chart`, `plot_fii_dii_chart`, `plot_nav_chart`, etc.) whenever the user asks for a chart, trend, plot, or visual representation of price, NAV, flows, or signals. Always prefer calling these tools to render visual plots in your responses.\n"
    "Your goal is to provide comprehensive, accurate investment insights on the user's Zerodha portfolio. "
    "Always reason step by step and use the available tools to gather data before answering. "
    "CRITICAL RULES:\n"
    "1. NEVER repeat your introductory welcome message ('Hello! I am the Mosaic-fund-agent...') once you have started using tools. "
    "2. If you have called tools, your final response MUST be a synthesis of the data returned by those tools (e.g. news headlines, financial metrics, sentiment). "
    "3. If multiple tools fail or return no data, state clearly what you tried and what was missing (e.g. 'I tried to fetch news but the service was unavailable').\n"
    "When presenting structured data, weight shifts, signals, returns, or tabular results from any tool, "
    "ALWAYS format the output in a clean, readable Markdown table rather than using lists or bullet points.\n\n"
    "NUMERIC COMPUTATION RULE (mandatory — never violate): "
    "NEVER compute, estimate, or derive any number (returns, ratios, averages, "
    "percentages, scores, sums, differences, CAGR, PE, Kelly fractions, etc.) "
    "inside your response. ALL numeric work MUST be performed by a tool call "
    "(Python, SQL, or a dedicated function). You may ONLY narrate or format "
    "numbers that were returned verbatim by a tool. If no tool has produced a "
    "number, state that the data is unavailable — do NOT approximate."
)

# Compact system prompt for low-context local models (≤ 4k tokens, e.g. gemma4).
# Used in direct-LLM fallback paths where the full prompt would exceed the budget.
AGENT_SYSTEM_PROMPT_COMPACT = (
    "You are Mosaic-fund-agent, a financial analyst for Indian equity markets (NSE/BSE). "
    "Answer concisely using your training knowledge. "
    "Use ₹ for Indian monetary values. Never invent figures. "
    "NEVER compute any number yourself — only narrate numbers returned by tools."
)

_CONN_TROUBLESHOOTING = (
    "\n\n**💡 Troubleshooting Local Connection Refused:**\n"
    "1. Verify your local LLM server (Ollama, LM Studio) is running and active.\n"
    "2. If running inside Docker, you must set `LLM_BASE_URL` in your `.env` to connect to the host machine:\n"
    "   - For Ollama on macOS/Windows: `http://host.docker.internal:11434/v1`\n"
    "   - For Ollama on Linux: `http://172.17.0.1:11434/v1`"
)


def _get_message_text(content: Any) -> str:
    """Extract string content from LangChain message content, which could be a list of blocks."""
    if isinstance(content, list):
        texts = []
        for block in content:
            if isinstance(block, dict):
                if block.get("type") == "text":
                    texts.append(block.get("text", ""))
            elif isinstance(block, str):
                texts.append(block)
        return "\n".join(texts)
    return str(content) if content else ""


# ── Mosaic Fund Agent ──────────────────────────────────────────────────────────

class MosaicFundAgent:
    """
    Orchestrates the full portfolio intelligence workflow.

    This agent works in a direct orchestration mode (not purely ReAct)
    for the main workflow, using the ReAct agent for ad-hoc queries.
    """

    def __init__(self, checkpointer: Any = None) -> None:
        self._checkpointer = checkpointer
        import os
        self._built_caveman_level = os.environ.get("CAVEMAN_LEVEL")
        # Install LLM response cache globally before building the LLM instances.
        from src.utils.llm_cache import setup_llm_cache
        setup_llm_cache()
        try:
            self._llm = self._build_llm()
            self._cloud_llm = self._build_cloud_llm()
            # When local LLM is disabled, promote cloud LLM to primary so the rest
            # of the code (ask, chat, sub-agents) works without special-casing.
            if self._llm is None and self._cloud_llm is not None:
                logger.info("__init__: local LLM disabled — promoting cloud LLM to primary")
                self._llm = self._cloud_llm
            self._agent = self._build_agent()
        except Exception as exc:
            logger.warning("LLM not available (%s).", exc)
            self._llm = None
            self._cloud_llm = None
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

        Returns None when LLM_LOCAL_DISABLED=true, deferring all traffic to the cloud LLM.
        [SENSITIVE] API keys are loaded from config/settings.py → .env
        """
        if settings.llm_local_disabled:
            logger.info("_build_llm: local LLM disabled (LLM_LOCAL_DISABLED=true) — using cloud LLM only")
            return None

        provider = settings.llm_provider.lower()

        # ── Local / custom OpenAI-compatible endpoint (Ollama, LM Studio, etc.) ──
        if settings.llm_base_url:
            from langchain_openai import ChatOpenAI
            logger.info(
                "Using local LLM: model=%s  base_url=%s  think=%s",
                settings.llm_model,
                settings.llm_base_url,
                settings.llm_think,
            )
            extra_body: dict = {"options": {"num_ctx": settings.llm_context_window}}
            if settings.llm_think:
                extra_body["think"] = True
            # Thinking mode needs a longer timeout — qwen3 reasoning can take 60-120s
            request_timeout = 300 if settings.llm_think else 120
            return ChatOpenAI(
                model=settings.llm_model,
                base_url=settings.llm_base_url,
                # Local servers don't need a real key; use a placeholder if empty
                api_key=settings.openai_api_key or "local",
                temperature=0,
                max_tokens=settings.llm_token_budget,
                extra_body=extra_body,
                timeout=request_timeout,
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
                extra_headers={"anthropic-beta": "prompt-caching-2024-07-31"},
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

    def _build_cloud_llm(self) -> Any:
        """
        Build the secondary cloud LLM for long-context / reasoning-heavy queries.

        Enabled only when LLM_CLOUD_PROVIDER is set in .env.
        Returns None when disabled — all traffic stays on the local model.
        """
        provider = settings.llm_cloud_provider.strip().lower()
        if not provider:
            return None

        logger.info(
            "Cloud LLM enabled: provider=%s  model=%s  context=%dk",
            provider, settings.llm_cloud_model, settings.llm_cloud_context_window // 1000,
        )
        cloud_budget = max(1024, settings.llm_cloud_context_window // 4)

        if provider == "anthropic":
            from langchain_anthropic import ChatAnthropic
            return ChatAnthropic(
                model=settings.llm_cloud_model,
                api_key=settings.anthropic_api_key,
                temperature=0,
                max_tokens=cloud_budget,
                extra_headers={"anthropic-beta": "prompt-caching-2024-07-31"},
            )

        from langchain_openai import ChatOpenAI
        return ChatOpenAI(
            model=settings.llm_cloud_model,
            api_key=settings.openai_api_key,
            temperature=0,
            max_tokens=cloud_budget,
        )

    def _build_code_llm(self) -> Any:
        """
        Build a dedicated LLM for the CodeSubAgent.

        Reads CODE_LLM_PROVIDER / CODE_LLM_MODEL / CODE_LLM_BASE_URL from settings.
        Supported providers: openai | anthropic | google (Gemini).
        Returns None when CODE_LLM_PROVIDER is blank — CodeSubAgent inherits the main LLM.
        """
        provider = settings.code_llm_provider.strip().lower()
        if not provider:
            return None

        model  = settings.code_llm_model or settings.llm_model
        ctx    = settings.code_llm_context_window or settings.llm_context_window
        budget = max(1024, ctx // 4)
        logger.info("Code LLM: provider=%s  model=%s  ctx=%d", provider, model, ctx)

        if settings.code_llm_base_url:
            from langchain_openai import ChatOpenAI
            return ChatOpenAI(
                model=model,
                base_url=settings.code_llm_base_url,
                api_key=settings.openai_api_key or "local",
                temperature=0,
                max_tokens=budget,
                extra_body={"options": {"num_ctx": ctx}},
            )

        if provider == "anthropic":
            from langchain_anthropic import ChatAnthropic
            return ChatAnthropic(
                model=model,
                api_key=settings.anthropic_api_key,
                temperature=0,
                max_tokens=budget,
                extra_headers={"anthropic-beta": "prompt-caching-2024-07-31"},
            )

        if provider == "google":
            from langchain_google_genai import ChatGoogleGenerativeAI
            return ChatGoogleGenerativeAI(
                model=model,
                google_api_key=settings.google_api_key,
                temperature=0,
                max_output_tokens=budget,
            )

        from langchain_openai import ChatOpenAI
        return ChatOpenAI(
            model=model,
            api_key=settings.openai_api_key,
            temperature=0,
            max_tokens=budget,
        )

    def _pick_llm(self, question: str) -> Any:
        """
        Return the appropriate LLM for this query.

        When LLM_LOCAL_DISABLED=true the cloud LLM is always used.
        Otherwise routes to cloud when _needs_cloud(question) matches.
        Falls back to the local LLM for everything else.
        """
        if self._cloud_llm is not None:
            if settings.llm_local_disabled:
                return self._cloud_llm
            from src.agents.sub_agents import _needs_cloud
            if _needs_cloud(question):
                logger.info("_pick_llm: routing to cloud LLM (%s)", settings.llm_cloud_model)
                return self._cloud_llm
        return self._llm

    def _build_agent(self) -> Any:
        """Build the LangGraph ReAct agent with all tools.

        Skipped for low-context models (< 8000 tokens): tool schemas alone can
        consume 2000+ tokens, causing HTTP 400 errors before the question is sent.
        When LLM_LOCAL_DISABLED=true the cloud context window is used instead.
        """
        effective_window = (
            settings.llm_cloud_context_window
            if settings.llm_local_disabled
            else settings.llm_context_window
        )
        if effective_window < 8000:
            logger.info(
                "_build_agent: skipping tool-calling agent (effective context_window=%d < 8000). "
                "route_intent fallback will be used instead.",
                effective_window,
            )
            return None
        from src.utils.caveman import get_caveman_prompt
        kwargs: dict[str, Any] = dict(
            model=self._llm,
            tools=ALL_TOOLS,
            prompt=AGENT_SYSTEM_PROMPT + get_caveman_prompt(),
        )
        if self._checkpointer is not None:
            kwargs["checkpointer"] = self._checkpointer
        return create_react_agent(**kwargs)

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
            task = progress.add_task(
                f"Analyzing {len(holdings)} holdings in parallel…", total=len(holdings)
            )
            with concurrent.futures.ThreadPoolExecutor(max_workers=5, initializer=_make_daemon_thread) as pool:
                future_map = {pool.submit(analyze_holding, h): h for h in holdings}
                for future in concurrent.futures.as_completed(future_map):
                    holding = future_map[future]
                    try:
                        analysis = future.result()
                        analyses.append(analysis)
                        dd = analysis.deepdive_data
                        dd_suffix = (
                            f" | DeepDive: ✓ ({dd['report_date']}, {dd['age_days']}d ago)"
                            if dd else ""
                        )
                        console.print(
                            f"  [green]✓[/green] {holding.tradingsymbol} "
                            f"| Sentiment: {analysis.sentiment_score:+.2f} "
                            f"| Risk: {analysis.risk_score:.0f}/10"
                            + dd_suffix
                        )
                        if dd and dd.get("age_days", 0) > 90:
                            console.print(
                                f"  [yellow]⚠ DeepDive data for {holding.tradingsymbol} is "
                                f"{dd['age_days']}d old — run: "
                                f"python src/main.py deepdive {holding.tradingsymbol}[/yellow]"
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
        if self._llm is None:
            return "Agent not available — LLM is not configured. Set LLM_PROVIDER and API key in .env."

        from langchain_core.messages import HumanMessage, SystemMessage
        import os
        import re

        # Re-build agent if Caveman level changed
        current_caveman = os.environ.get("CAVEMAN_LEVEL")
        if current_caveman != getattr(self, "_built_caveman_level", None):
            self._agent = self._build_agent()
            self._built_caveman_level = current_caveman

        # Heuristic for weak models: if question looks like a deep-dive request, trigger it manually
        clean_question = question
        if "[End of context]\n" in question:
            clean_question = question.split("[End of context]\n", 1)[1]

        if re.search(r"deep[\s.?-]?dive|deep[\s-]?down", clean_question.lower()):
            name_match = re.search(r"deep[\s.?-]?(?:dive[s]?|down)\s+(?:on\s+)?(.+)", clean_question, re.I)
            raw_query  = name_match.group(1).strip() if name_match else clean_question

            from src.tools.company_resolver import resolve_company_info
            info = resolve_company_info(raw_query)
            logger.info("ask: deep-dive resolved %r → %s (%s)", raw_query, info["symbol"], info["market"])

            if info["market"] == "India":
                from src.agents.sub_agents import run_subagent_for
                prompt = (
                    f"Research {info['company_name']} ({info['symbol']}) "
                    f"listed on {info['exchange']}. Provide a comprehensive research note."
                )
                try:
                    return run_subagent_for("india_equity", prompt)
                except Exception as exc:
                    logger.error("Indian equity research failed: %s", exc)
            else:
                ticker = info["symbol"]
                logger.info("ask: US deep-dive heuristic → %s", ticker)
                from src.tools.skills_tools import run_deepdive_analysis
                try:
                    return run_deepdive_analysis.invoke({"ticker": ticker})
                except Exception as exc:
                    logger.error("Heuristic deep-dive failed: %s", exc)

        # Broad intent routing — catches "find info about X", "research X", etc.
        from src.agents.intent_router import route_intent_llm
        from src.agents.sub_agents import run_subagent_for
        _intent = route_intent_llm(question)
        if _intent != "main":
            logger.info("ask: routing to %s sub-agent via LLM router", _intent)
            return run_subagent_for(_intent, question)

        # Low-context model (e.g. gemma4 at 3k): agent was not built, go direct to LLM.
        if self._agent is None:
            logger.info("ask: no tool-calling agent (low context window) — direct LLM fallback")
            try:
                llm = self._pick_llm(question)
                from src.utils.caveman import get_caveman_prompt
                compact_prompt = AGENT_SYSTEM_PROMPT_COMPACT + get_caveman_prompt()
                res = llm.invoke([
                    SystemMessage(content=compact_prompt),
                    HumanMessage(content=question),
                ])
                return str(res.content)
            except Exception as exc:
                err_msg = str(exc).lower()
                is_connection_error = any(term in err_msg for term in ["connection refused", "connecterror", "connection error", "api connection"])
                if is_connection_error and self._cloud_llm is not None and llm != self._cloud_llm:
                    logger.warning("Local LLM direct query failed. Trying cloud LLM fallback...")
                    try:
                        from src.utils.caveman import get_caveman_prompt
                        compact_prompt = AGENT_SYSTEM_PROMPT_COMPACT + get_caveman_prompt()
                        res = self._cloud_llm.invoke([
                            SystemMessage(content=compact_prompt),
                            HumanMessage(content=question),
                        ])
                        return str(res.content)
                    except Exception as cloud_exc:
                        logger.error("Cloud direct query fallback failed: %s", cloud_exc)
                if is_connection_error:
                    logger.error("Local LLM connection failed: %s", exc)
                    return f"Error: {exc}{_CONN_TROUBLESHOOTING}"
                logger.error("ask: direct LLM fallback failed: %s", exc)
                return f"Error: {exc}"

        try:
            messages = [HumanMessage(content=question)]
            config = {}
            if os.getenv("VERBOSE") == "1":
                config["callbacks"] = [RichConsoleCallbackHandler()]

            with concurrent.futures.ThreadPoolExecutor(max_workers=1, initializer=_make_daemon_thread) as _ex:
                _fut = _ex.submit(self._agent.invoke, {"messages": messages}, config)
                try:
                    result = _fut.result(timeout=120)
                except concurrent.futures.TimeoutError:
                    return (
                        "Agent timed out after 120 s. "
                        "A tool (news API / yfinance / Screener.in) likely stalled. "
                        "Try again or use `--max` to limit holdings."
                    )
            msgs = result.get("messages", [])
            if not msgs:
                return "No answer generated."
            
            # Search from the end for the first message with text content that isn't just a tool call
            for m in reversed(msgs):
                # If it's an AIMessage, it might have content or tool_calls
                content = m.content
                if content and not (hasattr(m, "tool_calls") and m.tool_calls):
                    if isinstance(content, list):
                        texts = [block.get("text", "") for block in content if isinstance(block, dict) and block.get("type") == "text"]
                        if texts:
                            return "\n".join(texts)
                    elif isinstance(content, str) and content.strip():
                        return content
            
            # Fallback to the very last message content if no pure text message found
            content = msgs[-1].content
            return str(content) if content else "No answer generated."
        except Exception as exc:
            err_msg = str(exc).lower()
            is_connection_error = any(term in err_msg for term in ["connection refused", "connecterror", "connection error", "api connection"])
            if is_connection_error and self._cloud_llm is not None and self._llm != self._cloud_llm:
                logger.warning("Connection to local LLM failed. Falling back to cloud LLM...")
                try:
                    from src.utils.caveman import get_caveman_prompt
                    temp_cloud_agent = create_react_agent(
                        model=self._cloud_llm,
                        tools=ALL_TOOLS,
                        prompt=AGENT_SYSTEM_PROMPT + get_caveman_prompt(),
                    )
                    result = temp_cloud_agent.invoke(
                        {"messages": [HumanMessage(content=question)]},
                        config=config,
                    )
                    msgs = result.get("messages", [])
                    return _get_message_text(msgs[-1].content) if msgs else "No answer generated."
                except Exception as cloud_exc:
                    logger.error("Cloud LLM fallback failed: %s", cloud_exc)
            
            if any(term in err_msg for term in ["does not support tool", "tool binding", "not support tool"]):
                logger.warning("Model does not support tools. Falling back to direct LLM Q&A. Error: %s", exc)
                try:
                    llm = self._pick_llm(question)
                    from src.utils.caveman import get_caveman_prompt
                    compact_prompt = AGENT_SYSTEM_PROMPT_COMPACT + get_caveman_prompt()
                    res = llm.invoke([
                        SystemMessage(content=compact_prompt),
                        HumanMessage(content=question),
                    ])
                    return str(res.content)
                except Exception as fallback_exc:
                    fb_err = str(fallback_exc).lower()
                    is_fb_conn_error = any(term in fb_err for term in ["connection refused", "connecterror", "connection error", "api connection"])
                    if is_fb_conn_error and self._cloud_llm is not None and llm != self._cloud_llm:
                        logger.warning("Fallback LLM failed due to connection. Trying cloud LLM...")
                        try:
                            res = self._cloud_llm.invoke([
                                SystemMessage(content=compact_prompt),
                                HumanMessage(content=question),
                            ])
                            return str(res.content)
                        except Exception as cloud_exc:
                            logger.error("Fallback cloud LLM query failed: %s", cloud_exc)
                    if is_fb_conn_error:
                        return f"Error: {fallback_exc}{_CONN_TROUBLESHOOTING}"
                    logger.error("Fallback LLM query failed: %s", fallback_exc)
                    return f"Error: {fallback_exc}"
            
            if is_connection_error:
                return f"Error: {exc}{_CONN_TROUBLESHOOTING}"
            logger.error("Agent query failed: %s", exc, exc_info=True)
            return f"Error: {exc}"

    def chat(self, question: str, thread_id: str = "default", forced_intent: str | None = None) -> str:
        """
        Single turn in an ongoing multi-turn conversation.

        Conversation memory is maintained across calls via the LangGraph
        checkpointer that was passed to ``__init__``.  When no checkpointer
        was supplied the call is stateless (equivalent to ``ask()``).

        Intent-based routing:
          - deepdive keywords  → DeepDiveSubAgent
          - signal keywords    → SignalSubAgent
          - macro keywords     → MacroSubAgent
          - everything else    → main agent (with memory)
        """
        import os
        import re

        # Re-build agent if Caveman level changed
        try:
            from src.tools.company_resolver import clear_turn_resolutions
            clear_turn_resolutions()
        except Exception:
            pass

        current_caveman = os.environ.get("CAVEMAN_LEVEL")
        if current_caveman != getattr(self, "_built_caveman_level", None):
            self._agent = self._build_agent()
            self._built_caveman_level = current_caveman
        from langchain_core.messages import HumanMessage
        from src.agents.intent_router import route_intent_llm
        from src.agents.sub_agents import get_subagent

        # Deep-dive heuristic: resolve company then route India vs US
        clean_question = question
        if "[End of context]\n" in question:
            clean_question = question.split("[End of context]\n", 1)[1]

        if re.search(r"deep[\s.?-]?dive|deep[\s-]?down", clean_question.lower()):
            # Extract everything after the deepdive keyword as the query
            name_match = re.search(r"deep[\s.?-]?(?:dive[s]?|down)\s+(?:on\s+)?(.+)", clean_question, re.I)
            raw_query  = name_match.group(1).strip() if name_match else clean_question

            from src.tools.company_resolver import resolve_company_info
            info = resolve_company_info(raw_query)
            logger.info(
                "chat: deep-dive resolved %r → %s (%s)",
                raw_query, info["symbol"], info["market"],
            )

            if info["market"] == "India":
                # Route to Indian equity research sub-agent
                prompt = (
                    f"Research {info['company_name']} ({info['symbol']}) "
                    f"listed on {info['exchange']}. Provide a comprehensive "
                    f"research note covering financials, earnings, MF holdings, "
                    f"cash flow, news, and FII/DII flows."
                )
                return get_subagent("india_equity").run(prompt)
            else:
                # US stock → SEC deepdive pipeline
                ticker = info["symbol"]
                logger.info("chat: US deep-dive heuristic → %s", ticker)
                from src.tools.skills_tools import run_deepdive_analysis
                try:
                    return run_deepdive_analysis.invoke({"ticker": ticker})
                except Exception as exc:
                    logger.error("Deep-dive heuristic failed: %s", exc)

        # Intent-based routing — use AI-planner override when provided
        intent = forced_intent if forced_intent else route_intent_llm(question)
        if intent != "main":
            logger.info("chat: routing to %s sub-agent", intent)
            from src.agents.sub_agents import run_subagent_for
            return run_subagent_for(intent, question)

        # Main agent with (optional) memory thread
        if self._agent is None:
            # Low-context model: no tool-calling agent — use compact direct LLM
            logger.info("chat: no tool-calling agent (low context window) — direct LLM fallback")
            try:
                from langchain_core.messages import SystemMessage
                llm = self._pick_llm(question)
                from src.utils.caveman import get_caveman_prompt
                compact_prompt = AGENT_SYSTEM_PROMPT_COMPACT + get_caveman_prompt()
                res = llm.invoke([
                    SystemMessage(content=compact_prompt),
                    HumanMessage(content=question),
                ])
                return str(res.content)
            except Exception as exc:
                logger.error("chat: direct LLM fallback failed: %s", exc)
                return f"Error: {exc}"

        config: dict = {"configurable": {"thread_id": thread_id}}
        if os.getenv("VERBOSE") == "1":
            config["callbacks"] = [RichConsoleCallbackHandler()]

        # Clean up incomplete tool calls in history if any
        if self._checkpointer is not None:
            try:
                state = self._agent.get_state(config)
                messages = state.values.get("messages", [])
                if messages:
                    from langchain_core.messages import RemoveMessage
                    removes = []
                    for i, msg in enumerate(messages):
                        if hasattr(msg, "tool_calls") and msg.tool_calls:
                            expected_ids = {tc["id"] for tc in msg.tool_calls}
                            found_ids = set()
                            j = i + 1
                            while j < len(messages) and messages[j].__class__.__name__ == "ToolMessage":
                                found_ids.add(messages[j].tool_call_id)
                                j += 1
                            if not expected_ids.issubset(found_ids):
                                logger.warning("chat: found orphaned tool calls in message %s. Cleaning up...", msg.id)
                                removes.append(RemoveMessage(id=msg.id))
                                # Also remove any partial/orphaned ToolMessages in that block
                                for k in range(i + 1, j):
                                    removes.append(RemoveMessage(id=messages[k].id))
                    if removes:
                        self._agent.update_state(config, {"messages": removes})
            except Exception as clean_exc:
                logger.warning("chat: failed to clean up incomplete state: %s", clean_exc)

        try:
            result = self._agent.invoke(
                {"messages": [HumanMessage(content=question)]},
                config=config,
            )
            msgs = result.get("messages", [])
            return _get_message_text(msgs[-1].content) if msgs else "No answer generated."
        except Exception as exc:
            err_msg = str(exc).lower()
            is_connection_error = any(term in err_msg for term in ["connection refused", "connecterror", "connection error", "api connection"])
            if is_connection_error and self._cloud_llm is not None and self._llm != self._cloud_llm:
                logger.warning("Connection to local LLM failed. Falling back to cloud LLM...")
                try:
                    from src.utils.caveman import get_caveman_prompt
                    temp_cloud_agent = create_react_agent(
                        model=self._cloud_llm,
                        tools=ALL_TOOLS,
                        prompt=AGENT_SYSTEM_PROMPT + get_caveman_prompt(),
                    )
                    result = temp_cloud_agent.invoke(
                        {"messages": [HumanMessage(content=question)]},
                        config=config,
                    )
                    msgs = result.get("messages", [])
                    return _get_message_text(msgs[-1].content) if msgs else "No answer generated."
                except Exception as cloud_exc:
                    logger.error("Cloud LLM fallback failed: %s", cloud_exc)
            
            if any(term in err_msg for term in ["does not support tool", "tool binding", "not support tool"]):
                logger.warning("chat: model doesn't support tools, falling back to direct LLM. Error: %s", exc)
                try:
                    from langchain_core.messages import SystemMessage
                    llm = self._pick_llm(question)
                    from src.utils.caveman import get_caveman_prompt
                    compact_prompt = AGENT_SYSTEM_PROMPT_COMPACT + get_caveman_prompt()
                    res = llm.invoke([
                        SystemMessage(content=compact_prompt),
                        HumanMessage(content=question),
                    ])
                    return str(res.content)
                except Exception as fb_exc:
                    fb_err = str(fb_exc).lower()
                    is_fb_conn_error = any(term in fb_err for term in ["connection refused", "connecterror", "connection error", "api connection"])
                    if is_fb_conn_error and self._cloud_llm is not None and llm != self._cloud_llm:
                        logger.warning("Fallback LLM failed due to connection. Trying cloud LLM...")
                        try:
                            res = self._cloud_llm.invoke([
                                SystemMessage(content=compact_prompt),
                                HumanMessage(content=question),
                            ])
                            return str(res.content)
                        except Exception as cloud_exc:
                            logger.error("Fallback cloud LLM query failed: %s", cloud_exc)
                    if is_fb_conn_error:
                        return f"Error: {fb_exc}{_CONN_TROUBLESHOOTING}"
                    return f"Error: {fb_exc}"
            
            if is_connection_error:
                return f"Error: {exc}{_CONN_TROUBLESHOOTING}"
            logger.error("chat() failed: %s", exc, exc_info=True)
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
