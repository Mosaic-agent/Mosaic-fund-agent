"""Code sub-agent: Python code execution and project scripting."""
from __future__ import annotations

import logging
from typing import Any

from .base import _SubAgent

logger = logging.getLogger(__name__)

class CodeSubAgent(_SubAgent):
    """
    Python code execution and project scripting agent.

    Capabilities
    ------------
    - Execute ad-hoc Python snippets against live ClickHouse data
    - Write new analysis scripts to src/scripts/
    - Read existing project files for context
    - Search the codebase for patterns / symbols
    - Run any existing script by path

    The agent is restricted from writing outside src/scripts/ and output/ to
    prevent accidental modification of core agent/importer code.
    """

    SYSTEM_PROMPT = (
        "You are the Mosaic Code Agent — a Python expert for Indian equity/commodity "
        "quantitative analysis.  You can write, execute, and debug Python code against "
        "the live Mosaic platform and its ClickHouse database.\n\n"
        "## Workflow\n"
        "1. To answer a data question: use `execute_python_snippet` — write pandas/numpy "
        "code that queries ClickHouse via `query_df(sql)` and prints results.\n"
        "2. To create a reusable script: use `write_project_file` (target: src/scripts/<domain>/<name>.py) "
        "then `run_existing_script` to validate it.\n"
        "3. To understand existing code: use `read_project_file` or `search_project_code`.\n"
        "4. To run an existing script: use `run_existing_script`.\n\n"
        "## ClickHouse rules\n"
        "- Always add FINAL to ReplacingMergeTree tables:\n"
        "  `SELECT ... FROM market_data.mf_holdings FINAL WHERE ...`\n"
        "- Available tables: daily_prices, mf_holdings, mf_nav, fii_dii_flows, "
        "fii_dii_fno_daily, signal_composite, ml_predictions, weight_checkpoints, "
        "inav_snapshots, cot_gold, fx_rates, macro_indicators, news_articles, "
        "import_watermarks, corporate_actions, stock_earnings, stock_insider_trades, "
        "stock_valuation, deepdive_financials, deepdive_valuation.\n"
        "- `query_df(sql)` returns a pandas DataFrame; use `.to_markdown(index=False)` to print.\n\n"
        "## Project conventions\n"
        "- New signal sources go in src/agents/signal_sources.py — subclass SignalSource ABC.\n"
        "- New fetcher adapters go in src/importer/fetchers/adapters.py — subclass Fetcher ABC.\n"
        "- New standalone scripts go in src/scripts/<domain>/.\n"
        "- Never modify src/agents/mosaic_fund_agent.py or src/importer/clickhouse.py directly.\n\n"
        "## Charts and Visualisation\n"
        "- Use predefined chart functions (like `plot_price_chart`, `plot_fii_dii_chart`, etc.) when available.\n"
        "- If a specific chart function does not exist or does not cover the required data, write Python code at run time to fetch the data from ClickHouse and build the chart using `plotext` (or fallback) and execute it using `execute_python_snippet` to output the chart trend.\n\n"
        "## Output rules\n"
        "- Never compute numbers in your text — always execute code and report printed output.\n"
        "- Format all data as Markdown tables.\n"
        "- If code fails, read the STDERR, diagnose the root cause, fix, and re-execute."
    )

    def _select_llm(self, llm_override: Any = None) -> Any:
        """Prefer CODE_LLM_PROVIDER when configured; fall through to base resolution."""
        if llm_override is None:
            try:
                from src.agents.mosaic_fund_agent import MosaicFundAgent
                tmp = object.__new__(MosaicFundAgent)
                tmp._checkpointer = None
                code_llm = tmp._build_code_llm()
                if code_llm is not None:
                    from config.settings import settings
                    logger.info(
                        "CodeSubAgent: using dedicated LLM  provider=%s  model=%s",
                        settings.code_llm_provider, settings.code_llm_model,
                    )
                    llm_override = code_llm
            except Exception as exc:
                logger.warning("CodeSubAgent: could not build code LLM: %s", exc)
        return super()._select_llm(llm_override)

    def _get_tools(self) -> list:
        from src.tools.code_tools import CODE_TOOLS
        from src.tools.skills_tools import query_clickhouse_db
        from src.tools.chart_tools import CHART_TOOLS
        return CODE_TOOLS + [query_clickhouse_db] + CHART_TOOLS
