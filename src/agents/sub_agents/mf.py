"""Mutual Fund sub-agent: holdings, NAV returns, cross-fund consensus."""
from __future__ import annotations

import logging
from typing import Any

from .base import _SubAgent

logger = logging.getLogger(__name__)

class MFSubAgent(_SubAgent):
    """
    Indian mutual-fund analysis agent.

    Specialises in fund-level holdings, NAV returns, cross-fund consensus, and
    institutional whale signals from `market_data.mf_holdings` / `mf_nav`.

    Universe handled
    ----------------
    Multi-asset funds : Nippon · Nippon FoF · DSP · DSP Omni · Bajaj · Quant · ICICI
    DSP active funds  : 13 active equity funds + ELSS/Tax-saver
    NAV history       : any Indian MF via MFAPI scheme code

    Uses the Plan-Execute-Replan StateGraph workflow (src/workflows/mf_planner.py)
    as the primary path (~55-76% token savings vs ReAct). Falls back to ReAct on failure.
    """

    RECURSION_LIMIT = 30

    def run(self, question: str, llm_override: Any = None, callbacks: list | None = None) -> str:
        """Try the StateGraph Plan-Execute-Replan workflow first, fall back to ReAct loop.

        Set MOSAIC_USE_WORKFLOWS=0 to force the ReAct agent for debugging.
        """
        import os
        if os.getenv("MOSAIC_USE_WORKFLOWS", "1") != "0":
            try:
                from src.workflows.mf_planner import run as _wf_run
                logger.info("MFSubAgent: routing → Plan-Execute-Replan workflow")
                return _wf_run(question, callbacks=callbacks)
            except Exception as exc:
                logger.warning(
                    "MFSubAgent: mf_planner workflow failed (%s), falling back to ReAct — track this", exc
                )
        return super().run(question, llm_override=llm_override, callbacks=callbacks)

    SYSTEM_PROMPT = (
        "You are an Indian mutual-fund analyst covering active equity, multi-asset, "
        "and hybrid schemes. You work off ClickHouse tables `market_data.mf_holdings` "
        "(position-level disclosures) and live NAV from mfapi.in.\n\n"
        "## Tool selection\n"
        "Match the user's intent to the right tool:\n\n"
        "| Intent                                    | Tool                                        |\n"
        "|-------------------------------------------|---------------------------------------------|\n"
        "| MoM/YoY position changes in ONE fund      | `run_multi_asset_holdings_mom_yoy`          |\n"
        "| Fund concentration, diversification, top holdings | `run_mf_concentration_risk`          |\n"
        "| Cross-fund consensus / smart-money overlap| `run_multi_asset_consensus`                 |\n"
        "| Theme tracking (gold/silver/nuclear/infra) & AMC archetypes| `run_whale_tracker`                         |\n"
        "| Cross-AMC consensus buys (+technical confirmation)| `scan_whale_accumulation`                   |\n"
        "| Single-stock institutional consensus lookup| `get_whale_consensus`                       |\n"
        "| NAV MoM returns for any MF                | `run_fund_mom_returns`                      |\n"
        "| DSP cross-fund weighted comparison        | `run_dsp_multi_asset_comparison`            |\n"
        "| Top holdings bar chart for a fund         | `plot_fund_holdings_chart`                  |\n"
        "| Which funds hold a stock (reverse lookup) | `get_mf_holdings_for_stock`                 |\n"
        "| Semantic: which funds hold a security/ISIN | `find_funds_holding`                       |\n"
        "| Find funds with similar portfolio mix     | `find_similar_funds`                        |\n"
        "| Find funds by asset category exposure     | `search_mf_exposure`                        |\n"
        "| Ad-hoc holdings query                     | `query_clickhouse_db` on `mf_holdings FINAL`|\n"
        "| Fast multi-fund holding count/value totals| `query_clickhouse_db` on `mf_holding_summaries FINAL` (Materialized View <100ms)|\n"
        "| AMC bullish small-cap stock picks (% NAV) | `query_clickhouse_db` on `mf_holdings FINAL` ordered by `pct_of_nav DESC` |\n"
        "| Import latest DSP holdings                | `run_dsp_multi_asset_importer`              |\n"
        "| Import latest Nippon holdings             | `run_nippon_importer`                       |\n"
        "| Import latest Quant holdings              | `run_quant_importer`                        |\n"
        "| Import latest ICICI Pru holdings          | `run_icici_importer`                        |\n"
        "| Import ALL multi-asset fund holdings      | `run_all_multi_asset_importers`             |\n"
        "| Retrieve breaking news for symbol/theme   | `get_stock_news`                            |\n"
        "| Search ClickHouse database schemas/SQL    | `search_db_metadata`                        |\n\n"
        "## Routing rules\n"
        "- 'pattern across multi-asset funds', 'what are funds collectively buying', "
        "'smart money consensus' → `run_multi_asset_consensus`\n"
        "- 'AMC multi asset archetypes', 'compare AMC styles', 'what pattern across AMCs', "
        "'gold/silver/nuclear theme exposure across funds' → `run_whale_tracker`\n"
        "- 'what are institutions accumulating', 'which stocks are multiple AMCs buying', "
        "'cross-AMC consensus buys', 'institutional accumulation', 'which consensus buys are "
        "technically attractive too' → `scan_whale_accumulation` (pass `with_technicals=True` "
        "only when the user asks about technical/price confirmation, not for a plain consensus check)\n"
        "- 'is DSP/Nippon buying <stock>', 'which AMCs hold <stock>', 'institutional consensus "
        "on <stock>' → `get_whale_consensus(symbol=<stock>)`\n"
        "- 'how did <fund> change MoM/YoY', 'top adds/exits in <fund>' → "
        "`run_multi_asset_holdings_mom_yoy(fund=...)`\n"
        "- 'concentration risk for <fund>', 'is <fund> diversified', 'top holdings in <fund>' → "
        "`run_mf_concentration_risk(fund=...)`; use `all_funds=True` only for a multi-asset comparison\n"
        "  Examples:\n"
        "  - 'DSP multi asset MoM' → `fund='DSP_MULTI_ASSET'`\n"
        "  - 'Nippon multi asset changes', 'how did Nippon change' → "
        "`fund='NIPPON_INDIA_MULTI_ASSET_ALLOCATION_FUND'`\n"
        "  - 'Nippon FoF holdings', 'Nippon Omni allocation' → "
        "`fund='NIPPON_INDIA_MULTI_ASSET_OMNI_FOF'`\n"
        "  - 'Bajaj multi asset' → `fund='BAJAJ_FINSERV_MULTI_ASSET_ALLOCATION_FUND'`\n"
        "  - 'Quant multi asset' → `fund='QUANT_MULTI_ASSET'`\n"
        "- 'NAV return of <fund>', 'how has <fund> performed' → `run_fund_mom_returns`\n"
        "- 'which funds hold <stock>' → `get_mf_holdings_for_stock`\n"
        "- 'which funds hold <stock/ISIN>' (Qdrant semantic) → `find_funds_holding(query=<stock>)`\n"
        "- 'which funds have gold/commodity/equity exposure' → `search_mf_exposure(category=<type>)`\n"
        "- 'find funds similar to <fund>' → `find_similar_funds(fund_name=<fund>)`\n"
        "- 'gold/silver/nuclear theme exposure across funds' → `run_whale_tracker`\n"
        "- 'list available funds', 'what funds have history' → "
        "`run_multi_asset_holdings_mom_yoy(list_funds=True)`\n"
        "- 'import all multi asset funds', 'refresh all fund holdings', 'sync all AMC holdings' → `run_all_multi_asset_importers` (runs DSP + Nippon + Quant + ICICI)\n"
        "- 'import / refresh DSP holdings' → `run_dsp_multi_asset_importer`\n"
        "- 'import / refresh Nippon holdings' → `run_nippon_importer`\n"
        "- 'import / refresh Quant holdings' → `run_quant_importer`\n"
        "- 'import / refresh ICICI holdings', 'import ICICI Pru' → `run_icici_importer`\n\n"
        "### Nippon fund canonical names\n"
        "Use these exact strings in `fund=` parameters:\n"
        "| Fund | Canonical name |\n"
        "|------|----------------|\n"
        "| Nippon India Multi Asset Allocation Fund | `NIPPON_INDIA_MULTI_ASSET_ALLOCATION_FUND` |\n"
        "| Nippon India Multi Asset Omni FoF        | `NIPPON_INDIA_MULTI_ASSET_OMNI_FOF`        |\n"
        "Note: RLMF806 was renamed multiple times in history — the tool handles name variants "
        "automatically; always pass the canonical name above.\n\n"
        "## Schema reference (authoritative — use these exact column names)\n"
        "```\n"
        "market_data.mf_holdings FINAL\n"
        "  scheme_code     String     -- AMFI scheme code\n"
        "  fund_name       String     -- e.g. DSP_MULTI_ASSET, BAJAJ_MULTI_ASSET\n"
        "  as_of_month     Date       -- portfolio disclosure month (use for date filters)\n"
        "  isin            String     -- security ISIN\n"
        "  security_name   String     -- holding name  ← NOT 'holding_name' or 'name'\n"
        "  asset_type      String     -- equity/gold/bond/cash  ← NOT 'asset_class'\n"
        "  market_value_cr Float64    -- market value in ₹ crores\n"
        "  pct_of_nav      Float64    -- weight as % of NAV  ← NOT 'weight_pct'\n"
        "  imported_at     DateTime\n"
        "\n"
        "market_data.mf_nav FINAL\n"
        "  symbol          String\n"
        "  scheme_code     String\n"
        "  nav_date        Date       -- date column  ← NOT 'date' or 'trade_date'\n"
        "  nav             Float64\n"
        "  imported_at     DateTime\n"
        "```\n\n"
        "## Critical rules\n"
        "- **Schema-first rule**: before writing ANY raw SQL against `mf_holdings` or `mf_nav`, \n"
        "  call `describe_db_table('mf_holdings')` to confirm column names. Never guess.\n"
        "- The `mf_holdings` table uses columns `fund_name`, `pct_of_nav`, `market_value_cr` "
        "and `as_of_month` (Date). Always query with `FINAL` to deduplicate. NEVER use "
        "`weight_pct`, `holding_name`, `asset_class`, or `name` — those columns do not exist.\n"
        "- DSP active funds (DSP_SMALL_CAP, DSP_MID_CAP, DSP_FLEXI_CAP, DSP_MULTICAP, "
        "DSP_FOCUSED, DSP_VALUE, DSP_BUSINESS_CYCLE, DSP_QUANT, DSP_HEALTHCARE, etc.) "
        "carry meaningful manager-discretion signal. Passive funds (DSP_NIFTY_*_INDEX, "
        "DSP_*_ETF) are index-trackers — call out that distinction when relevant.\n"
        "- Nippon active funds with manager-discretion signal: "
        "NIPPON_INDIA_MULTI_ASSET_ALLOCATION_FUND, NIPPON_INDIA_MULTI_ASSET_OMNI_FOF, "
        "NIPPON_INDIA_LARGE_CAP_FUND, NIPPON_INDIA_SMALL_CAP_FUND, "
        "NIPPON_INDIA_FLEXI_CAP_FUND, NIPPON_INDIA_VALUE_FUND, "
        "NIPPON_INDIA_MULTI_CAP_FUND, NIPPON_INDIA_GROWTH_FUND. "
        "Passive Nippon funds (NIPPON_CPSE_ETF, NIPPON_INDIA_NIFTY_* ETFs) are "
        "index-trackers and carry no manager-discretion signal.\n"
        "- Cross-fund overlap (same security held by 2+ active funds for 24+ months) is "
        "the strongest single-name conviction marker. Highlight when present.\n"
        "- Before declaring 'promoter sell-down' from a shareholding-pattern drop, verify "
        "that absolute share count did not expand from a QIP / preferential allotment / "
        "rights issue. A % drop with unchanged absolute shares = dilution, not a sale.\n"
        "- NEVER compute returns or weighted averages in your head. All numeric work must "
        "come from tool output — your job is to narrate and synthesise.\n\n"
        "## Synthesis pattern\n"
        "- **Present data in Markdown tables**: When reporting core holdings overlap, active shifts (adds/trims), or asset rotation, ALWAYS present the data in cleanly formatted markdown tables. Do not just write prose bullet points for structured data.\n"
        "- Default to a two-step plan for open-ended queries:\n"
        "  1. Run `run_multi_asset_consensus()` to see the cross-fund regime first.\n"
        "  2. Drill into the specific fund with `run_multi_asset_holdings_mom_yoy` for "
        "the asked-about fund, then comment on whether its moves align with or diverge "
        "from the consensus.\n"
        "- Always end with a short 'What this signals' paragraph that connects the "
        "position deltas to a directional view (e.g. risk-on rotation into gold, "
        "trimming cash, accumulating a single name across funds)."
    )

    def _get_tools(self) -> list:
        from src.tools.skills_tools import (
            run_multi_asset_holdings_mom_yoy,
            run_multi_asset_consensus,
            run_whale_tracker,
            run_dsp_multi_asset_comparison,
            run_fund_mom_returns,
            run_dsp_multi_asset_importer,
            run_nippon_importer,
            run_icici_importer,
            run_all_multi_asset_importers,
            run_mf_concentration_risk,
            query_clickhouse_db,
        )
        from src.tools.whale_tools import scan_whale_accumulation, get_whale_consensus
        from src.tools.db_tools import describe_db_table, list_db_tables, sample_db_table, search_db_metadata
        from src.tools.indian_equity_tools import get_mf_holdings_for_stock
        from src.tools.chart_tools import plot_fund_holdings_chart, plot_price_chart
        from src.tools.market.mf_tools import find_funds_holding, find_similar_funds, search_mf_exposure
        from src.tools.report_publisher import publish_consolidated_pdf
        from src.tools.news_search import get_stock_news
        return [
            run_multi_asset_holdings_mom_yoy,
            run_multi_asset_consensus,
            run_whale_tracker,
            run_dsp_multi_asset_comparison,
            run_fund_mom_returns,
            run_dsp_multi_asset_importer,
            run_nippon_importer,
            run_icici_importer,
            run_all_multi_asset_importers,
            run_mf_concentration_risk,
            scan_whale_accumulation,
            get_whale_consensus,
            get_mf_holdings_for_stock,
            find_funds_holding,
            find_similar_funds,
            search_mf_exposure,
            plot_fund_holdings_chart,
            plot_price_chart,
            query_clickhouse_db,
            describe_db_table,
            list_db_tables,
            sample_db_table,
            search_db_metadata,
            get_stock_news,
            publish_consolidated_pdf,
        ]

    def _fallback(self, question: str) -> str:
        """
        Programmatic routing for local models that cannot emit tool-call JSON.

        Keyword detection:
          consensus / pattern / collectively / smart money → run_multi_asset_consensus
          mom / yoy / changes / adds / exits / trimmed     → run_multi_asset_holdings_mom_yoy
          which funds hold / cross-ownership / fund holders → get_mf_holdings_for_stock
          theme / whale / gold theme / silver theme         → run_whale_tracker
          NAV return / mom return                           → run_fund_mom_returns
        """
        import re as _re
        q = question.lower()

        # ── Import all multi-asset fund holdings ──────────────────────────────
        if any(kw in q for kw in (
            "import all", "refresh all", "sync all", "update all",
            "all multi asset", "all fund holding", "all amc",
        )):
            logger.info("MFSubAgent._fallback: import all multi-asset funds")
            from src.tools.skills_tools import run_all_multi_asset_importers
            return run_all_multi_asset_importers.invoke({})

        # ── Individual fund imports ───────────────────────────────────────────
        if any(kw in q for kw in ("import", "refresh", "sync", "update")):
            if "dsp" in q:
                logger.info("MFSubAgent._fallback: import DSP")
                from src.tools.skills_tools import run_dsp_multi_asset_importer
                return run_dsp_multi_asset_importer.invoke({})
            if any(kw in q for kw in ("nippon", "reliance mf")):
                logger.info("MFSubAgent._fallback: import Nippon")
                from src.tools.skills_tools import run_nippon_importer
                return run_nippon_importer.invoke({})
            if any(kw in q for kw in ("icici", "icici pru", "prudential")):
                logger.info("MFSubAgent._fallback: import ICICI")
                from src.tools.skills_tools import run_icici_importer
                return run_icici_importer.invoke({})

        # ── Cross-fund consensus ──────────────────────────────────────────────
        if any(kw in q for kw in (
            "consensus", "pattern", "collectively", "smart money",
            "across multi", "across funds", "common holding", "overlap",
        )):
            logger.info("MFSubAgent._fallback: consensus path")
            from src.tools.skills_tools import run_multi_asset_consensus
            period = "yoy" if "yoy" in q or "year" in q else "mom"
            return run_multi_asset_consensus.invoke({"period": period, "top": 15})

        # ── Reverse lookup: which funds hold a stock ──────────────────────────
        if any(kw in q for kw in (
            "which fund", "who holds", "funds holding", "cross-ownership",
            "fund cross", "mf holders",
        )):
            from src.tools.indian_equity_tools import get_mf_holdings_for_stock
            m = _re.search(
                r"(?:hold(?:s|ing)?|own(?:s)?)\s+([A-Za-z0-9 &\-\.]+?)(?:\?|$|\.)",
                question, _re.I,
            )
            target = (m.group(1).strip() if m else question.strip())
            logger.info("MFSubAgent._fallback: reverse lookup → %r", target)
            return get_mf_holdings_for_stock.invoke({"company_name_or_symbol": target})

        # ── Qdrant: similar fund profiles ─────────────────────────────────────
        if any(kw in q for kw in (
            "similar fund", "fund similar", "find fund like", "funds like",
            "similar portfolio", "similar allocation",
        )):
            from src.tools.market.mf_tools import find_similar_funds
            m = _re.search(r"(dsp|nippon|bajaj|quant|icici)[\w_]*", q)
            fund = m.group(0).upper().replace(" ", "_") if m else ""
            logger.info("MFSubAgent._fallback: find_similar_funds → %r", fund)
            return find_similar_funds.invoke({"fund_name": fund or question.strip()})

        # ── Qdrant: category exposure search ─────────────────────────────────
        if any(kw in q for kw in (
            "gold exposure", "commodity exposure", "equity exposure",
            "bond exposure", "gold allocation", "commodity allocation",
        )):
            from src.tools.market.mf_tools import search_mf_exposure
            cat = (
                "gold" if any(k in q for k in ("gold", "commodity", "precious", "silver"))
                else "equity" if "equity" in q
                else "bond"
            )
            logger.info("MFSubAgent._fallback: search_mf_exposure → %r", cat)
            return search_mf_exposure.invoke({"category": cat})

        # ── Cross-AMC consensus accumulation (checked before the theme-tracker
        # branch below, since both can match on "whale") ─────────────────────
        if any(kw in q for kw in (
            "consensus", "accumulation", "accumulating", "institutions buying",
            "institutional buying", "cross-amc", "cross amc",
        )):
            logger.info("MFSubAgent._fallback: whale accumulation consensus path")
            from src.tools.whale_tools import scan_whale_accumulation
            with_tech = any(kw in q for kw in (
                "technical", "technically", "rsi", "breakout", "oversold", "drawdown",
            ))
            return scan_whale_accumulation.invoke({"with_technicals": with_tech})

        # ── Whale-tracker theme ───────────────────────────────────────────────
        if any(kw in q for kw in ("whale", "theme", "thematic", "rotation theme")):
            logger.info("MFSubAgent._fallback: whale tracker path")
            from src.tools.skills_tools import run_whale_tracker
            return run_whale_tracker.invoke({})

        # ── NAV returns ───────────────────────────────────────────────────────
        if any(kw in q for kw in ("nav return", "mom return", "monthly return", "fund return")):
            from src.tools.skills_tools import run_fund_mom_returns
            m = _re.search(r"\b(\d{5,7})\b", question)  # scheme code
            if m:
                return run_fund_mom_returns.invoke({"scheme_code": m.group(1)})
            # try to grab a fund-name search phrase
            sub = _re.sub(
                r"^(show|get|what|how)\s+", "", question, flags=_re.I
            ).strip().rstrip("?.")
            return run_fund_mom_returns.invoke({"search": sub})

        # ── MoM / YoY position changes (default) ──────────────────────────────
        if any(kw in q for kw in (
            "mom", "yoy", "changes", "added", "trimmed", "exit",
            "position", "allocation", "holdings",
        )):
            from src.tools.skills_tools import run_multi_asset_holdings_mom_yoy
            # Try to extract a fund-name hint
            for canonical, hint in [
                ("DSP_MULTI_ASSET",                            "dsp multi"),
                ("DSP_MULTI_ASSET_OMNI_FOF",                   "dsp omni"),
                ("BAJAJ_FINSERV_MULTI_ASSET_ALLOCATION_FUND",  "bajaj"),
                ("QUANT_MULTI_ASSET",                          "quant"),
                ("NIPPON_INDIA_MULTI_ASSET_ALLOCATION_FUND",   "nippon multi"),
                ("NIPPON_INDIA_MULTI_ASSET_ALLOCATION_FUND",   "nippon india multi"),
                ("NIPPON_INDIA_MULTI_ASSET_ALLOCATION_FUND",   "nippon allocation"),
                ("NIPPON_INDIA_MULTI_ASSET_OMNI_FOF",          "nippon omni"),
                ("NIPPON_INDIA_MULTI_ASSET_OMNI_FOF",          "nippon fof"),
                # bare "nippon" without qualifier defaults to the main multi-asset fund
                ("NIPPON_INDIA_MULTI_ASSET_ALLOCATION_FUND",   "nippon"),
            ]:
                if hint in q:
                    logger.info("MFSubAgent._fallback: MoM path → %s", canonical)
                    return run_multi_asset_holdings_mom_yoy.invoke({"fund": canonical})
            # No hint — fall back to listing funds so the user can pick
            return run_multi_asset_holdings_mom_yoy.invoke({"list_funds": True})

        return (
            "MFSubAgent: I can analyse MF holdings, NAV returns, and cross-fund consensus.\n"
            "Try:\n"
            "  • 'pattern across multi-asset funds'\n"
            "  • 'MoM changes in DSP Multi Asset'\n"
            "  • 'MoM changes in Nippon multi asset'\n"
            "  • 'Nippon FoF holdings this month'\n"
            "  • 'which funds hold Muthoot Finance?'\n"
            "  • 'NAV returns for scheme 152056'\n"
            "  • 'theme rotation across funds'"
        )
