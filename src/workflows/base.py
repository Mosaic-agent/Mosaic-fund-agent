"""
src/workflows/base.py
─────────────────────
Shared utilities for all StateGraph workflows:
  - _get_llm()   : reuse MosaicFundAgent LLM construction (prefer cloud → local)
  - _par()       : parallel execution via ThreadPoolExecutor
  - SYNTH_SUFFIX : injected into every synthesis prompt
"""
from __future__ import annotations

import contextvars
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from .context_manager import ContextManager, DatasetRef

logger = logging.getLogger(__name__)

# ── LLM factory ───────────────────────────────────────────────────────────────

def _get_llm(prefer_cloud: bool = True) -> Any:
    """
    Return a configured LLM, reusing MosaicFundAgent's build logic.

    Preference order (prefer_cloud=True): cloud LLM → local LLM → None
    Preference order (prefer_cloud=False): local LLM → cloud LLM → None
    """
    from src.agents.mosaic_fund_agent import MosaicFundAgent
    tmp = object.__new__(MosaicFundAgent)
    tmp._checkpointer = None
    if prefer_cloud:
        llm = tmp._build_cloud_llm()
        if llm is not None:
            return llm
        return tmp._build_llm()
    else:
        llm = tmp._build_llm()
        if llm is not None:
            return llm
        return tmp._build_cloud_llm()


def generate_plan_llm(
    question: str,
    intent: str,
    default_plan: list[str] | None = None,
) -> list[str]:
    """
    Generate a dynamic, step-by-step execution plan for a workflow using the LLM.

    Called during workflow plan initialization when MOSAIC_LLM_PLANNER="1" (default).
    Falls back gracefully to `default_plan` if LLM is unreachable or fails.
    """
    import os
    import re

    default_plan = default_plan or []

    if os.getenv("MOSAIC_LLM_PLANNER", "1") != "1":
        return default_plan

    try:
        llm = _get_llm(prefer_cloud=True)
        if llm is None:
            return default_plan

        prompt = (
            f"You are the Mosaic Quant Agent execution planner.\n"
            f"Workflow Domain: {intent}\n"
            f"User Question: \"{question}\"\n\n"
            f"Default Template Steps for reference:\n"
            + "\n".join(f"- {s}" for s in default_plan) + "\n\n"
            f"Task: Decompose the user's question into a highly specific, numbered 3 to 6 step research plan tailored to answer this exact question.\n"
            f"Each step must be a single concise line describing what analysis, fetch, or calculation to perform.\n"
            f"Output ONLY the numbered steps, 1 per line (e.g. '1. Step description'), with no extra intro or outro text."
        )

        resp = llm.invoke(prompt)
        raw_content = resp.content if hasattr(resp, "content") else str(resp)
        if isinstance(raw_content, list):
            text = "\n".join(
                item.get("text", "") if isinstance(item, dict) else getattr(item, "text", str(item))
                for item in raw_content
            )
        else:
            text = str(raw_content)

        lines = []
        for line in text.strip().splitlines():
            cleaned = re.sub(r"^\s*\d+[\.\)]\s*", "", line).strip()
            cleaned = re.sub(r"^[\-\*\•]\s*", "", cleaned).strip()
            if cleaned and len(cleaned) > 3:
                lines.append(cleaned)

        if len(lines) >= 2:
            return lines[:6]
    except Exception as exc:
        logger.warning("generate_plan_llm failed for intent=%s: %s — using default plan", intent, exc)

    return default_plan



# ── Parallel executor ─────────────────────────────────────────────────────────

# Concurrency cap for parallel fetch. Kept low (6) because several fetchers hit
# rate-limited external scrapers (Yahoo Finance, Screener.in, GNews); a 12-way
# burst intermittently trips their throttles and returns empty data. 6 keeps the
# fan-out fast while staying under the burst threshold.
_PAR_MAX_WORKERS = 6
_context_manager = ContextManager()


def _par(
    fetchers: dict[str, Any],
    max_workers: int | None = None,
    retries: int = 2,
    backoff: float = 1.5,
) -> dict[str, Any]:
    """
    Execute a dict of {key: callable} concurrently via ThreadPoolExecutor.

    Each fetcher is retried up to ``retries`` times with linear backoff on an
    exception (transient network / rate-limit blips), so a single throttle
    doesn't drop a whole section. Returns {key: result}, where result is
    whatever the fetcher returned (str, dict, ...) untouched; a fetcher that
    still fails yields a '*key unavailable: ...*' placeholder string so
    downstream callers always receive a complete dict.

    Use `_par_datasets()` instead when the caller wants compressed, prompt-ready
    text (dedup/truncation/audit metadata) rather than the raw fetcher output —
    e.g. a fetch-then-synthesize workflow whose TypedDict field is read directly
    by a synthesis prompt. Plain `_par()` is for callers that need the fetcher's
    raw return value untouched, such as a per-item dict a later node reads by
    field (see `portfolio_analysis._enrich_all_node`).

    Note: this retries on *raised* errors. A scraper that returns empty/zero
    data instead of raising (e.g. yfinance under throttle) can't be detected
    here — the low concurrency cap is the primary mitigation for that.
    """
    if not fetchers:
        return {}

    try:
        from src.importer.freshness import check_global_freshness
        check_global_freshness()
    except Exception as exc:
        logger.debug("Workflow auto-freshness check skipped: %s", exc)

    n = max_workers or min(len(fetchers), _PAR_MAX_WORKERS)

    def _run(fn: Any, key: str) -> Any:
        last: Exception | None = None
        for attempt in range(retries):
            try:
                return _context_manager.fetch_once(key, fn) or ""
            except Exception as exc:  # noqa: BLE001 — fetchers are plug-ins
                last = exc
                if attempt < retries - 1:
                    time.sleep(backoff * (attempt + 1))
        logger.warning("_par: %s failed after %d attempt(s): %s", key, retries, last)
        return f"*{key} unavailable: {last}*"

    results: dict[str, Any] = {}
    # A ContextVar is intentionally scoped to one fan-out.  Each worker receives
    # its own copied context because Python does not propagate ContextVars into
    # ThreadPoolExecutor workers automatically.
    with _context_manager.run_scope():
        with ThreadPoolExecutor(max_workers=n) as pool:
            futures = {
                pool.submit(contextvars.copy_context().run, _run, fn, key): key
                for key, fn in fetchers.items()
            }
            for f in as_completed(futures):
                key = futures[f]
                try:
                    results[key] = f.result()
                except Exception as exc:  # safety — _run should never raise
                    results[key] = f"*{key} unavailable: {exc}*"
    return results


def _par_datasets(
    fetchers: dict[str, Any],
    max_workers: int | None = None,
    retries: int = 2,
    backoff: float = 1.5,
) -> dict[str, DatasetRef]:
    """
    Like `_par()`, but compresses each raw result into a `DatasetRef` — bounded,
    deduplicated, prompt-ready text (`.content`) plus the metadata needed to
    audit compaction. Use this for fetch-then-synthesize workflows: assign
    `.content` to the workflow's existing named fields (unchanged synthesis
    prompts) and stash the full dict on `state["datasets"]` for audit.
    """
    raw = _par(fetchers, max_workers=max_workers, retries=retries, backoff=backoff)
    return {key: _context_manager.to_dataset_ref(key, value) for key, value in raw.items()}


# ── Shared prompt suffix ──────────────────────────────────────────────────────

SYNTH_SUFFIX = (
    "\n\nNUMERIC COMPUTATION RULE (mandatory): "
    "NEVER compute, estimate, or derive any number inside your response. "
    "Only narrate numbers that appear verbatim in the tool data above. "
    "If a number is not in the data, state that it is unavailable."
)


def _render_report(result: Any) -> str:
    """
    Extract clean Markdown from an LLM result, handling Anthropic extended-thinking
    responses (list of content blocks) and plain string content alike.

    Performs three post-processing passes after extraction:
      1. Print extended-thinking blocks to the console (analyst reasoning panel).
      2. Strip box-drawing chars reproduced by the LLM (cleanup runs BEFORE chart
         insertion so the real chart strings are never corrupted by the regex).
      3. Substitute [CHART:xxx] placeholders with ASCII chart strings saved by
         chart tools during the fetch phase.
    """
    import re as _re

    # ── 1. Extract text ────────────────────────────────────────────────────────
    from src.agents.sub_agents.base import _get_message_text
    content = result.content if hasattr(result, "content") else result
    text = _get_message_text(content).strip()

    # ── 2. Print thinking blocks to console (does nothing when absent) ────────
    try:
        from src.agents.sub_agents.infra import _print_thinking_blocks
        _print_thinking_blocks(content)
    except Exception:
        pass

    if not text:
        return text

    # ── 3. Cleanup (on LLM text, BEFORE chart substitution) ──────────────────
    # Strip box-drawing chars the LLM may have reproduced despite instructions.
    _CHART_LINE_RE = _re.compile(
        r"^.*[┤┼┌┐┘└├┬┴─]{3,}.*$|"
        r"^.*[████▓▓▒▒░░]{4,}.*$|"
        r"^.*▞▞.*▗▌.*$",
        _re.MULTILINE,
    )
    text = _CHART_LINE_RE.sub("", text)
    text = _re.sub(r"```\s*```", "", text)
    text = _re.sub(r"\n{3,}", "\n\n", text)

    # ── 4. Chart placeholder substitution ────────────────────────────────────
    # Real chart strings (with box-drawing + ANSI colour) are inserted AFTER
    # cleanup so the cleanup regex never touches the actual chart output.
    try:
        from src.tools.chart_tools import get_active_charts
        chart_by_type = get_active_charts().copy()

        for tname in list(chart_by_type.keys()):
            placeholders = [f"[CHART:{tname}]"]
            if tname.startswith("plot_") and tname.endswith("_chart"):
                short = tname[5:-6]  # "plot_price_chart" → "price"
                placeholders.append(f"[CHART:{short}]")

            for ph in placeholders:
                if ph in text:
                    text = text.replace(ph, chart_by_type.pop(tname))
                    break

        # Append any charts not placed by placeholder (e.g. charts from
        # workflows that didn't include a [CHART:xxx] directive in the prompt).
        for tname, chart_str in chart_by_type.items():
            title = tname.replace("plot_", "").replace("_chart", "").replace("_", " ").title()
            text += f"\n\n### {title}\n\n{chart_str}"
    except Exception:
        pass

    return text.strip()

# ── Checkpointing ─────────────────────────────────────────────────────────────

_checkpointer = None


def _get_checkpointer():
    """
    Return a module-level SqliteSaver checkpointer (same DB as chat sessions).

    Lazily initialised; the sqlite3 connection is kept open for the process
    lifetime — no context manager needed. Returns None if unavailable.
    """
    global _checkpointer
    if _checkpointer is not None:
        return _checkpointer
    try:
        import sqlite3
        import os
        from langgraph.checkpoint.sqlite import SqliteSaver
        db_path = os.path.join(os.getenv("OUTPUT_DIR", "output"), "checkpoints.db")
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        conn = sqlite3.connect(db_path, check_same_thread=False)
        _checkpointer = SqliteSaver(conn)
        logger.info("workflow checkpointer: %s", db_path)
    except Exception as exc:
        logger.warning("workflow checkpointer unavailable (resumption disabled): %s", exc)
        _checkpointer = None
    return _checkpointer


def _thread_id(workflow: str, key: str) -> str:
    """Deterministic thread ID: same workflow + key + calendar date = resumable."""
    import hashlib
    from datetime import date
    return hashlib.sha256(f"{workflow}:{key}:{date.today()}".encode()).hexdigest()[:16]


# ── Plan approval (human-in-the-loop) ────────────────────────────────────────

def _cli_prompt(prompt_text: str) -> str:
    """
    Write prompt_text to the terminal and return what the user typed.

    Uses /dev/tty so it works even when stdout is piped (e.g. to a log file).
    Falls back to stdout in non-Unix environments or CI.
    Returns "" (empty string) when stdin is not a tty — treated as auto-approve.
    """
    import sys
    try:
        with open("/dev/tty", "r+") as tty:
            tty.write(prompt_text)
            tty.flush()
            line = tty.readline()
            return line.strip()
    except OSError:
        if sys.stdin.isatty():
            sys.stdout.write(prompt_text)
            sys.stdout.flush()
            return sys.stdin.readline().strip()
        return ""   # non-interactive / CI → auto-approve


def _show_and_approve_plan(
    question: str,
    plan: list[str],
    intent: str = "unknown",
) -> list[str] | None:
    """
    Display the generated plan steps and ask for user approval.

    Called BEFORE graph.invoke() so the graph only executes approved steps.

    Behaviour
    ---------
    • MOSAIC_PLAN_APPROVAL != "1" (default)  → auto-approve, return plan unchanged
    • User types Enter / "y"                  → approve, return plan unchanged
    • User types "n" / "abort"               → return None  (caller returns "Plan cancelled")
    • User types anything else               → treat as comma/newline-separated edited steps

    Parameters
    ----------
    question : original user question (for display and similar-plan lookup)
    plan     : ordered list of step descriptions generated by the planner
    intent   : workflow name used for scoped similar-plan search

    Returns
    -------
    list[str] | None
        The (possibly edited) plan to execute, or None if the user aborted.
    """
    import os
    import re

    if os.getenv("MOSAIC_PLAN_APPROVAL", "1") != "1":
        return plan   # auto-approve in non-interactive / headless mode

    from rich.console import Console
    from rich.panel import Panel

    # ── Model & agent info for the Panel header ─────────────────────────────
    from config.settings import settings
    _backend = "ollama" if "11434" in settings.llm_base_url else (
        "local" if settings.llm_base_url else settings.llm_provider)
    _model = settings.llm_model
    _ctx = settings.llm_context_window

    _EMOJI_MAP = {
        "macro":        "🌍", "signal":      "📊", "news":         "📰",
        "mf":           "🏦", "research":    "🔬", "india_equity": "🇮🇳",
        "deepdive":     "🇺🇸", "intl_etf":    "🌐", "database":     "🗄️",
        "code":         "💻", "main":        "🤖",
    }
    _intent_emoji = _EMOJI_MAP.get(intent, "❓")

    _STEP_KEYWORDS = [
        (re.compile(r"\b(?:scanner|scan|theme|geopolit)\b", re.I), "🌍"),
        (re.compile(r"\b(?:comex|gold|silver|copper|bullion)\b", re.I), "🪙"),
        (re.compile(r"\b(?:fii|dii|flow|institutional)\b", re.I), "💰"),
        (re.compile(r"\b(?:dxy|dollar|usd|inr)\b", re.I), "💵"),
        (re.compile(r"\b(?:indicator|breadth|valuation|stress)\b", re.I), "📈"),
        (re.compile(r"\b(?:news|headline|sentiment)\b", re.I), "📰"),
        (re.compile(r"\b(?:goldbees|signal|pipeline|regime|ml)\b", re.I), "📊"),
        (re.compile(r"\b(?:chart|plot|trend|visual)\b", re.I), "📉"),
        (re.compile(r"\b(?:fund|mutual|holdings|consensus|nav)\b", re.I), "🏦"),
        (re.compile(r"\b(?:consensus|mom|yoy|change)\b", re.I), "🔄"),
        (re.compile(r"\b(?:research|fundamental|deepdive|10-k)\b", re.I), "🔬"),
        (re.compile(r"\b(?:stock|equity|share|company)\b", re.I), "🇮🇳"),
        (re.compile(r"\b(?:anomaly|regime|crash|spike)\b", re.I), "🚨"),
    ]

    def _step_emoji(step: str) -> str:
        for pattern, emoji in _STEP_KEYWORDS:
            if pattern.search(step):
                return emoji
        return _intent_emoji

    # ── Build the step list ──────────────────────────────────────────────────
    step_lines = "\n".join(
        f"  {_step_emoji(s)} [bold]{i + 1}.[/bold] {s}"
        for i, s in enumerate(plan)
    )

    body = f"  📋 [bold]Plan:[/bold] [dim]\"{question[:80]}\"[/dim]\n\n{step_lines}"

    # ── Similar plan hint (inside Panel, dim) ────────────────────────────────
    try:
        from src.workflows.plan_store import find_similar_plans
        similar = find_similar_plans(question, intent=intent, top_k=1)
        if similar:
            prev = similar[0]
            short = " → ".join(prev["steps"][:2])
            if len(prev["steps"]) > 2:
                short += " ..."
            body += (
                f"\n\n  💡 [dim]Similar plan ({prev['created_at']}, "
                f"sim={prev['similarity']}):[/dim] {short}"
            )
    except Exception:
        pass

    _LABEL = intent.replace("_", " ").title()
    Console().print(Panel(
        body,
        title=(
            f"{_intent_emoji} [bold]Plan[/bold]  "
            f"[dim]{_model} @ {_backend} ({_ctx} ctx)  →  {_LABEL}[/dim]"
        ),
        border_style="cyan",
        padding=(1, 2),
    ))

    # ── Approval prompt (plain text, outside Panel) ──────────────────────────
    user_input = _cli_prompt("[mosaic] Approve plan? [Y/n/edit]: ")

    if user_input.lower() in ("", "y", "yes"):
        return plan

    if user_input.lower() in ("n", "no", "abort", "cancel"):
        return None   # signal cancellation

    # Treat anything else as edited steps (comma or newline separated)
    edited = [s.strip() for s in re.split(r"[,\n]", user_input) if s.strip()]
    if not edited:
        return plan   # empty edit → keep original
    _cli_prompt(
        f"[mosaic] Using {len(edited)} edited step(s). Press Enter to confirm: "
    )
    return edited
