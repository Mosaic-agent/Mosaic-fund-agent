"""
src/tools/news_classifier.py
─────────────────────────────
Constrained causal classifier for anomaly-date news.

Instead of asking the LLM to *reason* over raw articles, we ask it to
*classify* a pre-summarised snippet against an anomaly:

    "Given a {daily_ret}% move in {asset} on {date}, does the article below
    plausibly drive that move?  Reply YES or NO on line 1, then one short
    sentence explaining why on line 2."

Why this shape:
  • Tiny output budget (~30 tokens) → small models stay coherent.
  • No JSON / no chain-of-thought → no parse failures.
  • One classification per article → ThreadPoolExecutor-friendly.

Transport reuses the Ollama OpenAI-compatible endpoint already wired into
`news_filter.py` (settings.news_filter_llm_*).  Fail-open: any HTTP / parse
error returns ("MAYBE", "[classifier unavailable]") so the rest of the
report continues to render.

[NON-SENSITIVE] No credentials.
"""

from __future__ import annotations

import json
import logging
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from typing import Literal

logger = logging.getLogger(__name__)

Verdict = Literal["YES", "NO", "MAYBE"]


# ── Prompt ───────────────────────────────────────────────────────────────────

_SYSTEM = (
    "You are a financial causality classifier. "
    "Given an anomaly and a news headline+description, decide whether the "
    "news plausibly drives that price move. "
    "Reply with exactly two lines:\n"
    "  Line 1: YES or NO\n"
    "  Line 2: one short sentence (≤20 words) explaining why.\n"
    "Do not output anything else."
)

_USER_TEMPLATE = """Anomaly:
- Asset: {asset}
- Date: {date}
- Daily return: {ret:+.2f}%
- Regime: {regime}

News:
- Title: {title}
- Description: {description}

Does this news plausibly drive a {ret:+.2f}% move in {asset}?"""


# ── Transport (Ollama OpenAI-compatible /chat/completions) ──────────────────

def _call_ollama(
    title: str,
    description: str,
    asset: str,
    date: str,
    daily_ret: float,
    regime: str,
    base_url: str,
    model: str,
    timeout: int,
) -> tuple[Verdict, str]:
    """Single round-trip to Ollama.  Returns (verdict, reason) or fail-open."""
    user_msg = _USER_TEMPLATE.format(
        asset=asset,
        date=date,
        ret=daily_ret,
        regime=regime or "—",
        title=(title or "")[:200],
        description=(description or "")[:500],
    )
    payload = json.dumps(
        {
            "model": model,
            "messages": [
                {"role": "system", "content": _SYSTEM},
                {"role": "user", "content": user_msg},
            ],
            "temperature": 0,
            "max_tokens": 60,
            "stream": False,
        }
    ).encode()

    url = base_url.rstrip("/") + "/chat/completions"
    req = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read())
        content = (
            data.get("choices", [{}])[0]
            .get("message", {})
            .get("content", "")
            .strip()
        )
    except Exception as exc:  # noqa: BLE001
        logger.debug("news_classifier call failed (%s) — defaulting to MAYBE", exc)
        return "MAYBE", "[classifier unavailable]"

    if not content:
        return "MAYBE", "[empty response]"

    # Parse: first line = verdict, remainder = reason
    lines = [ln.strip() for ln in content.splitlines() if ln.strip()]
    head = lines[0].upper() if lines else ""
    reason = " ".join(lines[1:])[:200] if len(lines) > 1 else ""

    if head.startswith("YES"):
        verdict: Verdict = "YES"
    elif head.startswith("NO"):
        verdict = "NO"
    else:
        verdict = "MAYBE"

    if not reason:
        # Some small models put the reason on the same line: "YES. Fed pause supports gold."
        rest = lines[0][len(head):].lstrip(" .:-—") if lines else ""
        reason = rest[:200] if rest else "[no reason given]"

    return verdict, reason


# ── Public API ───────────────────────────────────────────────────────────────

def classify_price_driver(
    title: str,
    description: str,
    *,
    asset: str,
    daily_ret_pct: float,
    date_str: str,
    regime: str = "",
) -> tuple[Verdict, str]:
    """
    Classify whether a single article plausibly drives an anomaly.

    Args:
        title:         Article title (truncated to 200 chars before send).
        description:   Article description (truncated to 500 chars).
        asset:         Asset label (e.g. "gold", "Nifty Bank", "Reliance Industries").
        daily_ret_pct: Anomaly daily return as percent (e.g. +2.3 or -5.1).
        date_str:      Anomaly date, ISO YYYY-MM-DD.
        regime:        Optional regime label from the anomaly pipeline.

    Returns:
        (verdict, reason) where verdict ∈ {"YES","NO","MAYBE"} and reason is
        a ≤200 char single-line explanation.  On any error returns
        ("MAYBE", "[classifier unavailable]").
    """
    # Lazy import to avoid pulling pydantic settings during unit tests that
    # patch sys.modules["config.settings"].
    try:
        from config.settings import settings  # noqa: PLC0415
        if not settings.news_classifier_enabled:
            return "MAYBE", "[classifier disabled]"
        base_url = settings.news_filter_llm_base_url
        model = settings.news_filter_llm_model
        timeout = settings.news_filter_llm_timeout
    except Exception as exc:  # noqa: BLE001
        logger.debug("news_classifier: settings unavailable (%s)", exc)
        return "MAYBE", "[classifier unavailable]"

    return _call_ollama(
        title=title,
        description=description,
        asset=asset,
        date=date_str,
        daily_ret=float(daily_ret_pct),
        regime=regime,
        base_url=base_url,
        model=model,
        timeout=timeout,
    )


def classify_articles_batch(
    articles: list[dict],
    *,
    asset: str,
    daily_ret_pct: float,
    date_str: str,
    regime: str = "",
    max_workers: int = 4,
) -> list[tuple[Verdict, str]]:
    """
    Classify a list of articles in parallel.

    Each input dict must have keys "title" and "description".
    Returns a list of (verdict, reason) tuples in the same order as inputs.
    """
    if not articles:
        return []

    def _one(art: dict) -> tuple[Verdict, str]:
        return classify_price_driver(
            title=art.get("title", "") or "",
            description=art.get("description", "") or "",
            asset=asset,
            daily_ret_pct=daily_ret_pct,
            date_str=date_str,
            regime=regime,
        )

    with ThreadPoolExecutor(max_workers=min(max_workers, len(articles))) as pool:
        return list(pool.map(_one, articles))
