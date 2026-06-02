"""
src/utils/caveman.py
────────────────────
Utility for Caveman Mode. Provides prompt injection instructions based on the
configured caveman intensity level to optimize output token efficiency.
"""

from __future__ import annotations
import os


def get_caveman_prompt(level: str | None = None) -> str:
    """
    Get the system prompt injection string for caveman mode.
    Returns empty string if level is None, 'off', or invalid.
    """
    if not level:
        level = os.environ.get("CAVEMAN_LEVEL")
        
    if not level:
        return ""
        
    level = level.lower().strip()
    if level in ("off", "none", "false", "stop", "normal", "disabled"):
        return ""
        
    intro = (
        "\n\n=======================================================\n"
        "CAVEMAN MODE ACTIVE (mandatory response style instructions):\n"
        "Respond terse like smart caveman. All technical substance stay. Only fluff die.\n"
        "ACTIVE EVERY RESPONSE. No revert after many turns. No filler drift. Still active if unsure.\n"
    )
    
    rules = (
        "Rules:\n"
        "- Drop: articles (a/an/the), filler (just/really/basically/actually/simply), pleasantries (sure/certainly/of course/happy to), hedging.\n"
        "- Fragments OK.\n"
        "- Short synonyms (big not extensive, fix not \"implement a solution for\").\n"
        "- Technical terms exact. Code blocks unchanged. Errors quoted exact.\n"
        "- Pattern: `[thing] [action] [reason]. [next step].` (e.g. \"Bug in auth middleware. Token expiry check use `<` not `<=`. Fix:\")\n"
        "- Auto-Clarity: Drop caveman ONLY for security warnings or destructive confirmations. Otherwise always obey.\n"
    )
    
    levels_info = {
        "lite": (
            "Intensity Level: lite\n"
            "- No filler/hedging. Keep articles + full sentences. Professional but tight."
        ),
        "full": (
            "Intensity Level: full (default)\n"
            "- Drop articles, fragments OK, short synonyms. Classic caveman."
        ),
        "ultra": (
            "Intensity Level: ultra\n"
            "- Bare fragments. Abbreviate prose words (DB/auth/config/req/res/fn/impl).\n"
            "- Strip conjunctions. Use arrows for causality (X -> Y).\n"
            "- One word when one word enough. Code symbols, function names, API names, error strings: never abbreviate."
        ),
        "wenyan-lite": (
            "Intensity Level: wenyan-lite\n"
            "- Semi-classical. Drop filler/hedging but keep grammar structure, classical register."
        ),
        "wenyan-full": (
            "Intensity Level: wenyan-full\n"
            "- Maximum classical terseness. Fully 文言文. 80-90% character reduction.\n"
            "- Classical sentence patterns, verbs precede objects, subjects often omitted, classical particles (之/乃/為/其)."
        ),
        "wenyan-ultra": (
            "Intensity Level: wenyan-ultra\n"
            "- Extreme abbreviation while keeping classical Chinese feel. Maximum compression, ultra terse."
        )
    }
    
    # Handle shorthand levels
    if level == "wenyan":
        level = "wenyan-full"
        
    level_specific = levels_info.get(level, levels_info["full"])
    
    return f"{intro}\n{rules}\n{level_specific}\n=======================================================\n"
