"""
scripts/sync_agy_agents.py
───────────────────────────
Regenerate .agents/agents/<name>-agent.md from .agents/skills/<name>/SKILL.md.

Each AGY agent file is a SKILL.md's frontmatter (name, description) plus body,
with an AGY-specific frontmatter block appended (tools, model, temperature,
max_turns). Running this script keeps every agent file in sync with its
source skill — no more hand-copying, no more silent drift.

Usage:
    python scripts/sync_agy_agents.py           # regenerate all
    python scripts/sync_agy_agents.py --check   # exit 1 if any file would change
"""
from __future__ import annotations

import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
SKILLS_DIR = REPO_ROOT / ".agents" / "skills"
AGENTS_DIR = REPO_ROOT / ".agents" / "agents"

# Shared AGY frontmatter, identical across every agent.
AGY_TOOLS = [
    "run_command",
    "view_file",
    "search_web",
    "read_url_content",
    "grep_search",
    "list_dir",
]
AGY_MODEL = "inherit"
AGY_TEMPERATURE = 0.1
DEFAULT_MAX_TURNS = 20   # fallback for any skill not listed below

# max_turns tiers — see plan-rich-plan-spicy-wind.md for the reasoning behind
# each tier. Heuristic ceiling based on each skill's actual execution shape
# (single fixed command vs. small fixed sequence vs. large pipeline / wide menu
# requiring dispatch reasoning) — not raw doc length.
MAX_TURNS: dict[str, int] = {
    # Tier 1 — single fixed command
    "etf-setups": 8,
    "intraday": 8,
    "ma-crossover": 8,
    "stock-quant-deepdive": 8,
    "shoonya-session": 8,
    "stock-anomaly-news": 8,
    "risk-governor": 8,
    "mosaic-agent": 8,
    "goldbees-pipeline": 8,
    "etf-premium-discount": 8,
    # Tier 2 — small fixed sequence / narrow dispatch
    "daily-signal-composite": 15,
    "etf-news": 15,
    "macro-scanner": 15,
    "live-monitor": 15,
    "dsp-multi-asset-importer": 15,
    "cavecrew": 15,
    # Tier 3 — large multi-step pipeline / wide menu
    "data-engineering-importer": 25,
    "db-freshness": 25,
    "macro-strategy": 25,
    "commit": 25,
    "mf-tracker": 25,
}


def _agent_filename(skill_name: str) -> str:
    """mosaic-agent's skill dir is already named '...-agent' — don't double the suffix."""
    if skill_name.endswith("-agent"):
        return f"{skill_name}.md"
    return f"{skill_name}-agent.md"


def _agent_name(skill_name: str) -> str:
    if skill_name.endswith("-agent"):
        return skill_name
    return f"{skill_name}-agent"


def _split_frontmatter(text: str) -> tuple[dict, str]:
    """Parse a '---\\nYAML\\n---\\nbody' file into (frontmatter_dict, body)."""
    if not text.startswith("---\n"):
        raise ValueError("SKILL.md must start with a YAML frontmatter block")
    _, _, rest = text.partition("---\n")
    fm_text, _, body = rest.partition("\n---\n")
    frontmatter = yaml.safe_load(fm_text) or {}
    return frontmatter, body.lstrip("\n")


class _IndentedListDumper(yaml.SafeDumper):
    """PyYAML doesn't indent block-sequence items under a mapping key by
    default (`tools:\n- x` instead of `tools:\n  - x`) — match the original
    file style so the diff stays minimal."""

    def increase_indent(self, flow=False, indentless=False):
        return super().increase_indent(flow, False)


def _render_agent_file(skill_name: str, description: str, body: str) -> str:
    max_turns = MAX_TURNS.get(skill_name, DEFAULT_MAX_TURNS)
    frontmatter = {
        "name": _agent_name(skill_name),
        "description": description,
        "tools": AGY_TOOLS,
        "model": AGY_MODEL,
        "temperature": AGY_TEMPERATURE,
        "max_turns": max_turns,
    }
    fm_yaml = yaml.dump(
        frontmatter,
        Dumper=_IndentedListDumper,
        sort_keys=False,
        default_flow_style=False,
        allow_unicode=True,
        width=float("inf"),   # keep description on one line, matching original style
    ).rstrip("\n")
    return f"---\n{fm_yaml}\n---\n\n{body.rstrip()}\n"


def sync(check: bool = False) -> bool:
    """Regenerate every agent file. Returns True if anything changed (or would)."""
    changed = False
    skill_dirs = sorted(p for p in SKILLS_DIR.iterdir() if p.is_dir())
    for skill_dir in skill_dirs:
        skill_md = skill_dir / "SKILL.md"
        if not skill_md.exists():
            continue
        skill_name = skill_dir.name
        frontmatter, body = _split_frontmatter(skill_md.read_text())
        description = frontmatter.get("description", "")
        rendered = _render_agent_file(skill_name, description, body)

        agent_path = AGENTS_DIR / _agent_filename(skill_name)
        existing = agent_path.read_text() if agent_path.exists() else None
        if existing != rendered:
            changed = True
            print(f"{'would update' if check else 'updating'}: {agent_path.relative_to(REPO_ROOT)}")
            if not check:
                agent_path.write_text(rendered)
        else:
            print(f"up to date: {agent_path.relative_to(REPO_ROOT)}")
    return changed


def main() -> int:
    check = "--check" in sys.argv[1:]
    changed = sync(check=check)
    if check and changed:
        print("\n.agents/agents/*.md is out of sync with .agents/skills/*/SKILL.md — run without --check to fix.")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
