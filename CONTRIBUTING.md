# Contributing to Mosaic Fund Agent

Thank you for contributing! This guide covers everything you need to get started, submit good issues, and open pull requests.

---

## Table of contents

- [Code of conduct](#code-of-conduct)
- [Getting started](#getting-started)
- [Development setup](#development-setup)
- [Project structure](#project-structure)
- [How to contribute](#how-to-contribute)
  - [Reporting bugs](#reporting-bugs)
  - [Requesting features](#requesting-features)
  - [Opening a pull request](#opening-a-pull-request)
- [Issue labels](#issue-labels)
- [Coding standards](#coding-standards)
- [Testing](#testing)
- [LangGraph integration guidelines](#langgraph-integration-guidelines)
- [Environment variables](#environment-variables)
- [Commit message format](#commit-message-format)

---

## Code of conduct

Be respectful. This is a personal research project — contributions should improve the quality, reliability, or capability of the tool. Constructive feedback and collaborative tone are expected.

---

## Getting started

```bash
git clone https://github.com/Mosaic-agent/Mosaic-fund-agent.git
cd Mosaic-fund-agent
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # fill in your API keys
docker compose up clickhouse -d
python src/main.py config   # verify setup
```

See `docs/configuration.md` for all environment variable options.

---

## Development setup

### Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| Python | 3.11+ | Runtime |
| Docker | any | ClickHouse |
| Node.js | 18+ | Optional — doc generation only |
| LangGraph | 0.2+ | Agent graph features |

### Additional dependencies for LangGraph features

```bash
pip install langgraph langchain-anthropic langchain-openai
```

Add to `requirements.txt` when submitting a PR that introduces graph nodes.

---

## Project structure

```
src/
  agents/         Standalone agent scripts (portfolio, comex, news, signals)
  analyzers/      Asset and portfolio analyzers
  clients/        Zerodha Kite MCP client
  importer/       ClickHouse ETL pipeline
  ml/             LightGBM forecaster, GARCH anomaly detection
  tools/          Standalone tool functions (iNAV, COMEX, risk governor, etc.)
  ui/             Streamlit data hub
scripts/          One-off analysis and backfill scripts
tests/            Unit and integration tests
docs/             Architecture, schema, and configuration docs
config/           Pydantic settings
```

When adding a LangGraph graph, place it in `src/agents/` alongside the script it replaces or augments.

---

## How to contribute

### Reporting bugs

1. Search existing issues first — the bug may already be reported.
2. Open a new issue using the **Bug report** template.
3. Include:
   - Python version and OS
   - Exact command run
   - Full traceback (redact API keys)
   - Whether you are in `--demo` mode or live

### Requesting features

1. Open a **Feature request** issue.
2. Describe the problem you are solving, not just the solution.
3. If the feature involves a new LangGraph graph, include a proposed state shape and node list (see [LangGraph integration guidelines](#langgraph-integration-guidelines)).

### Opening a pull request

1. Fork the repo and create a branch from `main`:
   ```bash
   git checkout -b feat/portfolio-agent-graph
   ```
2. Make your changes. Keep each PR focused on one thing.
3. Add or update tests in `tests/`.
4. Run the test suite locally before pushing:
   ```bash
   python tests/test_tools.py
   ```
5. Open a PR against `main`. Fill in the PR template — link the related issue.
6. A maintainer will review within a reasonable time. Expect feedback.

---

## Issue labels

| Label | Meaning |
|-------|---------|
| `bug` | Something is broken |
| `enhancement` | Improvement to existing behaviour |
| `feature` | New capability |
| `langgraph` | Relates to LangGraph agent graph work |
| `agent` | Agent orchestration (portfolio, signal, macro) |
| `ml` | LightGBM, GARCH, anomaly detection |
| `risk` | Risk Governor, position sizing |
| `cli` | `src/main.py` commands |
| `data` | ClickHouse, importers, schema |
| `ui` | Streamlit data hub |
| `docs` | Documentation only |
| `good first issue` | Small, well-scoped, suitable for newcomers |
| `help wanted` | Maintainer wants community input |

---

## Coding standards

- **Formatter:** `black` with default settings
- **Linter:** `flake8` — max line length 100
- **Type hints:** required for all new functions
- **Docstrings:** one-line summary + Args/Returns for any public function
- **No secrets in code:** use `.env` and `config/settings.py` for all keys

```python
# Good
def fetch_inav(symbol: str) -> dict:
    """Fetch live iNAV and premium/discount for an ETF.

    Args:
        symbol: NSE ticker symbol (e.g. "GOLDBEES")

    Returns:
        dict with keys: inav, market_price, premium_pct
    """
```

---

## Testing

```bash
# Unit tests (no API keys required for 10/11 tests)
python tests/test_tools.py

# Integration tests (requires ClickHouse running)
python tests/_test_importer.py

# Demo mode smoke test
python src/main.py analyze --demo --max 2
```

All new LangGraph node functions must have unit tests. Test each node in isolation by passing mock state dicts — do not test the full compiled graph in unit tests.

```python
# Example node unit test pattern
def test_classify_severity_high():
    state = {"anomaly_score": 3.1, "symbol": "GOLDBEES"}
    result = classify_severity(state)
    assert result == "high"
```

---

## LangGraph integration guidelines

When adding a new LangGraph graph to the project, follow these conventions.

### State definition

Define state as a `TypedDict` in the same file as the graph. Name it `<AgentName>State`.

```python
from typing import TypedDict, Annotated
from langgraph.graph import add_messages

class PortfolioState(TypedDict):
    holdings: list
    enriched: dict
    pending: list
    retry_count: dict
    report: dict
```

### Node naming

Name nodes with lowercase snake_case verbs: `fetch_holdings`, `enrich_batch`, `score_llm`, `build_report`.

### Loop guards

Every graph that can loop must include an iteration counter in state and a hard limit:

```python
def should_continue(state: MyState) -> str:
    if state["iteration"] >= MAX_ITERATIONS:
        return "end"
    if state["pending"]:
        return "continue"
    return "end"
```

### Checkpointing

Use `MemorySaver` for development. For production use (human-in-the-loop, async approval), use `SqliteSaver`:

```python
from langgraph.checkpoint.sqlite import SqliteSaver
checkpointer = SqliteSaver.from_conn_string("output/checkpoints.db")
app = graph.compile(checkpointer=checkpointer)
```

### Wrapping existing tools

Existing functions in `src/tools/` should be wrapped as `@tool` functions with no changes to their business logic:

```python
from langchain_core.tools import tool
from src.tools.inav_fetcher import fetch as _fetch_inav

@tool
def get_live_inav(symbol: str) -> str:
    """Fetch live iNAV and premium/discount percentage for an ETF symbol."""
    result = _fetch_inav(symbol)
    return str(result)
```

### Tracing

Set `LANGSMITH_API_KEY` in `.env` to enable automatic LangSmith tracing. Do not add manual tracing code — LangGraph handles it.

---

## Environment variables

Never commit `.env`. All new keys must be:

1. Added to `.env.example` with a placeholder value and a one-line comment
2. Documented in `docs/configuration.md`
3. Loaded via `config/settings.py` using Pydantic settings

```python
# config/settings.py
class Settings(BaseSettings):
    langsmith_api_key: str = ""  # Optional — enables LangSmith tracing
```

---

## Commit message format

Use [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <short summary>

[optional body]

[optional footer: closes #issue]
```

| Type | When to use |
|------|-------------|
| `feat` | New feature or graph node |
| `fix` | Bug fix |
| `refactor` | Code change with no behaviour change |
| `test` | Adding or fixing tests |
| `docs` | Documentation only |
| `chore` | Dependency updates, config changes |

**Examples:**

```
feat(agent): add LangGraph stateful graph for portfolio_agent

Replaces linear script with a retry-capable stateful graph.
State includes per-holding retry counters and enrichment tracking.

Closes #1

fix(tools): handle NewsAPI 429 rate limit in news_sentiment_agent

refactor(cli): extract ask command into standalone ask_agent module
```

---

## Questions?

Open a discussion or an issue tagged `help wanted`. This is a personal research tool and all questions are welcome.
