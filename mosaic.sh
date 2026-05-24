#!/bin/bash
# ── Mosaic CLI Docker Wrapper ────────────────────────────────────────────────
#
# This script forwards commands to the Docker container, removing the need
# to have Python or packages installed locally.
#
# Usage:
#   ./mosaic.sh                        — interactive chat REPL (default)
#   ./mosaic.sh [command/script] [options]
#
# Examples:
#   ./mosaic.sh                        — start interactive chat
#   ./mosaic.sh chat                   — same as above (explicit)
#   ./mosaic.sh analyze --max 3
#   ./mosaic.sh ask "what is my riskiest holding?"
#   ./mosaic.sh comex
#   ./mosaic.sh src/scripts/goldbees_report.py

# Check if Docker is running
if ! docker info >/dev/null 2>&1; then
    echo "ERROR: Docker Desktop is not running. Please start Docker and try again."
    exit 1
fi

# No arguments → start interactive chat (ensure ClickHouse is up first)
if [[ $# -eq 0 ]]; then
    docker compose up -d clickhouse 2>/dev/null
    docker compose run --rm -it mosaic chat
    exit 0
fi

FIRST_ARG="$1"

# If first arg is a python file, run it directly with python
if [[ "$FIRST_ARG" == *.py ]]; then
    echo "Running Python script in Docker..."
    docker compose run --rm --entrypoint python mosaic "$@"
else
    # Default: pass args to main.py
    docker compose run --rm mosaic "$@"
fi
