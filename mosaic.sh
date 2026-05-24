#!/bin/bash
# ── Mosaic CLI Docker Wrapper ────────────────────────────────────────────────
#
# This script forwards commands to the Docker container, removing the need
# to have Python or packages installed locally.
#
# Usage:
#   ./mosaic.sh [command/script] [options]
#
# Examples:
#   ./mosaic.sh analyze --max 3
#   ./mosaic.sh ask "what is my riskiest holding?"
#   ./mosaic.sh comex
#   ./mosaic.sh src/scripts/goldbees_report.py

# Check if Docker is running
if ! docker info >/dev/null 2>&1; then
    echo "ERROR: Docker Desktop is not running. Please start Docker and try again."
    exit 1
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
