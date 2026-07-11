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
#   ./mosaic.sh                        — start interactive chat (also starts Studio)
#   ./mosaic.sh studio                 — build & start the Studio UI at :8502
#   ./mosaic.sh chat                   — same as no-args (explicit)
#   ./mosaic.sh analyze --max 3
#   ./mosaic.sh ask "what is my riskiest holding?"
#   ./mosaic.sh comex
#   ./mosaic.sh intraday GOLDBEES
#   ./mosaic.sh live-monitor --dry-run
#   ./mosaic.sh live-monitor --check-session-only
#   ./mosaic.sh live-monitor -d          — detached, survives closing the terminal
#   ./mosaic.sh mf "compare Nippon and DSP Multi Asset"
#   ./mosaic.sh src/scripts/goldbees_report.py

# Check if Docker is running
if ! docker info >/dev/null 2>&1; then
    echo "ERROR: Docker Desktop is not running. Please start Docker and try again."
    exit 1
fi

# No arguments → start interactive chat (ensure ClickHouse + Ollama are up first)
if [[ $# -eq 0 ]]; then
    # Check if a local Ollama instance is already running on the host
    if curl -s -I http://localhost:11434/ >/dev/null 2>&1; then
        echo "Local Ollama detected running on host. Building and starting clickhouse + qdrant + ui + files + studio..."
        docker compose build studio 2>/dev/null
        docker compose up -d clickhouse qdrant ui files studio 2>/dev/null
    else
        echo "Starting services (pulling local embedding models on first run — grab a coffee)..."
        docker compose build studio
        docker compose up -d clickhouse qdrant ollama ui files studio
        docker compose run --rm ollama-init || true   # no-op if already done
    fi
    echo ""
    echo "  🖥️  UI (Data Hub):    http://localhost:8501
  📁 Reports:           http://localhost:8502
  🧠 Vector DB:         http://localhost:6333/dashboard"
    echo ""
    docker compose run --rm -it mosaic chat
    exit 0
fi

# Start a lightweight background helper on the host to update the shared cache
# so the containerised telemetry UI displays live host PC metrics.
(
    while true; do
        OLLAMA_STATS=$(ps -ax -o pid,%cpu,%mem,comm | grep -i "ollama" | grep -v "grep" | grep -v "system_telemetry" 2>/dev/null)
        if [ -n "$OLLAMA_STATS" ]; then
            TOTAL_CPU=$(echo "$OLLAMA_STATS" | awk '{s+=$2} END {print s}')
            TOTAL_MEM=$(echo "$OLLAMA_STATS" | awk '{s+=$3} END {print s}')
            PIDS=$(echo "$OLLAMA_STATS" | awk '{print $1}' | tr '\n' ',' | sed 's/,$//')
            echo "{\"cpu\": ${TOTAL_CPU:-0.0}, \"mem\": ${TOTAL_MEM:-0.0}, \"pids\": [${PIDS:-}], \"timestamp\": $(date +%s)}" > ./src/host_telemetry.json
        else
            echo "{\"cpu\": 0.0, \"mem\": 0.0, \"pids\": [], \"timestamp\": $(date +%s)}" > ./src/host_telemetry.json
        fi
        sleep 2
    done
) &
TELEMETRY_PID=$!
trap 'kill $TELEMETRY_PID 2>/dev/null; rm -f ./src/host_telemetry.json 2>/dev/null' EXIT INT TERM

echo ""
echo "  🖥️  UI (Data Hub):    http://localhost:8501
  📁 Reports:           http://localhost:8502
  🧠 Vector DB:         http://localhost:6333/dashboard"
echo ""

FIRST_ARG="$1"

# If first arg is a python file, run it directly with python
if [[ "$FIRST_ARG" == *.py ]]; then
    echo "Running Python script in Docker..."
    docker compose run --rm --entrypoint python mosaic "$@"
elif [[ "$FIRST_ARG" == "studio" ]]; then
    echo "Studio Workspace has been temporarily disabled in the docker-compose stack."
    echo "Please check git history or restore the 'studio' service in docker-compose.yml to run it."
    exit 1
elif [[ "$FIRST_ARG" == "intraday" ]]; then
    shift
    ARGS=()
    if [[ $# -gt 0 && ! "$1" =~ ^- ]]; then
        ARGS+=("--symbol" "$1")
        shift
    fi
    ARGS+=("$@")
    echo "Running Intraday Signal Agent in Docker..."
    # -it allocates a real TTY so the agent's in-place (top-style) dashboard
    # refresh activates; without it, stdout is a pipe and the agent falls
    # back to reprinting the full block every interval.
    docker compose run --rm -it --entrypoint python mosaic src/agents/intraday_agent.py "${ARGS[@]}"
elif [[ "$FIRST_ARG" == "live-monitor" ]]; then
    shift
    if [[ "$1" == "-d" || "$1" == "--detach" ]]; then
        echo "Starting Live Monitor in Docker (detached — survives closing this terminal)..."
        docker compose up -d live-monitor
        echo "View logs with: docker compose logs -f live-monitor"
        echo "Stop with:      docker compose stop live-monitor"
    else
        echo "Running Live Monitor in Docker (foreground, Ctrl+C to stop)..."
        # No -it: live_monitor.py logs headlessly (no TUI), and -it requires a
        # real TTY that isn't always available (e.g. scripted/CI invocation).
        docker compose run --rm live-monitor "$@"
    fi
elif [[ "$FIRST_ARG" == "mf" ]]; then
    echo "Running Mutual Fund Sub-Agent in Docker..."
    docker compose run --rm mosaic "$@"
elif [[ "$FIRST_ARG" == "chat" ]]; then
    # Run interactive chat with args (e.g. -t <thread_id>)
    docker compose run --rm -it mosaic "$@"
elif [[ "$FIRST_ARG" == "-t" || "$FIRST_ARG" == "--thread-id" ]]; then
    # Auto-prepend "chat" and run interactive
    docker compose run --rm -it mosaic chat "$@"
else
    # Default: pass args to main.py
    docker compose run --rm mosaic "$@"
fi
