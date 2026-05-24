#!/bin/bash
# ── Mosaic Fund Agent — Stop Services Script ────────────────────────────────
#
# Usage:
#   Double-click this file or run `./stop.sh` from the terminal.
#   It will gracefully stop all running Docker containers for the application.

# Check if Docker is running
if ! docker info >/dev/null 2>&1; then
    echo "Docker is not running, services are likely already stopped."
    exit 0
fi

echo "Stopping Mosaic Fund Agent services..."
docker compose down

echo "================================================================="
echo " All services have been stopped successfully."
echo "================================================================="
sleep 2
