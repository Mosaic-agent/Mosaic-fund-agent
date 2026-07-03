#!/bin/bash
# ── Mosaic Fund Agent — Click-to-Run Bootstrap Script ────────────────────────
#
# Usage:
#   Double-click this file or run `./run.sh` from the terminal.
#   It will verify Docker, set up your .env file, launch the containers,
#   and open the Streamlit dashboard automatically.

# 1. Check if Docker is running
if ! docker info >/dev/null 2>&1; then
    echo "================================================================="
    echo " ERROR: Docker Desktop is not running."
    echo " Please start Docker Desktop and try running this script again."
    echo "================================================================="
    read -p "Press Enter to exit..."
    exit 1
fi

# 2. Check for .env file
if [ ! -f .env ]; then
    echo "================================================================="
    echo " First-time setup: We need to configure your '.env' settings."
    echo "================================================================="
    if command -v python3 &> /dev/null; then
        python3 setup_wizard.py
    elif command -v python &> /dev/null; then
        python setup_wizard.py
    else
        echo "Creating '.env' from default template..."
        cp .env.example .env
        echo "Please open '.env' and insert your API keys manually."
    fi
    echo "================================================================="
fi

# 3. Spin up the stack in background
echo "Starting Mosaic Fund Agent services (ClickHouse + UI)..."
docker compose up -d --build
if [ $? -ne 0 ]; then
    echo "ERROR: Failed to run docker compose. Make sure Docker is running properly."
    read -p "Press Enter to exit..."
    exit 1
fi

# 4. Wait for Streamlit UI and Studio UI to be healthy
echo "Waiting for dashboard to start at http://localhost:8501..."
until curl -s -I http://localhost:8501 | grep -q "200 OK"; do
    printf "."
    sleep 1
done
echo ""
echo " Dashboard is live!"

echo "Waiting for Studio Workspace to start at http://localhost:8502..."
until curl -s -I http://localhost:8502 | grep -q "200 OK"; do
    printf "."
    sleep 1
done
echo ""
echo " Studio Workspace is live!"

# 5. Open browser automatically
URL1="http://localhost:8501"
URL2="http://localhost:8502"
echo "Opening browser at $URL2 (Studio) and $URL1 (Data Hub)..."
if [[ "$OSTYPE" == "darwin"* ]]; then
    open "$URL2"
    open "$URL1"
elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
    if command -v xdg-open > /dev/null; then
        xdg-open "$URL2"
        xdg-open "$URL1"
      else
        echo "Please open $URL2 and $URL1 manually in your browser."
      fi
elif [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" ]]; then
    start "$URL2"
    start "$URL1"
else
    echo "Please open $URL2 and $URL1 manually in your browser."
fi
