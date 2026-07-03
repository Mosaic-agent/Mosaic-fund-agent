#!/bin/bash
# ── Mosaic Fund Agent — Local Native Installer ──────────────────────────────
#
# This script automates native local installation:
# 1. Checks Python version (requires Python 3.11+)
# 2. Creates and activates virtual environment (.venv)
# 3. Installs requirements.txt
# 4. Installs system dependencies (like libomp on macOS for LightGBM)
# 5. Configures .env file
# 6. Starts background ClickHouse + Qdrant databases
# 7. Initializes Qdrant metadata schemas
#
# Usage:
#   chmod +x install_local.sh
#   ./install_local.sh

echo "================================================================="
echo " 🪙  Mosaic Local Installer"
echo "================================================================="

# 1. Check Python version
if ! command -v python3 &> /dev/null; then
    echo "ERROR: python3 is not installed. Please install Python 3.11+."
    exit 1
fi

PYTHON_VERSION=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
REQUIRED_MAJOR=3
REQUIRED_MINOR=11

# Compare version components
IFS='.' read -ra ADDR <<< "$PYTHON_VERSION"
if [ "${ADDR[0]}" -lt $REQUIRED_MAJOR ] || { [ "${ADDR[0]}" -eq $REQUIRED_MAJOR ] && [ "${ADDR[1]}" -lt $REQUIRED_MINOR ]; }; then
    echo "ERROR: Python 3.11+ is required. Found: $PYTHON_VERSION"
    exit 1
fi
echo "✓ Python version verified: $PYTHON_VERSION"

# 2. Check and install system dependencies (OS specific)
if [[ "$OSTYPE" == "darwin"* ]]; then
    echo "macOS detected. Checking for OpenMP library (libomp) required by LightGBM..."
    if ! brew list libomp &>/dev/null; then
        if command -v brew &> /dev/null; then
            echo "Installing libomp via Homebrew..."
            brew install libomp
        else
            echo "WARNING: Homebrew not found. Please install libomp manually (brew install libomp)."
        fi
    else
        echo "✓ libomp is already installed."
    fi
fi

# 3. Initialize virtual environment
if [ ! -d .venv ]; then
    echo "Creating virtual environment (.venv)..."
    python3 -m venv .venv
else
    echo "✓ Virtual environment (.venv) already exists."
fi

# Activate virtual environment
source .venv/bin/activate

# Upgrade pip and install requirements
echo "Installing/updating Python dependencies from requirements.txt..."
pip install --upgrade pip
pip install -r requirements.txt

# 4. Check for .env file
if [ ! -f .env ]; then
    echo "First-time setup: We need to configure your '.env' settings."
    python3 setup_wizard.py
else
    echo "✓ Configuration file (.env) already exists."
fi

# 5. Check if Docker is running (for DB services)
echo "Checking Docker daemon status..."
if ! docker info >/dev/null 2>&1; then
    echo "WARNING: Docker is not running. ClickHouse and Qdrant DB services cannot be started."
    echo "Please start Docker Desktop and run: 'docker compose up clickhouse qdrant -d' manually."
else
    echo "Starting background DB services (ClickHouse + Qdrant)..."
    docker compose up clickhouse qdrant -d
    
    # 6. Initialize database schema & metadata seeds
    echo "Waiting for ClickHouse and Qdrant database services to accept connections..."
    sleep 5
    echo "Initializing database schemas and metadata in Qdrant..."
    ALLOW_LOCAL_RUN=1 PYTHONPATH=. python src/scripts/db_metadata_init.py
fi

echo "================================================================="
echo " 🎉 Installation Complete!"
echo "================================================================="
echo " To start using Mosaic natively, run:"
echo "   source .venv/bin/activate"
echo "   python src/main.py --help"
echo "================================================================="
