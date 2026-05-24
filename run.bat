@echo off
REM ── Mosaic Fund Agent — Click-to-Run Bootstrap Script (Windows) ────────────
REM
REM Usage:
REM   Double-click run.bat to start.
REM   It will verify Docker, set up your .env file, launch the containers,
REM   and open the Streamlit dashboard automatically.

:: 1. Check if Docker is running
docker info >nul 2>&1
if %errorlevel% neq 0 (
    echo =================================================================
    echo  ERROR: Docker Desktop is not running.
    echo  Please start Docker Desktop and try running this script again.
    echo =================================================================
    pause
    exit /b 1
)

:: 2. Check for .env file
if not exist .env (
    echo =================================================================
    echo  First-time setup: Creating '.env' configuration file.
    echo =================================================================
    copy .env.example .env
    echo  Created '.env' from template.
    echo  IMPORTANT: Please open the '.env' file in this folder and add
    echo  your API keys (e.g. Zerodha login, OpenAI/Anthropic keys).
    echo =================================================================
    pause
    exit /b 1
)

:: 3. Spin up the stack in background
echo Starting Mosaic Fund Agent services (ClickHouse + UI)...
docker compose up -d --build
if %errorlevel% neq 0 (
    echo ERROR: Failed to run docker compose. Make sure Docker Desktop is running.
    pause
    exit /b 1
)

:: 4. Wait for Streamlit UI to be healthy
echo Waiting for dashboard to start at http://localhost:8501...
:wait_loop
curl -s -I http://localhost:8501 | findstr "200" >nul
if %errorlevel% neq 0 (
    timeout /t 1 /nobreak >nul
    goto wait_loop
)

echo.
echo Dashboard is live!

:: 5. Open browser automatically
echo Opening browser...
start http://localhost:8501
