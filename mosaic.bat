@echo off
REM ── Mosaic CLI Docker Wrapper (Windows) ────────────────────────────────────
REM
REM This script forwards commands to the Docker container, removing the need
REM to have Python or packages installed locally.
REM
REM Usage:
REM   mosaic.bat [command/script] [options]
REM
REM Examples:
REM   mosaic.bat kite                   — Kite MCP login/status check
REM   mosaic.bat analyze --max 3
REM   mosaic.bat ask "what is my riskiest holding?"
REM   mosaic.bat comex
REM   mosaic.bat src/scripts/goldbees_report.py

docker info >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Docker Desktop is not running. Please start Docker and try again.
    exit /b 1
)

:: Ensure persistent mosaic service is running
docker compose ps mosaic --status running -q >nul 2>&1
if %errorlevel% neq 0 (
    docker compose up -d mosaic >nul 2>&1
)

set FIRST_ARG=%1

if "%FIRST_ARG%"=="" goto run_chat
if "%FIRST_ARG%"=="chat" goto run_chat_interactive
if "%FIRST_ARG%"=="-t" goto run_chat_with_t
if "%FIRST_ARG%"=="--thread-id" goto run_chat_with_t
if "%FIRST_ARG%"=="kite" goto run_kite

:: Extract the last 3 characters to check if it ends with .py
set EXT=%FIRST_ARG:~-3%
if /I "%EXT%"==".py" (
    echo Running Python script in Docker...
    docker compose exec mosaic python %*
    exit /b %errorlevel%
)

:default_run
docker compose exec mosaic python src/main.py %*
exit /b %errorlevel%

:run_chat
docker compose exec -it mosaic python src/main.py chat
exit /b %errorlevel%

:run_chat_interactive
docker compose exec -it mosaic python src/main.py %*
exit /b %errorlevel%

:run_chat_with_t
docker compose exec -it mosaic python src/main.py chat %*
exit /b %errorlevel%

:run_kite
REM -it: the login flow blocks on input() waiting for you to complete
REM OAuth in the browser before it retries the profile fetch.
docker compose exec -it mosaic python src/main.py %*
exit /b %errorlevel%
