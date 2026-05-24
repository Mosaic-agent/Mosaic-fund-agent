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
REM   mosaic.bat analyze --max 3
REM   mosaic.bat ask "what is my riskiest holding?"
REM   mosaic.bat comex
REM   mosaic.bat src/scripts/goldbees_report.py

docker info >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Docker Desktop is not running. Please start Docker and try again.
    exit /b 1
)

set FIRST_ARG=%1

if "%FIRST_ARG%"=="" goto default_run

:: Extract the last 3 characters to check if it ends with .py
set EXT=%FIRST_ARG:~-3%
if /I "%EXT%"==".py" (
    echo Running Python script in Docker...
    docker compose run --rm --entrypoint python mosaic %*
    exit /b %errorlevel%
)

:default_run
docker compose run --rm mosaic %*
exit /b %errorlevel%
