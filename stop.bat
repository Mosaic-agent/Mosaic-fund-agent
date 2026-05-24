@echo off
REM ── Mosaic Fund Agent — Stop Services Script (Windows) ─────────────────────
REM
REM Usage:
REM   Double-click stop.bat to stop the services.

:: Check if Docker is running
docker info >nul 2>&1
if %errorlevel% neq 0 (
    echo Docker is not running, services are likely already stopped.
    timeout /t 3 >nul
    exit /b 0
)

echo Stopping Mosaic Fund Agent services...
docker compose down

echo =================================================================
echo  All services have been stopped successfully.
echo =================================================================
timeout /t 3 >nul
