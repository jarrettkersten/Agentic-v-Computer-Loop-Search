@echo off
title Loop Comparison App
color 0A

echo ============================================
echo   Agentic Loop vs Computer Loop Comparison
echo ============================================
echo.

REM ── Check Python is available ────────────────
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python not found. Please install Python 3.10+ and add it to PATH.
    pause
    exit /b 1
)

REM ── Set working directory to this script's folder ──
cd /d "%~dp0"

REM ── API key: .env file takes priority, fallback to prompt ──
if exist ".env" (
    echo Found .env file — API key loaded automatically.
    echo.
) else if "%ANTHROPIC_API_KEY%"=="" (
    echo No .env file found and ANTHROPIC_API_KEY is not set.
    echo.
    set /p ANTHROPIC_API_KEY="Paste your Anthropic API key and press Enter: "
    echo.
)

REM ── Install Python dependencies ──
echo Installing/checking Python dependencies...
pip install -r requirements.txt -q --disable-pip-version-check
echo Done.
echo.

REM ── Install Playwright + Chromium browser ──
python -c "import playwright" >nul 2>&1
if not errorlevel 1 (
    echo Installing Playwright browser ^(Chromium^)...
    python -m playwright install chromium
    echo Done.
    echo.
)

REM ── Launch the Flask app ─────────────────────
echo Starting app at http://localhost:5000
echo Press Ctrl+C to stop.
echo.
start "" http://localhost:5000
python app.py

pause
