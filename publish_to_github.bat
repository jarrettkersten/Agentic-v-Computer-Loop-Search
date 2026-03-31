@echo off
title Git Save — Agentic v Computer Loop Search
cd /d "%~dp0"

echo ============================================================
echo   Save a new version to GitHub
echo   Repo: Agentic-v-Computer-Loop-Search
echo ============================================================
echo.

:: ── Clear any stale git lock files ───────────────────────────
if exist ".git\index.lock"              del /f /q ".git\index.lock"
if exist ".git\config.lock"             del /f /q ".git\config.lock"
if exist ".git\HEAD.lock"               del /f /q ".git\HEAD.lock"
if exist ".git\objects\maintenance.lock" del /f /q ".git\objects\maintenance.lock"

:: ── First-time setup: wipe any broken .git and reinitialise ────
if exist ".git\config" (
    :: Check config is not empty/corrupt
    for %%A in (".git\config") do if %%~zA lss 10 (
        echo [FIX] Corrupt .git detected — reinitialising...
        rmdir /s /q ".git"
    )
)

if not exist ".git" (
    echo Setting up Git for the first time...
    git init
    git branch -M main
    git remote add origin https://github.com/jarrettkersten/Agentic-v-Computer-Loop-Search.git
    echo Git initialised and remote set.
    echo.
)

:: ── Always ensure remote is correct ───────────────────────────
git remote get-url origin >nul 2>&1
if errorlevel 1 (
    git remote add origin https://github.com/jarrettkersten/Agentic-v-Computer-Loop-Search.git
) else (
    git remote set-url origin https://github.com/jarrettkersten/Agentic-v-Computer-Loop-Search.git
)

:: ── Show what changed ─────────────────────────────────────────
echo Files changed:
git status --short
echo.

:: ── Prompt for a commit message ───────────────────────────────
set /p MSG="Describe what you changed (press Enter for default): "
if "%MSG%"=="" set MSG=Update — %DATE% %TIME%

:: ── Stage, exclude .env just in case, commit and push ─────────
git add -A
git reset HEAD .env 2>nul

git commit -m "%MSG%"

git push -u origin main 2>nul || git push

echo.
echo ============================================================
echo   Saved! Version committed and pushed to GitHub.
echo   https://github.com/jarrettkersten/Agentic-v-Computer-Loop-Search
echo ============================================================
echo.
pause
