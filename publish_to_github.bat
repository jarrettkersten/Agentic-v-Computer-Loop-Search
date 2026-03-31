@echo off
setlocal enabledelayedexpansion
title Git Save - Agentic v Computer Loop Search
cd /d "%~dp0"

echo ============================================================
echo   Save a new version to GitHub
echo   Repo: Agentic-v-Computer-Loop-Search
echo ============================================================
echo.

:: ── Clear any stale git lock files ───────────────────────────
if exist ".git\index.lock"               del /f /q ".git\index.lock"
if exist ".git\config.lock"              del /f /q ".git\config.lock"
if exist ".git\HEAD.lock"                del /f /q ".git\HEAD.lock"
if exist ".git\objects\maintenance.lock" del /f /q ".git\objects\maintenance.lock"

:: ── First-time setup: wipe any broken .git and reinitialise ──
if exist ".git\config" (
    for %%A in (".git\config") do if %%~zA lss 10 (
        echo [FIX] Corrupt .git detected - reinitialising...
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

:: ── Always ensure remote is correct ──────────────────────────
git remote get-url origin >nul 2>&1
if errorlevel 1 (
    git remote add origin https://github.com/jarrettkersten/Agentic-v-Computer-Loop-Search.git
) else (
    git remote set-url origin https://github.com/jarrettkersten/Agentic-v-Computer-Loop-Search.git
)

:: ── Fetch so we can compare against origin ───────────────────
git fetch origin main >nul 2>&1

:: ── Show uncommitted file changes ────────────────────────────
set HAS_UNCOMMITTED=0
for /f "delims=" %%i in ('git status --porcelain 2^>nul') do set HAS_UNCOMMITTED=1

if !HAS_UNCOMMITTED! EQU 1 (
    echo --- Uncommitted changes ---
    git status --short
    echo.
)

:: ── Show unpushed commits already in local git ───────────────
set PENDING=0
for /f %%i in ('git rev-list --count origin/main..HEAD 2^>nul') do set PENDING=%%i

if !PENDING! GTR 0 (
    echo --- Commits ready to push ---
    git log origin/main..HEAD --oneline --reverse
    echo.
)

:: ── Build suggested commit message ───────────────────────────
set SUGGESTED=

:: Priority 1: if there are already-committed messages, use the most recent
if !PENDING! GTR 0 (
    for /f "delims=" %%i in ('git log -1 --format^="%%s" 2^>nul') do (
        if not defined SUGGESTED set SUGGESTED=%%i
    )
    if !PENDING! GTR 1 set SUGGESTED=!SUGGESTED! ^(+!PENDING! commits^)
    goto :show_suggestion
)

:: Priority 2: build from changed file names
set FCOUNT=0
set FNAME=
for /f "delims=" %%i in ('git diff --name-only HEAD 2^>nul') do (
    set /a FCOUNT+=1
    if not defined FNAME set FNAME=%%i
)

if defined FNAME (
    :: Strip directory prefix to get just the filename
    for %%i in ("!FNAME!") do set FNAME=%%~nxi
    set SUGGESTED=Update !FNAME!
    if !FCOUNT! GTR 1 set SUGGESTED=Update !FNAME! ^(+ !FCOUNT! files changed^)
) else (
    set SUGGESTED=Update - %DATE%
)

:show_suggestion
echo --- Suggested commit message ---
echo   !SUGGESTED!
echo.

:: -- Stage everything, commit if needed, then push --
if !HAS_UNCOMMITTED! EQU 1 (
    git add -A
    git reset HEAD .env >nul 2>&1

    set MSG=!SUGGESTED!
    set /p MSG="Press Enter to use this message, or type your own: "
    if "!MSG!"=="" set MSG=!SUGGESTED!
    echo.
    git commit -m "!MSG!"
) else if !PENDING! EQU 0 (
    echo Nothing to commit or push - everything is up to date.
    echo.
    pause
    exit /b 0
) else (
    echo No uncommitted changes - pushing existing commits now.
    echo.
)

:: -- Push --
echo Pushing to GitHub...
git push -u origin main 2>nul || git push

echo.
echo ============================================================
echo   Done! Pushed to GitHub.
echo   https://github.com/jarrettkersten/Agentic-v-Computer-Loop-Search
echo ============================================================
echo.
pause
