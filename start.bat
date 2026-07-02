@echo off
setlocal enabledelayedexpansion
title LLM API Router
cd /d "%~dp0"

REM === LLM API Router - Start / Restart ===
REM Double-click to run. Ctrl+C to stop.

REM -- 1. Kill existing process on port 4000 --
echo [info] Checking port 4000...
for /f "tokens=5" %%P in ('netstat -ano ^| findstr ":4000.*LISTENING" 2^>nul') do (
    echo [kill] PID=%%P
    taskkill /f /pid %%P >nul 2>&1
)
timeout /t 2 /nobreak >nul

REM -- 2. Locate Python --
where py >nul 2>&1
if %errorlevel%==0 (
    set "PY=py -3"
) else (
    where python >nul 2>&1
    if %errorlevel%==0 (
        set "PY=python"
    ) else (
        echo [ERROR] Cannot find python/py. Install Python 3.10+
        goto :fail
    )
)
echo [info] Python: %PY%

REM -- 3. Check dependencies --
%PY% -c "import fastapi, uvicorn, httpx, yaml, ruamel.yaml" >nul 2>&1
if %errorlevel% neq 0 (
    echo [setup] Installing dependencies...
    %PY% -m pip install -e . ruamel.yaml -q
    if %errorlevel% neq 0 (
        echo [ERROR] Dependency install failed
        goto :fail
    )
)

REM -- 4. Config check --
if not exist "config.yaml" (
    if exist "config.example.yaml" (
        echo [WARN] config.yaml missing, copying from example
        copy /y "config.example.yaml" "config.yaml" >nul
    ) else (
        echo [ERROR] config.yaml missing and no example found
        goto :fail
    )
)
if not exist "keys.yaml" (
    echo [ERROR] keys.yaml missing. Copy from keys.example.yaml and fill in real keys.
    goto :fail
)

REM -- 5. Validate config --
%PY% -m llm_api_router.cli validate
if %errorlevel% neq 0 (
    echo [ERROR] Config validation failed
    goto :fail
)

REM -- 6. Print service info --
set "PYTHONIOENCODING=utf-8"
%PY% -m llm_api_router.cli info

REM -- 7. Background: open browser when ready --
start "" /b cmd /c "for /l %%i in (1,1,30) do (timeout /t 1 /nobreak >nul 2>nul & curl -s -o nul -w "" http://127.0.0.1:4000/healthz >nul 2>nul && (start http://127.0.0.1:4000/ui/ & exit /b 0))"

REM -- 8. Start server (foreground, Ctrl+C to stop) --
echo.
echo [start] Starting server... Will open http://127.0.0.1:4000/ui/ when ready.
echo         Press Ctrl+C to stop.
echo.
%PY% -m llm_api_router.cli serve
goto :end

:fail
echo.
echo === Start failed ===
pause
exit /b 1

:end
echo.
echo === Server stopped ===
pause
