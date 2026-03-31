@echo off
setlocal enabledelayedexpansion
REM Run dbt build, save HTML report and logs in reports/YYYYMMDD_HHMMSS/
REM No Streamlit - HTML report only.
REM Usage: scripts\run_dbt_build_and_report.bat [dbt args...]
REM Example: scripts\run_dbt_build_and_report.bat
REM          scripts\run_dbt_build_and_report.bat build

set "SCRIPT_DIR=%~dp0"
set "PROJECT_ROOT=%SCRIPT_DIR%.."
cd /d "%PROJECT_ROOT%"
REM Use resolved current directory so paths work regardless of how batch was invoked
set "PROJECT_ROOT=%CD%"

echo.
echo === Running dbt (%*) ===
echo.

if "%~1"=="" (
    dbt build
) else (
    dbt %*
)
set DBT_EXIT=%ERRORLEVEL%

REM Create run directory and save artifacts (report + logs)
set "REPORTS_DIR=%PROJECT_ROOT%\reports"
if exist "%PROJECT_ROOT%\target\run_results.json" (
    for /f "usebackq delims=" %%T in (`powershell -NoProfile -Command "Get-Date -Format 'yyyyMMdd_HHmmss'"`) do set TIMESTAMP=%%T
    set "RUN_DIR=%REPORTS_DIR%\!TIMESTAMP!"
    if not exist "%REPORTS_DIR%" mkdir "%REPORTS_DIR%"
    mkdir "%RUN_DIR%" 2>nul
    mkdir "%RUN_DIR%\logs" 2>nul
    copy /Y "%PROJECT_ROOT%\target\run_results.json" "%RUN_DIR%\" >nul
    if exist "%PROJECT_ROOT%\target\manifest.json" copy /Y "%PROJECT_ROOT%\target\manifest.json" "%RUN_DIR%\" >nul
    if exist "%PROJECT_ROOT%\logs" xcopy /E /Y /I "%PROJECT_ROOT%\logs\*" "%RUN_DIR%\logs\" >nul 2>nul

    echo.
    echo === Generating HTML report ===
    echo.
    python "%SCRIPT_DIR%dbt_run_report.py" --project-dir "%PROJECT_ROOT%" --output "%RUN_DIR%\dbt_run_report.html" --no-console
    set REPORT_EXIT=%ERRORLEVEL%

    if !REPORT_EXIT! EQU 0 (
        echo.
        echo Report saved to:
        echo   !RUN_DIR!\dbt_run_report.html
        echo.
    ) else (
        echo.
        echo WARNING: HTML report script returned error !REPORT_EXIT!
        echo.
    )
) else (
    echo.
    echo No target\run_results.json found; skipping report and archive.
    echo.
)

echo.
echo DbT exit code: %DBT_EXIT%
echo.
pause
exit /b %DBT_EXIT%