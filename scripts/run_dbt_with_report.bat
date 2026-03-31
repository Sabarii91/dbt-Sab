@echo off
setlocal enabledelayedexpansion
REM Run dbt with your arguments, then generate the user-friendly run report.
REM Usage: scripts\run_dbt_with_report.bat [dbt args...]
REM Example: scripts\run_dbt_with_report.bat build

set "SCRIPT_DIR=%~dp0"
set "PROJECT_ROOT=%SCRIPT_DIR%.."
cd /d "%PROJECT_ROOT%"

echo.
echo === Running dbt (%*) ===
echo.

if "%~1"=="" (
    dbt build
) else (
    dbt %*
)
set DBT_EXIT=%ERRORLEVEL%

echo.
echo === Generating run report ===
echo.

python "%SCRIPT_DIR%dbt_run_report.py" --project-dir "%PROJECT_ROOT%"
set REPORT_EXIT=%ERRORLEVEL%

echo.
if %DBT_EXIT% NEQ 0 (
    echo DbT failed with exit code %DBT_EXIT%.
) else (
    echo DbT completed successfully.
)
if %REPORT_EXIT% EQU 0 (
    echo Report generation completed successfully.
) else (
    echo Report script exited with code %REPORT_EXIT%.
)
echo.

pause
exit /b %DBT_EXIT%
