# Run dbt with your arguments, then generate the user-friendly run report.
# Usage: .\scripts\run_dbt_with_report.ps1 [dbt args...]
# Examples:
#   .\scripts\run_dbt_with_report.ps1 build
#   .\scripts\run_dbt_with_report.ps1 run --select staging
#   .\scripts\run_dbt_with_report.ps1 test

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
Set-Location $ProjectRoot

$dbtArgs = $args
if ($dbtArgs.Count -eq 0) {
    $dbtArgs = @("build")
}

Write-Host "Running: dbt $($dbtArgs -join ' ')" -ForegroundColor Cyan
& dbt @dbtArgs
$dbtExit = $LASTEXITCODE

Write-Host ""
Write-Host "Generating run report..." -ForegroundColor Cyan
& python "$PSScriptRoot\dbt_run_report.py" --project-dir $ProjectRoot
$reportExit = $LASTEXITCODE

if ($dbtExit -ne 0) {
    exit $dbtExit
}
exit $reportExit
