param(
    [ValidateSet('fixture','sample','real')][string]$Scope = 'fixture',
    [string]$Month = '2026-01',
    [switch]$Offline,
    [ValidateSet('all','prepare','classify','ingest','models','verify','snapshot')]
    [string]$Stage = 'all'
)
$ErrorActionPreference = 'Stop'
$taskRoot = Split-Path $PSScriptRoot -Parent
Push-Location $taskRoot
try {
    $env:PYTHONPATH = Join-Path $taskRoot 'src'
    $taskArgs = @('-m','mobilityops.cli','run-local','--scope',$Scope,'--month',$Month,'--stage',$Stage)
    if ($Offline) { $taskArgs += '--offline' }
    & (Join-Path $taskRoot '.venv\Scripts\python.exe') @taskArgs
    if ($LASTEXITCODE -ne 0) { throw "MobilityOps failed with exit code $LASTEXITCODE" }
} finally { Pop-Location }
