$ErrorActionPreference = 'SilentlyContinue'

$OriginalRunnerPid = 36352
$LogDir = 'C:\Users\avinoams\Dev\Agali\AGALI\scrapor\local_run_logs\20260528_160907'
$OriginalSummary = Join-Path $LogDir 'pipeline_summary.csv'
$RemainingScript = Join-Path $LogDir 'run_requested_remaining.ps1'
$RemainingStdout = Join-Path $LogDir 'requested_remaining_stdout.log'
$RemainingStderr = Join-Path $LogDir 'requested_remaining_stderr.log'
$MarkerPath = Join-Path $LogDir 'requested_remaining_started.txt'

while (Get-Process -Id $OriginalRunnerPid -ErrorAction SilentlyContinue) {
  Start-Sleep -Seconds 2
}

if (Test-Path $MarkerPath) {
  exit 0
}

$yaynoSucceeded = $false
if (Test-Path $OriginalSummary) {
  $yaynoSucceeded = Select-String -Path $OriginalSummary -Pattern '"YAYNO_BITAN_AND_CARREFOUR",0,0,' -Quiet
}

if (-not $yaynoSucceeded) {
  "YAYNO_BITAN_AND_CARREFOUR did not complete successfully; remaining requested stores were not started." |
    Set-Content -Path (Join-Path $LogDir 'requested_remaining_not_started.txt') -Encoding UTF8
  exit 1
}

"started_at=$((Get-Date).ToString('o'))" | Set-Content -Path $MarkerPath -Encoding UTF8
Start-Process -FilePath 'powershell.exe' `
  -ArgumentList @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', $RemainingScript) `
  -WindowStyle Hidden `
  -RedirectStandardOutput $RemainingStdout `
  -RedirectStandardError $RemainingStderr
