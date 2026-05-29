$ErrorActionPreference = 'SilentlyContinue'
$parentPid = 36352
$summary = 'C:\Users\avinoams\Dev\Agali\AGALI\scrapor\local_run_logs\20260528_160907\pipeline_summary.csv'
while ($true) {
  $p = Get-Process -Id $parentPid -ErrorAction SilentlyContinue
  if (-not $p) { break }
  if ((Test-Path $summary) -and (Select-String -Path $summary -Pattern '"YAYNO_BITAN_AND_CARREFOUR"' -Quiet)) {
    Stop-Process -Id $parentPid -Force -ErrorAction SilentlyContinue
    break
  }
  Start-Sleep -Milliseconds 250
}
