$ErrorActionPreference = 'Stop'

$RepoRoot = 'C:\Users\avinoams\Dev\Agali\AGALI\scrapor'
$EnvPath = 'C:\Users\avinoams\Dev\Agali\AGALI\web-backend\.env'
$LogDir = 'C:\Users\avinoams\Dev\Agali\AGALI\scrapor\local_run_logs\20260528_160907'
$StatePath = Join-Path $LogDir 'pipeline_state.json'
$SummaryPath = Join-Path $LogDir 'pipeline_summary_requested.csv'

[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new()
$env:PYTHONIOENCODING = 'utf-8'
$env:PYTHONUNBUFFERED = '1'
$env:TZ = 'Asia/Jerusalem'
$existingPythonPath = $env:PYTHONPATH
if ($existingPythonPath) {
  $env:PYTHONPATH = "$LogDir;$RepoRoot;$existingPythonPath"
} else {
  $env:PYTHONPATH = "$LogDir;$RepoRoot"
}

Set-Location $RepoRoot

Get-Content $EnvPath | ForEach-Object {
  if ($_ -match '^\s*#' -or $_ -notmatch '=') { return }
  $idx = $_.IndexOf('=')
  $name = $_.Substring(0, $idx).Trim()
  $value = $_.Substring($idx + 1).Trim().Trim('"')
  if ($name) { [Environment]::SetEnvironmentVariable($name, $value, 'Process') }
}
$env:POSTGRESQL_URL = $env:DATABASE_URL

$stores = @(
  'HAZI_HINAM',
  'HET_COHEN',
  'MAHSANI_ASHUK',
  'SUPER_PHARM',
  'VICTORY',
  'QUIK'
)

function Write-State {
  param(
    [string]$Phase,
    [string]$Store,
    [int]$Index,
    [int]$Total,
    [string]$Status,
    [string]$Message = ''
  )

  [pscustomobject]@{
    updated_at = (Get-Date).ToString('o')
    phase = $Phase
    store = $Store
    index = $Index
    total = $Total
    status = $Status
    message = $Message
  } | ConvertTo-Json | Set-Content -Path $StatePath -Encoding UTF8
}

function Invoke-LoggedStep {
  param(
    [string]$Title,
    [string]$LogPath,
    [scriptblock]$Body
  )

  "::group::$Title" | Tee-Object -FilePath $LogPath | Out-Null
  $previousErrorActionPreference = $ErrorActionPreference
  $ErrorActionPreference = 'Continue'
  try {
    & $Body 2>&1 | Tee-Object -FilePath $LogPath -Append | Out-Null
    $code = $LASTEXITCODE
    if ($null -eq $code) { $code = 0 }
  } finally {
    $ErrorActionPreference = $previousErrorActionPreference
  }
  "::endgroup::" | Tee-Object -FilePath $LogPath -Append | Out-Null
  "EXIT_CODE=$code" | Tee-Object -FilePath $LogPath -Append | Out-Null
  return [int]$code
}

"store,w2_exit,w3_exit,upload_data,outputs_count,started_at,finished_at" |
  Set-Content -Path $SummaryPath -Encoding UTF8

$total = $stores.Count
for ($i = 0; $i -lt $stores.Count; $i++) {
  $store = $stores[$i]
  $idx = $i + 1
  $started = (Get-Date).ToString('o')
  $appData = Join-Path $LogDir "app_data_$store"
  $w2Log = Join-Path $LogDir "W2_$store.log"
  $w3Log = Join-Path $LogDir "W3_$store.log"

  if (Test-Path $appData) {
    $resolvedAppData = (Resolve-Path -LiteralPath $appData).Path
    $resolvedLogDir = (Resolve-Path -LiteralPath $LogDir).Path
    if (-not $resolvedAppData.StartsWith($resolvedLogDir, [System.StringComparison]::OrdinalIgnoreCase)) {
      throw "Refusing to delete path outside log directory: $resolvedAppData"
    }
    Remove-Item -LiteralPath $appData -Recurse -Force
  }
  New-Item -ItemType Directory -Path $appData -Force | Out-Null

  Write-State -Phase 'W2' -Store $store -Index $idx -Total $total -Status 'running' -Message 'scraping,converting,clean_dump_files'

  $env:OUTPUT_DESTINATION = 'file'
  $env:PROCESSED_FILES_CACHE = Join-Path $RepoRoot 'processed_files_cache.json'
  $env:OPERATION = 'scraping,converting,clean_dump_files'
  $env:ENABLED_SCRAPERS = $store
  $env:ENABLED_FILE_TYPES = ''
  $env:LIMIT = ''
  $env:APP_DATA_PATH = $appData
  $env:LOG_LEVEL = 'WARNING'
  $env:SCRAPOR_INSECURE_SSL = '1'

  $w2Exit = Invoke-LoggedStep -Title "W2 Scrape $store" -LogPath $w2Log -Body {
    python (Join-Path $LogDir 'run_main_local.py')
  }

  $outputsPath = Join-Path $appData 'outputs'
  $outputFiles = @()
  if (Test-Path $outputsPath) {
    $outputFiles = @(Get-ChildItem -Path $outputsPath -File -Recurse -ErrorAction SilentlyContinue)
  }
  $hasUploadData = $outputFiles.Count -gt 0
  $w3Exit = -1

  if ($w2Exit -eq 0 -and $hasUploadData) {
    Write-State -Phase 'W3' -Store $store -Index $idx -Total $total -Status 'running' -Message 'api_update'

    $env:OUTPUT_DESTINATION = 'postgres'
    $env:POSTGRESQL_URL = $env:DATABASE_URL
    $env:ANALYZE_PRODUCTS_MIN_INTERVAL_MINUTES = '15'
    $env:OPERATION = 'api_update'
    $env:ENABLED_SCRAPERS = $store
    $env:APP_DATA_PATH = $appData
    $env:LOG_LEVEL = 'WARNING'
    $env:PROCESSED_FILES_CACHE = Join-Path $RepoRoot 'processed_files_cache.json'
    Remove-Item Env:\SCRAPOR_INSECURE_SSL -ErrorAction SilentlyContinue

    $w3Exit = Invoke-LoggedStep -Title "W3 Upload $store to PostgreSQL" -LogPath $w3Log -Body {
      python (Join-Path $LogDir 'run_main_local.py')
    }
  } elseif ($w2Exit -eq 0) {
    "No upload data for $store - skipping W3 trigger" | Set-Content -Path $w3Log -Encoding UTF8
    $w3Exit = 0
  } else {
    "W2 failed for $store - W3 skipped" | Set-Content -Path $w3Log -Encoding UTF8
  }

  $finished = (Get-Date).ToString('o')
  $line = '"{0}",{1},{2},{3},{4},"{5}","{6}"' -f $store, $w2Exit, $w3Exit, $hasUploadData, $outputFiles.Count, $started, $finished
  Add-Content -Path $SummaryPath -Value $line -Encoding UTF8

  if ($w2Exit -ne 0 -or $w3Exit -ne 0) {
    Write-State -Phase 'failed' -Store $store -Index $idx -Total $total -Status 'failed' -Message "w2_exit=$w2Exit w3_exit=$w3Exit"
    exit 1
  }

  Write-State -Phase 'store-complete' -Store $store -Index $idx -Total $total -Status 'completed' -Message "outputs=$($outputFiles.Count)"
}

Write-State -Phase 'complete' -Store '' -Index $total -Total $total -Status 'completed' -Message 'requested stores processed'
exit 0
