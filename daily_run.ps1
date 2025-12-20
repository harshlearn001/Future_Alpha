# =====================================================
# Future_Alpha | Daily One-Command Pipeline (SAFE)
# =====================================================
# ✔ PowerShell 5.1 compatible
# ✔ No Unicode / Emoji
# ✔ Scheduler safe
# ✔ Pipeline-safe (controlled exits)
# =====================================================

# ---------------- FORCE UTF-8 (best effort) ----------------
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new()
$OutputEncoding = [System.Text.UTF8Encoding]::new()

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ---------------- ROOT ----------------
$ROOT = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ROOT

# ---------------- TIME ----------------
$DATE     = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
$DATE_TAG = Get-Date -Format "yyyy-MM-dd"

# ---------------- PYTHON ----------------
$PYTHON = "C:\Users\Harshal\anaconda3\envs\TradeSense\python.exe"

if (!(Test-Path $PYTHON)) {
    Write-Host "ERROR: Python not found: $PYTHON" -ForegroundColor Red
    exit 1
}

# ---------------- LOG ----------------
$LOG_DIR = Join-Path $ROOT "logs"
if (!(Test-Path $LOG_DIR)) {
    New-Item -ItemType Directory -Path $LOG_DIR | Out-Null
}

$LOG_FILE = Join-Path $LOG_DIR "daily_run_$DATE_TAG.log"

# ---------------- HEADER ----------------
@"
=====================================
Future_Alpha Daily Run Started
Date      : $DATE
Project   : $ROOT
Python    : $PYTHON
Log file  : $LOG_FILE
=====================================
"@ | Tee-Object -FilePath $LOG_FILE

# =====================================================
# HELPER: SAFE PYTHON STEP RUNNER
# =====================================================
function Run-PythonStep {
    param (
        [string]$Title,
        [string]$ScriptPath
    )

    Write-Host ""
    Write-Host "STEP: $Title"
    Add-Content $LOG_FILE ""
    Add-Content $LOG_FILE "STEP: $Title"

    # Allow Python stderr without killing PowerShell
    $oldPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"

    & $PYTHON -u $ScriptPath *>> $LOG_FILE
    $exitCode = $LASTEXITCODE

    $ErrorActionPreference = $oldPreference

    if ($exitCode -ne 0) {
        Write-Host "FAILED: $Title" -ForegroundColor Red
        Write-Host "Check log: $LOG_FILE" -ForegroundColor Yellow
        exit 1
    }

    Write-Host "DONE: $Title"
}

# =====================================================
# STEP 1: Download Daily FO Data
# =====================================================
Run-PythonStep `
    "Download daily FO data" `
    "scripts\02_download_daily_fo.py"

# =====================================================
# STEP 2: Clean Daily FO Data
# =====================================================
Run-PythonStep `
    "Clean daily FO data" `
    "scripts\03_clean_daily_fo.py"

# =====================================================
# STEP 3: Append Daily Data to Master
# =====================================================
Run-PythonStep `
    "Append daily data to master" `
    "scripts\04_append_daily_to_master.py"

# =====================================================
# STEP 4: Build Full Daily Rankings
# =====================================================
Run-PythonStep `
    "Build full daily rankings" `
    "src\signals\build_daily_rankings.py"

# =====================================================
# STEP 5: Generate Confluence Trades
# =====================================================
Run-PythonStep `
    "Generate confluence trades" `
    "src\signals\combine_ml_rankings.py"

# =====================================================
# STEP 6: Position Sizing
# =====================================================
Run-PythonStep `
    "Position sizing" `
    "src\portfolio\run_position_sizing.py"

# =====================================================
# PIPELINE COMPLETE
# =====================================================
@"
-------------------------------------
PIPELINE COMPLETED SUCCESSFULLY
-------------------------------------

Signals:
 data\signal\confluence\confluence_trades_DDMMYYYY.csv

Orders:
 data\signal\orders\trade_orders_DDMMYYYY.csv
 data\signal\orders\trade_orders_today.csv
-------------------------------------
"@ | Tee-Object -FilePath $LOG_FILE -Append

Write-Host "PIPELINE COMPLETED SUCCESSFULLY" -ForegroundColor Green
