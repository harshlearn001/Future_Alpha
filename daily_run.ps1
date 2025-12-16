# =====================================================
# Future_Alpha | Daily One-Command Pipeline (PS 5.1 SAFE)
# =====================================================

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ---------------- ROOT ----------------
$ROOT = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ROOT

# ---------------- TIME ----------------
$DATE = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
$DATE_TAG = Get-Date -Format "yyyy-MM-dd"

# ---------------- PYTHON ----------------
$PYTHON = "C:\Users\Harshal\anaconda3\envs\TradeSense\python.exe"

if (!(Test-Path $PYTHON)) {
    Write-Host "ERROR: Python not found -> $PYTHON"
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
"@ | Out-File $LOG_FILE -Encoding utf8

Write-Host "====================================="
Write-Host "Future_Alpha Daily Run Started"
Write-Host "Date      : $DATE"
Write-Host "Project   : $ROOT"
Write-Host "Python    : $PYTHON"
Write-Host "Log file  : $LOG_FILE"
Write-Host "====================================="

# ---------------- RUNNER ----------------
function Run-Step {
    param (
        [string]$Title,
        [string]$ScriptPath
    )

    Write-Host ""
    Write-Host "> $Title"
    "[$(Get-Date -Format 'HH:mm:ss')] $Title" | Out-File $LOG_FILE -Append

    # IMPORTANT: propagate Python exit code correctly
    cmd /c "`"$PYTHON`" `"$ScriptPath`" >> `"$LOG_FILE`" 2>&1 & exit /b %errorlevel%"
    $code = $LASTEXITCODE

    if ($code -ne 0) {
        Write-Host "FAILED: $Title (exit code $code)"
        throw "Pipeline stopped"
    }

    Write-Host "DONE: $Title"
}

# ---------------- PIPELINE ----------------
try {
    Run-Step "Step 1: Download daily FO data" "scripts\02_download_daily_fo.py"
    Run-Step "Step 2: Clean daily FO data"    "scripts\03_clean_daily_fo.py"
    Run-Step "Step 3: Append to master"       "scripts\04_append_daily_to_master.py"
    Run-Step "Step 4: Generate rankings"      "scripts\generate_trade_signals.py"
    Run-Step "Step 5: Position sizing"        "src\portfolio\run_position_sizing.py"

    Write-Host ""
    Write-Host "PIPELINE COMPLETED SUCCESSFULLY"
    "PIPELINE SUCCESS" | Out-File $LOG_FILE -Append
}
catch {
    Write-Host ""
    Write-Host "PIPELINE FAILED - CHECK LOG"
    Write-Host $LOG_FILE
    "PIPELINE FAILED" | Out-File $LOG_FILE -Append
    exit 1
}
