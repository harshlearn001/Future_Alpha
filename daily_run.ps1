# =====================================================
# Future_Alpha | Daily One-Command Pipeline (SAFE)
# =====================================================

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
    Write-Host "❌ Python not found: $PYTHON" -ForegroundColor Red
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
# HELPER: RUN PYTHON STEP SAFELY
# =====================================================
function Run-PythonStep {
    param (
        [string]$Title,
        [string]$ScriptPath
    )

    Write-Host "`n> $Title"

    # Temporarily disable PowerShell panic on stderr
    $oldPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"

    & $PYTHON -u $ScriptPath *>> $LOG_FILE
    $exitCode = $LASTEXITCODE

    # Restore strict behavior
    $ErrorActionPreference = $oldPreference

    if ($exitCode -ne 0) {
        Write-Host "❌ FAILED: $Title" -ForegroundColor Red
        Write-Host "📄 Check log: $LOG_FILE" -ForegroundColor Yellow
        exit 1
    }

    Write-Host "DONE: $Title"
}


# =====================================================
# STEP 1: Download Daily F&O Data
# =====================================================
Run-PythonStep `
    "Step 1: Download daily FO data" `
    "scripts\02_download_daily_fo.py"

# =====================================================
# STEP 2: Clean Daily F&O Data
# =====================================================
Run-PythonStep `
    "Step 2: Clean daily FO data" `
    "scripts\03_clean_daily_fo.py"

# =====================================================
# STEP 3: Append to Master
# =====================================================
Run-PythonStep `
    "Step 3: Append to master" `
    "scripts\04_append_daily_to_master.py"

# =====================================================
# STEP 4: Build FULL Daily Rankings
# =====================================================
Run-PythonStep `
    "Step 4: Build full daily rankings" `
    "src\signals\build_daily_rankings.py"

# =====================================================
# STEP 5: ML + Ranking Confluence
# =====================================================
Run-PythonStep `
    "Step 5: Generate confluence trades" `
    "scripts\generate_trade_signals.py"

# =====================================================
# STEP 6: Position Sizing
# =====================================================
Run-PythonStep `
    "Step 6: Position sizing" `
    "src\portfolio\run_position_sizing.py"

# =====================================================
# PIPELINE COMPLETE
# =====================================================
@"
-------------------------------------
PIPELINE COMPLETED SUCCESSFULLY
-------------------------------------
"@ | Tee-Object -FilePath $LOG_FILE -Append

Write-Host "`n✅ PIPELINE COMPLETED SUCCESSFULLY" -ForegroundColor Green
