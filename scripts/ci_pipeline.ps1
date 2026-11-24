<#
.SYNOPSIS
  Runs the full Claims Data Quality Copilot pipeline from scratch in a fresh work directory.

.DESCRIPTION
  Used by .github/workflows/demo-pipeline.yml and for local runs; both run the same steps.
  Compatible with Windows PowerShell 5.1 and PowerShell 7.

  1. Creates a virtual environment in <WorkDir>\env\venv and installs requirements.txt (wheels only).
  2. Milestone 1: generates 90,000 synthetic payments twice, loads SQLite, validates, compares runs.
  3. Milestone 2: loads DuckDB twice, reconciles, compares reruns, zero-row simulation.
  4. Milestone 3: healthy detection plus all anomaly scenarios.
  5. Milestone 4: builds the runbook vector index and verifies retrieval.
  6. Milestone 5: verifies the investigation agent in MOCK mode, then LIVE mode (PENDING without a key).
  7. Confirms the Git checkout gained no files and prints an aggregate summary.

  Everything generated goes under WorkDir, which must be outside the repository and new or
  empty. The script never deletes anything. Output is aggregate only (counts, totals,
  check results); no individual records are printed.

.PARAMETER WorkDir
  New or empty directory outside the Git checkout (CLAIMS_WORK_DIR for this run).
.PARAMETER BasePython
  Python 3.15 interpreter used to create the virtual environment.
.PARAMETER PipCacheDir
  pip download cache (default <WorkDir>\env\pip-cache).
.PARAMETER VenvPython
  Optional: python.exe of an existing environment that already has requirements.txt installed.
  Skips creating and installing the virtual environment (CI always does a fresh install).
.PARAMETER RunLive
  Keep ANTHROPIC_API_KEY for the live Claude API verification. Without this switch the key
  is removed from the environment for this run, so live verification reports PENDING and
  no API call is made.

.EXAMPLE
  powershell -NoProfile -ExecutionPolicy Bypass -File scripts\ci_pipeline.ps1 -WorkDir D:\claims-ci-run
#>
param(
    [Parameter(Mandatory = $true)][string]$WorkDir,
    [string]$BasePython = "python",
    [string]$PipCacheDir = "",
    [string]$VenvPython = "",
    [switch]$RunLive
)

$ErrorActionPreference = "Stop"
$Repo = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$InCI = $env:GITHUB_ACTIONS -eq "true"
$Results = New-Object System.Collections.Generic.List[object]

function Write-Section([string]$Text) {
    if ($InCI) { Write-Host "::group::$Text" } else { Write-Host ""; Write-Host "=== $Text ===" }
}
function Close-Section { if ($InCI) { Write-Host "::endgroup::" } }

function Invoke-Step {
    param([string]$Name, [string]$Exe, [string[]]$Arguments, [int[]]$OkCodes = @(0))
    Write-Section $Name
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    & $Exe @Arguments | Out-Host  # to the console/log, not into this function's return value
    $code = $LASTEXITCODE
    $sw.Stop()
    Close-Section
    $ok = $OkCodes -contains $code
    $Results.Add([pscustomobject]@{ Step = $Name; ExitCode = $code; Seconds = [math]::Round($sw.Elapsed.TotalSeconds, 1); Ok = $ok })
    Write-Host ("[{0}] {1} (exit {2}, {3:N1}s)" -f ($(if ($ok) { "PASS" } else { "FAIL" })), $Name, $code, $sw.Elapsed.TotalSeconds)
    if (-not $ok) { throw "Step '$Name' failed with exit code $code (expected $($OkCodes -join ' or '))" }
    return $code
}

function Get-RepoState {
    # Every untracked or ignored path in the checkout; generated files must never appear here.
    $lines = & git -C $Repo status --porcelain --untracked-files=all --ignored
    if ($LASTEXITCODE -ne 0) { throw "git status failed" }
    return @($lines | Sort-Object)
}

# ---------------------------------------------------------------- work directory
$WorkFull = [System.IO.Path]::GetFullPath($WorkDir)
$RepoNorm = $Repo.TrimEnd('\') + '\'
if (($WorkFull.TrimEnd('\') + '\').StartsWith($RepoNorm, [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "WorkDir ($WorkFull) is inside the Git checkout ($Repo). Choose a directory outside it."
}
if ((Test-Path $WorkFull) -and (Get-ChildItem -Force $WorkFull | Select-Object -First 1)) {
    throw "WorkDir ($WorkFull) is not empty. Use a new directory so the run starts from scratch."
}
New-Item -ItemType Directory -Force $WorkFull | Out-Null
if (-not $PipCacheDir) { $PipCacheDir = Join-Path $WorkFull "env\pip-cache" }

$env:CLAIMS_WORK_DIR = $WorkFull
$env:PYTHONPYCACHEPREFIX = Join-Path $WorkFull "env\pycache"
$env:TMP = Join-Path $WorkFull "env\tmp"; $env:TEMP = $env:TMP
$env:PIP_CACHE_DIR = $PipCacheDir
$env:PYTHONIOENCODING = "utf-8"
$env:PIP_DISABLE_PIP_VERSION_CHECK = "1"
Remove-Item Env:\HF_HUB_OFFLINE -ErrorAction SilentlyContinue  # a clean run may need to download the model
if (-not $RunLive) { Remove-Item Env:\ANTHROPIC_API_KEY -ErrorAction SilentlyContinue }
New-Item -ItemType Directory -Force $env:TMP | Out-Null

Write-Host "Repository : $Repo"
Write-Host "Work dir   : $WorkFull"
Write-Host "Pip cache  : $PipCacheDir"
Write-Host ("Live API   : {0}" -f $(if ($RunLive -and $env:ANTHROPIC_API_KEY) { "key provided" } else { "no key (live verification will be PENDING)" }))

$RepoBefore = Get-RepoState
Push-Location $Repo
try {
    # ------------------------------------------------------------ environment
    if ($VenvPython) {
        $Py = (Resolve-Path $VenvPython).Path
        Write-Host "Reusing existing environment: $Py (fresh install skipped)"
        Invoke-Step "Python version" $Py @("--version") | Out-Null
        $pinCheck = "import importlib.metadata as m, sys; pins = [l.strip().split('==') for l in open('requirements.txt') if l.strip()]; bad = [p for p in pins if m.version(p[0]) != p[1]]; print('requirements.txt pins installed:', len(pins) - len(bad), 'of', len(pins)); sys.exit(1 if bad else 0)"
        Invoke-Step "Check requirements.txt pins are installed" $Py @("-c", $pinCheck) | Out-Null
    } else {
        $Venv = Join-Path $WorkFull "env\venv"
        Invoke-Step "Create virtual environment" $BasePython @("-m", "venv", $Venv) | Out-Null
        $Py = Join-Path $Venv "Scripts\python.exe"
        Invoke-Step "Python version" $Py @("--version") | Out-Null
        Invoke-Step "Install requirements.txt (wheels only)" $Py @("-m", "pip", "install", "--only-binary=:all:", "--progress-bar", "off", "-q", "-r", "requirements.txt") | Out-Null
    }

    $val = Join-Path $WorkFull "validation"
    $m2 = Join-Path $WorkFull "reports\milestone2"
    New-Item -ItemType Directory -Force $val, $m2 | Out-Null

    # ------------------------------------------------------------ Milestone 1
    Invoke-Step "M1 generate data (run 1)" $Py @("scripts\generate_data.py") | Out-Null
    Invoke-Step "M1 load SQLite (run 1)" $Py @("scripts\load_sqlite.py") | Out-Null
    Invoke-Step "M1 validate (run 1)" $Py @("scripts\validate_data.py", "--output", "$val\run1.json") | Out-Null
    Invoke-Step "M1 generate data (run 2)" $Py @("scripts\generate_data.py") | Out-Null
    Invoke-Step "M1 load SQLite (run 2)" $Py @("scripts\load_sqlite.py") | Out-Null
    Invoke-Step "M1 validate (run 2)" $Py @("scripts\validate_data.py", "--output", "$val\run2.json") | Out-Null
    Invoke-Step "M1 compare runs" $Py @("scripts\validate_data.py", "--compare", "$val\run1.json", "$val\run2.json") | Out-Null

    # ------------------------------------------------------------ Milestone 2
    Invoke-Step "M2 load DuckDB (load 1)" $Py @("scripts\load_duckdb.py") | Out-Null
    Invoke-Step "M2 reconcile (load 1)" $Py @("scripts\reconcile_duckdb.py", "--output", "$m2\load1.json") | Out-Null
    Invoke-Step "M2 load DuckDB (rerun)" $Py @("scripts\load_duckdb.py") | Out-Null
    Invoke-Step "M2 reconcile (rerun)" $Py @("scripts\reconcile_duckdb.py", "--output", "$m2\load2.json") | Out-Null
    Invoke-Step "M2 compare reruns" $Py @("scripts\reconcile_duckdb.py", "--compare", "$m2\load1.json", "$m2\load2.json") | Out-Null
    $sim = Join-Path $WorkFull "warehouse\claims_zero_row_sim.duckdb"
    Invoke-Step "M2 zero-row simulation load" $Py @("scripts\load_duckdb.py", "--db", $sim, "--simulate-drop-batch", "BATCH-20260115") | Out-Null
    Invoke-Step "M2 zero-row simulation reconcile" $Py @("scripts\reconcile_duckdb.py", "--db", $sim, "--expect-dropped-batch", "BATCH-20260115", "--output", "$m2\zero_row_sim.json") | Out-Null

    # ------------------------------------------------------------ Milestone 3
    Invoke-Step "M3 healthy detection (expects no findings)" $Py @("scripts\detect_anomalies.py", "--demo-date", "2026-03-01", "--label", "healthy") | Out-Null
    Invoke-Step "M3 anomaly scenarios" $Py @("scripts\verify_anomaly_scenarios.py") | Out-Null

    # ------------------------------------------------------------ Milestone 4
    Invoke-Step "M4 build runbook index" $Py @("scripts\runbook_rag.py", "build") | Out-Null
    Invoke-Step "M4 verify retrieval" $Py @("scripts\runbook_rag.py", "verify") | Out-Null

    # ------------------------------------------------------------ Milestone 5
    Invoke-Step "M5 agent - MOCK verification" $Py @("scripts\investigate_incident.py", "verify", "--mode", "mock") | Out-Null
    $live = Invoke-Step "M5 agent - LIVE Claude API verification" $Py @("scripts\investigate_incident.py", "verify", "--mode", "live") -OkCodes @(0, 3)
    if ($live -eq 3) { Write-Host "LIVE Claude API verification: PENDING (no ANTHROPIC_API_KEY for this run)" }

    # ------------------------------------------------------------ repository hygiene and summary
    $RepoAfter = Get-RepoState
    $added = @(Compare-Object -ReferenceObject $RepoBefore -DifferenceObject $RepoAfter | Where-Object SideIndicator -eq "=>" | ForEach-Object InputObject)
    Write-Section "Git checkout unchanged by the run"
    if ($added.Count) { $added | ForEach-Object { Write-Host "  new in checkout: $_" } }
    Close-Section
    $Results.Add([pscustomobject]@{ Step = "Git checkout gained no files"; ExitCode = $added.Count; Seconds = 0; Ok = ($added.Count -eq 0) })
    if ($added.Count) { throw "The run created $($added.Count) path(s) inside the Git checkout" }
    Write-Host "[PASS] Git checkout gained no files"

    $summaryArgs = @("scripts\ci_summary.py", "--markdown-out", (Join-Path $WorkFull "ci_summary.md"))
    if ($env:GITHUB_STEP_SUMMARY) { $summaryArgs += @("--append", $env:GITHUB_STEP_SUMMARY) }
    Invoke-Step "Aggregate summary" $Py $summaryArgs | Out-Null
}
finally {
    Pop-Location
    Write-Host ""
    Write-Host "Step timings:"
    $Results | Format-Table -AutoSize Step, ExitCode, Seconds, Ok | Out-String -Width 200 | Write-Host
}
