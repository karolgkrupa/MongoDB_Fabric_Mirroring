<#
.SYNOPSIS
    Installs/updates and starts the "MongoDB to Fabric Mirroring" Windows service.

.DESCRIPTION
    Run from an elevated (Administrator) PowerShell prompt. The script is
    idempotent: run it the first time to install, and re-run it any time to
    update an existing install. It:
      1. Verifies uv is installed (and installs it if missing).
      2. Creates/updates the project's virtual environment with `uv sync`.
      3. Downloads the WinSW service wrapper next to the service config (if needed).
      3b. Stamps the current git commit into the service description.
      4. If the service does not exist, installs and starts it (auto-start on boot).
         If it already exists, refreshes the config and restarts it so the latest
         code and configuration take effect.

    Before running, make sure you have created and populated a .env file in the
    repo root (copy .env_example to .env and fill in the values).

.EXAMPLE
    Set-ExecutionPolicy -Scope Process Bypass
    .\install.ps1
#>

[CmdletBinding()]
param(
    # Version of WinSW to download if the wrapper executable is not already present.
    [string]$WinSwVersion = "v2.12.0"
)

$ErrorActionPreference = "Stop"

$ServiceDir = $PSScriptRoot
$RepoRoot = Split-Path -Parent $ServiceDir
$ServiceId = "mongodb-fabric-mirroring"
$WinSwExe = Join-Path $ServiceDir "$ServiceId.exe"
$WinSwConfig = Join-Path $ServiceDir "$ServiceId.xml"
$LogsDir = Join-Path $ServiceDir "logs"

function Test-Administrator {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = New-Object Security.Principal.WindowsPrincipal($identity)
    return $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

if (-not (Test-Administrator)) {
    throw "Please run this script from an elevated (Administrator) PowerShell prompt."
}

# --- 1. Ensure uv is available ----------------------------------------------
if (-not (Get-Command uv -ErrorAction SilentlyContinue)) {
    Write-Host "uv not found. Installing uv..." -ForegroundColor Yellow
    Invoke-RestMethod https://astral.sh/uv/install.ps1 | Invoke-Expression
    # Make uv available in the current session.
    $env:Path = "$env:USERPROFILE\.local\bin;$env:Path"
    if (-not (Get-Command uv -ErrorAction SilentlyContinue)) {
        throw "uv installation appears to have failed. Install it manually from https://docs.astral.sh/uv/ and re-run."
    }
}

# --- 2. Sync the virtual environment ----------------------------------------
Write-Host "Syncing Python environment with uv (this also installs Python 3.14 if needed)..." -ForegroundColor Cyan
Push-Location $RepoRoot
try {
    uv sync
    if ($LASTEXITCODE -ne 0) { throw "uv sync failed with exit code $LASTEXITCODE." }
}
finally {
    Pop-Location
}

$VenvPython = Join-Path $RepoRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $VenvPython)) {
    throw "Expected virtual environment interpreter not found at $VenvPython."
}

if (-not (Test-Path (Join-Path $RepoRoot ".env"))) {
    Write-Warning "No .env file found in $RepoRoot. The service will fail to start until you create one (copy .env_example to .env and fill it in)."
}

# --- 3. Ensure the WinSW wrapper executable is present ----------------------
New-Item -ItemType Directory -Force -Path $LogsDir | Out-Null

if (-not (Test-Path $WinSwExe)) {
    $downloadUrl = "https://github.com/winsw/winsw/releases/download/$WinSwVersion/WinSW-x64.exe"
    Write-Host "Downloading WinSW $WinSwVersion from $downloadUrl ..." -ForegroundColor Cyan
    Invoke-WebRequest -Uri $downloadUrl -OutFile $WinSwExe
}

if (-not (Test-Path $WinSwConfig)) {
    throw "Service configuration not found at $WinSwConfig."
}

# --- 3b. Stamp the current git commit into the service description ----------
# This makes services.msc / Get-Service show exactly which build is deployed,
# so it's easy to tell whether (and to what) the service has been updated. The
# stamp is rewritten on every run; the regex strips any prior "(build ...)"
# suffix so repeated installs/updates don't accumulate suffixes.
if (Get-Command git -ErrorAction SilentlyContinue) {
    $commit = (& git -C $RepoRoot rev-parse --short HEAD 2>$null)
    if ($LASTEXITCODE -eq 0 -and -not [string]::IsNullOrWhiteSpace($commit)) {
        $commit = $commit.Trim()
        # Flag a dirty working tree so an ad-hoc edited deployment is distinguishable.
        & git -C $RepoRoot diff --quiet HEAD 2>$null
        if ($LASTEXITCODE -eq 1) { $commit = "$commit-dirty" }

        $installedUtc = (Get-Date).ToUniversalTime().ToString("yyyy-MM-dd HH:mm 'UTC'")

        [xml]$xml = Get-Content $WinSwConfig
        $descNode = $xml.SelectSingleNode('/service/description')
        $baseDesc = $descNode.InnerText -replace '\s*\(build [^)]*\)\s*$', ''
        $descNode.InnerText = "$baseDesc (build $commit, installed $installedUtc)"
        $xml.Save($WinSwConfig)
        Write-Host "Service description stamped: build $commit (installed $installedUtc)." -ForegroundColor Cyan
    } else {
        Write-Warning "Could not read git commit hash; service description left unchanged."
    }
} else {
    Write-Warning "git not found; service description left unchanged."
}

# --- 4. Install (first run) or update (re-run) the service ------------------
# Re-running this script should update an existing install rather than fail, so
# we branch on whether the service is already registered. `refresh` re-applies
# the .xml (e.g. the new description above) to the registered service, and the
# stop/start picks up the latest code from the working directory.
$existing = Get-Service -Name $ServiceId -ErrorAction SilentlyContinue
if ($existing) {
    Write-Host "Service '$ServiceId' already exists; updating it..." -ForegroundColor Cyan
    & $WinSwExe refresh $WinSwConfig
    if ($LASTEXITCODE -ne 0) { throw "WinSW refresh failed with exit code $LASTEXITCODE." }

    Write-Host "Restarting the service to pick up the latest code and config..." -ForegroundColor Cyan
    # Stop first (ignore failure if it's already stopped), then start, so this
    # works regardless of the service's current state.
    & $WinSwExe stop $WinSwConfig
    & $WinSwExe start $WinSwConfig
    if ($LASTEXITCODE -ne 0) { throw "WinSW start failed with exit code $LASTEXITCODE." }

    Write-Host ""
    Write-Host "Done. The '$ServiceId' service has been updated and restarted." -ForegroundColor Green
} else {
    Write-Host "Installing the Windows service '$ServiceId'..." -ForegroundColor Cyan
    & $WinSwExe install $WinSwConfig
    if ($LASTEXITCODE -ne 0) { throw "WinSW install failed with exit code $LASTEXITCODE." }

    Write-Host "Starting the service..." -ForegroundColor Cyan
    & $WinSwExe start $WinSwConfig
    if ($LASTEXITCODE -ne 0) { throw "WinSW start failed with exit code $LASTEXITCODE." }

    Write-Host ""
    Write-Host "Done. The '$ServiceId' service is installed and set to start automatically on boot." -ForegroundColor Green
}
Write-Host "Logs:        $LogsDir" -ForegroundColor Green
Write-Host "App log:     $(Join-Path $RepoRoot 'mirroring.log')" -ForegroundColor Green
Write-Host "Manage it:   services.msc  (or)  .\$ServiceId.exe status|stop|restart $ServiceId.xml" -ForegroundColor Green
