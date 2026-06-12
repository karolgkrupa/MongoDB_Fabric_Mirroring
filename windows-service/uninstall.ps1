<#
.SYNOPSIS
    Stops and uninstalls the "MongoDB to Fabric Mirroring" Windows service.

.DESCRIPTION
    Run from an elevated (Administrator) PowerShell prompt. This stops the
    service if it is running and removes it from the Service Control Manager.
    It does not delete the virtual environment, logs, or your .env file.

.EXAMPLE
    Set-ExecutionPolicy -Scope Process Bypass
    .\uninstall.ps1
#>

[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"

$ServiceDir = $PSScriptRoot
$ServiceId = "mongodb-fabric-mirroring"
$WinSwExe = Join-Path $ServiceDir "$ServiceId.exe"
$WinSwConfig = Join-Path $ServiceDir "$ServiceId.xml"

function Test-Administrator {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = New-Object Security.Principal.WindowsPrincipal($identity)
    return $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

if (-not (Test-Administrator)) {
    throw "Please run this script from an elevated (Administrator) PowerShell prompt."
}

if (-not (Test-Path $WinSwExe)) {
    throw "WinSW executable not found at $WinSwExe. Nothing to uninstall."
}

Write-Host "Stopping the service '$ServiceId' (if running)..." -ForegroundColor Cyan
& $WinSwExe stop $WinSwConfig
# Ignore a non-zero exit code here: the service may already be stopped.

Write-Host "Uninstalling the service..." -ForegroundColor Cyan
& $WinSwExe uninstall $WinSwConfig
if ($LASTEXITCODE -ne 0) { throw "WinSW uninstall failed with exit code $LASTEXITCODE." }

Write-Host "Done. The '$ServiceId' service has been removed." -ForegroundColor Green
