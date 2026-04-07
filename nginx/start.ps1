#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Lakehouse Launchpad — Full Stack Dev Launcher

.DESCRIPTION
    Starts all services in separate windows:
      1. FastAPI Gateway        (port 8000)
      2. Vite React UI          (port 5173)
      3. System Overseer        (standalone)
      4. Nginx Bouncer          (port 8080)

.USAGE
    .\start.ps1                    # Uses default Nginx path
    .\start.ps1 -NginxPath "C:\nginx"
#>

param(
    [string]$NginxPath = "C:\nginx"
)

$Root    = Split-Path $PSScriptRoot -Parent
$AppCode = Join-Path $Root "app-code"
$NginxConf = Join-Path $PSScriptRoot "nginx.conf"

# Resolve absolute path for nginx -c (required by nginx on Windows)
$NginxConf = (Resolve-Path $NginxConf).Path

Write-Host "`n=== Lakehouse Launchpad Startup ===" -ForegroundColor Cyan
Write-Host "Root:       $Root"
Write-Host "App-Code:   $AppCode"
Write-Host "Nginx conf: $NginxConf"
Write-Host "Nginx path: $NginxPath`n"

# ------------------------------------------------------------------
# 1. FastAPI Gateway
# ------------------------------------------------------------------
Write-Host "[1/4] Starting FastAPI Gateway on port 8000..." -ForegroundColor Green
Start-Process powershell -ArgumentList "-NoExit", "-Command",
    "cd '$AppCode'; uv run uvicorn gateway.api.main:app --port 8000 --reload" `
    -WindowStyle Normal

Start-Sleep -Seconds 2   # Give the gateway a moment to bind its port

# ------------------------------------------------------------------
# 2. Vite React Dev Server
# ------------------------------------------------------------------
Write-Host "[2/4] Starting Vite React UI on port 5173..." -ForegroundColor Green
Start-Process powershell -ArgumentList "-NoExit", "-Command",
    "cd '$AppCode\frontend'; npm run dev" `
    -WindowStyle Normal

Start-Sleep -Seconds 2

# ------------------------------------------------------------------
# 3. System Overseer (standalone process)
# ------------------------------------------------------------------
Write-Host "[3/4] Starting System Overseer..." -ForegroundColor Green
Start-Process powershell -ArgumentList "-NoExit", "-Command",
    "cd '$AppCode'; uv run python -m overseer.main" `
    -WindowStyle Normal

Start-Sleep -Seconds 1

# ------------------------------------------------------------------
# 4. Nginx Bouncer
# ------------------------------------------------------------------
$NginxExe = Join-Path $NginxPath "nginx.exe"

if (-not (Test-Path $NginxExe)) {
    Write-Host "[4/4] nginx.exe not found at '$NginxExe'." -ForegroundColor Yellow
    Write-Host "      Install Nginx for Windows from https://nginx.org/en/download.html" -ForegroundColor Yellow
    Write-Host "      Then re-run with: .\start.ps1 -NginxPath 'C:\your\nginx\folder'" -ForegroundColor Yellow
} else {
    Write-Host "[4/4] Starting Nginx Bouncer on port 8080..." -ForegroundColor Green

    # Stop any stale Nginx process before starting a fresh one
    & $NginxExe -s quit 2>$null
    Start-Sleep -Milliseconds 500

    Start-Process $NginxExe -ArgumentList "-c", "`"$NginxConf`"", "-p", "`"$NginxPath`"" `
        -WindowStyle Hidden

    Write-Host "`n=== All services started! ===" -ForegroundColor Cyan
    Write-Host ">>> Open your browser at http://localhost:8080 <<<" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "  Dashboard:        http://localhost:8080/" -ForegroundColor White
    Write-Host "  Prefect (gated):  http://localhost:8080/prefect/" -ForegroundColor White
    Write-Host "  Ray (gated):      http://localhost:8080/ray/" -ForegroundColor White
    Write-Host "  MinIO (gated):    http://localhost:8080/minio/" -ForegroundColor White
    Write-Host ""
    Write-Host "  Overseer logs:    Check the 'System Overseer' terminal window" -ForegroundColor DarkGray
}

