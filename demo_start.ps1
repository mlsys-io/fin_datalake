# Market Pulse - Demo Startup Orchestrator
# This script starts the full AI Lakehouse stack in the correct order for the presentation.

Write-Host "🚀 INITIALIZING AI LAKEHOUSE DEMO STACK..." -ForegroundColor Cyan

# 1. Start Ray (if not head)
if (-not (Get-Process "ray" -ErrorAction SilentlyContinue)) {
    Write-Host "[1/5] Starting Ray Head Node..." -ForegroundColor Yellow
    Start-Process "ray" -ArgumentList "start --head" -NoNewWindow
    Start-Sleep -Seconds 5
} else {
    Write-Host "[1/5] Ray is already running." -ForegroundColor Gray
}

# 2. Start Redis (via Docker)
Write-Host "[2/5] Starting ContextStore (Redis)..." -ForegroundColor Yellow
docker start redis 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "WARNING: Redis docker container could not be started. Ensure Docker Desktop is running." -ForegroundColor Red
}

# 3. Start the Gateway API
Write-Host "[3/5] Starting Gateway API (Port 8000)..." -ForegroundColor Yellow
$gateway_proc = Start-Process "uv" -ArgumentList "run uvicorn gateway.api.main:app --port 8000 --reload" -PassThru -NoNewWindow
Start-Sleep -Seconds 3

# 4. Start the Overseer (Autonomic Loop)
Write-Host "[4/5] Starting Autonomic Overseer..." -ForegroundColor Yellow
$overseer_proc = Start-Process "uv" -ArgumentList "run python -m overseer.main" -PassThru -NoNewWindow

# 5. Start the MCP Server
Write-Host "[5/5] Starting MCP Protocol Bridge..." -ForegroundColor Yellow
$mcp_proc = Start-Process "uv" -ArgumentList "run python -m gateway.mcp.server" -PassThru -NoNewWindow

# Final Health Check for ALL components
Write-Host "`n🔍 PERFORMING SYSTEM HEALTH CHECK..." -ForegroundColor Cyan
Start-Sleep -Seconds 3

$allOk = $true

# 1. Gateway
try {
    $resp = Invoke-WebRequest -Uri "http://localhost:8000/healthz" -UseBasicParsing -ErrorAction Stop
    if ($resp.StatusCode -eq 200) {
        Write-Host "  ✅ Gateway       : ONLINE (port 8000)" -ForegroundColor Green
    }
} catch {
    Write-Host "  ❌ Gateway       : OFFLINE — check terminal for errors" -ForegroundColor Red
    $allOk = $false
}

# 2. Redis
try {
    $ping = docker exec redis redis-cli ping 2>$null
    if ($ping -eq "PONG") {
        Write-Host "  ✅ Redis (Docker): ONLINE" -ForegroundColor Green
    } else {
        throw "No PONG"
    }
} catch {
    Write-Host "  ❌ Redis (Docker): OFFLINE — run: docker start redis" -ForegroundColor Red
    $allOk = $false
}

# 3. Ray Dashboard (KubeRay NodePort exposed on 32382)
try {
    $ray_resp = Invoke-WebRequest -Uri "http://localhost:32382" -UseBasicParsing -TimeoutSec 3 -ErrorAction Stop
    Write-Host "  ✅ Ray Cluster   : ONLINE (dashboard port 32382)" -ForegroundColor Green
} catch {
    Write-Host "  ⚠️  Ray Cluster   : Not reachable on port 32382 (may still be starting)" -ForegroundColor Yellow
}

# 4. Overseer process
if ($overseer_proc -and -not $overseer_proc.HasExited) {
    Write-Host "  ✅ Overseer      : Running (PID $($overseer_proc.Id))" -ForegroundColor Green
} else {
    Write-Host "  ❌ Overseer      : Process has exited — check logs" -ForegroundColor Red
    $allOk = $false
}

# 5. MCP Server process
if ($mcp_proc -and -not $mcp_proc.HasExited) {
    Write-Host "  ✅ MCP Server    : Running (PID $($mcp_proc.Id))" -ForegroundColor Green
} else {
    Write-Host "  ❌ MCP Server    : Process has exited — check logs" -ForegroundColor Red
    $allOk = $false
}

if ($allOk) {
    Write-Host "`n✅ ALL SYSTEMS ONLINE. Demo stack is ready." -ForegroundColor Green
} else {
    Write-Host "`n⚠️  One or more components failed. Fix issues above before presenting." -ForegroundColor Yellow
}

Write-Host "`nYou can now run the following scripts:" -ForegroundColor Cyan
Write-Host "1. Ingest Data:   uv run python -m pipelines.market_pulse_ingest"
Write-Host "2. Run Full Demo: uv run python -m pipelines.market_pulse_demo"
Write-Host "3. Self-Healing:  uv run python -m pipelines.self_healing_demo"
Write-Host "4. Benchmarks:    uv run python -m pipelines.benchmark_market_pulse"
Write-Host "`nPress Ctrl+C to terminate background processes (or use Task Manager)."
