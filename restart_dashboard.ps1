# Restart Dashboard Script
Write-Host "🔄 Restarting Dashboard..." -ForegroundColor Cyan
Write-Host ""

# Kill all Python processes
Write-Host "Stopping all Python processes..." -ForegroundColor Yellow
Get-Process | Where-Object { $_.ProcessName -eq "python" } | ForEach-Object {
    Write-Host "  Killing PID: $($_.Id)"
    $_ | Stop-Process -Force -ErrorAction SilentlyContinue
}

Start-Sleep -Seconds 2

# Activate virtual environment and start dashboard
Write-Host ""
Write-Host "Starting new dashboard instance..." -ForegroundColor Green
cd $PSScriptRoot
& .\.venv\Scripts\Activate.ps1
python main.py --dashboard
