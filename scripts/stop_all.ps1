#!/usr/bin/env pwsh
# Script para detener todo el sistema DFS

Write-Host "=== Deteniendo Sistema DFS ===" -ForegroundColor Cyan

$tempDir = if ($IsWindows -or $env:OS -match "Windows") { $env:TEMP } else { "/tmp" }
$pidsFile = Join-Path $tempDir "dfs-pids.txt"

if (Test-Path $pidsFile) {
    Write-Host "Leyendo PIDs de $pidsFile..." -ForegroundColor Yellow
    
    $processIds = Get-Content $pidsFile
    
    foreach ($processId in $processIds) {
        $processId = $processId.Trim()
        if ($processId -and $processId -match '^\d+$') {
            try {
                $process = Get-Process -Id $processId -ErrorAction Stop
                Write-Host "Deteniendo proceso $processId ($($process.ProcessName))..." -ForegroundColor Yellow
                Stop-Process -Id $processId -Force
                Write-Host "Proceso $processId detenido" -ForegroundColor Green
            }
            catch {
                Write-Host "Proceso $processId no encontrado o ya detenido" -ForegroundColor Gray
            }
        }
    }
    
    Remove-Item $pidsFile -Force
    Write-Host "Todos los procesos detenidos" -ForegroundColor Green
}
else {
    Write-Host "Archivo de PIDs no encontrado en: $pidsFile" -ForegroundColor Red
    Write-Host ""
    Write-Host "Buscando procesos manualmente..." -ForegroundColor Yellow
    
    # Buscar procesos de Python relacionados con DFS
    $dfsProcesses = Get-Process python* -ErrorAction SilentlyContinue | Where-Object {
        $_.CommandLine -like "*main_simple*" -or 
        $_.CommandLine -like "*datanode_simple*"
    }
    
    if ($dfsProcesses) {
        Write-Host "Procesos DFS encontrados:" -ForegroundColor Cyan
        foreach ($proc in $dfsProcesses) {
            Write-Host "  PID: $($proc.Id) - $($proc.ProcessName)" -ForegroundColor White
        }
        Write-Host ""
        $response = Read-Host "¿Desea detener estos procesos? (S/N)"
        if ($response -eq "S" -or $response -eq "s" -or $response -eq "Y" -or $response -eq "y") {
            foreach ($proc in $dfsProcesses) {
                Write-Host "Deteniendo proceso $($proc.Id)..." -ForegroundColor Yellow
                Stop-Process -Id $proc.Id -Force
            }
            Write-Host "Procesos detenidos" -ForegroundColor Green
        }
    }
    else {
        Write-Host "No se encontraron procesos DFS en ejecución" -ForegroundColor Gray
    }
}

Write-Host ""
Write-Host "Sistema DFS detenido" -ForegroundColor Green