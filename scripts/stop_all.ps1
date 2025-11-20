#!/usr/bin/env pwsh
# Script para detener todo el sistema DFS - Adaptado para estructura backend/

# Configurar UTF-8 para la terminal
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8
chcp 65001 | Out-Null  # Solo en Windows

$ErrorActionPreference = "Continue"

Write-Host "=== Deteniendo Sistema DFS ===" -ForegroundColor Cyan

$tempDir = if ($IsWindows -or $env:OS -match "Windows") { $env:TEMP } else { "/tmp" }
$pidsFile = Join-Path $tempDir "dfs-system-pids.txt"

$processesStopped = 0

# Primero intentar con Docker Compose si está disponible
if (Get-Command docker-compose -ErrorAction SilentlyContinue) {
    $scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
    $projectRoot = Split-Path -Parent $scriptDir
    $composeFile = Join-Path $projectRoot "docker-compose.yml"
    
    if (Test-Path $composeFile) {
        Write-Host "🐳 Deteniendo servicios Docker..." -ForegroundColor Yellow
        Set-Location $projectRoot
        
        # Verificar si hay servicios en ejecución
        $services = docker-compose ps --services
        if ($services -and $services -notmatch "^\s*$") {
            Write-Host "Deteniendo contenedores Docker..." -ForegroundColor Yellow
            docker-compose down
            if ($LASTEXITCODE -eq 0) {
                Write-Host "Servicios Docker detenidos" -ForegroundColor Green
                $processesStopped++
            } else {
                Write-Host "⚠️  No se pudieron detener algunos servicios Docker" -ForegroundColor Yellow
            }
        } else {
            Write-Host "ℹ️  No hay servicios Docker en ejecución" -ForegroundColor Gray
        }
    }
}

# Luego detener procesos nativos
if (Test-Path $pidsFile) {
    Write-Host ""
    Write-Host "Leyendo PIDs de $pidsFile..." -ForegroundColor Yellow
    
    $pidLines = Get-Content $pidsFile
    $pids = @{}
    
    foreach ($line in $pidLines) {
        if ($line -match '^(\w+)=(\d+)$') {
            $service = $matches[1]
            $processId = $matches[2]  # Cambiado de 'pid' a 'processId'
            $pids[$service] = $processId
        }
    }
    
    # Detener procesos en orden inverso (DataNodes primero, Metadata último)
    $stopOrder = @("DATANODE3", "DATANODE2", "DATANODE1", "METADATA")
    
    foreach ($service in $stopOrder) {
        if ($pids.ContainsKey($service)) {
            $processId = $pids[$service]
            try {
                # Removida la variable no utilizada 'process'
                Get-Process -Id $processId -ErrorAction Stop | Out-Null
                Write-Host "Deteniendo $service (PID: $processId)..." -ForegroundColor Yellow
                Stop-Process -Id $processId -Force
                Write-Host "$service detenido" -ForegroundColor Green
                $processesStopped++
            }
            catch {
                Write-Host "⚠️  $service (PID: $processId) no encontrado o ya detenido" -ForegroundColor Gray
            }
        }
    }
    
    Remove-Item $pidsFile -Force
    Write-Host "Archivo de PIDs removido" -ForegroundColor Green
}
else {
    Write-Host "ℹ️  Archivo de PIDs no encontrado en: $pidsFile" -ForegroundColor Gray
}

# Buscar procesos residuales de backend
Write-Host ""
Write-Host "Buscando procesos residuales de backend..." -ForegroundColor Yellow

$backendProcesses = Get-Process python* -ErrorAction SilentlyContinue | Where-Object {
    $_.CommandLine -like "*backend.metadata.server*" -or 
    $_.CommandLine -like "*backend.datanode.server*" -or
    $_.CommandLine -like "*uvicorn*backend*" -or
    $_.CommandLine -like "*python*backend*"
}

if ($backendProcesses) {
    Write-Host "Procesos backend residuales encontrados:" -ForegroundColor Cyan
    foreach ($proc in $backendProcesses) {
        $cmdLine = if ($proc.CommandLine) { $proc.CommandLine } else { "N/A" }
        Write-Host "  PID: $($proc.Id) - $($proc.ProcessName)" -ForegroundColor White
        Write-Host "    Comando: $cmdLine" -ForegroundColor Gray
    }
    
    $response = Read-Host "`n¿Desea detener estos procesos? (S/N)"
    if ($response -eq "S" -or $response -eq "s" -or $response -eq "Y" -or $response -eq "y") {
        foreach ($proc in $backendProcesses) {
            Write-Host "Deteniendo proceso $($proc.Id)..." -ForegroundColor Yellow
            try {
                Stop-Process -Id $proc.Id -Force
                Write-Host "Proceso $($proc.Id) detenido" -ForegroundColor Green
                $processesStopped++
            }
            catch {
                Write-Host "No se pudo detener proceso $($proc.Id)" -ForegroundColor Red
            }
        }
    } else {
        Write-Host "ℹ️  Procesos no detenidos por el usuario" -ForegroundColor Yellow
    }
} else {
    Write-Host "No se encontraron procesos backend residuales" -ForegroundColor Green
}

# Limpiar variables de entorno
Write-Host ""
Write-Host "🧹 Limpiando variables de entorno..." -ForegroundColor Yellow

$envVars = @(
    "DFS_METADATA_HOST",
    "DFS_METADATA_PORT", 
    "DFS_DATANODE_HOST",
    "DFS_DATANODE_PORT",
    "DFS_STORAGE_PATH",
    "NODE_ID",
    "PYTHONPATH"
)

$cleanedVars = 0
foreach ($envVar in $envVars) {
    if (Get-Item "env:$envVar" -ErrorAction SilentlyContinue) {
        Remove-Item "env:$envVar" -ErrorAction SilentlyContinue
        $cleanedVars++
    }
}

Write-Host "$cleanedVars variables de entorno limpiadas" -ForegroundColor Green

# Limpiar archivos temporales de DFS
Write-Host ""
Write-Host "🗑️  Limpiando archivos temporales..." -ForegroundColor Yellow

$tempFiles = @(
    "$tempDir/dfs-*.log",
    "$tempDir/dfs-*-errors.log",
    "$tempDir/dfs-data-*",
    "$tempDir/dfs-metadata"
)

$cleanedFiles = 0
foreach ($pattern in $tempFiles) {
    Get-ChildItem -Path $pattern -ErrorAction SilentlyContinue | ForEach-Object {
        try {
            Remove-Item $_.FullName -Recurse -Force -ErrorAction Stop
            Write-Host "  Limpiado: $($_.Name)" -ForegroundColor Gray
            $cleanedFiles++
        } catch {
            Write-Host "  No se pudo limpiar: $($_.Name)" -ForegroundColor DarkYellow
        }
    }
}

Write-Host "$cleanedFiles archivos temporales limpiados" -ForegroundColor Green

Write-Host ""
if ($processesStopped -gt 0) {
    Write-Host "Sistema DFS detenido completamente. $processesStopped procesos/containers terminados." -ForegroundColor Green
} else {
    Write-Host "ℹ️  No se encontraron procesos de DFS en ejecución." -ForegroundColor Cyan
}

Write-Host ""
Write-Host "Para reiniciar el sistema:" -ForegroundColor Cyan
Write-Host "  Con Docker: ./scripts/start.ps1" -ForegroundColor White
Write-Host "  Sin Docker: ./scripts/start_all.ps1" -ForegroundColor White
Write-Host ""
Write-Host "Para verificar procesos residuales:" -ForegroundColor Cyan
Write-Host "  Get-Process python* | Where-Object { `$_.CommandLine -like '*backend*' }" -ForegroundColor White
Write-Host "  docker-compose ps" -ForegroundColor White