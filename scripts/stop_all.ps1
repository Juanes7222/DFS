#!/usr/bin/env pwsh
# Script para detener todo el sistema DFS

$ErrorActionPreference = "Continue"

Write-Host "=== Deteniendo Sistema DFS ===" -ForegroundColor Cyan

$tempDir = if ($IsWindows -or $env:OS -match "Windows") { $env:TEMP } else { "/tmp" }
$jobsFile = Join-Path $tempDir "dfs-system-jobs.txt"

$processesStopped = 0

# Primero intenta con Docker Compose si está disponible (No creo que se incluya, pero lo pongo por si las moscas)
if (Get-Command docker-compose -ErrorAction SilentlyContinue) {
    $scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
    $projectRoot = Split-Path -Parent $scriptDir
    $composeFile = Join-Path $projectRoot "docker-compose.yml"
    
    if (Test-Path $composeFile) {
        Write-Host "Deteniendo servicios Docker..." -ForegroundColor Yellow
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
                Write-Host "No se pudieron detener algunos servicios Docker" -ForegroundColor Yellow
            }
        } else {
            Write-Host "No hay servicios Docker en ejecucion" -ForegroundColor Gray
        }
    }
}

# Detiene los Jobs de PowerShell
if (Test-Path $jobsFile) {
    Write-Host ""
    Write-Host "Leyendo Jobs de $jobsFile..." -ForegroundColor Yellow
    
    $jobLines = Get-Content $jobsFile
    $jobsToStop = @()
    
    foreach ($line in $jobLines) {
        if ($line -match '^(\w+)=(.+)$') {
            $jobType = $matches[1]
            $jobIds = $matches[2] -split ','
            
            foreach ($jobId in $jobIds) {
                if ($jobId -match '^\d+$') {
                    $jobsToStop += [PSCustomObject]@{
                        Type = $jobType
                        Id = [int]$jobId
                    }
                }
            }
        }
    }
    
    # Detiene los jobs en orden inverso (DataNodes primero, Metadata último)
    $stopOrder = @("DATANODE_JOBS", "METADATA_JOB")
    
    foreach ($jobType in $stopOrder) {
        $jobs = $jobsToStop | Where-Object { $_.Type -eq $jobType }
        foreach ($job in $jobs) {
            try {
                $jobInfo = Get-Job -Id $job.Id -ErrorAction Stop
                Write-Host "Deteniendo $($job.Type) (Job ID: $($job.Id))..." -ForegroundColor Yellow
                Stop-Job -Id $job.Id -ErrorAction Stop
                Remove-Job -Id $job.Id -Force -ErrorAction Stop
                Write-Host "$($job.Type) detenido" -ForegroundColor Green
                $processesStopped++
            }
            catch {
                Write-Host "$($job.Type) (Job ID: $($job.Id)) no encontrado o ya detenido" -ForegroundColor Gray
            }
        }
    }
    
    Remove-Item $jobsFile -Force
    Write-Host "Archivo de Jobs removido" -ForegroundColor Green
}
else {
    Write-Host "Archivo de Jobs no encontrado en: $jobsFile" -ForegroundColor Gray
    Write-Host "Buscando Jobs activos de DFS..." -ForegroundColor Yellow
    
    # Buscar jobs activos que puedan ser de DFS
    $dfsJobs = Get-Job | Where-Object { 
        $_.Command -like "*metadata*" -or 
        $_.Command -like "*datanode*" -or
        $_.Name -like "*dfs*" -or
        ($_.ChildJobs -and $_.ChildJobs.Command -like "*python*backend*")
    }
    
    if ($dfsJobs) {
        Write-Host "Jobs de DFS encontrados:" -ForegroundColor Cyan
        foreach ($job in $dfsJobs) {
            Write-Host "  Job ID: $($job.Id) - Estado: $($job.State) - Comando: $($job.Command)" -ForegroundColor White
        }
        
        $response = Read-Host "¿Desea detener estos jobs? (S/N)"
        if ($response -eq "S" -or $response -eq "s" -or $response -eq "Y" -or $response -eq "y") {
            foreach ($job in $dfsJobs) {
                Write-Host "Deteniendo Job $($job.Id)..." -ForegroundColor Yellow
                try {
                    Stop-Job -Id $job.Id -ErrorAction Stop
                    Remove-Job -Id $job.Id -Force -ErrorAction Stop
                    Write-Host "Job $($job.Id) detenido" -ForegroundColor Green
                    $processesStopped++
                }
                catch {
                    Write-Host "No se pudo detener Job $($job.Id)" -ForegroundColor Red
                }
            }
        }
    } else {
        Write-Host "No se encontraron Jobs activos de DFS" -ForegroundColor Green
    }
}

# Busca los procesos residuales del backend (como respaldo)
Write-Host ""
Write-Host "Buscando procesos residuales de backend..." -ForegroundColor Yellow

$backendProcesses = Get-Process python* -ErrorAction SilentlyContinue | Where-Object {
    $_.ProcessName -eq "python" -or 
    $_.ProcessName -eq "python3" -or
    $_.ProcessName -eq "pythonw"
} | Where-Object {
    $_.CommandLine -like "*metadata*server*" -or 
    $_.CommandLine -like "*datanode*server*" -or
    $_.CommandLine -like "*uvicorn*" -or
    $_.CommandLine -like "*backend*" -or
    $_.CommandLine -like "*dfs*"
}

if ($backendProcesses) {
    Write-Host "Procesos backend residuales encontrados:" -ForegroundColor Cyan
    foreach ($proc in $backendProcesses) {
        $cmdLine = if ($proc.CommandLine) { 
            if ($proc.CommandLine.Length -gt 100) { 
                $proc.CommandLine.Substring(0, 100) + "..." 
            } else { 
                $proc.CommandLine 
            }
        } else { "N/A" }
        Write-Host "  PID: $($proc.Id) - $($proc.ProcessName)" -ForegroundColor White
        Write-Host "    Comando: $cmdLine" -ForegroundColor Gray
    }
    
    $response = Read-Host "¿Desea detener estos procesos? (S/N)"
    if ($response -eq "S" -or $response -eq "s" -or $response -eq "Y" -or $response -eq "y") {
        foreach ($proc in $backendProcesses) {
            Write-Host "Deteniendo proceso $($proc.Id)..." -ForegroundColor Yellow
            try {
                Stop-Process -Id $proc.Id -Force -ErrorAction Stop
                Write-Host "Proceso $($proc.Id) detenido" -ForegroundColor Green
                $processesStopped++
            }
            catch {
                Write-Host "No se pudo detener proceso $($proc.Id)" -ForegroundColor Red
            }
        }
    } else {
        Write-Host "Procesos no detenidos por el usuario" -ForegroundColor Yellow
    }
} else {
    Write-Host "No se encontraron procesos backend residuales" -ForegroundColor Green
}

# Limpia las variables de entorno
Write-Host ""
Write-Host "Limpiando variables de entorno..." -ForegroundColor Yellow

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

# Limpia los archivos temporales de DFS
Write-Host ""
Write-Host "Limpiando archivos temporales..." -ForegroundColor Yellow

$tempFiles = @(
    "$tempDir/dfs-*.log",
    "$tempDir/dfs-*-errors.log"
)

$tempDirs = @(
    "$tempDir/dfs-data-node1",
    "$tempDir/dfs-data-node2", 
    "$tempDir/dfs-data-node3",
    "$tempDir/dfs-metadata"
)

$cleanedFiles = 0
$cleanedDirs = 0

# Limpia los archivos (Limpia el sistema, es especialmente útil si algo sale mal)
foreach ($pattern in $tempFiles) {
    Get-ChildItem -Path $pattern -ErrorAction SilentlyContinue | ForEach-Object {
        try {
            Remove-Item $_.FullName -Force -ErrorAction Stop
            Write-Host "  Limpiado archivo: $($_.Name)" -ForegroundColor Gray
            $cleanedFiles++
        } catch {
            Write-Host "  No se pudo limpiar archivo: $($_.Name)" -ForegroundColor DarkYellow
        }
    }
}

# Limpia los directorios
foreach ($dir in $tempDirs) {
    if (Test-Path $dir) {
        try {
            Remove-Item $dir -Recurse -Force -ErrorAction Stop
            Write-Host "  Limpiado directorio: $(Split-Path $dir -Leaf)" -ForegroundColor Gray
            $cleanedDirs++
        } catch {
            Write-Host "  No se pudo limpiar directorio: $(Split-Path $dir -Leaf)" -ForegroundColor DarkYellow
        }
    }
}

Write-Host "$cleanedFiles archivos y $cleanedDirs directorios temporales limpiados" -ForegroundColor Green

Write-Host ""
if ($processesStopped -gt 0) {
    Write-Host "Sistema DFS detenido completamente. $processesStopped procesos/jobs terminados." -ForegroundColor Green
} else {
    Write-Host "No se encontraron procesos de DFS en ejecucion." -ForegroundColor Cyan
}

Write-Host ""
Write-Host "Para reiniciar el sistema:" -ForegroundColor Cyan
Write-Host "  Con Docker: ./scripts/start.ps1" -ForegroundColor White
Write-Host "  Sin Docker: ./scripts/start_all.ps1" -ForegroundColor White
Write-Host ""
Write-Host "Para verificar procesos residuales:" -ForegroundColor Cyan
Write-Host "  Get-Job" -ForegroundColor White
Write-Host "  Get-Process python* | Where-Object { `$_.CommandLine -like '*backend*' }" -ForegroundColor White
Write-Host "  docker-compose ps" -ForegroundColor White