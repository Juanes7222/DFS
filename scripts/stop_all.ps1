#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Script para detener el sistema DFS completo

.DESCRIPTION
    Detiene todos los servicios DFS (Metadata Service y DataNodes).
    Puede detener jobs de PowerShell, procesos nativos y contenedores Docker.

.PARAMETER Force
    Detiene todos los procesos sin pedir confirmación

.PARAMETER CleanData
    Limpia todos los datos almacenados además de detener los servicios

.PARAMETER KeepLogs
    Mantiene los archivos de log después de detener

.PARAMETER CleanAllJobs
    Limpia TODOS los jobs de PowerShell, no solo los de DFS

.EXAMPLE
    .\stop-dfs.ps1
    .\stop-dfs.ps1 -Force
    .\stop-dfs.ps1 -Force -CleanData
    .\stop-dfs.ps1 -KeepLogs
    .\stop-dfs.ps1 -CleanAllJobs
#>

param(
    [switch]$Force,
    [switch]$CleanData,
    [switch]$KeepLogs,
    [switch]$CleanAllJobs
)

# Configurar UTF-8 para la terminal
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8
if ($IsWindows -or $env:OS -match "Windows") {
    chcp 65001 | Out-Null
}

$ErrorActionPreference = "Continue"

# Guardar el script path al inicio antes de entrar en funciones
$SCRIPT_PATH = $PSCommandPath
if (-not $SCRIPT_PATH) {
    $SCRIPT_PATH = $MyInvocation.MyCommand.Path
}

# Colores para output
function Write-Section { param([string]$Message) Write-Host "`n=== $Message ===" -ForegroundColor Cyan }
function Write-Success { param([string]$Message) Write-Host "✓ $Message" -ForegroundColor Green }
function Write-Info { param([string]$Message) Write-Host "  $Message" -ForegroundColor Gray }
function Write-Warning { param([string]$Message) Write-Host "⚠ $Message" -ForegroundColor Yellow }
function Write-Failure { param([string]$Message) Write-Host "✗ $Message" -ForegroundColor Red }

Write-Section "Deteniendo Sistema DFS"

$tempDir = if ($IsWindows -or $env:OS -match "Windows") { $env:TEMP } else { "/tmp" }
$pidsFile = Join-Path $tempDir "dfs-system-pids.txt"
$processesStopped = 0

function Stop-PowerShellJobs {
    <#
    .SYNOPSIS
        Detiene todos los jobs de PowerShell relacionados con DFS
    #>
    param([bool]$CleanAll)
    
    Write-Section "Deteniendo PowerShell Jobs"
    
    $allJobs = Get-Job
    
    if (-not $allJobs) {
        Write-Info "No hay jobs de PowerShell activos"
        return 0
    }
    
    # Filtrar solo jobs Running o Failed (ignorar Completed)
    $activeJobs = $allJobs | Where-Object { 
        $_.State -eq "Running" -or $_.State -eq "Failed" -or $_.State -eq "Blocked"
    }
    
    # Si CleanAll, incluir también los Completed
    if ($CleanAll) {
        $jobsToProcess = $allJobs
        Write-Info "Limpiando TODOS los jobs ($($allJobs.Count) total)"
    } else {
        $jobsToProcess = $activeJobs
        $completedCount = ($allJobs | Where-Object { $_.State -eq "Completed" }).Count
        
        if ($completedCount -gt 0) {
            Write-Info "Encontrados $completedCount job(s) completado(s) (serán limpiados)"
            Write-Info "Usa -CleanAllJobs para limpiar todos los jobs"
        }
    }
    
    if (-not $jobsToProcess -or $jobsToProcess.Count -eq 0) {
        Write-Info "No hay jobs activos para detener"
        
        # Limpiar jobs completados silenciosamente
        $allJobs | Where-Object { $_.State -eq "Completed" } | Remove-Job -Force -ErrorAction SilentlyContinue
        
        return 0
    }
    
    Write-Info "Procesando $($jobsToProcess.Count) job(s)"
    
    $stopped = 0
    foreach ($job in $jobsToProcess) {
        $stateInfo = if ($job.State -eq "Running") { 
            "Estado: $($job.State), deteniendo..." 
        } else { 
            "Estado: $($job.State)" 
        }
        
        Write-Info "$($job.Name) (ID: $($job.Id)) - $stateInfo"
        
        if ($job.State -eq "Running") {
            Stop-Job -Job $job -ErrorAction SilentlyContinue
            $stopped++
        }
        
        Remove-Job -Job $job -Force -ErrorAction SilentlyContinue
    }
    
    # Limpiar todos los jobs completados restantes
    Get-Job | Where-Object { $_.State -eq "Completed" } | Remove-Job -Force -ErrorAction SilentlyContinue
    
    Write-Success "Jobs procesados: $stopped detenido(s), $($jobsToProcess.Count - $stopped) limpiado(s)"
    return $stopped
}

function Stop-DockerServices {
    <#
    .SYNOPSIS
        Detiene contenedores Docker si están disponibles
    #>
    
    if (-not (Get-Command docker-compose -ErrorAction SilentlyContinue)) {
        return 0
    }
    
    Write-Section "Verificando servicios Docker"
    
    # Usar la ruta del script guardada
    if (-not $SCRIPT_PATH) {
        Write-Info "No se pudo determinar la ruta del script, omitiendo Docker..."
        return 0
    }
    
    try {
        $scriptDir = Split-Path -Parent $SCRIPT_PATH
        $projectRoot = Split-Path -Parent $scriptDir
        $composeFile = Join-Path $projectRoot "docker-compose.yml"
        
        if (-not (Test-Path $composeFile)) {
            Write-Info "docker-compose.yml no encontrado, omitiendo..."
            return 0
        }
        
        Push-Location $projectRoot
        
        try {
            $services = docker-compose ps --services 2>$null
            
            if ($services -and ($services | Where-Object { $_ -match '\S' })) {
                Write-Info "Deteniendo contenedores Docker..."
                docker-compose down 2>&1 | Out-Null
                
                if ($LASTEXITCODE -eq 0) {
                    Write-Success "Servicios Docker detenidos"
                    return 1
                } else {
                    Write-Warning "Algunos servicios Docker no se pudieron detener"
                    return 0
                }
            } else {
                Write-Info "No hay servicios Docker en ejecución"
                return 0
            }
        } finally {
            Pop-Location
        }
    } catch {
        Write-Warning "Error verificando Docker: $_"
        return 0
    }
}

function Stop-NativeProcesses {
    <#
    .SYNOPSIS
        Detiene procesos nativos usando archivo de PIDs
    #>
    
    if (-not (Test-Path $pidsFile)) {
        Write-Info "Archivo de PIDs no encontrado: $pidsFile"
        return 0
    }
    
    Write-Section "Deteniendo procesos desde archivo de PIDs"
    
    $stopped = 0
    $pidLines = Get-Content $pidsFile -ErrorAction SilentlyContinue
    
    if (-not $pidLines) {
        Write-Info "Archivo de PIDs vacío"
        Remove-Item $pidsFile -Force -ErrorAction SilentlyContinue
        return 0
    }
    
    $pids = @{}
    
    foreach ($line in $pidLines) {
        if ($line -match '^(\w+)=(\d+)$') {
            $service = $matches[1]
            $processId = [int]$matches[2]
            $pids[$service] = $processId
        }
    }
    
    if ($pids.Count -eq 0) {
        Write-Info "No hay PIDs válidos en el archivo"
        Remove-Item $pidsFile -Force -ErrorAction SilentlyContinue
        return 0
    }
    
    # Orden de detención: DataNodes primero, Metadata último
    $stopOrder = @()
    
    # Agregar DataNodes en orden inverso
    $dataNodeKeys = $pids.Keys | Where-Object { $_ -like "DATANODE*" } | Sort-Object -Descending
    $stopOrder += $dataNodeKeys
    
    # Agregar Metadata al final
    if ($pids.ContainsKey("METADATA")) {
        $stopOrder += "METADATA"
    }
    
    foreach ($service in $stopOrder) {
        $processId = $pids[$service]
        
        try {
            $process = Get-Process -Id $processId -ErrorAction Stop
            Write-Info "Deteniendo $service (PID: $processId)..."
            Stop-Process -Id $processId -Force -ErrorAction Stop
            Write-Success "$service detenido"
            $stopped++
        } catch {
            Write-Info "$service (PID: $processId) no encontrado o ya detenido"
        }
    }
    
    Remove-Item $pidsFile -Force -ErrorAction SilentlyContinue
    Write-Success "Archivo de PIDs removido"
    
    return $stopped
}

function Find-ResidualProcesses {
    <#
    .SYNOPSIS
        Busca procesos residuales de Python relacionados con DFS
    #>
    
    Write-Section "Buscando procesos residuales"
    
    $patterns = @(
        "*backend.metadata.server*",
        "*backend.datanode.server*",
        "*datanode.server*",
        "*metadata.server*",
        "*uvicorn*backend*"
    )
    
    $residualProcesses = Get-Process python*, py* -ErrorAction SilentlyContinue | Where-Object {
        $cmdLine = $_.CommandLine
        if (-not $cmdLine) { return $false }
        
        foreach ($pattern in $patterns) {
            if ($cmdLine -like $pattern) { return $true }
        }
        return $false
    }
    
    if (-not $residualProcesses) {
        Write-Success "No se encontraron procesos residuales"
        return 0
    }
    
    Write-Warning "Procesos residuales encontrados:"
    foreach ($proc in $residualProcesses) {
        $cmdLine = if ($proc.CommandLine) { 
            $proc.CommandLine.Substring(0, [Math]::Min(100, $proc.CommandLine.Length))
        } else { 
            "N/A" 
        }
        Write-Info "PID: $($proc.Id) - $($proc.ProcessName)"
        Write-Host "    $cmdLine" -ForegroundColor DarkGray
    }
    
    return $residualProcesses
}

function Stop-ResidualProcesses {
    <#
    .SYNOPSIS
        Detiene procesos residuales con confirmación del usuario
    #>
    param([array]$Processes)
    
    if (-not $Processes -or $Processes.Count -eq 0) {
        return 0
    }
    
    $shouldStop = $Force
    
    if (-not $Force) {
        Write-Host ""
        $response = Read-Host "¿Desea detener estos $($Processes.Count) proceso(s)? (S/N)"
        $shouldStop = $response -match '^[SsYy]$'
    }
    
    if (-not $shouldStop) {
        Write-Info "Procesos no detenidos por el usuario"
        return 0
    }
    
    $stopped = 0
    foreach ($proc in $Processes) {
        try {
            Write-Info "Deteniendo proceso $($proc.Id)..."
            Stop-Process -Id $proc.Id -Force -ErrorAction Stop
            Write-Success "Proceso $($proc.Id) detenido"
            $stopped++
        } catch {
            Write-Failure "No se pudo detener proceso $($proc.Id): $_"
        }
    }
    
    return $stopped
}

function Clear-EnvironmentVariables {
    <#
    .SYNOPSIS
        Limpia variables de entorno relacionadas con DFS
    #>
    
    Write-Section "Limpiando variables de entorno"
    
    $envVars = @(
        "DFS_METADATA_HOST",
        "DFS_METADATA_PORT",
        "DFS_DATANODE_HOST",
        "DFS_DATANODE_PORT",
        "DFS_STORAGE_PATH",
        "DFS_DB_PATH",
        "NODE_ID",
        "CORS_ALLOW_ALL",
        "CORS_ORIGINS"
    )
    
    $cleaned = 0
    foreach ($envVar in $envVars) {
        if (Test-Path "env:$envVar") {
            Remove-Item "env:$envVar" -ErrorAction SilentlyContinue
            $cleaned++
        }
    }
    
    if ($cleaned -gt 0) {
        Write-Success "$cleaned variable(s) de entorno limpiada(s)"
    } else {
        Write-Info "No hay variables de entorno para limpiar"
    }
}

function Clear-TemporaryFiles {
    <#
    .SYNOPSIS
        Limpia archivos temporales y logs
    #>
    param([string]$TempDir, [bool]$IncludeLogs, [bool]$IncludeData)
    
    Write-Section "Limpiando archivos temporales"
    
    $patterns = @()
    
    if ($IncludeLogs) {
        $patterns += "$TempDir/dfs-*.log"
        $patterns += "$TempDir/dfs-*-errors.log"
    }
    
    if ($IncludeData) {
        $patterns += "$TempDir/dfs-data-*"
        $patterns += "$TempDir/dfs-metadata"
    }
    
    if ($patterns.Count -eq 0) {
        Write-Info "No hay archivos para limpiar"
        return
    }
    
    $cleaned = 0
    foreach ($pattern in $patterns) {
        Get-ChildItem -Path $pattern -ErrorAction SilentlyContinue | ForEach-Object {
            try {
                $itemType = if ($_.PSIsContainer) { "directorio" } else { "archivo" }
                Remove-Item $_.FullName -Recurse -Force -ErrorAction Stop
                Write-Info "Limpiado $itemType : $($_.Name)"
                $cleaned++
            } catch {
                Write-Warning "No se pudo limpiar: $($_.Name)"
            }
        }
    }
    
    if ($cleaned -gt 0) {
        Write-Success "$cleaned elemento(s) limpiado(s)"
    } else {
        Write-Info "No se encontraron archivos para limpiar"
    }
}

function Show-CleanupSummary {
    <#
    .SYNOPSIS
        Muestra un resumen de la limpieza
    #>
    param([int]$TotalStopped)
    
    Write-Section "Resumen"
    
    if ($TotalStopped -gt 0) {
        Write-Success "Sistema DFS detenido completamente"
        Write-Info "$TotalStopped proceso(s)/servicio(s) terminado(s)"
    } else {
        Write-Info "No se encontraron procesos de DFS activos"
    }
    
    Write-Host "`nPara reiniciar el sistema:" -ForegroundColor Cyan
    Write-Host "  .\scripts\start-dfs.ps1" -ForegroundColor White
    
    Write-Host "`nPara verificar procesos residuales:" -ForegroundColor Cyan
    Write-Host "  Get-Process python* | Where-Object { `$_.CommandLine -like '*backend*' }" -ForegroundColor White
    Write-Host "  Get-Job" -ForegroundColor White
    
    if (Get-Command docker-compose -ErrorAction SilentlyContinue) {
        Write-Host "  docker-compose ps" -ForegroundColor White
    }
    
    Write-Host ""
}

# Ejecutar secuencia de detención
$totalStopped = 0

# 1. Detener PowerShell Jobs
$totalStopped += Stop-PowerShellJobs -CleanAll $CleanAllJobs

# 2. Detener Docker si está disponible
$totalStopped += Stop-DockerServices

# 3. Detener procesos nativos desde archivo de PIDs
$totalStopped += Stop-NativeProcesses

# 4. Buscar y detener procesos residuales
$residualProcesses = Find-ResidualProcesses
if ($residualProcesses) {
    $totalStopped += Stop-ResidualProcesses -Processes $residualProcesses
}

# 5. Limpiar variables de entorno
Clear-EnvironmentVariables

# 6. Limpiar archivos temporales
$includeLogs = -not $KeepLogs
$includeData = $CleanData
Clear-TemporaryFiles -TempDir $tempDir -IncludeLogs $includeLogs -IncludeData $includeData

# 7. Mostrar resumen
Show-CleanupSummary -TotalStopped $totalStopped
