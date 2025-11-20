#!/usr/bin/env pwsh
# Script para iniciar todo el sistema DFS - Version ASCII

$ErrorActionPreference = "Continue"

Write-Host "=== Iniciando Sistema DFS ===" -ForegroundColor Cyan

# Obtener el directorio donde esta el script
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Split-Path -Parent $scriptDir
$backendRoot = Join-Path $projectRoot "backend"

# Verificar estructura del proyecto
if (-not (Test-Path $backendRoot)) {
    Write-Host "ERROR: No se encuentra el directorio backend en: $backendRoot" -ForegroundColor Red
    Write-Host "Estructura esperada:" -ForegroundColor Yellow
    Write-Host "  DFS/" -ForegroundColor White
    Write-Host "  +-- backend/" -ForegroundColor White
    Write-Host "      +-- client/" -ForegroundColor White
    Write-Host "      +-- core/" -ForegroundColor White
    Write-Host "      +-- datanode/" -ForegroundColor White
    Write-Host "      +-- shared/" -ForegroundColor White
    Write-Host "  +-- scripts/" -ForegroundColor White
    exit 1
}

# Funcion para encontrar Python
function Get-PythonCommand {
    $pythonCommands = @("python", "py", "python3")
    
    foreach ($cmd in $pythonCommands) {
        try {
            $pythonPath = Get-Command $cmd -ErrorAction Stop | Select-Object -ExpandProperty Source
            $versionOutput = & $pythonPath --version 2>&1
            if ($LASTEXITCODE -eq 0 -and $versionOutput -match "Python") {
                Write-Host "OK Encontrado: $pythonPath" -ForegroundColor Green
                return $pythonPath
            }
        } catch {
            continue
        }
    }
    
    if ($IsWindows -or $env:OS -match "Windows") {
        $commonPaths = @(
            "$env:LOCALAPPDATA\Programs\Python\Python311\python.exe",
            "$env:LOCALAPPDATA\Programs\Python\Python310\python.exe",
            "$env:LOCALAPPDATA\Programs\Python\Python39\python.exe",
            "$env:ProgramFiles\Python311\python.exe",
            "$env:ProgramFiles\Python310\python.exe",
            "$env:ProgramFiles\Python39\python.exe"
        )
        
        foreach ($path in $commonPaths) {
            if (Test-Path $path) {
                Write-Host "OK Encontrado en ruta comun: $path" -ForegroundColor Green
                return $path
            }
        }
    }
    
    return $null
}

Write-Host "Buscando Python..." -ForegroundColor Yellow
$pythonCmd = Get-PythonCommand

if (-not $pythonCmd) {
    Write-Host "ERROR: No se pudo encontrar Python instalado" -ForegroundColor Red
    Write-Host "Por favor instale Python 3.8+ desde https://python.org" -ForegroundColor Yellow
    Write-Host "O asegurese de que este en el PATH del sistema" -ForegroundColor Yellow
    exit 1
}

Write-Host "Usando Python: $pythonCmd" -ForegroundColor Cyan

# Verificar version Python
try {
    $versionInfo = & $pythonCmd -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"
    Write-Host "Version de Python: $versionInfo" -ForegroundColor Green
    
    $majorVersion = [int]($versionInfo.Split('.')[0])
    $minorVersion = [int]($versionInfo.Split('.')[1])
    
    if ($majorVersion -lt 3 -or ($majorVersion -eq 3 -and $minorVersion -lt 8)) {
        Write-Host "ERROR: Se requiere Python 3.8 o superior" -ForegroundColor Red
        exit 1
    }
    
    if ($majorVersion -eq 3 -and $minorVersion -gt 11) {
        Write-Host "ADVERTENCIA: Python $versionInfo puede tener compatibilidad limitada" -ForegroundColor Yellow
        Write-Host "Se recomienda Python 3.8 a 3.11" -ForegroundColor Yellow
    }
} catch {
    Write-Host "ERROR: No se pudo verificar la version de Python" -ForegroundColor Red
    Write-Host "Error: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

# Crear directorios de datos
$tempDir = if ($IsWindows -or $env:OS -match "Windows") { $env:TEMP } else { "/tmp" }
$dataDirs = @(
    "$tempDir/dfs-data-node1",
    "$tempDir/dfs-data-node2", 
    "$tempDir/dfs-data-node3",
    "$tempDir/dfs-metadata"
)

Write-Host "Creando directorios de datos..." -ForegroundColor Yellow
foreach ($dir in $dataDirs) {
    if (-not (Test-Path $dir)) {
        Write-Host "  Creando: $dir" -ForegroundColor Gray
        New-Item -ItemType Directory -Path $dir -Force | Out-Null
    } else {
        Write-Host "  Ya existe: $dir" -ForegroundColor Gray
    }
}

# Agregar backend al PYTHONPATH
$env:PYTHONPATH = "$backendRoot$([System.IO.Path]::PathSeparator)$env:PYTHONPATH"

# Verificar import backend
try {
    Write-Host "Verificando paquete backend..." -ForegroundColor Yellow
    & $pythonCmd -c "import backend; print('Paquete backend encontrado')"
} catch {
    Write-Host "ERROR: No se puede importar el paquete backend" -ForegroundColor Red
    Write-Host "Detalle: $($_.Exception.Message)" -ForegroundColor Red
    Write-Host "PYTHONPATH: $env:PYTHONPATH" -ForegroundColor Yellow
    Write-Host "Backend root: $backendRoot" -ForegroundColor Yellow
    
    Write-Host "Verificando modulos..." -ForegroundColor Yellow
    $expectedModules = @("metadata", "datanode", "client", "core", "shared")
    foreach ($module in $expectedModules) {
        $modulePath = Join-Path $backendRoot $module
        if (Test-Path $modulePath) {
            Write-Host "  OK $module" -ForegroundColor Green
        } else {
            Write-Host "  FALTA $module" -ForegroundColor Red
        }
    }
    exit 1
}

# Variables Metadata
$env:DFS_METADATA_HOST = "localhost"
$env:DFS_METADATA_PORT = "8000"
$env:DFS_DB_PATH = "$tempDir/dfs-metadata/dfs_metadata.db"

Write-Host "Configuracion completada. Iniciando servicios..." -ForegroundColor Cyan

# Metadata Service
Write-Host "Iniciando Metadata Service en puerto 8000..." -ForegroundColor Yellow
$metadataLog = Join-Path $tempDir "dfs-metadata.log"
$metadataErrLog = Join-Path $tempDir "dfs-metadata-errors.log"

try {
    Write-Host "  Ejecutando: $pythonCmd -m backend.metadata.server" -ForegroundColor Gray
    $metadataProcess = Start-Process -FilePath $pythonCmd `
        -ArgumentList "-m", "backend.metadata.server" `
        -WorkingDirectory $backendRoot `
        -RedirectStandardOutput $metadataLog `
        -RedirectStandardError $metadataErrLog `
        -PassThru -NoNewWindow

    $METADATA_PID = $metadataProcess.Id
    Write-Host "  Metadata Service PID: $METADATA_PID" -ForegroundColor Green
} catch {
    Write-Host "ERROR: No se pudo iniciar Metadata Service: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

# Esperar Metadata
Write-Host "Esperando a que Metadata Service este listo..." -ForegroundColor Yellow
$maxRetries = 12
$retryCount = 0
$metadataReady = $false

while ($retryCount -lt $maxRetries -and -not $metadataReady) {
    try {
        $healthResponse = Invoke-RestMethod -Uri "http://localhost:8000/api/v1/health" -TimeoutSec 5
        Write-Host "Metadata Service responde: $($healthResponse.status)" -ForegroundColor Green
        $metadataReady = $true
    } catch {
        $retryCount++
        if ($retryCount -eq $maxRetries) {
            Write-Host "ERROR: Metadata Service no responde despues de $maxRetries intentos" -ForegroundColor Red
            Write-Host "Revisar log: $metadataErrLog" -ForegroundColor Yellow
            if (Test-Path $metadataErrLog) {
                Write-Host "Ultimas lineas del log:" -ForegroundColor Yellow
                Get-Content $metadataErrLog -Tail 15 | ForEach-Object { Write-Host "  $_" -ForegroundColor Gray }
            }
            exit 1
        } else {
            Write-Host "  Intento $retryCount/$maxRetries..." -ForegroundColor Gray
            Start-Sleep -Seconds 2
        }
    }
}

Write-Host "Metadata Service listo. Continuando con DataNodes..." -ForegroundColor Green

# DataNode 1
Write-Host "Iniciando DataNode 1 en puerto 8001..." -ForegroundColor Yellow
$datanode1Log = Join-Path $tempDir "dfs-datanode1.log"
$datanode1ErrLog = Join-Path $tempDir "dfs-datanode1-errors.log"

$env:DFS_DATANODE_HOST = "localhost"
$env:DFS_DATANODE_PORT = "8001"
$env:DFS_STORAGE_PATH = "$tempDir/dfs-data-node1"
$env:NODE_ID = "node-localhost-8001"

try {
    Write-Host "  Ejecutando DataNode..." -ForegroundColor Gray
    $datanode1Process = Start-Process -FilePath $pythonCmd `
        -ArgumentList "-m", "backend.datanode.server" `
        -WorkingDirectory $backendRoot `
        -RedirectStandardOutput $datanode1Log `
        -RedirectStandardError $datanode1ErrLog `
        -PassThru -NoNewWindow

    $DN1_PID = $datanode1Process.Id
    Write-Host "  DataNode 1 PID: $DN1_PID" -ForegroundColor Green
} catch {
    Write-Host "ERROR: No se pudo iniciar DataNode 1: $($_.Exception.Message)" -ForegroundColor Red
}

Write-Host "=== Script de inicio completado ===" -ForegroundColor Cyan
