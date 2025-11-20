#!/usr/bin/env pwsh
param(
    [int]$nodes = 3
)

# Script para iniciar todo el sistema DFS - Version parametrizable
$ErrorActionPreference = "Continue"

Write-Host "=== Iniciando Sistema DFS ==="

# Obtener el directorio donde esta el script
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Split-Path -Parent $scriptDir
$backendRoot = Join-Path $projectRoot "backend"

if (-not (Test-Path $backendRoot)) {
    Write-Host "ERROR: No se encuentra el directorio backend en: $backendRoot" -ForegroundColor Red
    exit 1
}

function Get-PythonCommand {
    # Primero buscar en el venv del proyecto
    $venvPython = Join-Path $backendRoot ".venv\Scripts\python.exe"
    if (Test-Path $venvPython) {
        try {
            $versionOutput = & $venvPython --version 2>&1
            if ($LASTEXITCODE -eq 0 -and $versionOutput -match "Python") {
                Write-Host "OK Encontrado en venv: $venvPython" -ForegroundColor Green
                return $venvPython
            }
        } catch { }
    }

    $pythonCommands = @("python", "py", "python3")
    foreach ($cmd in $pythonCommands) {
        try {
            $pythonPath = Get-Command $cmd -ErrorAction Stop | Select-Object -ExpandProperty Source
            $versionOutput = & $pythonPath --version 2>&1
            if ($LASTEXITCODE -eq 0 -and $versionOutput -match "Python") {
                Write-Host "OK Encontrado: $pythonPath" -ForegroundColor Green
                return $pythonPath
            }
        } catch { continue }
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

function Test-Port {
    param([string]$HostName, [int]$Port, [int]$TimeoutMs = 1000)
    try {
        $tcpClient = New-Object System.Net.Sockets.TcpClient
        $connect = $tcpClient.BeginConnect($HostName, $Port, $null, $null)
        $wait = $connect.AsyncWaitHandle.WaitOne($TimeoutMs, $false)
        if ($wait) { $tcpClient.EndConnect($connect); $tcpClient.Close(); return $true }
        $tcpClient.Close(); return $false
    } catch { return $false }
}

Write-Host "Buscando Python..." -ForegroundColor Yellow
$pythonCmd = Get-PythonCommand
if (-not $pythonCmd) {
    Write-Host "ERROR: No se pudo encontrar Python instalado" -ForegroundColor Red
    exit 1
}
Write-Host "Usando Python: $pythonCmd" -ForegroundColor Cyan

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
    }
} catch {
    Write-Host "ERROR: No se pudo verificar la version de Python" -ForegroundColor Red
    exit 1
}

# Temp dir y directorios base
$tempDir = if ($IsWindows -or $env:OS -match "Windows") { $env:TEMP } else { "/tmp" }
$metadataDir = Join-Path $tempDir "dfs-metadata"
if (-not (Test-Path $metadataDir)) { New-Item -Path $metadataDir -ItemType Directory -Force | Out-Null }

# Crear directorios por defecto para nodos (se crearán también por nodo en el loop)
Write-Host "Creando directorios de datos..." -ForegroundColor Yellow
for ($i = 1; $i -le $nodes; $i++) {
    $d = Join-Path $tempDir ("dfs-data-node{0}" -f $i)
    if (-not (Test-Path $d)) { New-Item -ItemType Directory -Path $d -Force | Out-Null; Write-Host "  Creando: $d" } else { Write-Host "  Ya existe: $d" }
}
Write-Host "  Ya existe/creado: $metadataDir"

# PYTHONPATH
$env:PYTHONPATH = "$backendRoot$([System.IO.Path]::PathSeparator)$env:PYTHONPATH"

# Verificar import backend
try {
    Write-Host "Verificando paquete backend..." -ForegroundColor Yellow
    & $pythonCmd -c "import backend; print('Paquete backend encontrado')"
} catch {
    Write-Host "ERROR: No se puede importar el paquete backend" -ForegroundColor Red
    exit 1
}

# Variables Metadata (configuración fija)
$env:DFS_METADATA_HOST = "0.0.0.0"
$env:DFS_METADATA_PORT = "8000"
$env:DFS_DB_PATH = Join-Path $metadataDir "dfs_metadata.db"

Write-Host "Configuracion completada. Iniciando servicios..." -ForegroundColor Cyan

# Iniciar Metadata Service (igual que antes)
$metadataLog = Join-Path $tempDir "dfs-metadata.log"
$metadataErrLog = Join-Path $tempDir "dfs-metadata-errors.log"

try {
    Write-Host "Iniciando Metadata Service en puerto 8000..."
    $metadataJob = Start-Job -ScriptBlock {
        param($pythonPath, $serverScript, $workDir, $logFile, $errFile)
        Set-Location $workDir
        & $pythonPath $serverScript *> $logFile 2> $errFile
    } -ArgumentList $pythonCmd, (Join-Path $backendRoot "metadata\server.py"), $backendRoot, $metadataLog, $metadataErrLog

    Write-Host "  Metadata Service Job ID: $($metadataJob.Id)" -ForegroundColor Green
    $global:METADATA_JOB = $metadataJob
} catch {
    Write-Host "ERROR: No se pudo iniciar Metadata Service: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

# Esperar puerto metadata
Write-Host "Esperando a que el puerto 8000 este disponible..."
$portReady = $false; $portRetries = 0; $maxPortRetries = 30
while (-not $portReady -and $portRetries -lt $maxPortRetries) {
    if (Test-Port -HostName "127.0.0.1" -Port 8000 -TimeoutMs 500) { $portReady = $true; Write-Host "Puerto 8000 esta escuchando!" -ForegroundColor Green }
    else {
        $portRetries++
        $jobState = (Get-Job -Id $metadataJob.Id).State
        if ($jobState -ne "Running") {
            Write-Host "ERROR: El job de Metadata Service termino inesperadamente (Estado: $jobState)" -ForegroundColor Red
            if (Test-Path $metadataErrLog) { Get-Content $metadataErrLog -Tail 40 | ForEach-Object { Write-Host "  $_" } }
            exit 1
        }
        Start-Sleep -Milliseconds 1000
        Write-Host "  Esperando puerto... $portRetries/$maxPortRetries"
    }
}
if (-not $portReady) {
    Write-Host "ERROR: Puerto 8000 no esta disponible despues de $maxPortRetries intentos" -ForegroundColor Red
    if (Test-Path $metadataErrLog) { Get-Content $metadataErrLog -Tail 40 | ForEach-Object { Write-Host "  $_" } }
    exit 1
}

# Verificar health endpoint
Write-Host "Verificando health endpoint..."
$maxRetries = 10; $retryCount = 0; $metadataReady = $false
while ($retryCount -lt $maxRetries -and -not $metadataReady) {
    try {
        $healthResponse = Invoke-RestMethod -Uri "http://127.0.0.1:8000/api/v1/health" -TimeoutSec 3 -ErrorAction Stop
        Write-Host "Metadata Service responde: $($healthResponse.status)" -ForegroundColor Green
        $metadataReady = $true
    } catch {
        $retryCount++
        if ($retryCount -eq $maxRetries) {
            Write-Host "ERROR: Metadata Service no responde despues de $maxRetries intentos" -ForegroundColor Red
            if (Test-Path $metadataErrLog) { Get-Content $metadataErrLog -Tail 40 | ForEach-Object { Write-Host "  $_" } }
            exit 1
        } else {
            Write-Host "  Intento $retryCount/$maxRetries..."
            Start-Sleep -Seconds 2
        }
    }
}

Write-Host "Metadata Service listo y respondiendo!" -ForegroundColor Green

# Lanzar N DataNodes (parametrizable)
Write-Host "Iniciando $nodes DataNode(s)..."
$basePort = 8001
$global:DATANODE_JOBS = @()

for ($i = 1; $i -le $nodes; $i++) {
    $port = $basePort + ($i - 1)
    $storagePath = Join-Path $tempDir ("dfs-data-node{0}" -f $i)
    $logFile = Join-Path $tempDir ("dfs-datanode{0}.log" -f $i)
    $errFile = Join-Path $tempDir ("dfs-datanode{0}-errors.log" -f $i)
    $nodeId = "node-localhost-$port"

    Write-Host "  Iniciando DataNode $i en puerto $port (Storage: $storagePath)"

    # Preparar env vars para el job
    $envVars = @{
        "DFS_METADATA_HOST" = "localhost"
        "DFS_METADATA_PORT" = "8000"
        "DFS_DATANODE_HOST" = "0.0.0.0"
        "DFS_DATANODE_PORT" = "$port"
        "DFS_STORAGE_PATH" = "$storagePath"
        "NODE_ID" = $nodeId
        "PYTHONPATH" = $env:PYTHONPATH
    }

    $datanodeJob = Start-Job -ScriptBlock {
        param($pythonPath, $workDir, $logFile, $errFile, $envVars)
        foreach ($key in $envVars.Keys) { Set-Item -Path "env:$key" -Value $envVars[$key] }
        Set-Location $workDir
        & $pythonPath -m datanode.server *> $logFile 2> $errFile
    } -ArgumentList $pythonCmd, $backendRoot, $logFile, $errFile, $envVars

    $global:DATANODE_JOBS += $datanodeJob
    Write-Host "    Job ID: $($datanodeJob.Id)"
    
    # Opcional: esperar que el puerto del datanode quede listo (pequeña comprobación)
    $tries = 0; $maxTries = 10; $ready = $false
    while ($tries -lt $maxTries -and -not $ready) {
        if (Test-Port -HostName "127.0.0.1" -Port $port -TimeoutMs 500) { $ready = $true; Write-Host "    Puerto $port escuchando!" }
        else { Start-Sleep -Milliseconds 500; $tries++ }
    }
    if (-not $ready) { Write-Host "    Advertencia: puerto $port no respondió en el tiempo esperado. Revisa $errFile" -ForegroundColor Yellow }
}

Write-Host ""
Write-Host "=== Sistema DFS iniciado ==="
Write-Host "Servicios corriendo:"
Write-Host "  - Metadata Service: http://127.0.0.1:8000"
for ($i = 1; $i -le $nodes; $i++) {
    $port = $basePort + ($i - 1)
    Write-Host ("  - DataNode {0}: http://127.0.0.1:{1}" -f $i, $port)
}
Write-Host ""
Write-Host "Logs disponibles en:"
Write-Host "  - Metadata: $metadataLog"
Write-Host "  - Metadata Errors: $metadataErrLog"
for ($i = 1; $i -le $nodes; $i++) {
    $logFile = Join-Path $tempDir ("dfs-datanode{0}.log" -f $i)
    $errFile = Join-Path $tempDir ("dfs-datanode{0}-errors.log" -f $i)
    Write-Host ("  - DataNode {0}: {1}" -f $i, $logFile)
    Write-Host ("  - DataNode {0} Errors: {1}" -f $i, $errFile)
}

Write-Host ""
Write-Host "Para detener los servicios, ejecuta: Get-Job | Stop-Job | Remove-Job"
Write-Host "Para ver el estado: Get-Job"
Write-Host ""
