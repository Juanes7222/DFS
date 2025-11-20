#!/usr/bin/env pwsh
# Script para iniciar todo el sistema DFS - Version Mejorada

$ErrorActionPreference = "Continue"

Write-Host "=== Iniciando Sistema DFS ===" -ForegroundColor Cyan

# Obtener el directorio donde esta el script
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Split-Path -Parent $scriptDir
$backendRoot = Join-Path $projectRoot "backend"

# Verificar estructura del proyecto
if (-not (Test-Path $backendRoot)) {
    Write-Host "ERROR: No se encuentra el directorio backend en: $backendRoot" -ForegroundColor Red
    exit 1
}

# Funcion para encontrar Python
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
        } catch {
            # Continuar buscando
        }
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

# Funcion para verificar si un puerto esta abierto
function Test-Port {
    param(
        [string]$HostName,
        [int]$Port,
        [int]$TimeoutMs = 1000
    )
    
    try {
        $tcpClient = New-Object System.Net.Sockets.TcpClient
        $connect = $tcpClient.BeginConnect($HostName, $Port, $null, $null)
        $wait = $connect.AsyncWaitHandle.WaitOne($TimeoutMs, $false)
        
        if ($wait) {
            $tcpClient.EndConnect($connect)
            $tcpClient.Close()
            return $true
        } else {
            $tcpClient.Close()
            return $false
        }
    } catch {
        return $false
    }
}

Write-Host "Buscando Python..." -ForegroundColor Yellow
$pythonCmd = Get-PythonCommand

if (-not $pythonCmd) {
    Write-Host "ERROR: No se pudo encontrar Python instalado" -ForegroundColor Red
    Write-Host "Por favor instale Python 3.8+ desde https://python.org" -ForegroundColor Yellow
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
    exit 1
}

# Variables Metadata
$env:DFS_METADATA_HOST = "0.0.0.0"
$env:DFS_METADATA_PORT = "8000"
$env:DFS_DB_PATH = "$tempDir/dfs-metadata/dfs_metadata.db"

Write-Host "Configuracion completada. Iniciando servicios..." -ForegroundColor Cyan

# Metadata Service
Write-Host "Iniciando Metadata Service en puerto 8000..." -ForegroundColor Yellow
$metadataLog = Join-Path $tempDir "dfs-metadata.log"
$metadataErrLog = Join-Path $tempDir "dfs-metadata-errors.log"

try {
    Write-Host "  Ejecutando: $pythonCmd $($backendRoot)/metadata/server.py" -ForegroundColor Gray
    
    # Iniciar proceso en background con job en lugar de Start-Process
    $metadataJob = Start-Job -ScriptBlock {
        param($pythonPath, $serverScript, $workDir, $logFile, $errFile)
        
        Set-Location $workDir
        & $pythonPath $serverScript *> $logFile 2> $errFile
    } -ArgumentList $pythonCmd, "$backendRoot/metadata/server.py", $backendRoot, $metadataLog, $metadataErrLog
    
    Write-Host "  Metadata Service Job ID: $($metadataJob.Id)" -ForegroundColor Green
    
    # Guardar el Job ID para cleanup
    $global:METADATA_JOB = $metadataJob
} catch {
    Write-Host "ERROR: No se pudo iniciar Metadata Service: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

# Esperar a que el puerto este disponible
Write-Host "Esperando a que el puerto 8000 este disponible..." -ForegroundColor Yellow
$portReady = $false
$portRetries = 0
$maxPortRetries = 30

while (-not $portReady -and $portRetries -lt $maxPortRetries) {
    if (Test-Port -HostName "127.0.0.1" -Port 8000 -TimeoutMs 500) {
        $portReady = $true
        Write-Host "Puerto 8000 esta escuchando!" -ForegroundColor Green
    } else {
        $portRetries++
        
        # Verificar que el job siga corriendo
        $jobState = Get-Job -Id $metadataJob.Id | Select-Object -ExpandProperty State
        if ($jobState -ne "Running") {
            Write-Host "ERROR: El job de Metadata Service termino inesperadamente (Estado: $jobState)" -ForegroundColor Red
            Write-Host "Revisar logs:" -ForegroundColor Yellow
            if (Test-Path $metadataErrLog) {
                Get-Content $metadataErrLog -Tail 20 | ForEach-Object { Write-Host "  $_" -ForegroundColor Gray }
            }
            exit 1
        }
        
        Write-Host "  Esperando puerto... $portRetries/$maxPortRetries" -ForegroundColor Gray
        Start-Sleep -Milliseconds 1000
    }
}

if (-not $portReady) {
    Write-Host "ERROR: Puerto 8000 no esta disponible despues de $maxPortRetries intentos" -ForegroundColor Red
    Write-Host "Revisar logs:" -ForegroundColor Yellow
    if (Test-Path $metadataErrLog) {
        Get-Content $metadataErrLog -Tail 20 | ForEach-Object { Write-Host "  $_" -ForegroundColor Gray }
    }
    exit 1
}

# Verificar health endpoint
Write-Host "Verificando health endpoint..." -ForegroundColor Yellow
$maxRetries = 10
$retryCount = 0
$metadataReady = $false

while ($retryCount -lt $maxRetries -and -not $metadataReady) {
    try {
        $healthResponse = Invoke-RestMethod -Uri "http://127.0.0.1:8000/api/v1/health" -TimeoutSec 3 -ErrorAction Stop
        Write-Host "Metadata Service responde: $($healthResponse.status)" -ForegroundColor Green
        $metadataReady = $true
    } catch {
        $retryCount++
        if ($retryCount -eq $maxRetries) {
            Write-Host "ERROR: Metadata Service no responde despues de $maxRetries intentos" -ForegroundColor Red
            Write-Host "El puerto esta abierto pero el servicio no responde correctamente" -ForegroundColor Yellow
            Write-Host "" -ForegroundColor Yellow
            Write-Host "Revisar logs:" -ForegroundColor Yellow
            if (Test-Path $metadataErrLog) {
                Get-Content $metadataErrLog -Tail 30 | ForEach-Object { Write-Host "  $_" -ForegroundColor Gray }
            }
            exit 1
        } else {
            Write-Host "  Intento $retryCount/$maxRetries..." -ForegroundColor Gray
            Start-Sleep -Seconds 2
        }
    }
}

Write-Host "Metadata Service listo y respondiendo!" -ForegroundColor Green

# DataNode 1
Write-Host "Iniciando DataNode 1 en puerto 8001..." -ForegroundColor Yellow
$datanode1Log = Join-Path $tempDir "dfs-datanode1.log"
$datanode1ErrLog = Join-Path $tempDir "dfs-datanode1-errors.log"

# Configurar variables de entorno para DataNode
$env:DFS_METADATA_HOST = "localhost"
$env:DFS_METADATA_PORT = "8000"
$env:DFS_DATANODE_HOST = "0.0.0.0"
$env:DFS_DATANODE_PORT = "8001"
$env:DFS_STORAGE_PATH = "$tempDir/dfs-data-node1"
$env:NODE_ID = "node-localhost-8001"

try {
    Write-Host "  Ejecutando DataNode 1..." -ForegroundColor Gray
    
    $datanode1Job = Start-Job -ScriptBlock {
        param($pythonPath, $workDir, $logFile, $errFile, $envVars)
        
        # Establecer variables de entorno
        foreach ($key in $envVars.Keys) {
            Set-Item -Path "env:$key" -Value $envVars[$key]
        }
        
        Set-Location $workDir
        & $pythonPath -m datanode *> $logFile 2> $errFile
    } -ArgumentList $pythonCmd, $backendRoot, $datanode1Log, $datanode1ErrLog, @{
        DFS_METADATA_HOST = $env:DFS_METADATA_HOST
        DFS_METADATA_PORT = $env:DFS_METADATA_PORT
        DFS_DATANODE_HOST = $env:DFS_DATANODE_HOST
        DFS_DATANODE_PORT = $env:DFS_DATANODE_PORT
        DFS_STORAGE_PATH = $env:DFS_STORAGE_PATH
        NODE_ID = $env:NODE_ID
        PYTHONPATH = $env:PYTHONPATH
    }
    
    Write-Host "  DataNode 1 Job ID: $($datanode1Job.Id)" -ForegroundColor Green
    $global:DATANODE1_JOB = $datanode1Job
} catch {
    Write-Host "ERROR: No se pudo iniciar DataNode 1: $($_.Exception.Message)" -ForegroundColor Red
}

Write-Host "" -ForegroundColor White
Write-Host "=== Sistema DFS iniciado ===" -ForegroundColor Green
Write-Host "" -ForegroundColor White
Write-Host "Servicios corriendo:" -ForegroundColor Cyan
Write-Host "  - Metadata Service: http://127.0.0.1:8000" -ForegroundColor White
Write-Host "  - DataNode 1: http://127.0.0.1:8001" -ForegroundColor White
Write-Host "" -ForegroundColor White
Write-Host "Logs disponibles en:" -ForegroundColor Cyan
Write-Host "  - Metadata: $metadataLog" -ForegroundColor Gray
Write-Host "  - Metadata Errors: $metadataErrLog" -ForegroundColor Gray
Write-Host "  - DataNode 1: $datanode1Log" -ForegroundColor Gray
Write-Host "  - DataNode 1 Errors: $datanode1ErrLog" -ForegroundColor Gray
Write-Host "" -ForegroundColor White
Write-Host "Para detener los servicios, ejecuta: Get-Job | Stop-Job | Remove-Job" -ForegroundColor Yellow
Write-Host "Para ver el estado: Get-Job" -ForegroundColor Yellow
Write-Host "" -ForegroundColor Whit