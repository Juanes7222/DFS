#!/usr/bin/env pwsh
# Script para iniciar todo el sistema DFS

Write-Host "=== Iniciando Sistema DFS ===" -ForegroundColor Cyan

# Obtener el directorio donde está el script
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

# Crear directorios de datos
$tempDir = if ($IsWindows -or $env:OS -match "Windows") { $env:TEMP } else { "/tmp" }
$dataDirs = @("$tempDir\dfs-data1", "$tempDir\dfs-data2", "$tempDir\dfs-data3")
foreach ($dir in $dataDirs) {
    if (-not (Test-Path $dir)) {
        New-Item -ItemType Directory -Path $dir -Force | Out-Null
    }
}

# Rutas base - ajustar según la estructura real del proyecto
$basePath = Split-Path -Parent $scriptDir
$metadataPath = Join-Path $basePath "backend/metadata-service"
$datanodePath = Join-Path $basePath "backend/datanode"

# Verificar que las rutas existen
if (-not (Test-Path $metadataPath)) {
    Write-Host "Error: No se encuentra el directorio metadata-service en: $metadataPath" -ForegroundColor Red
    Write-Host "Directorio actual: $scriptDir" -ForegroundColor Yellow
    exit 1
}

if (-not (Test-Path $datanodePath)) {
    Write-Host "Error: No se encuentra el directorio datanode en: $datanodePath" -ForegroundColor Red
    exit 1
}

# Determinar el comando de Python
$pythonCmd = "python"
if (Get-Command "python3.11" -ErrorAction SilentlyContinue) {
    $pythonCmd = "python3.11"
} elseif (Get-Command "python3" -ErrorAction SilentlyContinue) {
    $pythonCmd = "python3"
}

Write-Host "Usando comando Python: $pythonCmd" -ForegroundColor Cyan

# Iniciar Metadata Service
Write-Host "Iniciando Metadata Service en puerto 8000..." -ForegroundColor Yellow
$metadataLog = Join-Path $tempDir "metadata.log"
$metadataErrLog = Join-Path $tempDir "metadata.err.log"

$metadataProcess = Start-Process -FilePath $pythonCmd `
    -ArgumentList "main_simple.py" `
    -WorkingDirectory $metadataPath `
    -RedirectStandardOutput $metadataLog `
    -RedirectStandardError $metadataErrLog `
    -PassThru -NoNewWindow

$METADATA_PID = $metadataProcess.Id
Write-Host "Metadata Service PID: $METADATA_PID" -ForegroundColor Green

Start-Sleep -Seconds 3

# Iniciar DataNode 1
Write-Host "Iniciando DataNode 1 en puerto 8001..." -ForegroundColor Yellow
$datanode1Log = Join-Path $tempDir "datanode1.log"
$datanode1ErrLog = Join-Path $tempDir "datanode1.err.log"

$env:NODE_ID = "node-localhost-8001"
$env:PORT = "8001"
$env:STORAGE_PATH = Join-Path $tempDir "dfs-data1"

$datanode1Process = Start-Process -FilePath $pythonCmd `
    -ArgumentList "datanode_simple.py" `
    -WorkingDirectory $datanodePath `
    -RedirectStandardOutput $datanode1Log `
    -RedirectStandardError $datanode1ErrLog `
    -PassThru -NoNewWindow

$DN1_PID = $datanode1Process.Id
Write-Host "DataNode 1 PID: $DN1_PID" -ForegroundColor Green

# Iniciar DataNode 2
Write-Host "Iniciando DataNode 2 en puerto 8002..." -ForegroundColor Yellow
$datanode2Log = Join-Path $tempDir "datanode2.log"
$datanode2ErrLog = Join-Path $tempDir "datanode2.err.log"

$env:NODE_ID = "node-localhost-8002"
$env:PORT = "8002"
$env:STORAGE_PATH = Join-Path $tempDir "dfs-data2"

$datanode2Process = Start-Process -FilePath $pythonCmd `
    -ArgumentList "datanode_simple.py" `
    -WorkingDirectory $datanodePath `
    -RedirectStandardOutput $datanode2Log `
    -RedirectStandardError $datanode2ErrLog `
    -PassThru -NoNewWindow

$DN2_PID = $datanode2Process.Id
Write-Host "DataNode 2 PID: $DN2_PID" -ForegroundColor Green

# Iniciar DataNode 3
Write-Host "Iniciando DataNode 3 en puerto 8003..." -ForegroundColor Yellow
$datanode3Log = Join-Path $tempDir "datanode3.log"
$datanode3ErrLog = Join-Path $tempDir "datanode3.err.log"

$env:NODE_ID = "node-localhost-8003"
$env:PORT = "8003"
$env:STORAGE_PATH = Join-Path $tempDir "dfs-data3"

$datanode3Process = Start-Process -FilePath $pythonCmd `
    -ArgumentList "datanode_simple.py" `
    -WorkingDirectory $datanodePath `
    -RedirectStandardOutput $datanode3Log `
    -RedirectStandardError $datanode3ErrLog `
    -PassThru -NoNewWindow

$DN3_PID = $datanode3Process.Id
Write-Host "DataNode 3 PID: $DN3_PID" -ForegroundColor Green

Start-Sleep -Seconds 5

Write-Host ""
Write-Host "=== Sistema DFS Iniciado ===" -ForegroundColor Green
Write-Host ""
Write-Host "Servicios:" -ForegroundColor Cyan
Write-Host "  - Metadata Service: http://localhost:8000"
Write-Host "  - DataNode 1: http://localhost:8001"
Write-Host "  - DataNode 2: http://localhost:8002"
Write-Host "  - DataNode 3: http://localhost:8003"
Write-Host ""
Write-Host "API Docs: http://localhost:8000/docs" -ForegroundColor Cyan
Write-Host ""
Write-Host "Logs:" -ForegroundColor Cyan
Write-Host "  - Metadata: $metadataLog"
Write-Host "  - Metadata Errors: $metadataErrLog"
Write-Host "  - DataNode 1: $datanode1Log"
Write-Host "  - DataNode 1 Errors: $datanode1ErrLog"
Write-Host "  - DataNode 2: $datanode2Log"
Write-Host "  - DataNode 2 Errors: $datanode2ErrLog"
Write-Host "  - DataNode 3: $datanode3Log"
Write-Host "  - DataNode 3 Errors: $datanode3ErrLog"
Write-Host ""
Write-Host "Para verificar el estado:" -ForegroundColor Cyan
Write-Host "  curl http://localhost:8000/api/v1/health"
Write-Host "  curl http://localhost:8000/api/v1/nodes"
Write-Host ""

# Guardar PIDs
$pidsFile = Join-Path $tempDir "dfs-pids.txt"
Write-Host "PIDs guardados en $pidsFile" -ForegroundColor Cyan
@($METADATA_PID, $DN1_PID, $DN2_PID, $DN3_PID) | Out-File -FilePath $pidsFile -Encoding UTF8