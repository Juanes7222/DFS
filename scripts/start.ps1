#!/usr/bin/env pwsh
# Script de inicio rápido para DFS con Docker - Adaptado para estructura backend/

[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8
chcp 65001 | Out-Null  # Solo en Windows

$ErrorActionPreference = "Stop"

Write-Host "🚀 Iniciando DFS System con Docker..." -ForegroundColor Cyan

# Verificar Docker
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Host "Docker no está instalado o no está en el PATH" -ForegroundColor Red
    exit 1
}

if (-not (Get-Command docker-compose -ErrorAction SilentlyContinue)) {
    Write-Host "Docker Compose no está instalado o no está en el PATH" -ForegroundColor Red
    exit 1
}

# Obtener directorio del script
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Split-Path -Parent $scriptDir

# Verificar que docker-compose.yml existe
$composeFile = Join-Path $projectRoot "docker-compose.yml"
if (-not (Test-Path $composeFile)) {
    Write-Host "No se encuentra docker-compose.yml en: $projectRoot" -ForegroundColor Red
    
    # Crear docker-compose.yml básico si no existe
    Write-Host "📝 Creando docker-compose.yml básico..." -ForegroundColor Yellow
    $composeContent = @"
version: '3.8'

services:
  metadata-service:
    build: 
      context: .
      dockerfile: Dockerfile
    command: python -m backend.metadata.server
    ports:
      - "8000:8000"
    environment:
      - DFS_METADATA_HOST=0.0.0.0
      - DFS_METADATA_PORT=8000
      - DFS_DB_PATH=/data/metadata.db
      - DFS_LOG_LEVEL=INFO
    volumes:
      - metadata_data:/data
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/health"]
      interval: 30s
      timeout: 10s
      retries: 3

  datanode-1:
    build:
      context: .
      dockerfile: Dockerfile
    command: python -m backend.datanode.server
    environment:
      - DFS_DATANODE_HOST=0.0.0.0
      - DFS_DATANODE_PORT=8001
      - DFS_METADATA_URL=http://metadata-service:8000
      - NODE_ID=node-datanode-1-8001
      - STORAGE_PATH=/data
    volumes:
      - datanode_1_data:/data
    depends_on:
      - metadata-service

  datanode-2:
    build:
      context: .
      dockerfile: Dockerfile
    command: python -m backend.datanode.server
    environment:
      - DFS_DATANODE_HOST=0.0.0.0
      - DFS_DATANODE_PORT=8002
      - DFS_METADATA_URL=http://metadata-service:8000
      - NODE_ID=node-datanode-2-8002
      - STORAGE_PATH=/data
    volumes:
      - datanode_2_data:/data
    depends_on:
      - metadata-service

  datanode-3:
    build:
      context: .
      dockerfile: Dockerfile
    command: python -m backend.datanode.server
    environment:
      - DFS_DATANODE_HOST=0.0.0.0
      - DFS_DATANODE_PORT=8003
      - DFS_METADATA_URL=http://metadata-service:8000
      - NODE_ID=node-datanode-3-8003
      - STORAGE_PATH=/data
    volumes:
      - datanode_3_data:/data
    depends_on:
      - metadata-service

volumes:
  metadata_data:
  datanode_1_data:
  datanode_2_data:
  datanode_3_data:
"@
    
    $composeContent | Out-File -FilePath $composeFile -Encoding UTF8
    Write-Host "docker-compose.yml creado" -ForegroundColor Green
}

# Verificar que existe Dockerfile
$dockerfile = Join-Path $projectRoot "Dockerfile"
if (-not (Test-Path $dockerfile)) {
    Write-Host "📝 Creando Dockerfile básico..." -ForegroundColor Yellow
    $dockerfileContent = @"
FROM python:3.11-slim

WORKDIR /app

# Copiar el proyecto
COPY backend/ ./backend/
COPY requirements/ ./requirements/
COPY pyproject.toml .

# Instalar dependencias
RUN pip install --no-cache-dir -e .

# Exponer puertos
EXPOSE 8000 8001 8002 8003

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1
"@
    
    $dockerfileContent | Out-File -FilePath $dockerfile -Encoding UTF8
    Write-Host "Dockerfile creado" -ForegroundColor Green
}

# Construir imágenes
Write-Host "🔨 Construyendo imágenes Docker..." -ForegroundColor Yellow
Set-Location $projectRoot
docker-compose build

if ($LASTEXITCODE -ne 0) {
    Write-Host "Error construyendo imágenes Docker" -ForegroundColor Red
    exit 1
}

# Iniciar servicios
Write-Host "🔄 Iniciando servicios..." -ForegroundColor Yellow
docker-compose up -d

if ($LASTEXITCODE -ne 0) {
    Write-Host "Error iniciando servicios con Docker Compose" -ForegroundColor Red
    exit 1
}

# Esperar a que los servicios estén listos
Write-Host "⏳ Esperando a que los servicios estén listos..." -ForegroundColor Yellow
Start-Sleep -Seconds 15

# Verificar estado
Write-Host "🔍 Verificando estado de servicios..." -ForegroundColor Yellow
docker-compose ps

# Verificar salud del Metadata Service
try {
    $healthResponse = Invoke-RestMethod -Uri "http://localhost:8000/api/v1/health" -TimeoutSec 10
    Write-Host "Metadata Service: $($healthResponse.status)" -ForegroundColor Green
} catch {
    Write-Host "Metadata Service no responde" -ForegroundColor Red
    Write-Host "Revisa los logs con: docker-compose logs metadata-service" -ForegroundColor Yellow
}

# Verificar nodos
try {
    $nodesResponse = Invoke-RestMethod -Uri "http://localhost:8000/api/v1/nodes" -TimeoutSec 5
    $activeNodes = $nodesResponse | Where-Object { $_.state -eq "active" }
    Write-Host "$($activeNodes.Count) DataNodes activos" -ForegroundColor Green
    
    # Mostrar información de nodos
    foreach ($node in $activeNodes) {
        Write-Host "   - $($node.node_id): $($node.free_space) bytes libres" -ForegroundColor Gray
    }
} catch {
    Write-Host "⚠️  No se pudo verificar el estado de los DataNodes" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "🎉 DFS System iniciado correctamente!" -ForegroundColor Green
Write-Host ""
Write-Host "Servicios disponibles:" -ForegroundColor Cyan
Write-Host "  - Metadata Service: http://localhost:8000" -ForegroundColor White
Write-Host "  - DataNode 1: http://localhost:8001" -ForegroundColor White
Write-Host "  - DataNode 2: http://localhost:8002" -ForegroundColor White
Write-Host "  - DataNode 3: http://localhost:8003" -ForegroundColor White
Write-Host ""
Write-Host "Documentación API: http://localhost:8000/docs" -ForegroundColor Cyan
Write-Host "Métricas: http://localhost:8000/metrics" -ForegroundColor Cyan
Write-Host ""
Write-Host "Comandos útiles:" -ForegroundColor Cyan
Write-Host "  Ver logs: docker-compose logs -f" -ForegroundColor White
Write-Host "  Ver logs específicos: docker-compose logs metadata-service" -ForegroundColor White
Write-Host "  Ver estado: docker-compose ps" -ForegroundColor White
Write-Host "  Detener: docker-compose down" -ForegroundColor White
Write-Host "  Detener y limpiar: docker-compose down -v" -ForegroundColor White
Write-Host ""
Write-Host "Para usar el CLI:" -ForegroundColor Cyan
Write-Host "  # Desde el host:" -ForegroundColor White
Write-Host "  python -m backend.client.cli --help" -ForegroundColor White
Write-Host "  python -m backend.client.cli nodes" -ForegroundColor White
Write-Host "  python -m backend.client.cli upload ./archivo.txt /mi-archivo.txt" -ForegroundColor White
Write-Host ""
Write-Host "  # Desde dentro del contenedor:" -ForegroundColor White
Write-Host "  docker-compose exec metadata-service python -m backend.client.cli nodes" -ForegroundColor White
Write-Host ""
Write-Host "Para testing:" -ForegroundColor Cyan
Write-Host "  cd backend/tests && ./run_test.sh" -ForegroundColor White