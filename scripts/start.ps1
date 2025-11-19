#!/usr/bin/env pwsh
# Script de inicio rápido para DFS

$ErrorActionPreference = "Stop"

Write-Host "Iniciando DFS System..." -ForegroundColor Cyan

# Verificar Docker
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Host "Docker no está instalado" -ForegroundColor Red
    exit 1
}

if (-not (Get-Command docker-compose -ErrorAction SilentlyContinue)) {
    Write-Host "Docker Compose no está instalado" -ForegroundColor Red
    exit 1
}

# Construir imágenes
Write-Host "Construyendo imágenes Docker..." -ForegroundColor Yellow
docker-compose build

# Iniciar servicios
Write-Host "Iniciando servicios..." -ForegroundColor Yellow
docker-compose up -d

# Esperar a que los servicios estén listos
Write-Host "Esperando a que los servicios estén listos..." -ForegroundColor Yellow
Start-Sleep -Seconds 10

# Verificar estado
Write-Host "Verificando estado de servicios..." -ForegroundColor Yellow
docker-compose ps

Write-Host ""
Write-Host "DFS System iniciado correctamente!" -ForegroundColor Green
Write-Host ""
Write-Host "Servicios disponibles:" -ForegroundColor Cyan
Write-Host "  - Metadata Service: http://localhost:8000"
Write-Host "  - DataNode 1: http://localhost:8001"
Write-Host "  - DataNode 2: http://localhost:8002"
Write-Host "  - DataNode 3: http://localhost:8003"
Write-Host ""
Write-Host "Ver logs:" -ForegroundColor Cyan
Write-Host "  docker-compose logs -f"
Write-Host ""
Write-Host "Detener:" -ForegroundColor Cyan
Write-Host "  docker-compose down"
Write-Host ""