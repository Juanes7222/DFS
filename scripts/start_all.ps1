#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Script para iniciar el sistema DFS completo (Metadata Service + DataNodes)

.DESCRIPTION
    Inicia automáticamente el Metadata Service y N DataNodes configurables.
    Verifica dependencias, puertos y salud de los servicios.

.PARAMETER Nodes
    Número de DataNodes a iniciar (default: 3)

.PARAMETER MetadataPort
    Puerto para el Metadata Service (default: 8000, se ignora si MetadataHost incluye protocolo)

.PARAMETER MetadataHost
    Host para el Metadata Service (default: localhost). Puede incluir protocolo (http:// o https://)

.PARAMETER WithoutMetadata
    Si se necesita omitir el inicio del Metadata Service

.PARAMETER BaseDataNodePort
    Puerto base para los DataNodes (default: 8001)

.PARAMETER CleanStart
    Si está presente, limpia los datos existentes antes de iniciar

.PARAMETER TempDir
    Directorio temporal para almacenar datos y logs (default: directorio temporal del sistema)

.EXAMPLE
    .\start-dfs.ps1
    .\start-dfs.ps1 -Nodes 5
    .\start-dfs.ps1 -Nodes 3 -CleanStart
    .\start-dfs.ps1 -TempDir "C:\DFS-Data"
    .\start-dfs.ps1 -WithoutMetadata -MetadataHost "https://dfs-0z6t.onrender.com"
#>

param(
    [int]$Nodes = 3,
    [int]$MetadataPort = 8000,
    [string]$MetadataHost = "localhost",
    [int]$BaseDataNodePort = 8001,
    [switch]$WithoutMetadata,
    [switch]$CleanStart,
    [string]$TempDir = ""
)

$ErrorActionPreference = "Continue"

# Constantes
$PYTHON_MIN_VERSION = @{Major = 3; Minor = 8}
$PYTHON_MAX_VERSION = @{Major = 3; Minor = 11}
$PORT_CHECK_TIMEOUT = 500
$MAX_PORT_RETRIES = 30
$MAX_HEALTH_RETRIES = 10
$DATANODE_STARTUP_RETRIES = 10

# Determinar directorio temporal si no se especificó
if ([string]::IsNullOrWhiteSpace($TempDir)) {
    if ($IsWindows -or $env:OS -match "Windows") {
        $TempDir = $env:TEMP
    } else {
        $TempDir = "/tmp"
    }
    $usingDefaultTemp = $true
} else {
    # Convertir a ruta absoluta
    $TempDir = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($TempDir)
    $usingDefaultTemp = $false
    
    # Crear el directorio si no existe
    if (-not (Test-Path $TempDir)) {
        try {
            New-Item -Path $TempDir -ItemType Directory -Force | Out-Null
            Write-Host "Directorio creado: $TempDir" -ForegroundColor Green
        } catch {
            Write-Host "Error creando directorio $TempDir : $_" -ForegroundColor Red
            exit 1
        }
    }
}

# Parsear MetadataHost para determinar URL completa
function Get-MetadataUrl {
    param([string]$MetadataServiceHost, [int]$Port)
    
    # Si ya incluye protocolo, usarlo directamente
    if ($MetadataServiceHost -match '^https?://') {
        return $MetadataServiceHost
    }
    
    # Si es localhost o IP, usar http con el puerto especificado
    if ($MetadataServiceHost -eq "localhost" -or $MetadataServiceHost -match '^\d+\.\d+\.\d+\.\d+$') {
        return "http://${MetadataServiceHost}:${Port}"
    }
    
    # Para dominios externos, asumir HTTPS sin puerto
    return "https://${MetadataServiceHost}"
}

$metadataUrl = Get-MetadataUrl -MetadataServiceHost $MetadataHost -Port $MetadataPort
Write-Host "Metadata URL configurada: $metadataUrl" -ForegroundColor Gray

# Colores para output
function Write-Section { param([string]$Message) Write-Host "`n=== $Message ===" -ForegroundColor Cyan }
function Write-Success { param([string]$Message) Write-Host "$Message" -ForegroundColor Green }
function Write-Info { param([string]$Message) Write-Host "$Message" -ForegroundColor Gray }
function Write-Warning { param([string]$Message) Write-Host "$Message" -ForegroundColor Yellow }
function Write-Failure { param([string]$Message) Write-Host "$Message" -ForegroundColor Red }

Write-Section "Iniciando Sistema DFS"
Write-Info "DataNodes a iniciar: $Nodes"
if (-not $WithoutMetadata) {
    Write-Info "Puerto Metadata: $MetadataPort"
}
Write-Info "URL Metadata: $metadataUrl"
Write-Info "Puertos DataNode: $BaseDataNodePort-$($BaseDataNodePort + $Nodes - 1)"
if ($usingDefaultTemp) {
    Write-Info "Directorio datos/logs: $TempDir (por defecto)"
} else {
    Write-Success "Directorio datos/logs: $TempDir (personalizado)"
}

# Obtener directorios del proyecto
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Split-Path -Parent $scriptDir
$backendRoot = Join-Path $projectRoot "backend"

if (-not (Test-Path $backendRoot)) {
    Write-Failure "No se encuentra el directorio backend en: $backendRoot"
    exit 1
}

function Get-PythonCommand {
    <#
    .SYNOPSIS
        Encuentra el ejecutable de Python en el sistema
    #>
    
    # Buscar en venv del proyecto primero
    $venvPython = Join-Path $backendRoot ".venv\Scripts\python.exe"
    if (Test-Path $venvPython) {
        try {
            $null = & $venvPython --version 2>&1
            if ($LASTEXITCODE -eq 0) {
                Write-Success "Python encontrado en venv: $venvPython"
                return $venvPython
            }
        } catch { }
    }

    # Buscar en PATH
    $pythonCommands = @("python", "py", "python3")
    foreach ($cmd in $pythonCommands) {
        try {
            $pythonPath = (Get-Command $cmd -ErrorAction Stop).Source
            $null = & $pythonPath --version 2>&1
            if ($LASTEXITCODE -eq 0) {
                Write-Success "Python encontrado: $pythonPath"
                return $pythonPath
            }
        } catch { continue }
    }

    # Buscar en ubicaciones comunes de Windows
    if ($IsWindows -or $env:OS -match "Windows") {
        $pythonVersions = @("313", "312", "311", "310", "39", "38")
        $basePaths = @($env:LOCALAPPDATA, $env:ProgramFiles, "${env:ProgramFiles(x86)}")
        
        foreach ($base in $basePaths) {
            foreach ($ver in $pythonVersions) {
                $path = Join-Path $base "Programs\Python\Python$ver\python.exe"
                if (Test-Path $path) {
                    Write-Success "Python encontrado en: $path"
                    return $path
                }
            }
        }
    }

    return $null
}

function Test-PythonVersion {
    <#
    .SYNOPSIS
        Verifica que la versión de Python sea compatible
    #>
    param([string]$PythonPath)
    
    try {
        $versionStr = & $PythonPath -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')" 2>&1
        if ($LASTEXITCODE -ne 0) { return $false }
        
        $parts = $versionStr.Split('.')
        $major = [int]$parts[0]
        $minor = [int]$parts[1]
        
        Write-Info "Versión de Python detectada: $versionStr"
        
        if ($major -lt $PYTHON_MIN_VERSION.Major -or 
            ($major -eq $PYTHON_MIN_VERSION.Major -and $minor -lt $PYTHON_MIN_VERSION.Minor)) {
            Write-Failure "Se requiere Python $($PYTHON_MIN_VERSION.Major).$($PYTHON_MIN_VERSION.Minor) o superior"
            return $false
        }
        
        if ($major -gt $PYTHON_MAX_VERSION.Major -or 
            ($major -eq $PYTHON_MAX_VERSION.Major -and $minor -gt $PYTHON_MAX_VERSION.Minor)) {
            Write-Warning "Python $versionStr puede tener problemas de compatibilidad"
        }
        
        return $true
    } catch {
        Write-Failure "Error verificando versión de Python: $_"
        return $false
    }
}

function Test-Port {
    <#
    .SYNOPSIS
        Verifica si un puerto está escuchando
    #>
    param(
        [string]$HostName = "127.0.0.1",
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
        }
        
        $tcpClient.Close()
        return $false
    } catch {
        return $false
    }
}

function Wait-ForPort {
    <#
    .SYNOPSIS
        Espera a que un puerto esté disponible
    #>
    param(
        [int]$Port,
        [int]$MaxRetries = 30,
        [int]$JobId
    )
    
    Write-Info "Esperando a que el puerto $Port esté disponible..."
    $retries = 0
    
    while ($retries -lt $MaxRetries) {
        if (Test-Port -Port $Port -TimeoutMs $PORT_CHECK_TIMEOUT) {
            Write-Success "Puerto $Port está escuchando"
            return $true
        }
        
        # Verificar que el job siga corriendo
        $jobState = (Get-Job -Id $JobId -ErrorAction SilentlyContinue).State
        if ($jobState -and $jobState -ne "Running") {
            Write-Failure "El servicio terminó inesperadamente (Estado: $jobState)"
            return $false
        }
        
        $retries++
        Start-Sleep -Milliseconds 1000
        Write-Host "." -NoNewline
    }
    
    Write-Host ""
    Write-Failure "Puerto $Port no disponible después de $MaxRetries intentos"
    return $false
}

function Test-MetadataHealth {
    <#
    .SYNOPSIS
        Verifica el health endpoint del Metadata Service
    #>
    param([string]$BaseUrl, [int]$MaxRetries = 10)
    
    Write-Info "Verificando health endpoint en $BaseUrl/health..."
    $retries = 0
    
    while ($retries -lt $MaxRetries) {
        try {
            $response = Invoke-RestMethod -Uri "$BaseUrl/health" `
                                         -TimeoutSec 5 `
                                         -ErrorAction Stop
            Write-Success "Metadata Service responde: $($response.status)"
            return $true
        } catch {
            $retries++
            if ($retries -lt $MaxRetries) {
                Write-Host "." -NoNewline
                Start-Sleep -Seconds 2
            }
        }
    }
    
    Write-Host ""
    Write-Failure "Metadata Service no responde después de $MaxRetries intentos"
    return $false
}

function Initialize-DataDirectories {
    <#
    .SYNOPSIS
        Crea o limpia directorios de datos
    #>
    param([string]$BaseDir, [int]$NodeCount, [bool]$Clean)
    
    Write-Section "Preparando directorios de datos"
    Write-Info "Directorio base: $BaseDir"
    
    $metadataDir = Join-Path $BaseDir "dfs-metadata"
    
    if ($Clean -and (Test-Path $metadataDir)) {
        Write-Warning "Limpiando datos existentes del metadata..."
        Remove-Item -Path $metadataDir -Recurse -Force -ErrorAction SilentlyContinue
    }
    
    if (-not (Test-Path $metadataDir)) {
        New-Item -Path $metadataDir -ItemType Directory -Force | Out-Null
        Write-Success "Directorio metadata creado: $metadataDir"
    } else {
        Write-Info "Directorio metadata existe: $metadataDir"
    }
    
    for ($i = 1; $i -le $NodeCount; $i++) {
        $nodeDir = Join-Path $BaseDir "dfs-data-node$i"
        
        if ($Clean -and (Test-Path $nodeDir)) {
            Write-Warning "Limpiando datos existentes del DataNode $i..."
            Remove-Item -Path $nodeDir -Recurse -Force -ErrorAction SilentlyContinue
        }
        
        if (-not (Test-Path $nodeDir)) {
            New-Item -Path $nodeDir -ItemType Directory -Force | Out-Null
            Write-Success "Directorio DataNode $i creado: $nodeDir"
        } else {
            Write-Info "Directorio DataNode $i existe: $nodeDir"
        }
    }
    
    return $metadataDir
}

function Show-Logs {
    <#
    .SYNOPSIS
        Muestra las últimas líneas de un archivo de log
    #>
    param([string]$LogPath, [int]$Lines = 30)
    
    if (Test-Path $LogPath) {
        Write-Host "`nÚltimas $Lines líneas de $LogPath :" -ForegroundColor Yellow
        Get-Content $LogPath -Tail $Lines | ForEach-Object { Write-Host "  $_" }
    } else {
        Write-Host "`nArchivo de log no encontrado: $LogPath" -ForegroundColor Red
    }
}

# Verificar Python
Write-Section "Verificando dependencias"
$pythonCmd = Get-PythonCommand

if (-not $pythonCmd) {
    Write-Failure "No se encontró Python instalado"
    Write-Info "Instala Python desde: https://www.python.org/downloads/"
    exit 1
}

if (-not (Test-PythonVersion -PythonPath $pythonCmd)) {
    exit 1
}

# Configurar PYTHONPATH
$env:PYTHONPATH = "$backendRoot$([System.IO.Path]::PathSeparator)$env:PYTHONPATH"

# Verificar que se pueda importar backend
try {
    Write-Info "Verificando paquete backend..."
    $importResult = & $pythonCmd -c "import backend; print('OK')" 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Failure "No se puede importar el paquete backend"
        Write-Info "Salida: $importResult"
        exit 1
    }
    Write-Success "Paquete backend disponible"
} catch {
    Write-Failure "Error verificando backend: $_"
    exit 1
}

# Preparar directorios
$metadataDir = Initialize-DataDirectories -BaseDir $TempDir -NodeCount $Nodes -Clean $CleanStart

# Configurar variables de entorno para Metadata Service
$env:DFS_METADATA_HOST = $metadataUrl
$env:DFS_METADATA_PORT = "$MetadataPort"
$env:DFS_DB_PATH = Join-Path $metadataDir "dfs_metadata.db"
$env:CORS_ALLOW_ALL = "true"

# Iniciar Metadata Service
if (-not $WithoutMetadata) {
    Write-Section "Iniciando Metadata Service"

    $metadataLog = Join-Path $TempDir "dfs-metadata.log"
    $metadataErrLog = Join-Path $TempDir "dfs-metadata-errors.log"

    try {
        $metadataJob = Start-Job -ScriptBlock {
            param($pythonPath, $workDir, $logFile, $errFile)
            Set-Location $workDir
            & $pythonPath -m metadata.server *> $logFile 2> $errFile
        } -ArgumentList $pythonCmd, $backendRoot, $metadataLog, $metadataErrLog

        Write-Success "Metadata Service iniciado (Job ID: $($metadataJob.Id))"
        $global:METADATA_JOB = $metadataJob
    } catch {
        Write-Failure "Error iniciando Metadata Service: $_"
        exit 1
    }

    # Esperar a que el Metadata Service esté listo
    if (-not (Wait-ForPort -Port $MetadataPort -JobId $metadataJob.Id)) {
        Show-Logs -LogPath $metadataErrLog
        Get-Job | Stop-Job | Remove-Job
        exit 1
    }

    if (-not (Test-MetadataHealth -BaseUrl "http://127.0.0.1:$MetadataPort")) {
        Show-Logs -LogPath $metadataErrLog
        Get-Job | Stop-Job | Remove-Job
        exit 1
    }
} else {
    Write-Section "Omitiendo inicio de Metadata Service"
    Write-Warning "Conectando al Metadata Service en: $metadataUrl"
    
    # Verificar que el metadata service esté disponible
    if (-not (Test-MetadataHealth -BaseUrl $metadataUrl)) {
        Write-Failure "No se puede conectar al Metadata Service"
        exit 1
    }
}

# Iniciar DataNodes
Write-Section "Iniciando DataNodes"

$global:DATANODE_JOBS = @()

for ($i = 1; $i -le $Nodes; $i++) {
    $port = $BaseDataNodePort + ($i - 1)
    $storagePath = Join-Path $TempDir "dfs-data-node$i"
    $logFile = Join-Path $TempDir "dfs-datanode$i.log"
    $errFile = Join-Path $TempDir "dfs-datanode$i-errors.log"
    $nodeId = "node-localhost-$port"

    Write-Info "Iniciando DataNode $i en puerto $port..."

    $envVars = @{
        "METADATA_URL" = $metadataUrl
        "DATA_PORT" = "$port"
        "DFS_DATANODE_HOST" = "0.0.0.0"
        "DFS_DATANODE_PORT" = "$port"
        "DFS_STORAGE_PATH" = $storagePath
        "NODE_ID" = $nodeId
        "PYTHONPATH" = $env:PYTHONPATH
        "CORS_ALLOW_ALL" = "true"
    }

    $datanodeJob = Start-Job -ScriptBlock {
        param($pythonPath, $workDir, $logFile, $errFile, $envVars)
        foreach ($key in $envVars.Keys) { 
            Set-Item -Path "env:$key" -Value $envVars[$key] 
        }
        Set-Location $workDir
        & $pythonPath -m datanode.server *> $logFile 2> $errFile
    } -ArgumentList $pythonCmd, $backendRoot, $logFile, $errFile, $envVars

    $global:DATANODE_JOBS += $datanodeJob
    Write-Success "DataNode $i iniciado (Job ID: $($datanodeJob.Id))"
    
    # Esperar que el puerto esté disponible
    $ready = $false
    $tries = 0
    while ($tries -lt $DATANODE_STARTUP_RETRIES -and -not $ready) {
        if (Test-Port -Port $port -TimeoutMs $PORT_CHECK_TIMEOUT) {
            $ready = $true
            Write-Success "DataNode $i escuchando en puerto $port"
        } else {
            Start-Sleep -Milliseconds 500
            $tries++
        }
    }
    
    if (-not $ready) {
        Write-Warning "DataNode $i puede no estar listo"
        Show-Logs -LogPath $errFile -Lines 10
    }
}

# Resumen final
Write-Section "Sistema DFS iniciado correctamente"

Write-Host "`nServicios en ejecución:" -ForegroundColor Green
Write-Host "  Metadata Service: $metadataUrl"

for ($i = 1; $i -le $Nodes; $i++) {
    $port = $BaseDataNodePort + ($i - 1)
    Write-Host "  DataNode $i       : http://127.0.0.1:$port"
}

Write-Host "`nDirectorio de datos:" -ForegroundColor Cyan
Write-Host "  $TempDir"

Write-Host "`nArchivos de log:" -ForegroundColor Cyan
if (-not $WithoutMetadata) {
    Write-Host "  Metadata        : $metadataLog"
    Write-Host "  Metadata Errors : $metadataErrLog"
}

for ($i = 1; $i -le $Nodes; $i++) {
    $logFile = Join-Path $TempDir "dfs-datanode$i.log"
    $errFile = Join-Path $TempDir "dfs-datanode$i-errors.log"
    Write-Host "  DataNode $i      : $logFile"
    Write-Host "  DataNode $i Err  : $errFile"
}

Write-Host "`nComandos útiles:" -ForegroundColor Yellow
Write-Host "  Ver estado    : Get-Job"
Write-Host "  Ver logs DN1  : Get-Content `"$(Join-Path $TempDir "dfs-datanode1.log")`" -Wait"
Write-Host "  Ver errores   : Get-Content `"$(Join-Path $TempDir "dfs-datanode1-errors.log")`" -Wait"
Write-Host "  Detener todo  : Get-Job | Stop-Job | Remove-Job"
Write-Host ""