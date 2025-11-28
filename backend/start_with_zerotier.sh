#!/bin/bash
set -e

echo "================================================"
echo "  DFS Metadata Service - Starting with ZeroTier"
echo "================================================"

# Variables de entorno
ZEROTIER_NETWORK_ID=${ZEROTIER_NETWORK_ID:-}
ZEROTIER_API_TOKEN=${ZEROTIER_API_TOKEN:-}
PORT=${PORT:-8000}

# Función para logging con timestamp
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1"
}

# Función para obtener la IP de ZeroTier
get_zerotier_ip() {
    local network_id=$1
    local max_retries=30
    local retry=0
    
    log "Esperando asignación de IP de ZeroTier..."
    
    while [ $retry -lt $max_retries ]; do
        sleep 2
        
        # Intentar obtener la IP asignada
        local zt_ip=$(zerotier-cli listnetworks 2>/dev/null | grep "$network_id" | awk '{print $9}' | cut -d'/' -f1)
        
        if [ ! -z "$zt_ip" ] && [ "$zt_ip" != "-" ]; then
            echo "$zt_ip"
            return 0
        fi
        
        retry=$((retry+1))
        log "  Intento $retry/$max_retries..."
    done
    
    log "ERROR: No se pudo obtener IP después de $max_retries intentos"
    return 1
}

# Auto-autorizar el nodo en ZeroTier Central
authorize_node() {
    local network_id=$1
    local node_id=$2
    local api_token=$3
    
    log "Autorizando nodo $node_id en ZeroTier Central..."
    
    local response=$(curl -s -X POST \
        "https://my.zerotier.com/api/v1/network/${network_id}/member/${node_id}" \
        -H "Authorization: token ${api_token}" \
        -H "Content-Type: application/json" \
        -d "{\"config\": {\"authorized\": true}, \"name\": \"Railway-Metadata-${node_id:0:8}\"}")
    
    if echo "$response" | grep -q '"authorized":true'; then
        log "  Nodo autorizado correctamente"
        return 0
    else
        log "  Advertencia: Respuesta inesperada de la API"
        log "  Puede que necesites autorizar manualmente en https://my.zerotier.com"
        return 1
    fi
}

# Iniciar ZeroTier si está configurado
if [ ! -z "$ZEROTIER_NETWORK_ID" ]; then
    log "Iniciando servicio ZeroTier..."
    
    # Crear directorio para ZeroTier si no existe
    mkdir -p /var/lib/zerotier-one
    
    # Iniciar daemon de ZeroTier
    zerotier-one -d
    
    # Esperar a que el daemon inicie
    sleep 3
    
    # Verificar que el daemon esté corriendo
    if ! zerotier-cli info &> /dev/null; then
        log "ERROR: ZeroTier daemon no pudo iniciarse"
        log "Continuando sin ZeroTier..."
    else
        log "  ZeroTier daemon iniciado"
        
        # Obtener Node ID
        ZEROTIER_NODE_ID=$(zerotier-cli info | awk '{print $3}')
        log "  Node ID: $ZEROTIER_NODE_ID"
        
        # Unirse a la red
        log "Uniéndose a la red ZeroTier: $ZEROTIER_NETWORK_ID"
        zerotier-cli join "$ZEROTIER_NETWORK_ID"
        sleep 2
        
        # Auto-autorizar si se proporciona el token
        if [ ! -z "$ZEROTIER_API_TOKEN" ]; then
            authorize_node "$ZEROTIER_NETWORK_ID" "$ZEROTIER_NODE_ID" "$ZEROTIER_API_TOKEN"
        else
            log "  ZEROTIER_API_TOKEN no configurado"
            log "  Debes autorizar manualmente el nodo en: https://my.zerotier.com"
            log "  Node ID para autorizar: $ZEROTIER_NODE_ID"
        fi
        
        # Obtener IP asignada
        ZEROTIER_IP=$(get_zerotier_ip "$ZEROTIER_NETWORK_ID")
        
        if [ ! -z "$ZEROTIER_IP" ]; then
            log "  ZeroTier configurado correctamente"
            log "  Network ID: $ZEROTIER_NETWORK_ID"
            log "  Node ID: $ZEROTIER_NODE_ID"
            log "  IP ZeroTier: $ZEROTIER_IP"
            
            # Exportar IP para que la aplicación la use
            export ZEROTIER_IP="$ZEROTIER_IP"
            
            # Mostrar estado
            log ""
            log "Estado de redes ZeroTier:"
            zerotier-cli listnetworks
        else
            log "⚠ No se pudo obtener IP de ZeroTier"
            log "  Verifica la autorización en: https://my.zerotier.com"
        fi
    fi
else
    log "ZEROTIER_NETWORK_ID no configurado - Omitiendo ZeroTier"
fi

log ""
log "================================================"
log "  Iniciando servidor Uvicorn en puerto $PORT"
log "================================================"
log ""

# Iniciar el servidor
exec uvicorn metadata.server:app \
    --host 0.0.0.0 \
    --port $PORT \
    --log-level info