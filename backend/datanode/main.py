"""
DataNode - Nodo de almacenamiento del DFS
"""
import asyncio
import logging
import os
import shutil
from pathlib import Path
from typing import Optional
from uuid import UUID

from fastapi import FastAPI, HTTPException, UploadFile, status, Query
from fastapi.responses import StreamingResponse
import httpx

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from shared import (
    HeartbeatRequest,
    HealthResponse,
    calculate_checksum,
)
from metrics import (
    metrics_endpoint,
    MetricsMiddleware,
    update_storage_metrics,
    chunk_read_operations_total,
    chunk_write_operations_total,
    chunk_delete_operations_total,
    bytes_read_total,
    bytes_written_total,
    heartbeat_sent_total,
    heartbeat_failed_total,
)

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuración
NODE_ID = os.getenv("NODE_ID", "node-localhost-8001")
STORAGE_PATH = Path(os.getenv("STORAGE_PATH", "/tmp/dfs_data"))
METADATA_SERVICE_URL = os.getenv("METADATA_SERVICE_URL", "http://localhost:8000")
HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", "10"))  # segundos

# Crear directorio de almacenamiento
STORAGE_PATH.mkdir(parents=True, exist_ok=True)


# Crear aplicación FastAPI
app = FastAPI(
    title="DFS DataNode",
    description="Nodo de almacenamiento para Sistema de Archivos Distribuido",
    version="1.0.0",
)

# Metrics middleware
app.add_middleware(MetricsMiddleware)


# Background task para heartbeats
async def send_heartbeats():
    """Envía heartbeats periódicos al Metadata Service"""
    logger.info("Iniciando heartbeat loop...")
    
    async with httpx.AsyncClient() as client:
        while True:
            try:
                # Obtener información del nodo
                stat = shutil.disk_usage(STORAGE_PATH)
                
                # Listar chunks almacenados
                chunk_ids = []
                if STORAGE_PATH.exists():
                    for chunk_file in STORAGE_PATH.glob("*.chunk"):
                        try:
                            chunk_id = UUID(chunk_file.stem)
                            chunk_ids.append(chunk_id)
                        except ValueError:
                            pass
                
                # Enviar heartbeat
                heartbeat = HeartbeatRequest(
                    node_id=NODE_ID,
                    free_space=stat.free,
                    total_space=stat.total,
                    chunk_ids=chunk_ids
                )
                
                response = await client.post(
                    f"{METADATA_SERVICE_URL}/api/v1/nodes/heartbeat",
                    json=heartbeat.model_dump(mode='json'),
                    timeout=5.0
                )
                
                if response.status_code == 200:
                    logger.debug(f"Heartbeat enviado: {len(chunk_ids)} chunks")
                    heartbeat_sent_total.inc()
                else:
                    logger.warning(f"Heartbeat falló: {response.status_code}")
                    heartbeat_failed_total.inc()
                
            except Exception as e:
                logger.error(f"Error enviando heartbeat: {e}")
                heartbeat_failed_total.inc()
            
            await asyncio.sleep(HEARTBEAT_INTERVAL)


@app.on_event("startup")
async def startup_event():
    """Inicia background tasks"""
    logger.info(f"DataNode iniciado: {NODE_ID}")
    logger.info(f"Storage path: {STORAGE_PATH}")
    logger.info(f"Metadata service: {METADATA_SERVICE_URL}")
    
    # Iniciar heartbeat loop
    asyncio.create_task(send_heartbeats())
    
    # Iniciar task de actualización de métricas de storage
    async def metrics_updater():
        while True:
            update_storage_metrics(str(STORAGE_PATH))
            await asyncio.sleep(10)  # Actualizar cada 10 segundos
    
    asyncio.create_task(metrics_updater())


# ============================================================================
# ENDPOINTS - CHUNKS
# ============================================================================

@app.put("/api/v1/chunks/{chunk_id}")
async def put_chunk(
    chunk_id: UUID, 
    file: UploadFile,
    replicate_to: Optional[str] = Query(None, description="URL del siguiente nodo en el pipeline")
):
    """
    Almacena un chunk con replicación en pipeline.
    
    Pipeline replication: El cliente sube al primer nodo, este nodo guarda 
    localmente y luego replica al siguiente nodo de la cadena.
    """
    logger.info(f"Recibiendo chunk: {chunk_id}, replicate_to: {replicate_to}")
    
    chunk_path = STORAGE_PATH / f"{chunk_id}.chunk"
    checksum_path = STORAGE_PATH / f"{chunk_id}.checksum"
    
    try:
        # Leer chunk
        chunk_data = await file.read()
        
        # Calcular checksum
        checksum = calculate_checksum(chunk_data)
        
        # Guardar chunk localmente PRIMERO
        with open(chunk_path, 'wb') as f:
            f.write(chunk_data)
        
        # Guardar checksum
        with open(checksum_path, 'w') as f:
            f.write(checksum)
        
        logger.info(f"Chunk almacenado localmente: {chunk_id}, size: {len(chunk_data)}, checksum: {checksum}")
        
        # Pipeline replication: replicar al siguiente nodo si está especificado
        replicated_nodes = [NODE_ID]  # Este nodo
        
        if replicate_to:
            try:
                logger.info(f"Replicando chunk {chunk_id} a {replicate_to}")
                async with httpx.AsyncClient(timeout=60.0) as client:
                    files = {'file': ('chunk', chunk_data, 'application/octet-stream')}
                    
                    # Extraer siguiente nodo si hay más en la cadena
                    next_nodes = replicate_to.split('|')
                    current_target = next_nodes[0]
                    remaining_chain = '|'.join(next_nodes[1:]) if len(next_nodes) > 1 else None
                    
                    params = {}
                    if remaining_chain:
                        params['replicate_to'] = remaining_chain
                    
                    response = await client.put(
                        f"{current_target}/api/v1/chunks/{chunk_id}",
                        files=files,
                        params=params,
                        timeout=60.0
                    )
                    
                    if response.status_code == 200:
                        result = response.json()
                        # Agregar nodos downstream que confirmaron escritura
                        downstream_nodes = result.get('nodes', [])
                        replicated_nodes.extend(downstream_nodes)
                        logger.info(f"Replicación exitosa, total nodos: {len(replicated_nodes)}")
                    else:
                        logger.error(f"Error replicando a {current_target}: {response.status_code}")
                        # No fallar si la replicación falla, el metadata service se encargará
            except Exception as e:
                logger.error(f"Excepción replicando a {replicate_to}: {e}")
                # No fallar, continuar con éxito local
        
        # Métricas
        chunk_write_operations_total.labels(status="success").inc()
        bytes_written_total.inc(len(chunk_data))
        
        return {
            "status": "stored",
            "chunk_id": str(chunk_id),
            "size": len(chunk_data),
            "checksum": checksum,
            "node_id": NODE_ID,
            "nodes": replicated_nodes  # Lista de todos los nodos que almacenaron
        }
    
    except Exception as e:
        logger.error(f"Error almacenando chunk {chunk_id}: {e}")
        chunk_write_operations_total.labels(status="error").inc()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error almacenando chunk: {str(e)}"
        )


@app.get("/api/v1/chunks/{chunk_id}")
async def get_chunk(chunk_id: UUID):
    """
    Recupera un chunk.
    Devuelve el chunk via streaming.
    """
    logger.info(f"Sirviendo chunk: {chunk_id}")
    
    chunk_path = STORAGE_PATH / f"{chunk_id}.chunk"
    checksum_path = STORAGE_PATH / f"{chunk_id}.checksum"
    
    if not chunk_path.exists():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Chunk no encontrado: {chunk_id}"
        )
    
    # Verificar checksum
    try:
        with open(chunk_path, 'rb') as f:
            chunk_data = f.read()
        
        calculated_checksum = calculate_checksum(chunk_data)
        
        if checksum_path.exists():
            with open(checksum_path, 'r') as f:
                stored_checksum = f.read().strip()
            
            if calculated_checksum != stored_checksum:
                logger.error(f"Checksum mismatch para chunk {chunk_id}")
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Checksum verification failed"
                )
        
        # Métricas
        chunk_read_operations_total.labels(status="success").inc()
        bytes_read_total.inc(len(chunk_data))
        
        # Streaming response
        async def chunk_generator():
            yield chunk_data
        
        return StreamingResponse(
            chunk_generator(),
            media_type="application/octet-stream",
            headers={
                "X-Chunk-ID": str(chunk_id),
                "X-Checksum": calculated_checksum,
                "Content-Length": str(len(chunk_data))
            }
        )
    
    except HTTPException:
        chunk_read_operations_total.labels(status="error").inc()
        raise
    except Exception as e:
        logger.error(f"Error sirviendo chunk {chunk_id}: {e}")
        chunk_read_operations_total.labels(status="error").inc()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error leyendo chunk: {str(e)}"
        )


@app.delete("/api/v1/chunks/{chunk_id}")
async def delete_chunk(chunk_id: UUID):
    """
    Elimina un chunk.
    """
    logger.info(f"Eliminando chunk: {chunk_id}")
    
    chunk_path = STORAGE_PATH / f"{chunk_id}.chunk"
    checksum_path = STORAGE_PATH / f"{chunk_id}.checksum"
    
    deleted = False
    
    if chunk_path.exists():
        chunk_path.unlink()
        deleted = True
    
    if checksum_path.exists():
        checksum_path.unlink()
    
    if not deleted:
        chunk_delete_operations_total.labels(status="not_found").inc()
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Chunk no encontrado: {chunk_id}"
        )
    
    chunk_delete_operations_total.labels(status="success").inc()
    return {"status": "deleted", "chunk_id": str(chunk_id)}


# ============================================================================
# ENDPOINTS - HEALTH
# ============================================================================

@app.get("/api/v1/health", response_model=HealthResponse)
async def health_check():
    """
    Health check del DataNode.
    """
    stat = shutil.disk_usage(STORAGE_PATH)
    
    # Contar chunks
    chunk_count = len(list(STORAGE_PATH.glob("*.chunk")))
    
    return HealthResponse(
        status="healthy",
        details={
            "node_id": NODE_ID,
            "storage_path": str(STORAGE_PATH),
            "total_space": stat.total,
            "free_space": stat.free,
            "used_space": stat.used,
            "chunk_count": chunk_count,
        }
    )


@app.get("/")
async def root():
    """Endpoint raíz"""
    return {
        "service": "DFS DataNode",
        "version": "1.0.0",
        "node_id": NODE_ID,
        "status": "running"
    }


@app.get("/metrics")
async def metrics():
    """Endpoint de métricas Prometheus"""
    return metrics_endpoint()


# ============================================================================
# SCRUBBING (Background task)
# ============================================================================

async def scrub_chunks():
    """
    Verifica checksums de chunks en background.
    En un sistema real, esto correría periódicamente.
    """
    logger.info("Iniciando scrubbing de chunks...")
    
    for chunk_path in STORAGE_PATH.glob("*.chunk"):
        try:
            chunk_id = UUID(chunk_path.stem)
            checksum_path = STORAGE_PATH / f"{chunk_id}.checksum"
            
            if not checksum_path.exists():
                logger.warning(f"Checksum faltante para chunk {chunk_id}")
                continue
            
            # Leer chunk y checksum
            with open(chunk_path, 'rb') as f:
                chunk_data = f.read()
            
            with open(checksum_path, 'r') as f:
                stored_checksum = f.read().strip()
            
            # Verificar
            calculated_checksum = calculate_checksum(chunk_data)
            
            if calculated_checksum != stored_checksum:
                logger.error(f"Corrupción detectada en chunk {chunk_id}")
                # En un sistema real, se reportaría al Metadata Service
            else:
                logger.debug(f"Chunk {chunk_id} verificado OK")
        
        except Exception as e:
            logger.error(f"Error en scrubbing de {chunk_path}: {e}")


if __name__ == "__main__":
    import uvicorn
    
    # Extraer puerto del NODE_ID si está presente
    port = 8001
    if NODE_ID:
        parts = NODE_ID.split('-')
        if len(parts) > 2:
            try:
                port = int(parts[2])
            except ValueError:
                pass
    
    uvicorn.run(app, host="0.0.0.0", port=port)
