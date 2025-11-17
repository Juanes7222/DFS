"""
Metadata Service - Servicio principal de metadatos del DFS
"""
import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import List, Optional
from uuid import UUID

from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware

from storage import MetadataStorage
from replicator import ReplicationManager
from metrics import (
    metrics_endpoint,
    MetricsMiddleware,
    update_system_metrics,
    upload_operations_total,
    download_operations_total,
    delete_operations_total,
)

# Importar modelos compartidos
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))
print(sys.path)

from shared import (
    CommitRequest,
    FileMetadata,
    HeartbeatRequest,
    LeaseRequest,
    LeaseResponse,
    NodeInfo,
    UploadInitRequest,
    UploadInitResponse,
    HealthResponse,
)

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuración
REPLICATION_FACTOR = 3
CHUNK_SIZE = 64 * 1024 * 1024  # 64MB


# Lifecycle management
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Gestión del ciclo de vida de la aplicación"""
    logger.info("Iniciando Metadata Service...")
    
    # Inicializar storage
    app.state.storage = MetadataStorage()
    await app.state.storage.initialize()
    
    # Inicializar replication manager
    app.state.replicator = ReplicationManager(app.state.storage, REPLICATION_FACTOR)
    
    # Iniciar background tasks
    replicator_task = asyncio.create_task(app.state.replicator.run())
    
    # Iniciar task de actualización de métricas
    async def metrics_updater():
        while True:
            await update_system_metrics(app.state.storage)
            await asyncio.sleep(10)  # Actualizar cada 10 segundos
    
    metrics_task = asyncio.create_task(metrics_updater())
    
    logger.info("Metadata Service iniciado correctamente")
    
    yield
    
    # Cleanup
    logger.info("Deteniendo Metadata Service...")
    app.state.replicator.stop()
    replicator_task.cancel()
    try:
        await replicator_task
    except asyncio.CancelledError:
        pass
    await app.state.storage.close()
    logger.info("Metadata Service detenido")


# Crear aplicación FastAPI
app = FastAPI(
    title="DFS Metadata Service",
    description="Servicio de metadatos para Sistema de Archivos Distribuido",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # En producción, especificar orígenes permitidos
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Metrics middleware
app.add_middleware(MetricsMiddleware)


# ============================================================================
# ENDPOINTS - FILES
# ============================================================================

@app.post("/api/v1/files/upload-init", response_model=UploadInitResponse)
async def upload_init(request: UploadInitRequest):
    """
    Inicia una subida de archivo.
    Devuelve un plan de chunks con targets para cada réplica.
    """
    logger.info(f"Upload init: {request.path}, size: {request.size}")
    
    storage: MetadataStorage = app.state.storage
    
    # Calcular número de chunks
    num_chunks = (request.size + request.chunk_size - 1) // request.chunk_size
    
    # Obtener nodos activos
    nodes = await storage.get_active_nodes()
    if len(nodes) < REPLICATION_FACTOR:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Nodos insuficientes: {len(nodes)} < {REPLICATION_FACTOR}"
        )
    
    # Crear plan de chunks
    chunks = []
    for i in range(num_chunks):
        chunk_size = min(request.chunk_size, request.size - i * request.chunk_size)
        
        # Seleccionar nodos para réplicas (round-robin simple)
        target_nodes = []
        for j in range(REPLICATION_FACTOR):
            node_idx = (i * REPLICATION_FACTOR + j) % len(nodes)
            node = nodes[node_idx]
            target_nodes.append(f"http://{node.host}:{node.port}")
        
        chunk_target = await storage.create_chunk_plan(chunk_size, target_nodes)
        chunks.append(chunk_target)
    
    # Crear metadata de archivo
    file_metadata = await storage.create_file_metadata(
        path=request.path,
        size=request.size,
        chunks=chunks
    )
    
    return UploadInitResponse(
        file_id=file_metadata.file_id,
        chunks=chunks
    )


@app.post("/api/v1/files/commit")
async def commit_upload(request: CommitRequest):
    """
    Confirma que los chunks han sido subidos correctamente.
    Valida que se hayan creado las réplicas necesarias.
    """
    logger.info(f"Commit upload: file_id={request.file_id}, chunks={len(request.chunks)}")
    
    storage: MetadataStorage = app.state.storage
    
    # Validar que cada chunk tenga suficientes réplicas
    for chunk_info in request.chunks:
        if len(chunk_info.nodes) < REPLICATION_FACTOR:
            logger.warning(
                f"Chunk {chunk_info.chunk_id} solo tiene {len(chunk_info.nodes)} réplicas "
                f"(esperado: {REPLICATION_FACTOR})"
            )
            # En producción, aquí podrías rechazar o intentar crear réplicas faltantes
            # Por ahora, lo permitimos pero loggeamos
    
    # Actualizar metadata con checksums y nodos
    success = await storage.commit_file(request.file_id, request.chunks)
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No se pudo confirmar la subida"
        )
    
    # Contar réplicas totales
    total_replicas = sum(len(c.nodes) for c in request.chunks)
    logger.info(f"Commit exitoso: {len(request.chunks)} chunks, {total_replicas} réplicas totales")
    
    return {
        "status": "committed",
        "file_id": str(request.file_id),
        "chunks": len(request.chunks),
        "total_replicas": total_replicas
    }


@app.get("/api/v1/files/{path:path}", response_model=FileMetadata)
async def get_file_metadata(path: str):
    """
    Obtiene metadata de un archivo.
    """
    storage: MetadataStorage = app.state.storage
    
    file_metadata = await storage.get_file_by_path(path)
    if not file_metadata:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Archivo no encontrado: {path}"
        )
    
    return file_metadata


@app.get("/api/v1/files", response_model=List[FileMetadata])
async def list_files(
    prefix: Optional[str] = None,
    limit: int = 100,
    offset: int = 0
):
    """
    Lista archivos con paginación y filtros opcionales.
    """
    storage: MetadataStorage = app.state.storage
    files = await storage.list_files(prefix=prefix, limit=limit, offset=offset)
    return files


@app.delete("/api/v1/files/{path:path}")
async def delete_file(path: str, permanent: bool = False):
    """
    Elimina un archivo (soft-delete por default).
    """
    logger.info(f"Delete file: {path}, permanent={permanent}")
    
    storage: MetadataStorage = app.state.storage
    
    success = await storage.delete_file(path, permanent=permanent)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Archivo no encontrado: {path}"
        )
    
    return {"status": "deleted", "path": path, "permanent": permanent}


# ============================================================================
# ENDPOINTS - LEASES
# ============================================================================

@app.post("/api/v1/leases/acquire", response_model=LeaseResponse)
async def acquire_lease(request: LeaseRequest):
    """
    Adquiere un lease exclusivo para una operación.
    """
    storage: MetadataStorage = app.state.storage
    
    lease = await storage.acquire_lease(
        path=request.path,
        operation=request.operation,
        timeout_seconds=request.timeout_seconds
    )
    
    if not lease:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"No se pudo adquirir lease para {request.path}"
        )
    
    return lease


@app.post("/api/v1/leases/release")
async def release_lease(lease_id: UUID):
    """
    Libera un lease.
    """
    storage: MetadataStorage = app.state.storage
    
    success = await storage.release_lease(lease_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Lease no encontrado: {lease_id}"
        )
    
    return {"status": "released", "lease_id": str(lease_id)}


# ============================================================================
# ENDPOINTS - NODES
# ============================================================================

@app.post("/api/v1/nodes/heartbeat")
async def node_heartbeat(request: HeartbeatRequest):
    """
    Recibe heartbeat de un DataNode.
    """
    storage: MetadataStorage = app.state.storage
    
    await storage.update_node_heartbeat(
        node_id=request.node_id,
        free_space=request.free_space,
        total_space=request.total_space,
        chunk_ids=request.chunk_ids
    )
    
    return {"status": "ok"}


@app.get("/api/v1/nodes", response_model=List[NodeInfo])
async def list_nodes():
    """
    Lista todos los nodos registrados.
    """
    storage: MetadataStorage = app.state.storage
    nodes = await storage.list_nodes()
    return nodes


@app.get("/api/v1/nodes/{node_id}", response_model=NodeInfo)
async def get_node(node_id: str):
    """
    Obtiene información de un nodo específico.
    """
    storage: MetadataStorage = app.state.storage
    
    node = await storage.get_node(node_id)
    if not node:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Nodo no encontrado: {node_id}"
        )
    
    return node


# ============================================================================
# ENDPOINTS - HEALTH
# ============================================================================

@app.get("/api/v1/health", response_model=HealthResponse)
async def health_check():
    """
    Health check del servicio.
    """
    storage: MetadataStorage = app.state.storage
    
    # Verificar estado de nodos
    nodes = await storage.list_nodes()
    active_nodes = [n for n in nodes if n.state.value == "active"]
    
    return HealthResponse(
        status="healthy" if len(active_nodes) >= REPLICATION_FACTOR else "degraded",
        details={
            "total_nodes": len(nodes),
            "active_nodes": len(active_nodes),
            "replication_factor": REPLICATION_FACTOR,
        }
    )


@app.get("/")
async def root():
    """Endpoint raíz"""
    return {
        "service": "DFS Metadata Service",
        "version": "1.0.0",
        "status": "running"
    }


@app.get("/metrics")
async def metrics():
    """Endpoint de métricas Prometheus"""
    return metrics_endpoint()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
