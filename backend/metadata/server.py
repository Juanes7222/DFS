"""
Metadata Service - Servicio principal de metadatos del DFS
Versión refactorizada para estructura backend/
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import List, Optional
from uuid import UUID

from fastapi import FastAPI, HTTPException, status, Query
from fastapi.middleware.cors import CORSMiddleware

from core.config import config
from backend.metadata.storage import MetadataStorage
from backend.metadata.replicator import ReplicationManager
from backend.metadata.leases import LeaseManager
from backend.monitoring.metrics import (
    metrics_endpoint,
    MetricsMiddleware,
    update_system_metrics,
    record_upload_operation,
    record_download_operation,
    record_delete_operation,
    update_lease_metrics,
)
from backend.shared import (
    CommitRequest,
    FileMetadata,
    HeartbeatRequest,
    LeaseRequest,
    LeaseResponse,
    NodeInfo,
    UploadInitRequest,
    UploadInitResponse,
    HealthResponse,
    SystemStats,
    format_bytes,
)

# Configurar logging
logger = logging.getLogger(__name__)

# Global instances
storage: Optional[MetadataStorage] = None
replicator: Optional[ReplicationManager] = None
lease_manager: Optional[LeaseManager] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Gestión del ciclo de vida de la aplicación"""
    global storage, replicator, lease_manager

    logger.info("🚀 Iniciando Metadata Service...")

    try:
        # Inicializar storage
        storage = MetadataStorage()
        await storage.initialize()

        # Inicializar replication manager
        replicator = ReplicationManager(storage, config.replication_factor)

        # Inicializar lease manager
        lease_manager = LeaseManager(storage)

        # Iniciar background tasks
        await replicator.start()

        # Iniciar task de actualización de métricas
        async def metrics_updater():
            while True:
                try:
                    await update_system_metrics(storage)

                    # Actualizar métricas de leases
                    if lease_manager:
                        lease_stats = lease_manager.get_lease_stats()
                        update_lease_metrics(lease_stats["active_leases"])

                    await asyncio.sleep(10)  # Actualizar cada 10 segundos
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Error en metrics updater: {e}")
                    await asyncio.sleep(30)  # Esperar más en caso de error

        metrics_task = asyncio.create_task(metrics_updater())

        logger.info("Metadata Service iniciado correctamente")

        yield

    except Exception as e:
        logger.error(f"Error iniciando Metadata Service: {e}")
        raise

    finally:
        # Cleanup
        logger.info("🛑 Deteniendo Metadata Service...")

        if replicator:
            await replicator.stop()

        if storage:
            await storage.close()

        logger.info("Metadata Service detenido correctamente")


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
    logger.info(f"📤 Upload init: {request.path}, size: {format_bytes(request.size)}")

    if not storage:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Storage no inicializado",
        )

    try:
        # Calcular número de chunks
        num_chunks = (request.size + request.chunk_size - 1) // request.chunk_size

        # Obtener nodos activos
        nodes = await storage.get_active_nodes()
        if len(nodes) < config.replication_factor:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Nodos insuficientes: {len(nodes)} < {config.replication_factor}",
            )

        logger.info(f"📊 Planificando {num_chunks} chunks para {request.path}")

        # Crear plan de chunks
        chunks = []
        for i in range(num_chunks):
            chunk_size = min(request.chunk_size, request.size - i * request.chunk_size)

            # Seleccionar nodos para réplicas (estrategia round-robin)
            target_nodes = []
            for j in range(config.replication_factor):
                node_idx = (i * config.replication_factor + j) % len(nodes)
                node = nodes[node_idx]
                target_nodes.append(f"http://{node.host}:{node.port}")

            chunk_target = await storage.create_chunk_plan(chunk_size, target_nodes)
            chunks.append(chunk_target)

        # Crear metadata de archivo
        file_metadata = await storage.create_file_metadata(
            path=request.path, size=request.size, chunks=chunks
        )

        logger.info(
            f"Upload init exitoso: {request.path} (ID: {file_metadata.file_id})"
        )

        return UploadInitResponse(file_id=file_metadata.file_id, chunks=chunks)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error en upload-init: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error interno del servidor: {str(e)}",
        )


@app.post("/api/v1/files/commit")
async def commit_upload(request: CommitRequest):
    """
    Confirma que los chunks han sido subidos correctamente.
    Valida que se hayan creado las réplicas necesarias.
    """
    logger.info(
        f"Commit upload: file_id={request.file_id}, chunks={len(request.chunks)}"
    )

    if not storage:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Storage no inicializado",
        )

    try:
        # Validar que cada chunk tenga suficientes réplicas
        under_replicated_chunks = []
        for chunk_info in request.chunks:
            if len(chunk_info.nodes) < config.replication_factor:
                under_replicated_chunks.append(
                    {
                        "chunk_id": chunk_info.chunk_id,
                        "current_replicas": len(chunk_info.nodes),
                        "expected_replicas": config.replication_factor,
                    }
                )

        if under_replicated_chunks:
            logger.warning(
                f"⚠️  Chunks con replicación insuficiente: {under_replicated_chunks}"
            )

        # Actualizar metadata con checksums y nodos
        success = await storage.commit_file(request.file_id, request.chunks)

        if not success:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No se pudo confirmar la subida",
            )

        # Registrar métrica
        record_upload_operation(True)

        # Contar réplicas totales
        total_replicas = sum(len(c.nodes) for c in request.chunks)
        logger.info(
            f"Commit exitoso: {len(request.chunks)} chunks, "
            f"{total_replicas} réplicas totales"
        )

        return {
            "status": "committed",
            "file_id": str(request.file_id),
            "chunks": len(request.chunks),
            "total_replicas": total_replicas,
            "under_replicated_chunks": under_replicated_chunks,
        }

    except HTTPException:
        record_upload_operation(False)
        raise
    except Exception as e:
        logger.error(f"Error en commit: {e}")
        record_upload_operation(False)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error interno del servidor: {str(e)}",
        )


@app.get("/api/v1/files/{path:path}", response_model=FileMetadata)
async def get_file_metadata(path: str):
    """
    Obtiene metadata de un archivo.
    """
    logger.debug(f"📁 Get file metadata: {path}")

    if not storage:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Storage no inicializado",
        )

    try:
        file_metadata = await storage.get_file_by_path(path)
        if not file_metadata:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Archivo no encontrado: {path}",
            )

        record_download_operation(True)
        return file_metadata

    except HTTPException:
        record_download_operation(False)
        raise
    except Exception as e:
        logger.error(f"Error obteniendo metadata: {e}")
        record_download_operation(False)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error interno del servidor: {str(e)}",
        )


@app.get("/api/v1/files", response_model=List[FileMetadata])
async def list_files(
    prefix: Optional[str] = Query(None, description="Filtrar por prefijo"),
    limit: int = Query(100, description="Límite de resultados", ge=1, le=1000),
    offset: int = Query(0, description="Offset para paginación", ge=0),
):
    """
    Lista archivos con paginación y filtros opcionales.
    """
    logger.debug(f"List files: prefix={prefix}, limit={limit}, offset={offset}")

    if not storage:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Storage no inicializado",
        )

    try:
        files = await storage.list_files(prefix=prefix, limit=limit, offset=offset)
        return files

    except Exception as e:
        logger.error(f"Error listando archivos: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error interno del servidor: {str(e)}",
        )


@app.delete("/api/v1/files/{path:path}")
async def delete_file(
    path: str, permanent: bool = Query(False, description="Eliminar permanentemente")
):
    """
    Elimina un archivo (soft-delete por default).
    """
    logger.info(f"🗑️  Delete file: {path}, permanent={permanent}")

    if not storage:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Storage no inicializado",
        )

    try:
        # Adquirir lease para operación de eliminación
        if lease_manager:
            lease = await lease_manager.acquire_lease(
                path=path, operation="delete", timeout_seconds=300
            )
        else:
            lease = None

        try:
            success = await storage.delete_file(path, permanent=permanent)
            if not success:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Archivo no encontrado: {path}",
                )

            record_delete_operation(True)

            action = (
                "eliminado permanentemente" if permanent else "marcado como eliminado"
            )
            logger.info(f"Archivo {action}: {path}")

            return {
                "status": "deleted",
                "path": path,
                "permanent": permanent,
                "action": action,
            }

        finally:
            # Liberar lease
            if lease and lease_manager:
                await lease_manager.release_lease(lease.lease_id, path)

    except HTTPException:
        record_delete_operation(False)
        raise
    except Exception as e:
        logger.error(f"Error eliminando archivo: {e}")
        record_delete_operation(False)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error interno del servidor: {str(e)}",
        )


# ============================================================================
# ENDPOINTS - LEASES
# ============================================================================


@app.post("/api/v1/leases/acquire", response_model=LeaseResponse)
async def acquire_lease(request: LeaseRequest):
    """
    Adquiere un lease exclusivo para una operación.
    """
    logger.debug(f"🔒 Acquire lease: {request.path}, operation={request.operation}")

    if not lease_manager:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Lease manager no inicializado",
        )

    try:
        lease = await lease_manager.acquire_lease(
            path=request.path,
            operation=request.operation,
            timeout_seconds=request.timeout_seconds,
        )

        if not lease:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"No se pudo adquirir lease para {request.path}",
            )

        return lease

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error adquiriendo lease: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error interno del servidor: {str(e)}",
        )


@app.post("/api/v1/leases/release")
async def release_lease(lease_id: UUID):
    """
    Libera un lease.
    """
    logger.debug(f"🔓 Release lease: {lease_id}")

    if not lease_manager:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Lease manager no inicializado",
        )

    try:
        success = await lease_manager.release_lease(lease_id)
        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Lease no encontrado: {lease_id}",
            )

        return {"status": "released", "lease_id": str(lease_id)}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error liberando lease: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error interno del servidor: {str(e)}",
        )


# ============================================================================
# ENDPOINTS - NODES
# ============================================================================


@app.post("/api/v1/nodes/heartbeat")
async def node_heartbeat(request: HeartbeatRequest):
    """
    Recibe heartbeat de un DataNode.
    """
    logger.debug(f"Heartbeat: {request.node_id}, chunks={len(request.chunk_ids)}")

    if not storage:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Storage no inicializado",
        )

    try:
        await storage.update_node_heartbeat(
            node_id=request.node_id,
            free_space=request.free_space,
            total_space=request.total_space,
            chunk_ids=request.chunk_ids,
        )

        return {"status": "ok", "node_id": request.node_id}

    except Exception as e:
        logger.error(f"Error procesando heartbeat: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error interno del servidor: {str(e)}",
        )


@app.get("/api/v1/nodes", response_model=List[NodeInfo])
async def list_nodes():
    """
    Lista todos los nodos registrados.
    """
    logger.debug("List nodes")

    if not storage:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Storage no inicializado",
        )

    try:
        nodes = await storage.list_nodes()
        return nodes

    except Exception as e:
        logger.error(f"Error listando nodos: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error interno del servidor: {str(e)}",
        )


@app.get("/api/v1/nodes/{node_id}", response_model=NodeInfo)
async def get_node(node_id: str):
    """
    Obtiene información de un nodo específico.
    """
    logger.debug(f"🔍 Get node: {node_id}")

    if not storage:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Storage no inicializado",
        )

    try:
        node = await storage.get_node(node_id)
        if not node:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Nodo no encontrado: {node_id}",
            )

        return node

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error obteniendo nodo: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error interno del servidor: {str(e)}",
        )


# ============================================================================
# ENDPOINTS - HEALTH & SYSTEM
# ============================================================================


@app.get("/api/v1/health", response_model=HealthResponse)
async def health_check():
    """
    Health check del servicio.
    """
    if not storage:
        return HealthResponse(
            status="unhealthy", details={"error": "Storage no inicializado"}
        )

    try:
        # Verificar estado de nodos
        nodes = await storage.list_nodes()
        active_nodes = [n for n in nodes if n.state.value == "active"]

        status = "healthy"
        if len(active_nodes) < config.replication_factor:
            status = "degraded"
        elif len(active_nodes) == 0:
            status = "unhealthy"

        details = {
            "total_nodes": len(nodes),
            "active_nodes": len(active_nodes),
            "replication_factor": config.replication_factor,
            "timestamp": datetime.utcnow().isoformat(),
        }

        # Agregar stats del sistema si es posible
        try:
            stats = await storage.get_system_stats()
            details.update(stats)
        except Exception as e:
            logger.warning(f"No se pudieron obtener stats del sistema: {e}")

        return HealthResponse(status=status, details=details)

    except Exception as e:
        logger.error(f"Error en health check: {e}")
        return HealthResponse(status="unhealthy", details={"error": str(e)})


@app.get("/api/v1/stats")
async def get_system_stats():
    """
    Obtiene estadísticas detalladas del sistema.
    """
    if not storage:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Storage no inicializado",
        )

    try:
        stats = await storage.get_system_stats()

        # Agregar información de replicación
        if replicator:
            replication_stats = replicator.get_stats()
            stats["replication"] = replication_stats

        # Agregar información de leases
        if lease_manager:
            lease_stats = lease_manager.get_lease_stats()
            stats["leases"] = lease_stats

        return SystemStats(**stats)

    except Exception as e:
        logger.error(f"Error obteniendo stats del sistema: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error interno del servidor: {str(e)}",
        )


# ============================================================================
# ENDPOINTS - ROOT & MONITORING
# ============================================================================


@app.get("/")
async def root():
    """Endpoint raíz"""
    return {
        "service": "DFS Metadata Service",
        "version": "1.0.0",
        "status": "running",
        "timestamp": datetime.utcnow().isoformat(),
        "endpoints": {
            "api_docs": "/docs",
            "health": "/api/v1/health",
            "metrics": "/metrics",
            "files": "/api/v1/files",
            "nodes": "/api/v1/nodes",
        },
    }


@app.get("/health")
async def simple_health():
    """Health check simple (para load balancers)"""
    health_data = await health_check()
    return health_data


@app.get("/metrics")
async def metrics():
    """Endpoint de métricas Prometheus"""
    return metrics_endpoint()


# ============================================================================
# UTILITIES
# ============================================================================


def main():
    """Función principal para ejecutar el servidor"""
    import uvicorn

    logger.info(
        f"🚀 Iniciando Metadata Service en {config.metadata_host}:{config.metadata_port}"
    )
    logger.info(
        f"📊 Configuración: {config.replication_factor} réplicas, chunks de {config.chunk_size} bytes"
    )

    uvicorn.run(
        app,
        host=config.metadata_host,
        port=config.metadata_port,
        log_level="info",
        access_log=True,
    )


if __name__ == "__main__":
    main()
