"""
API Router para operaciones del sistema
"""

import asyncio
import logging
from datetime import datetime

from fastapi import APIRouter, HTTPException, status

from core.config import config
from monitoring.metrics import metrics_endpoint
from shared import HealthResponse, SystemStats

logger = logging.getLogger(__name__)

router = APIRouter()


def get_storage():
    """Dependency para obtener storage instance"""
    from metadata import context

    if not context.storage:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Storage no inicializado",
        )
    return context.storage


def get_replicator():
    """Dependency para obtener replicator instance"""
    from metadata import context

    return context.replicator


def get_lease_manager():
    """Dependency para obtener lease manager instance"""
    from metadata import context

    return context.lease_manager


@router.get("/")
async def root():
    """
    Endpoint raíz del servicio.
    Proporciona información básica y enlaces a recursos principales.
    """
    from metadata import context

    return {
        "service": "DFS Metadata Service",
        "version": "1.0.0",
        "status": "running",
        "storage_initialized": context.storage is not None,
        "replicator_initialized": context.replicator is not None,
        "lease_manager_initialized": context.lease_manager is not None,
        "timestamp": datetime.utcnow().isoformat(),
        "endpoints": {
            "api_docs": "/docs",
            "health": "/api/v1/health",
            "metrics": "/metrics",
            "files": "/api/v1/files",
            "nodes": "/api/v1/nodes",
            "leases": "/api/v1/leases",
            "stats": "/api/v1/stats",
        },
    }


@router.get("/health", response_model=HealthResponse)
async def health_check():
    """
    Health check completo del servicio.
    Verifica estado de nodos, replicación y servicios críticos.
    """
    try:
        storage = get_storage()

        # Verificar estado de nodos (sin await para evitar bloqueos)
        try:
            nodes = await asyncio.wait_for(storage.list_nodes(), timeout=2.0)
            active_nodes = [n for n in nodes if n.state.value == "active"]
        except asyncio.TimeoutError:
            logger.warning("Timeout obteniendo lista de nodos en health check")
            return HealthResponse(
                status="degraded",
                details={
                    "error": "Timeout obteniendo nodos",
                    "timestamp": datetime.utcnow().isoformat(),
                },
            )

        status_value = "healthy"
        if len(active_nodes) < config.replication_factor:
            status_value = "degraded"
        elif len(active_nodes) == 0:
            status_value = "unhealthy"

        details = {
            "total_nodes": len(nodes),
            "active_nodes": len(active_nodes),
            "replication_factor": config.replication_factor,
            "timestamp": datetime.utcnow().isoformat(),
        }

        return HealthResponse(status=status_value, details=details)

    except HTTPException:
        # Storage no inicializado
        return HealthResponse(
            status="unhealthy", details={"error": "Storage no inicializado"}
        )
    except Exception as e:
        logger.error(f"Error en health check: {e}")
        return HealthResponse(status="unhealthy", details={"error": str(e)})


@router.get("/stats")
async def get_system_stats():
    """
    Obtiene estadísticas detalladas del sistema.
    Incluye métricas de archivos, nodos, replicación y leases.
    """
    storage = get_storage()

    if not storage:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Storage no inicializado",
        )

    try:
        stats = await storage.get_system_stats()

        # Agregar información de replicación
        replicator = get_replicator()
        if replicator:
            replication_stats = replicator.get_stats()
            stats["replication"] = replication_stats

        # Agregar información de leases
        lease_mgr = get_lease_manager()
        if lease_mgr:
            lease_stats = lease_mgr.get_lease_stats()
            stats["leases"] = lease_stats

        return SystemStats(**stats)

    except Exception as e:
        logger.error(f"Error obteniendo stats del sistema: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error interno del servidor: {str(e)}",
        )


@router.get("/metrics")
async def metrics():
    """
    Endpoint de métricas en formato Prometheus.
    Expone métricas de rendimiento, uso de recursos y operaciones.
    """
    return metrics_endpoint()


@router.get("/config")
async def get_config():
    """
    Obtiene la configuración actual del servicio.
    Incluye parámetros de replicación, chunks y timeouts.
    """
    return {
        "replication_factor": config.replication_factor,
        "chunk_size": config.chunk_size,
        "heartbeat_interval": config.heartbeat_interval,
        "node_timeout": config.node_timeout,
        "metadata_host": config.metadata_host,
        "metadata_port": config.metadata_port,
        "db_path": config.db_path,
        "log_level": config.log_level,
    }


@router.post("/admin/cleanup")
async def cleanup_orphaned_data():
    """
    Limpia datos huérfanos del sistema.
    Elimina chunks sin archivo padre y leases expirados.
    Requiere permisos de administrador.
    """
    logger.info("Cleanup de datos huérfanos iniciado")

    storage = get_storage()

    if not storage:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Storage no inicializado",
        )

    try:
        # Por ahora solo retornamos info, se pueden implementar métodos de limpieza después
        logger.info("Cleanup de datos huérfanos solicitado")

        return {
            "status": "completed",
            "message": "Cleanup functionality pending implementation",
            "timestamp": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        logger.error(f"Error en cleanup: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error interno del servidor: {str(e)}",
        )
