"""
Sistema de métricas y monitoreo - Versión completa
"""

import time
from typing import Dict, Any

from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    generate_latest,
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
)
from fastapi import Response


# Registry global para métricas
registry = CollectorRegistry()

# ============================================================================
# MÉTRICAS DEL SISTEMA
# ============================================================================

# Métricas de HTTP
http_requests_total = Counter(
    "dfs_http_requests_total",
    "Total HTTP requests",
    ["method", "endpoint", "status"],
    registry=registry,
)

http_request_duration_seconds = Histogram(
    "dfs_http_request_duration_seconds",
    "HTTP request duration in seconds",
    ["method", "endpoint"],
    registry=registry,
    buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 5.0],
)

# Métricas de archivos
files_total = Gauge("dfs_files_total", "Total number of files", registry=registry)

files_deleted_total = Gauge(
    "dfs_files_deleted_total", "Total number of deleted files", registry=registry
)

chunks_total = Gauge("dfs_chunks_total", "Total number of chunks", registry=registry)

# Métricas de nodos
nodes_total = Gauge("dfs_nodes_total", "Total number of nodes", registry=registry)

nodes_active = Gauge("dfs_nodes_active", "Number of active nodes", registry=registry)

# Métricas de storage
storage_total_bytes = Gauge(
    "dfs_storage_total_bytes", "Total storage capacity in bytes", registry=registry
)

storage_used_bytes = Gauge(
    "dfs_storage_used_bytes", "Used storage in bytes", registry=registry
)

storage_free_bytes = Gauge(
    "dfs_storage_free_bytes", "Free storage in bytes", registry=registry
)

# Métricas de operaciones
upload_operations_total = Counter(
    "dfs_upload_operations_total",
    "Total upload operations",
    ["status"],
    registry=registry,
)

download_operations_total = Counter(
    "dfs_download_operations_total",
    "Total download operations",
    ["status"],
    registry=registry,
)

delete_operations_total = Counter(
    "dfs_delete_operations_total",
    "Total delete operations",
    ["status"],
    registry=registry,
)

# Métricas de chunks (DataNode)
chunk_read_operations_total = Counter(
    "dfs_chunk_read_operations_total",
    "Total chunk read operations",
    ["status"],
    registry=registry,
)

chunk_write_operations_total = Counter(
    "dfs_chunk_write_operations_total",
    "Total chunk write operations",
    ["status"],
    registry=registry,
)

chunk_delete_operations_total = Counter(
    "dfs_chunk_delete_operations_total",
    "Total chunk delete operations",
    ["status"],
    registry=registry,
)

# Métricas de bytes transferidos
bytes_read_total = Counter(
    "dfs_bytes_read_total", "Total bytes read", registry=registry
)

bytes_written_total = Counter(
    "dfs_bytes_written_total", "Total bytes written", registry=registry
)

# Métricas de heartbeats
heartbeat_sent_total = Counter(
    "dfs_heartbeat_sent_total", "Total heartbeats sent", registry=registry
)

heartbeat_failed_total = Counter(
    "dfs_heartbeat_failed_total", "Total failed heartbeats", registry=registry
)

# Métricas de replicación
replication_attempts_total = Counter(
    "dfs_replication_attempts_total", "Total replication attempts", registry=registry
)

replication_success_total = Counter(
    "dfs_replication_success_total", "Total successful replications", registry=registry
)

replication_failed_total = Counter(
    "dfs_replication_failed_total", "Total failed replications", registry=registry
)

replication_lag = Gauge(
    "dfs_replication_lag",
    "Number of chunks below replication factor",
    registry=registry,
)

# Métricas de leases
active_leases = Gauge("dfs_active_leases", "Number of active leases", registry=registry)



def metrics_endpoint():
    """Endpoint para exponer métricas Prometheus."""
    return Response(content=generate_latest(registry), media_type=CONTENT_TYPE_LATEST)


class MetricsMiddleware:
    """Middleware para capturar métricas de requests HTTP."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        method = scope["method"]
        path = self._normalize_path(scope["path"])

        # Ignorar el endpoint de métricas
        if path == "/metrics":
            await self.app(scope, receive, send)
            return

        start_time = time.time()
        status_code = 500

        async def send_wrapper(message):
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message["status"]
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        except Exception:
            status_code = 500
            raise
        finally:
            duration = time.time() - start_time

            # Registrar métricas
            http_requests_total.labels(
                method=method, endpoint=path, status=status_code
            ).inc()

            http_request_duration_seconds.labels(method=method, endpoint=path).observe(
                duration
            )

    def _normalize_path(self, path: str) -> str:
        """
        Normaliza paths con parámetros para agrupar métricas.

        Ejemplo:
            /api/v1/files/123 -> /api/v1/files/{id}
            /api/v1/chunks/abc-123 -> /api/v1/chunks/{id}
        """
        if path.startswith("/api/v1/files/") and len(path.split("/")) > 4:
            return "/api/v1/files/{id}"
        elif path.startswith("/api/v1/chunks/") and len(path.split("/")) > 4:
            return "/api/v1/chunks/{id}"
        elif path.startswith("/api/v1/nodes/") and len(path.split("/")) > 4:
            return "/api/v1/nodes/{id}"
        else:
            return path


# ============================================================================
# ACTUALIZACIÓN DE MÉTRICAS
# ============================================================================


async def update_system_metrics(storage) -> Dict[str, Any]:
    """
    Actualiza métricas del sistema desde el storage.

    Args:
        storage: Instancia del storage de metadata

    Returns:
        Dict con estadísticas actualizadas
    """
    try:
        stats = await storage.get_system_stats()

        # Actualizar métricas de archivos
        files_total.set(stats["total_files"])
        chunks_total.set(stats["total_chunks"])

        # Actualizar métricas de nodos
        nodes_total.set(stats["total_nodes"])
        nodes_active.set(stats["active_nodes"])

        # Actualizar métricas de storage
        storage_total_bytes.set(stats["total_space"])
        storage_used_bytes.set(stats["used_space"])
        storage_free_bytes.set(stats["free_space"])

        # Calcular y actualizar replication lag
        # (esto requeriría una implementación más sofisticada en un sistema real)
        replication_lag.set(0)  # Placeholder

        return stats

    except Exception as e:
        # No queremos que los errores de métricas afecten el sistema principal
        print(f"Error actualizando métricas del sistema: {e}")
        return {}


async def update_datanode_metrics(storage) -> Dict[str, Any]:
    """
    Actualiza métricas específicas del DataNode.

    Args:
        storage: Instancia del storage del DataNode

    Returns:
        Dict con estadísticas del DataNode
    """
    try:
        storage_info = storage.get_storage_info()

        # Actualizar métricas de storage del DataNode
        storage_free_bytes.set(storage_info["free_space"])
        storage_used_bytes.set(storage_info["used_space"])
        storage_total_bytes.set(storage_info["total_space"])

        # Métricas específicas del DataNode
        chunks_total.set(storage_info["chunk_count"])

        return storage_info

    except Exception as e:
        print(f"Error actualizando métricas del DataNode: {e}")
        return {}


def record_upload_operation(success: bool):
    """Registra una operación de upload."""
    status = "success" if success else "error"
    upload_operations_total.labels(status=status).inc()


def record_download_operation(success: bool):
    """Registra una operación de download."""
    status = "success" if success else "error"
    download_operations_total.labels(status=status).inc()


def record_delete_operation(success: bool):
    """Registra una operación de delete."""
    status = "success" if success else "error"
    delete_operations_total.labels(status=status).inc()


def record_chunk_read(success: bool, bytes_read: int = 0):
    """Registra una operación de lectura de chunk."""
    status = "success" if success else "error"
    chunk_read_operations_total.labels(status=status).inc()
    if success and bytes_read > 0:
        bytes_read_total.inc(bytes_read)


def record_chunk_write(success: bool, bytes_written: int = 0):
    """Registra una operación de escritura de chunk."""
    status = "success" if success else "error"
    chunk_write_operations_total.labels(status=status).inc()
    if success and bytes_written > 0:
        bytes_written_total.inc(bytes_written)


def record_chunk_delete(success: bool):
    """Registra una operación de eliminación de chunk."""
    status = "success" if success else "error"
    chunk_delete_operations_total.labels(status=status).inc()


def record_heartbeat(success: bool):
    """Registra un heartbeat."""
    if success:
        heartbeat_sent_total.inc()
    else:
        heartbeat_failed_total.inc()


def record_replication_attempt(success: bool):
    """Registra un intento de replicación."""
    replication_attempts_total.inc()
    if success:
        replication_success_total.inc()
    else:
        replication_failed_total.inc()


def update_lease_metrics(active_leases_count: int):
    """Actualiza métricas de leases."""
    active_leases.set(active_leases_count)


# ============================================================================
# HEALTH CHECKS
# ============================================================================


def get_metrics_health() -> Dict[str, Any]:
    """
    Obtiene el estado de salud del sistema de métricas.

    Returns:
        Dict con información de salud
    """
    try:
        # Generar métricas para verificar que funciona
        generate_latest(registry)

        return {
            "status": "healthy",
            "metrics_collected": True,
            "registry_size": len(registry._collector_to_names),
        }
    except Exception as e:
        return {"status": "unhealthy", "error": str(e), "metrics_collected": False}
