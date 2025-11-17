"""
Prometheus metrics para Metadata Service
"""
from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST
from fastapi import Response
import time

# Métricas de requests
http_requests_total = Counter(
    'dfs_metadata_http_requests_total',
    'Total HTTP requests',
    ['method', 'endpoint', 'status']
)

http_request_duration_seconds = Histogram(
    'dfs_metadata_http_request_duration_seconds',
    'HTTP request duration in seconds',
    ['method', 'endpoint']
)

# Métricas de archivos
files_total = Gauge(
    'dfs_metadata_files_total',
    'Total number of files'
)

files_deleted_total = Gauge(
    'dfs_metadata_files_deleted_total',
    'Total number of deleted files'
)

chunks_total = Gauge(
    'dfs_metadata_chunks_total',
    'Total number of chunks'
)

# Métricas de nodos
nodes_total = Gauge(
    'dfs_metadata_nodes_total',
    'Total number of nodes'
)

nodes_active = Gauge(
    'dfs_metadata_nodes_active',
    'Number of active nodes'
)

# Métricas de storage
storage_total_bytes = Gauge(
    'dfs_metadata_storage_total_bytes',
    'Total storage capacity in bytes'
)

storage_used_bytes = Gauge(
    'dfs_metadata_storage_used_bytes',
    'Used storage in bytes'
)

storage_free_bytes = Gauge(
    'dfs_metadata_storage_free_bytes',
    'Free storage in bytes'
)

# Métricas de operaciones
upload_operations_total = Counter(
    'dfs_metadata_upload_operations_total',
    'Total upload operations',
    ['status']
)

download_operations_total = Counter(
    'dfs_metadata_download_operations_total',
    'Total download operations',
    ['status']
)

delete_operations_total = Counter(
    'dfs_metadata_delete_operations_total',
    'Total delete operations',
    ['status']
)

# Métricas de leases
active_leases = Gauge(
    'dfs_metadata_active_leases',
    'Number of active leases'
)

# Métricas de replicación
replication_lag = Gauge(
    'dfs_metadata_replication_lag',
    'Number of chunks below replication factor'
)


def metrics_endpoint():
    """Endpoint para exponer métricas Prometheus"""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


class MetricsMiddleware:
    """Middleware para capturar métricas de requests"""
    
    def __init__(self, app):
        self.app = app
    
    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        
        method = scope["method"]
        path = scope["path"]
        
        # Ignorar el endpoint de métricas
        if path == "/metrics":
            await self.app(scope, receive, send)
            return
        
        start_time = time.time()
        
        # Wrapper para capturar el status code
        status_code = 500
        
        async def send_wrapper(message):
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message["status"]
            await send(message)
        
        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            duration = time.time() - start_time
            
            # Registrar métricas
            http_requests_total.labels(
                method=method,
                endpoint=path,
                status=status_code
            ).inc()
            
            http_request_duration_seconds.labels(
                method=method,
                endpoint=path
            ).observe(duration)


async def update_system_metrics(storage):
    """Actualiza métricas del sistema"""
    try:
        # Métricas de archivos
        files = await storage.list_files(limit=10000)
        files_total.set(len(files))
        
        deleted_files = [f for f in files if f.is_deleted]
        files_deleted_total.set(len(deleted_files))
        
        total_chunks = sum(len(f.chunks) for f in files)
        chunks_total.set(total_chunks)
        
        # Métricas de nodos
        nodes = await storage.list_nodes()
        nodes_total.set(len(nodes))
        
        active_nodes = [n for n in nodes if n.state.value == "active"]
        nodes_active.set(len(active_nodes))
        
        # Métricas de storage
        total_storage = sum(n.total_space for n in nodes)
        free_storage = sum(n.free_space for n in nodes)
        used_storage = total_storage - free_storage
        
        storage_total_bytes.set(total_storage)
        storage_free_bytes.set(free_storage)
        storage_used_bytes.set(used_storage)
        
    except Exception as e:
        print(f"Error updating metrics: {e}")
