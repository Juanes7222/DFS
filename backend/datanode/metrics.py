"""
Prometheus metrics para DataNode
"""
from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST
from fastapi import Response
import time
import os

# Métricas de requests
http_requests_total = Counter(
    'dfs_datanode_http_requests_total',
    'Total HTTP requests',
    ['method', 'endpoint', 'status']
)

http_request_duration_seconds = Histogram(
    'dfs_datanode_http_request_duration_seconds',
    'HTTP request duration in seconds',
    ['method', 'endpoint']
)

# Métricas de chunks
chunks_stored = Gauge(
    'dfs_datanode_chunks_stored',
    'Number of chunks stored'
)

chunks_total_bytes = Gauge(
    'dfs_datanode_chunks_total_bytes',
    'Total size of chunks in bytes'
)

# Métricas de storage
disk_total_bytes = Gauge(
    'dfs_datanode_disk_total_bytes',
    'Total disk capacity in bytes'
)

disk_used_bytes = Gauge(
    'dfs_datanode_disk_used_bytes',
    'Used disk space in bytes'
)

disk_free_bytes = Gauge(
    'dfs_datanode_disk_free_bytes',
    'Free disk space in bytes'
)

# Métricas de operaciones
chunk_read_operations_total = Counter(
    'dfs_datanode_chunk_read_operations_total',
    'Total chunk read operations',
    ['status']
)

chunk_write_operations_total = Counter(
    'dfs_datanode_chunk_write_operations_total',
    'Total chunk write operations',
    ['status']
)

chunk_delete_operations_total = Counter(
    'dfs_datanode_chunk_delete_operations_total',
    'Total chunk delete operations',
    ['status']
)

# Métricas de bytes transferidos
bytes_read_total = Counter(
    'dfs_datanode_bytes_read_total',
    'Total bytes read'
)

bytes_written_total = Counter(
    'dfs_datanode_bytes_written_total',
    'Total bytes written'
)

# Métricas de heartbeat
heartbeat_sent_total = Counter(
    'dfs_datanode_heartbeat_sent_total',
    'Total heartbeats sent'
)

heartbeat_failed_total = Counter(
    'dfs_datanode_heartbeat_failed_total',
    'Total heartbeat failures'
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


def update_storage_metrics(storage_dir: str):
    """Actualiza métricas de almacenamiento"""
    try:
        # Contar chunks
        chunk_count = 0
        total_size = 0
        
        if os.path.exists(storage_dir):
            for filename in os.listdir(storage_dir):
                if filename.endswith('.chunk'):
                    chunk_count += 1
                    filepath = os.path.join(storage_dir, filename)
                    total_size += os.path.getsize(filepath)
        
        chunks_stored.set(chunk_count)
        chunks_total_bytes.set(total_size)
        
        # Métricas de disco
        stat = os.statvfs(storage_dir)
        total_bytes = stat.f_blocks * stat.f_frsize
        free_bytes = stat.f_bavail * stat.f_frsize
        used_bytes = total_bytes - free_bytes
        
        disk_total_bytes.set(total_bytes)
        disk_free_bytes.set(free_bytes)
        disk_used_bytes.set(used_bytes)
        
    except Exception as e:
        print(f"Error updating storage metrics: {e}")
