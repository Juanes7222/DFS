# Gunicorn configuration for DFS Metadata Service
# Optimizado para manejar chunks grandes (64MB+)

import multiprocessing
import os

# Server socket
bind = f"0.0.0.0:{os.getenv('PORT', '8000')}"
backlog = 2048

# Worker processes
workers = multiprocessing.cpu_count() * 2 + 1
worker_class = "uvicorn.workers.UvicornWorker"
worker_connections = 1000
max_requests = 1000
max_requests_jitter = 50
timeout = 300  # 5 minutos para chunks grandes

# Logging
accesslog = "-"
errorlog = "-"
loglevel = os.getenv("LOG_LEVEL", "info").lower()
access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s" %(D)s'

# Server mechanics
daemon = False
pidfile = None
umask = 0
user = None
group = None
tmp_upload_dir = None

# SSL (si es necesario)
# keyfile = None
# certfile = None

# Process naming
proc_name = "dfs-metadata"

# Server hooks
def on_starting(server):
    print("Starting DFS Metadata Service")

def on_reload(server):
    print("Reloading DFS Metadata Service")

def when_ready(server):
    print("DFS Metadata Service is ready")
def on_exit(server):
    print("Shutting down DFS Metadata Service")

# Request limits - CRÍTICO para chunks grandes
limit_request_line = 8190  # Línea de request (URL)
limit_request_fields = 100  # Número de headers
limit_request_field_size = 8190  # Tamaño de cada header

# Keepalive
keepalive = 65
