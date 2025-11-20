"""Metadata Service refactorizado"""

from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI

from core.config import config
from core.logging import setup_logging
from metadata.storage import MetadataStorage
from metadata.replicator import ReplicationManager
from monitoring.metrics import metrics_endpoint, MetricsMiddleware

logger = setup_logging()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Gestión del ciclo de vida de la aplicación"""
    # Inicializar componentes
    app.state.storage = MetadataStorage()
    await app.state.storage.initialize()
    
    app.state.replicator = ReplicationManager(
        storage=app.state.storage,
        replication_factor=config.replication_factor
    )
    
    # Iniciar background tasks
    await app.state.replicator.start()
    
    yield
    
    # Cleanup
    await app.state.replicator.stop()
    await app.state.storage.close()

app = FastAPI(
    title="DFS Metadata Service",
    description="Servicio de metadatos para Sistema de Archivos Distribuido",
    version="1.0.0",
    lifespan=lifespan,
)

# Middleware
app.add_middleware(MetricsMiddleware)

# Endpoints (implementar según la versión existente pero organizados)
# ...