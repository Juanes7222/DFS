import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from core.config import config
from shared.protocols import MetadataStorageBase
from metadata.replicator import ReplicationManager
from metadata.leases import LeaseManager
from metadata import context
from metadata.api import file_router, node_router, lease_router, system_router
from metadata.api.proxy import router as proxy_router
from monitoring.metrics import MetricsMiddleware
from metadata.init_storage import create_metadata_storage

# Configura logging
logging.basicConfig(
    level=getattr(logging, config.log_level.upper()),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class ServiceManager:
    """Servicio principal de metadatos del DFS. Gestor centralizado de servicios del Metadata Service"""

    def __init__(self):
        self.storage: Optional[MetadataStorageBase] = None
        self.replicator: Optional[ReplicationManager] = None
        self.lease_manager: Optional[LeaseManager] = None
        self.metrics_task: Optional[asyncio.Task] = None

    async def initialize(self):
        """Inicializa todos los servicios"""
        logger.info("Iniciando Metadata Service...")

        try:
            # Inicializar storage
            self.storage = create_metadata_storage()
            await self.storage.initialize()
            logger.info("Storage inicializado correctamente")

            # Inicializar replication manager
            self.replicator = ReplicationManager(
                self.storage,
                config.replication_factor,
                enable_rebalancing=config.enable_rebalancing
            )
            logger.info(
                f"Replication Manager inicializado "
                f"(rebalancing={'habilitado' if config.enable_rebalancing else 'deshabilitado'})"
            )

            # Inicializar lease manager
            self.lease_manager = LeaseManager(self.storage)
            logger.info("Lease Manager inicializado")

            # Iniciar background tasks
            await self.replicator.start()
            logger.info("Replication Manager iniciado")

            # Iniciar metrics updater
            self.metrics_task = asyncio.create_task(self._metrics_updater())
            logger.info("Metrics updater iniciado")

            logger.info("Metadata Service iniciado correctamente")

        except Exception as e:
            logger.error(f"Error iniciando Metadata Service: {e}")
            await self.cleanup()
            raise

    async def cleanup(self):
        """Limpia y cierra todos los servicios"""
        logger.info("Deteniendo Metadata Service...")

        # Cancela metrics task
        if self.metrics_task and not self.metrics_task.done():
            self.metrics_task.cancel()
            try:
                await self.metrics_task
            except asyncio.CancelledError:
                pass

        # Detiene replicator
        if self.replicator:
            try:
                await self.replicator.stop()
                logger.info("Replication Manager detenido")
            except Exception as e:
                logger.error(f"Error deteniendo Replication Manager: {e}")

        # Cerra storage
        if self.storage:
            try:
                await self.storage.close()
                logger.info("Storage cerrado")
            except Exception as e:
                logger.error(f"Error cerrando Storage: {e}")

        logger.info("Metadata Service detenido correctamente")

    async def _metrics_updater(self):
        """Task en background para actualizar métricas del sistema"""
        from monitoring.metrics import update_lease_metrics

        while True:
            try:
                # Solo actualiza las métricas de leases para no bloquear el lock de storage
                if self.lease_manager:
                    lease_stats = self.lease_manager.get_lease_stats()
                    update_lease_metrics(lease_stats["active_leases"])

                # Reduce frecuencia de actualización
                await asyncio.sleep(30)  # Actualiza cada 30 segundos en lugar de 10

            except asyncio.CancelledError:
                logger.info("Metrics updater cancelado")
                break
            except Exception as e:
                logger.error(f"Error en metrics updater: {e}")
                await asyncio.sleep(60)  # Espera más en caso de error


# Instancia global del service manager
service_manager = ServiceManager()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Gestión del ciclo de vida de la aplicación"""

    # Inicializa servicios
    await service_manager.initialize()

    # Expone referencias en el módulo de contexto
    context.set_storage(service_manager.storage)
    context.set_replicator(service_manager.replicator)
    context.set_lease_manager(service_manager.lease_manager)

    logger.info("Variables globales actualizadas en contexto")
    logger.info(f"Storage: {context.get_storage() is not None}")
    logger.info(f"Replicator: {context.get_replicator() is not None}")
    logger.info(f"Lease Manager: {context.get_lease_manager() is not None}")

    try:
        yield

    finally:
        # Hace limpieza
        await service_manager.cleanup()


def create_app() -> FastAPI:
    """Factory function para crear la aplicación FastAPI"""

    app = FastAPI(
        title="DFS Metadata Service",
        description="Servicio de metadatos para Sistema de Archivos Distribuido",
        version="1.0.0",
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url="/redoc",
    )

    # CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # En producción, especifica orígenes permitidos
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Metrics middleware
    app.add_middleware(MetricsMiddleware)

    # Registro de routers
    app.include_router(file_router, prefix="/api/v1", tags=["Files"])
    app.include_router(node_router, prefix="/api/v1", tags=["Nodes"])
    app.include_router(lease_router, prefix="/api/v1", tags=["Leases"])
    app.include_router(system_router, prefix="/api/v1", tags=["System"])
    app.include_router(proxy_router, prefix="/api/v1/proxy", tags=["Proxy"])

    return app

app = create_app()

@app.get("/health")
async def health_check():
    return {"status": "ok"}

def main():
    """Función principal para ejecutar el servidor"""
    import uvicorn

    logger.info("DFS Metadata Service")
    logger.info(f"Host: {config.metadata_host}")
    logger.info(f"Port: {config.metadata_port}")
    logger.info(f"Replication Factor: {config.replication_factor}")
    logger.info(f"Chunk Size: {config.chunk_size} bytes")
    logger.info(f"Database: {config.db_path}")

    uvicorn.run(
        app,
        host="0.0.0.0",  # Escuchar en todas las interfaces
        port=config.metadata_port,
        log_level=config.log_level.lower(),
        access_log=True,
        limit_max_requests=1000,
        timeout_keep_alive=65,
        # Aumentar límites para chunks grandes (64MB + overhead)
        limit_concurrency=1000,
    )


if __name__ == "__main__":
    main()
