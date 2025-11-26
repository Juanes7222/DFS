"""DataNode unificado - Versión mejorada con mejor manejo de recursos y errores"""

import logging
import sys
from contextlib import asynccontextmanager
from typing import Optional
from uuid import UUID

from fastapi import FastAPI, HTTPException, UploadFile, Query, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import uvicorn

from core.config import config
from core.exceptions import DFSStorageError
from datanode.storage import ChunkStorage
from datanode.heartbeat import HeartbeatManager
import datanode.agent as agent
# from monitoring.metrics import metrics_endpoint, MetricsMiddleware

logger = logging.getLogger(__name__)


class DataNodeServer:
    """Servidor DataNode unificado"""

    def __init__(self, node_id: Optional[str] = None, port: Optional[int] = None):
        self.port = port or config.datanode_port
        
        # Usar node_id persistente si se proporciona, sino generarlo
        if node_id:
            self.node_id = node_id
        else:
            # Intentar obtener un node_id persistente del agent
            try:
                from datanode.agent import get_node_id
                self.node_id = get_node_id()
                logger.info(f"Usando node_id persistente: {self.node_id}")
            except Exception as e:
                logger.warning(f"No se pudo obtener node_id persistente: {e}")
                # Fallback temporal
                import uuid
                self.node_id = str(uuid.uuid4())
                logger.warning(f"Generando node_id temporal: {self.node_id}")
        
        self.storage_path = config.storage_path / self.node_id

        self.storage: Optional[ChunkStorage] = None
        self.heartbeat_manager: Optional[HeartbeatManager] = None
        self.app = self._create_app()

    def _create_app(self) -> FastAPI:
        """Crea la aplicación FastAPI"""

        @asynccontextmanager
        async def lifespan(app: FastAPI):
            """Gestión del ciclo de vida del DataNode"""
            try:
                await self.start()
                logger.info(f"DataNode {self.node_id} listo para recibir peticiones")
                yield
            except Exception as e:
                logger.error(f"Error durante inicio del DataNode: {e}", exc_info=True)
                raise
            finally:
                await self.stop()
        
        app = FastAPI(
            title=f"DFS DataNode {self.node_id}",
            description="Nodo de almacenamiento para el Sistema de Archivos Distribuido (DFS)",
            version="1.0.0",
            lifespan=lifespan,
        )

        # CORS Middleware - Permitir peticiones desde el frontend
        cors_origins = ["*"] if config.cors_allow_all else config.cors_origins
        
        app.add_middleware(
            CORSMiddleware,
            allow_origins=cors_origins,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        # Metrics Middleware
        # app.add_middleware(MetricsMiddleware)

        @app.put(
            "/api/v1/chunks/{chunk_id}",
            status_code=status.HTTP_201_CREATED,
            response_model=dict
        )
        async def put_chunk(
            chunk_id: UUID, 
            file: UploadFile, 
            replicate_to: Optional[str] = Query(None, description="URL del siguiente nodo para replicación")
        ):
            """Almacena un chunk con replicación en pipeline."""
            if not self.storage:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail="Storage no inicializado"
                )

            try:
                chunk_data = await file.read()
                
                if not chunk_data:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="El chunk está vacío"
                    )

                result = await self.storage.store_chunk(
                    chunk_id, chunk_data, replicate_to
                )
                
                logger.info(f"Chunk {chunk_id} almacenado exitosamente")
                return result
                
            except DFSStorageError as e:
                logger.error(f"Error almacenando chunk {chunk_id}: {e}")
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Error de almacenamiento: {str(e)}"
                )
            except Exception as e:
                logger.error(f"Error inesperado almacenando chunk {chunk_id}: {e}", exc_info=True)
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Error interno del servidor"
                )

        @app.get("/api/v1/chunks/{chunk_id}")
        async def get_chunk(chunk_id: UUID):
            """Recupera un chunk."""
            if not self.storage:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail="Storage no inicializado"
                )

            try:
                chunk_data, checksum = await self.storage.retrieve_chunk(chunk_id)

                async def chunk_generator():
                    yield chunk_data

                return StreamingResponse(
                    chunk_generator(),
                    media_type="application/octet-stream",
                    headers={
                        "X-Chunk-ID": str(chunk_id),
                        "X-Checksum": checksum,
                        "Content-Length": str(len(chunk_data)),
                    },
                )
                
            except DFSStorageError as e:
                logger.warning(f"Chunk {chunk_id} no encontrado: {e}")
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Chunk no encontrado: {str(e)}"
                )
            except Exception as e:
                logger.error(f"Error inesperado recuperando chunk {chunk_id}: {e}", exc_info=True)
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Error interno del servidor"
                )

        @app.get("/health")
        async def health():
            """Health check con información detallada."""
            is_healthy = (
                self.storage is not None and 
                self.heartbeat_manager is not None and
                self.heartbeat_manager.is_running()
            )
            
            return {
                "status": "healthy" if is_healthy else "unhealthy",
                "node_id": self.node_id,
                "port": self.port,
                "storage_initialized": self.storage is not None,
                "heartbeat_active": self.heartbeat_manager is not None and self.heartbeat_manager.is_running()
            }

        @app.get("/metrics")
        async def metrics():
            """Endpoint de métricas."""
            # return metrics_endpoint()
            return {"message": "Metrics endpoint placeholder"}

        return app

    async def start(self):
        """Inicia el DataNode."""
        logger.info(f"Iniciando DataNode {self.node_id} en puerto {self.port}")

        try:
            # Obtener ZeroTier IP si está disponible
            zerotier_ip = None
            zerotier_node_id = None
            
            try:
                from datanode.agent import get_zerotier_ip, get_zerotier_node_id_from_cli
                zerotier_ip = get_zerotier_ip()
                if zerotier_ip:
                    logger.info(f"ZeroTier IP detectada: {zerotier_ip}")
                    try:
                        zerotier_node_id = get_zerotier_node_id_from_cli()
                        if zerotier_node_id:
                            logger.info(f"ZeroTier Node ID: {zerotier_node_id}")
                    except Exception as e:
                        logger.warning(f"No se pudo obtener ZeroTier Node ID: {e}")
                else:
                    logger.warning("No se detectó IP de ZeroTier, usando configuración local")
            except ImportError:
                logger.warning("Módulo agent no disponible, continuando sin ZeroTier")
            except Exception as e:
                logger.warning(f"Error obteniendo ZeroTier info: {e}")
            
            # Inicializar storage
            self.storage = ChunkStorage(self.storage_path)
            await self.storage.initialize()
            logger.info("Storage inicializado correctamente")

            # Iniciar heartbeat con información de ZeroTier
            self.heartbeat_manager = HeartbeatManager(
                node_id=self.node_id,
                storage=self.storage,
                metadata_url=config.metadata_url,
                port=self.port,
                zerotier_ip=zerotier_ip,
                zerotier_node_id=zerotier_node_id
            )
            await self.heartbeat_manager.start()
            logger.info("Heartbeat manager iniciado correctamente")

        except Exception as e:
            logger.error(f"Error durante la inicialización: {e}", exc_info=True)
            await self.stop()
            raise

    async def stop(self):
        """Detiene el DataNode de forma segura."""
        logger.info(f"Deteniendo DataNode {self.node_id}")
        
        try:
            if self.heartbeat_manager:
                await self.heartbeat_manager.stop()
                logger.info("Heartbeat manager detenido")
                
            if self.storage:
                # Agregar cleanup si ChunkStorage lo soporta
                logger.info("Storage cerrado")
                
        except Exception as e:
            logger.error(f"Error durante el apagado: {e}", exc_info=True)
        finally:
            logger.info(f"DataNode {self.node_id} detenido")


def setup_logging():
    """Configura el sistema de logging."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    
    # Reducir verbosidad de librerías externas
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)


def main():
    """Función principal para ejecutar el DataNode."""
    setup_logging()
    
    try:
        server = DataNodeServer()
        logger.info(f"Iniciando servidor en {config.datanode_host}:{server.port}")
        
        uvicorn.run(
            server.app,
            host=config.datanode_host,
            port=server.port,
            log_config=None  # Usar nuestra configuración de logging
        )
        
    except KeyboardInterrupt:
        logger.info("Servidor detenido por el usuario")
    except Exception as e:
        logger.error(f"Error fatal: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()