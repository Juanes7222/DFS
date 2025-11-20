"""DataNode unificado - Combina las mejores características de ambas implementaciones"""

import logging
from contextlib import asynccontextmanager
from typing import Optional
from uuid import UUID

from fastapi import FastAPI, HTTPException, UploadFile, Query
from fastapi.responses import StreamingResponse

from core.config import config
from core.exceptions import DFSStorageError
from datanode.storage import ChunkStorage
from datanode.heartbeat import HeartbeatManager
from monitoring.metrics import metrics_endpoint, MetricsMiddleware

logger = logging.getLogger(__name__)


class DataNodeServer:
    """Servidor DataNode unificado."""

    def __init__(self, node_id: Optional[str] = None, port: Optional[int] = None):
        self.node_id = (
            node_id or f"node-{config.datanode_host}-{port or config.datanode_port}"
        )
        self.port = port or config.datanode_port
        self.storage_path = config.storage_path / self.node_id

        self.storage = ChunkStorage(self.storage_path)
        self.heartbeat_manager = HeartbeatManager(
            node_id=self.node_id, storage=self.storage, metadata_url=config.metadata_url
        )

        self.app = self._create_app()

    def _create_app(self) -> FastAPI:
        """Crea la aplicación FastAPI."""
        
        @asynccontextmanager
        async def lifespan(app: FastAPI):
            """Gestión del ciclo de vida del DataNode"""
            # Startup
            await self.start()
            yield
            # Shutdown
            await self.stop()
        
        app = FastAPI(
            title=f"DFS DataNode {self.node_id}",
            description="Nodo de almacenamiento para Sistema de Archivos Distribuido",
            version="1.0.0",
            lifespan=lifespan,
        )

        # Middleware
        app.add_middleware(MetricsMiddleware)

        # Endpoints
        @app.put("/api/v1/chunks/{chunk_id}")
        async def put_chunk(
            chunk_id: UUID, file: UploadFile, replicate_to: Optional[str] = Query(None)
        ):
            """Almacena un chunk con replicación en pipeline."""
            try:
                chunk_data = await file.read()
                result = await self.storage.store_chunk(
                    chunk_id, chunk_data, replicate_to
                )
                return result
            except DFSStorageError as e:
                raise HTTPException(status_code=500, detail=str(e))

        @app.get("/api/v1/chunks/{chunk_id}")
        async def get_chunk(chunk_id: UUID):
            """Recupera un chunk."""
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
                raise HTTPException(status_code=404, detail=str(e))

        # Health endpoints
        @app.get("/health")
        async def health():
            return {"status": "healthy", "node_id": self.node_id}

        @app.get("/metrics")
        async def metrics():
            return metrics_endpoint()

        return app

    async def start(self):
        """Inicia el DataNode."""
        import sys
        sys.stderr.write(f"[DEBUG] Iniciando DataNode {self.node_id} en puerto {self.port}\n")
        sys.stderr.flush()
        logger.info(f"Iniciando DataNode {self.node_id} en puerto {self.port}")

        # Inicializar storage
        await self.storage.initialize()
        sys.stderr.write(f"[DEBUG] Storage inicializado\n")
        sys.stderr.flush()

        # Iniciar heartbeat
        sys.stderr.write(f"[DEBUG] Metadata URL: {config.metadata_url}\n")
        sys.stderr.flush()
        await self.heartbeat_manager.start()
        sys.stderr.write(f"[DEBUG] Heartbeat manager iniciado\n")
        sys.stderr.flush()

        logger.info(f"DataNode {self.node_id} iniciado correctamente")

    async def stop(self):
        """Detiene el DataNode."""
        logger.info(f"Deteniendo DataNode {self.node_id}")
        await self.heartbeat_manager.stop()
        logger.info(f"DataNode {self.node_id} detenido")


def main():
    """Función principal para ejecutar el DataNode."""
    import uvicorn
    import sys
    
    # Configurar logging básico
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )

    server = DataNodeServer()
    uvicorn.run(server.app, host=config.datanode_host, port=server.port)


if __name__ == "__main__":
    main()
