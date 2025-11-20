"""Gestión de heartbeats para DataNode"""

import asyncio
import logging
from typing import List, Optional
from uuid import UUID

import httpx

from core.config import config
from core.exceptions import DFSMetadataError

logger = logging.getLogger(__name__)


class HeartbeatManager:
    """Gestiona el envío periódico de heartbeats al Metadata Service."""

    def __init__(self, node_id: str, storage, metadata_url: str):
        self.node_id = node_id
        self.storage = storage
        self.metadata_url = metadata_url
        self.running = False
        self.task: Optional[asyncio.Task] = None

    async def start(self):
        """Inicia el envío de heartbeats."""
        self.running = True
        self.task = asyncio.create_task(self._heartbeat_loop())
        logger.info(f"Heartbeat manager iniciado para {self.node_id}")

    async def stop(self):
        """Detiene el envío de heartbeats."""
        self.running = False
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
        logger.info(f"Heartbeat manager detenido para {self.node_id}")

    async def _heartbeat_loop(self):
        """Loop principal de envío de heartbeats."""
        while self.running:
            try:
                await self._send_heartbeat()
                await asyncio.sleep(config.heartbeat_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error en heartbeat loop: {e}")
                await asyncio.sleep(5)  # Esperar antes de reintentar

    async def _send_heartbeat(self):
        """Envía un heartbeat al Metadata Service."""
        try:
            storage_info = self.storage.get_storage_info()
            chunk_ids = self._get_stored_chunk_ids()

            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(
                    f"{self.metadata_url}/api/v1/nodes/heartbeat",
                    json={
                        "node_id": self.node_id,
                        "free_space": storage_info["free_space"],
                        "total_space": storage_info["total_space"],
                        "chunk_ids": [str(chunk_id) for chunk_id in chunk_ids],
                    },
                )

                if response.status_code == 200:
                    logger.debug(f"Heartbeat enviado: {self.node_id}")
                else:
                    logger.warning(f"Heartbeat falló: {response.status_code}")

        except Exception as e:
            logger.error(f"Error enviando heartbeat: {e}")

    def _get_stored_chunk_ids(self) -> List[UUID]:
        """Obtiene la lista de chunks almacenados."""
        chunk_ids = []
        if self.storage.storage_path.exists():
            for chunk_file in self.storage.storage_path.glob("*.chunk"):
                try:
                    chunk_id = UUID(chunk_file.stem)
                    chunk_ids.append(chunk_id)
                except ValueError:
                    continue
        return chunk_ids
