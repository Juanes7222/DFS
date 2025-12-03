"""Gestión de heartbeats para DataNode al Metadata Service."""

import asyncio
import logging
from typing import List, Optional
from uuid import UUID

import httpx

from core.config import config

logger = logging.getLogger(__name__)


class HeartbeatManager:
    """Gestiona el envío periódico de heartbeats al Metadata Service"""

    def __init__(self, node_id: str, storage, metadata_url: str, port: int, zerotier_ip: Optional[str] = None, zerotier_node_id: Optional[str] = None):
        self.node_id = node_id
        self.storage = storage
        self.metadata_url = metadata_url.rstrip('/')
        self.port = port
        self.zerotier_ip = zerotier_ip
        self.zerotier_node_id = zerotier_node_id
        self.running = False
        self.task: Optional[asyncio.Task] = None
        self.consecutive_failures = 0
        self.max_failures = 3

    def _get_public_url(self) -> str:
        """
        Obtiene la URL pública del DataNode.
        Para desarrollo local: usa localhost
        Para producción: usa ZeroTier IP
        """
        # Si el metadata service es localhost, usar localhost también
        if "localhost" in self.metadata_url or "127.0.0.1" in self.metadata_url:
            host = "127.0.0.1"
            logger.info(f"Desarrollo local detectado - usando {host}:{self.port}")
        elif self.zerotier_ip:
            # Producción: usar ZeroTier IP
            host = self.zerotier_ip
            logger.info(f"Producción - usando ZeroTier IP: {host}:{self.port}")
        else:
            # Fallback
            host = config.datanode_host
            if host == "0.0.0.0":
                host = "localhost"
                logger.warning("Usando localhost como fallback")
        
        return f"http://{host}:{self.port}"

    def is_running(self) -> bool:
        """Verifica si el heartbeat está activo."""
        return self.running and self.task is not None and not self.task.done()

    async def start(self):
        """Inicia el envío de heartbeats."""
        if self.running:
            logger.warning(f"Heartbeat manager ya está ejecutándose para {self.node_id}")
            return

        self.running = True
        self.consecutive_failures = 0
        
        public_url = self._get_public_url()
        logger.info(f"Heartbeat manager iniciado para {self.node_id}")
        logger.info(f"URL pública del nodo: {public_url}")
        logger.info(f"Metadata URL: {self.metadata_url}")
        logger.info(f"Intervalo de heartbeat: {config.heartbeat_interval}s")
        
        self.task = asyncio.create_task(self._heartbeat_loop())

    async def stop(self):
        """Detiene el envío de heartbeats de forma segura."""
        if not self.running:
            return

        logger.info(f"Deteniendo heartbeat manager para {self.node_id}")
        self.running = False
        
        if self.task and not self.task.done():
            self.task.cancel()
            try:
                await asyncio.wait_for(self.task, timeout=5.0)
            except asyncio.TimeoutError:
                logger.warning("Timeout esperando cancelación del heartbeat")
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.error(f"Error deteniendo heartbeat: {e}")
        
        logger.info(f"Heartbeat manager detenido para {self.node_id}")

    async def _heartbeat_loop(self):
        """Loop principal de envío de heartbeats con backoff exponencial."""
        retry_delay = config.heartbeat_interval
        
        while self.running:
            try:
                success = await self._send_heartbeat()
                
                if success:
                    self.consecutive_failures = 0
                    retry_delay = config.heartbeat_interval
                else:
                    self.consecutive_failures += 1
                    retry_delay = min(retry_delay * 2, 60)
                    
                    if self.consecutive_failures >= self.max_failures:
                        logger.error(
                            f"Heartbeat falló {self.consecutive_failures} veces consecutivas. "
                            f"Continuando con intervalo de {retry_delay}s"
                        )
                
                await asyncio.sleep(retry_delay)
                
            except asyncio.CancelledError:
                logger.info("Heartbeat loop cancelado")
                break
            except Exception as e:
                logger.error(f"Error inesperado en heartbeat loop: {e}", exc_info=True)
                await asyncio.sleep(5)

    async def _send_heartbeat(self) -> bool:
        """
        Envía un heartbeat al Metadata Service.
        
        Returns:
            bool: True si el heartbeat fue exitoso, False en caso contrario
        """
        try:
            storage_info = self.storage.get_storage_info()
            chunk_ids = self._get_stored_chunk_ids()
            
            # Usar URL pública en lugar de la dirección de bind
            public_url = self._get_public_url()
            
            url = f"{self.metadata_url}/api/v1/nodes/heartbeat"
            payload = {
                "node_id": self.node_id,
                "url": public_url,  # Enviar URL accesible desde clientes
                "free_space": storage_info["free_space"],
                "total_space": storage_info["total_space"],
                "chunk_ids": [str(chunk_id) for chunk_id in chunk_ids],
            }
            
            # Agregar campos de ZeroTier si están disponibles
            if self.zerotier_ip:
                payload["zerotier_ip"] = self.zerotier_ip
            if self.zerotier_node_id:
                payload["zerotier_node_id"] = self.zerotier_node_id
            
            logger.debug(f"Enviando heartbeat a: {url}")
            logger.debug(f"URL pública: {public_url}")
            logger.info(f"Reportando {len(chunk_ids)} chunks almacenados en heartbeat")
            if len(chunk_ids) > 0:
                logger.debug(f"Chunks: {[str(c) for c in chunk_ids[:5]]}{'...' if len(chunk_ids) > 5 else ''}")

            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(url, json=payload)

                if response.status_code == 200:
                    logger.debug(f"Heartbeat enviado exitosamente: {self.node_id}")
                    return True
                elif response.status_code == 404:
                    logger.error(
                        f"Endpoint de heartbeat no encontrado: {url}. "
                        "Verifica que el Metadata Service esté ejecutándose."
                    )
                    return False
                else:
                    logger.warning(
                        f"Heartbeat rechazado con código {response.status_code}: "
                        f"{response.text[:200]}"
                    )
                    return False

        except httpx.TimeoutException:
            logger.warning(f"Timeout enviando heartbeat a {self.metadata_url}")
            return False
        except httpx.ConnectError as e:
            logger.warning(
                f"No se pudo conectar al Metadata Service en {self.metadata_url}: {e}"
            )
            return False
        except httpx.HTTPError as e:
            logger.error(f"Error HTTP enviando heartbeat: {e}")
            return False
        except Exception as e:
            logger.error(f"Error inesperado enviando heartbeat: {e}", exc_info=True)
            return False

    def _get_stored_chunk_ids(self) -> List[UUID]:
        """
        Obtiene la lista de chunks almacenados de forma segura.
        
        Returns:
            List[UUID]: Lista de IDs de chunks válidos
        """
        chunk_ids = []
        
        if not self.storage.storage_path.exists():
            logger.warning(f"Directorio de storage no existe: {self.storage.storage_path}")
            return chunk_ids

        try:
            chunk_files = list(self.storage.storage_path.glob("*.chunk"))
            logger.debug(f"Buscando chunks en: {self.storage.storage_path}")
            logger.debug(f"Archivos .chunk encontrados: {len(chunk_files)}")
            
            for chunk_file in chunk_files:
                try:
                    chunk_id = UUID(chunk_file.stem)
                    chunk_ids.append(chunk_id)
                except ValueError:
                    logger.warning(f"Archivo con nombre inválido ignorado: {chunk_file.name}")
                    continue
                    
            logger.info(f"Encontrados {len(chunk_ids)} chunks válidos en {self.storage.storage_path}")
            
        except Exception as e:
            logger.error(f"Error escaneando chunks: {e}", exc_info=True)
        
        return chunk_ids