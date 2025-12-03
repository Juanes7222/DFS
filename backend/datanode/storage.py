import asyncio
import gzip
import logging
import shutil
from pathlib import Path
from typing import Optional, Tuple, List
from uuid import UUID

import httpx

from core.config import config
from core.exceptions import DFSStorageError
from shared.utils import calculate_checksum
from shared.protocols import ChunkStorageProtocol

logger = logging.getLogger(__name__)


class ChunkStorage(ChunkStorageProtocol):
    """Gestiona el almacenamiento y recuperación de chunks"""

    def __init__(self, storage_path: Path):
        self.storage_path = storage_path
        self.lock = asyncio.Lock()

    async def initialize(self):
        """Inicializa el almacenamiento"""
        self.storage_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Storage inicializado en: {self.storage_path}")

    async def store_chunk(
        self, chunk_id: UUID, chunk_data: bytes, replicate_to: Optional[str] = None
    ) -> dict:
        """Almacena un chunk con replicación en pipeline y compresión"""
        async with self.lock:
            chunk_path = self.storage_path / f"{chunk_id}.chunk"
            checksum_path = self.storage_path / f"{chunk_id}.checksum"

            try:
                # Calcular checksum del dato original (antes de comprimir)
                checksum = calculate_checksum(chunk_data)
                original_size = len(chunk_data)
                
                # Comprimir chunk con gzip (nivel 6 = balance velocidad/compresión)
                compressed_data = gzip.compress(chunk_data, compresslevel=6)
                compressed_size = len(compressed_data)
                compression_ratio = (1 - compressed_size / original_size) * 100 if original_size > 0 else 0

                # Guardar chunk comprimido localmente
                with open(chunk_path, "wb") as f:
                    f.write(compressed_data)

                # Guardar checksum del dato original
                with open(checksum_path, "w") as f:
                    f.write(checksum)

                logger.info(
                    f"Chunk almacenado: {chunk_id}, size: {original_size} bytes → "
                    f"{compressed_size} bytes (compresión: {compression_ratio:.1f}%)"
                )

                # Pipeline replication con datos comprimidos
                replicated_nodes = [self._get_node_id()]
                if replicate_to:
                    replicated_nodes.extend(
                        await self._replicate_to_nodes(
                            chunk_id, compressed_data, replicate_to
                        )
                    )

                return {
                    "status": "stored",
                    "chunk_id": str(chunk_id),
                    "size": original_size,
                    "compressed_size": compressed_size,
                    "compression_ratio": f"{compression_ratio:.1f}%",
                    "checksum": checksum,
                    "node_id": self._get_node_id(),
                    "nodes": replicated_nodes,
                }

            except Exception as e:
                # Hace limpieza en caso de error
                if chunk_path.exists():
                    chunk_path.unlink()
                if checksum_path.exists():
                    checksum_path.unlink()
                raise DFSStorageError(f"Error almacenando chunk {chunk_id}: {e}")

    async def retrieve_chunk(self, chunk_id: UUID) -> Tuple[bytes, str]:
        """Recupera un chunk, lo descomprime y verifica su checksum"""
        chunk_path = self.storage_path / f"{chunk_id}.chunk"
        checksum_path = self.storage_path / f"{chunk_id}.checksum"

        if not chunk_path.exists():
            raise DFSStorageError(f"Chunk no encontrado: {chunk_id}")

        try:
            # Lee el chunk (posiblemente comprimido)
            with open(chunk_path, "rb") as f:
                stored_data = f.read()

            # Intentar descomprimir (compatibilidad con chunks legacy)
            try:
                chunk_data = gzip.decompress(stored_data)
                logger.debug(f"Chunk {chunk_id} descomprimido: {len(stored_data)} → {len(chunk_data)} bytes")
            except gzip.BadGzipFile:
                # Si falla, asumir que es un chunk sin comprimir (legacy)
                chunk_data = stored_data
                logger.debug(f"Chunk {chunk_id} sin compresión (legacy)")

            # Verifica el checksum del dato descomprimido
            calculated_checksum = calculate_checksum(chunk_data)

            if checksum_path.exists():
                with open(checksum_path, "r") as f:
                    stored_checksum = f.read().strip()

                if calculated_checksum != stored_checksum:
                    raise DFSStorageError(f"Checksum mismatch para chunk {chunk_id}")

            return chunk_data, calculated_checksum

        except DFSStorageError:
            raise
        except Exception as e:
            raise DFSStorageError(f"Error recuperando chunk {chunk_id}: {e}")

    async def delete_chunk(self, chunk_id: UUID) -> bool:
        """Elimina un chunk"""
        async with self.lock:
            chunk_path = self.storage_path / f"{chunk_id}.chunk"
            checksum_path = self.storage_path / f"{chunk_id}.checksum"

            deleted = False
            if chunk_path.exists():
                chunk_path.unlink()
                deleted = True

            if checksum_path.exists():
                checksum_path.unlink()

            logger.info(f"Chunk eliminado: {chunk_id}")
            return deleted

    async def _replicate_to_nodes(
        self, chunk_id: UUID, chunk_data: bytes, replicate_to: str
    ) -> List[str]:
        """Replica el chunk a los nodos en pipeline (host:port|host:port)
        
        Args:
            chunk_data: Datos del chunk (ya comprimidos si compression está habilitada)
        """
        replicated_nodes = []
        
        if not replicate_to or not replicate_to.strip():
            return replicated_nodes
        
        # Parsear cadena de nodos
        next_nodes = replicate_to.split("|")
        if not next_nodes:
            return replicated_nodes

        current_target = next_nodes[0].strip()
        remaining_chain = "|".join(next_nodes[1:]) if len(next_nodes) > 1 else None

        # Agregar http:// si no está presente
        if not current_target.startswith("http://") and not current_target.startswith("https://"):
            current_target = f"http://{current_target}"

        try:
            logger.info(f"Replicando chunk {chunk_id} a {current_target} (pipeline: {bool(remaining_chain)})")
            
            async with httpx.AsyncClient(timeout=120.0) as client:
                from io import BytesIO
                
                files = {"file": ("chunk", BytesIO(chunk_data), "application/octet-stream")}
                params = {}

                if remaining_chain:
                    params["replicate_to"] = remaining_chain
                    logger.info(f"Cadena restante: {remaining_chain}")

                response = await client.put(
                    f"{current_target}/api/v1/chunks/{chunk_id}",
                    files=files,
                    params=params,
                    timeout=120.0,
                )

                if response.status_code in (200, 201):
                    result = response.json()
                    downstream_nodes = result.get("nodes", [])
                    replicated_nodes.extend(downstream_nodes)
                    logger.info(f"✅ Replicación exitosa: {current_target} -> {len(downstream_nodes)} nodos downstream")
                else:
                    logger.error(
                        f"❌ Error replicando a {current_target}: {response.status_code} - {response.text[:200]}"
                    )

        except httpx.TimeoutException:
            logger.error(f"⏱️ Timeout replicando a {current_target}")
        except httpx.ConnectError as e:
            logger.error(f"🔌 Error de conexión a {current_target}: {e}")
        except Exception as e:
            logger.error(f"❌ Excepción replicando a {current_target}: {e}", exc_info=True)

        return replicated_nodes

    def _get_node_id(self) -> str:
        """Obtiene el ID del nodo actual"""
        return f"node-{config.datanode_host}-{config.datanode_port}"

    def get_storage_info(self) -> dict:
        """Obtiene información del almacenamiento"""
        if not self.storage_path.exists():
            return {
                "free_space": 0,
                "total_space": 0,
                "chunk_count": 0,
                "used_space": 0,
            }

        try:
            stat = shutil.disk_usage(self.storage_path)
            chunk_files = list(self.storage_path.glob("*.chunk"))
            chunk_count = len(chunk_files)

            # Calcular espacio usado por chunks
            used_space = sum(chunk_file.stat().st_size for chunk_file in chunk_files)

            return {
                "free_space": stat.free,
                "total_space": stat.total,
                "used_space": used_space,
                "chunk_count": chunk_count,
            }
        except Exception as e:
            logger.error(f"Error obteniendo información de storage: {e}")
            return {
                "free_space": 0,
                "total_space": 0,
                "used_space": 0,
                "chunk_count": 0,
            }

    async def get_stored_chunks(self) -> List[UUID]:
        """Obtiene la lista de chunks almacenados"""
        chunk_ids = []
        if self.storage_path.exists():
            for chunk_file in self.storage_path.glob("*.chunk"):
                try:
                    chunk_id = UUID(chunk_file.stem)
                    chunk_ids.append(chunk_id)
                except ValueError:
                    logger.warning(
                        f"Nombre de archivo de chunk inválido: {chunk_file.name}"
                    )
                    continue
        return chunk_ids

    async def verify_chunk_integrity(self, chunk_id: UUID) -> bool:
        """Verifica la integridad de un chunk"""
        try:
            chunk_data, calculated_checksum = await self.retrieve_chunk(chunk_id)

            # Verifica contra checksum almacenado
            checksum_path = self.storage_path / f"{chunk_id}.checksum"
            if checksum_path.exists():
                with open(checksum_path, "r") as f:
                    stored_checksum = f.read().strip()
                return calculated_checksum == stored_checksum

            return True  # Si no hay checksum almacenado, asume OK

        except DFSStorageError:
            return False

    async def cleanup_corrupted_chunks(self) -> List[UUID]:
        """Elimina chunks corruptos y retorna la lista de IDs eliminados"""
        corrupted_chunks = []
        stored_chunks = await self.get_stored_chunks()

        for chunk_id in stored_chunks:
            if not await self.verify_chunk_integrity(chunk_id):
                logger.warning(f"Eliminando chunk corrupto: {chunk_id}")
                await self.delete_chunk(chunk_id)
                corrupted_chunks.append(chunk_id)

        return corrupted_chunks
