"""
Gestión de almacenamiento de chunks para DataNode - Versión completa
"""

import asyncio
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
    """Gestiona el almacenamiento y recuperación de chunks."""
    
    def __init__(self, storage_path: Path):
        self.storage_path = storage_path
        self.lock = asyncio.Lock()
    
    async def initialize(self):
        """Inicializa el almacenamiento."""
        self.storage_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Storage inicializado en: {self.storage_path}")
    
    async def store_chunk(
        self, 
        chunk_id: UUID, 
        chunk_data: bytes,
        replicate_to: Optional[str] = None
    ) -> dict:
        """Almacena un chunk con replicación en pipeline."""
        async with self.lock:
            chunk_path = self.storage_path / f"{chunk_id}.chunk"
            checksum_path = self.storage_path / f"{chunk_id}.checksum"
            
            try:
                # Calcular y guardar checksum
                checksum = calculate_checksum(chunk_data)
                
                # Guardar chunk localmente
                with open(chunk_path, 'wb') as f:
                    f.write(chunk_data)
                
                # Guardar checksum
                with open(checksum_path, 'w') as f:
                    f.write(checksum)
                
                logger.info(f"Chunk almacenado: {chunk_id}, size: {len(chunk_data)}")
                
                # Pipeline replication
                replicated_nodes = [self._get_node_id()]
                if replicate_to:
                    replicated_nodes.extend(
                        await self._replicate_to_nodes(chunk_id, chunk_data, replicate_to)
                    )
                
                return {
                    "status": "stored",
                    "chunk_id": str(chunk_id),
                    "size": len(chunk_data),
                    "checksum": checksum,
                    "node_id": self._get_node_id(),
                    "nodes": replicated_nodes
                }
                
            except Exception as e:
                # Cleanup en caso de error
                if chunk_path.exists():
                    chunk_path.unlink()
                if checksum_path.exists():
                    checksum_path.unlink()
                raise DFSStorageError(f"Error almacenando chunk {chunk_id}: {e}")
    
    async def retrieve_chunk(self, chunk_id: UUID) -> Tuple[bytes, str]:
        """Recupera un chunk y verifica su checksum."""
        chunk_path = self.storage_path / f"{chunk_id}.chunk"
        checksum_path = self.storage_path / f"{chunk_id}.checksum"
        
        if not chunk_path.exists():
            raise DFSStorageError(f"Chunk no encontrado: {chunk_id}")
        
        try:
            # Leer chunk
            with open(chunk_path, 'rb') as f:
                chunk_data = f.read()
            
            # Verificar checksum
            calculated_checksum = calculate_checksum(chunk_data)
            
            if checksum_path.exists():
                with open(checksum_path, 'r') as f:
                    stored_checksum = f.read().strip()
                
                if calculated_checksum != stored_checksum:
                    raise DFSStorageError(f"Checksum mismatch para chunk {chunk_id}")
            
            return chunk_data, calculated_checksum
            
        except DFSStorageError:
            raise
        except Exception as e:
            raise DFSStorageError(f"Error recuperando chunk {chunk_id}: {e}")
    
    async def delete_chunk(self, chunk_id: UUID) -> bool:
        """Elimina un chunk."""
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
        self, 
        chunk_id: UUID, 
        chunk_data: bytes, 
        replicate_to: str
    ) -> List[str]:
        """Replica el chunk a los nodos especificados."""
        replicated_nodes = []
        next_nodes = replicate_to.split('|')
        
        if not next_nodes:
            return replicated_nodes
        
        current_target = next_nodes[0]
        remaining_chain = '|'.join(next_nodes[1:]) if len(next_nodes) > 1 else None
        
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                files = {'file': ('chunk', chunk_data, 'application/octet-stream')}
                params = {}
                
                if remaining_chain:
                    params['replicate_to'] = remaining_chain
                
                response = await client.put(
                    f"{current_target}/api/v1/chunks/{chunk_id}",
                    files=files,
                    params=params,
                    timeout=60.0
                )
                
                if response.status_code == 200:
                    result = response.json()
                    downstream_nodes = result.get('nodes', [])
                    replicated_nodes.extend(downstream_nodes)
                    logger.info(f"Replicación exitosa a {current_target}")
                else:
                    logger.error(f"Error replicando a {current_target}: {response.status_code}")
                    
        except Exception as e:
            logger.error(f"Excepción replicando a {replicate_to}: {e}")
        
        return replicated_nodes
    
    def _get_node_id(self) -> str:
        """Obtiene el ID del nodo actual."""
        return f"node-{config.datanode_host}-{config.datanode_port}"
    
    def get_storage_info(self) -> dict:
        """Obtiene información del almacenamiento."""
        if not self.storage_path.exists():
            return {
                "free_space": 0, 
                "total_space": 0, 
                "chunk_count": 0,
                "used_space": 0
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
                "chunk_count": chunk_count
            }
        except Exception as e:
            logger.error(f"Error obteniendo información de storage: {e}")
            return {
                "free_space": 0, 
                "total_space": 0, 
                "used_space": 0,
                "chunk_count": 0
            }
    
    async def get_stored_chunks(self) -> List[UUID]:
        """Obtiene la lista de chunks almacenados."""
        chunk_ids = []
        if self.storage_path.exists():
            for chunk_file in self.storage_path.glob("*.chunk"):
                try:
                    chunk_id = UUID(chunk_file.stem)
                    chunk_ids.append(chunk_id)
                except ValueError:
                    logger.warning(f"Nombre de archivo de chunk inválido: {chunk_file.name}")
                    continue
        return chunk_ids
    
    async def verify_chunk_integrity(self, chunk_id: UUID) -> bool:
        """Verifica la integridad de un chunk."""
        try:
            chunk_data, calculated_checksum = await self.retrieve_chunk(chunk_id)
            
            # Verificar contra checksum almacenado
            checksum_path = self.storage_path / f"{chunk_id}.checksum"
            if checksum_path.exists():
                with open(checksum_path, 'r') as f:
                    stored_checksum = f.read().strip()
                return calculated_checksum == stored_checksum
            
            return True  # Si no hay checksum almacenado, asumir OK
            
        except DFSStorageError:
            return False
    
    async def cleanup_corrupted_chunks(self) -> List[UUID]:
        """Elimina chunks corruptos y retorna la lista de IDs eliminados."""
        corrupted_chunks = []
        stored_chunks = await self.get_stored_chunks()
        
        for chunk_id in stored_chunks:
            if not await self.verify_chunk_integrity(chunk_id):
                logger.warning(f"Eliminando chunk corrupto: {chunk_id}")
                await self.delete_chunk(chunk_id)
                corrupted_chunks.append(chunk_id)
        
        return corrupted_chunks