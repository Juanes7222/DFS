"""
Protocolos y interfaces para el sistema DFS
"""

from abc import ABC, abstractmethod
from typing import List, Optional
from uuid import UUID

from shared.models import (
    FileMetadata,
    NodeInfo,
    ChunkTarget,
    ChunkCommitInfo,
    LeaseResponse,
)


class MetadataStorageBase(ABC):
    """Clase base abstracta para implementaciones de metadata storage"""
    
    @abstractmethod
    async def initialize(self) -> None:
        """Inicializa la base de datos"""
        pass
    
    @abstractmethod
    async def close(self) -> None:
        """Cierra la conexión"""
        pass
    
    @abstractmethod
    async def create_file_metadata(
        self, path: str, size: int, chunks: List[ChunkTarget]
    ) -> FileMetadata:
        """Crea metadata de archivo"""
        pass
    
    @abstractmethod
    async def create_chunk_plan(
        self, chunk_size: int, target_nodes: List[str]
    ) -> ChunkTarget:
        """Crea un plan de chunk con targets"""
        pass
    
    @abstractmethod
    async def commit_file(self, file_id: UUID, chunks: List[ChunkCommitInfo]) -> bool:
        """Confirma la subida de un archivo"""
        pass
    
    @abstractmethod
    async def get_file_by_path(self, path: str) -> Optional[FileMetadata]:
        """Obtiene metadata de archivo por path"""
        pass
    
    @abstractmethod
    async def list_files(
        self, prefix: Optional[str] = None, limit: int = 100, offset: int = 0
    ) -> List[FileMetadata]:
        """Lista los archivos"""
        pass
    
    @abstractmethod
    async def delete_file(self, path: str, permanent: bool = False) -> bool:
        """Elimina un archivo"""
        pass
    
    @abstractmethod
    async def register_node(
        self,
        node_id: str,
        zerotier_node_id: Optional[str],
        zerotier_ip: str,
        listening_ports: Optional[dict] = None,
        capacity_gb: Optional[float] = None,
        version: Optional[str] = None,
        lease_ttl: Optional[int] = None,
        boot_token: Optional[str] = None,
        rack: Optional[str] = None,
    ) -> None:
        """Registra o actualiza un nodo"""
        pass
    
    @abstractmethod
    async def update_node_heartbeat(
        self, 
        node_id: str, 
        free_space: int, 
        total_space: int, 
        chunk_ids: List[UUID],
        zerotier_ip: Optional[str] = None,
        zerotier_node_id: Optional[str] = None,
        url: Optional[str] = None,
    ) -> None:
        """Actualiza heartbeat de un nodo"""
        pass
    
    @abstractmethod
    async def get_node(self, node_id: str) -> Optional[NodeInfo]:
        """Obtiene información de un nodo"""
        pass
    
    @abstractmethod
    async def list_nodes(self) -> List[NodeInfo]:
        """Lista todos los nodos"""
        pass
    
    @abstractmethod
    async def get_active_nodes(self) -> List[NodeInfo]:
        """Obtiene nodos activos"""
        pass
    
    @abstractmethod
    async def acquire_lease(
        self, path: str, operation: str, timeout_seconds: int
    ) -> Optional[LeaseResponse]:
        """Adquiere un lease"""
        pass
    
    @abstractmethod
    async def release_lease(self, lease_id: UUID) -> bool:
        """Libera un lease"""
        pass
    
    @abstractmethod
    async def cleanup_expired_leases(self) -> None:
        """Limpia leases expirados"""
        pass
    
    @abstractmethod
    async def get_system_stats(self) -> dict:
        """Obtiene estadísticas del sistema"""
        pass


class ChunkStorageProtocol(ABC):
    """Protocolo para el almacenamiento de chunks"""

    @abstractmethod
    async def initialize(self):
        """Inicializa el storage"""
        pass

    @abstractmethod
    async def store_chunk(
        self, chunk_id: UUID, chunk_data: bytes, replicate_to: Optional[str] = None
    ) -> dict:
        """Almacena un chunk"""
        pass

    @abstractmethod
    async def retrieve_chunk(self, chunk_id: UUID) -> tuple[bytes, str]:
        """Recupera un chunk"""
        pass

    @abstractmethod
    async def delete_chunk(self, chunk_id: UUID) -> bool:
        """Elimina un chunk"""
        pass

    @abstractmethod
    def get_storage_info(self) -> dict:
        """Obtiene información del almacenamiento"""
        pass


class ReplicationProtocol(ABC):
    """Protocolo para la gestión de replicación"""

    @abstractmethod
    async def start(self):
        """Inicia el replicator"""
        pass

    @abstractmethod
    async def stop(self):
        """Detiene el replicator"""
        pass

    @abstractmethod
    async def check_and_replicate(self):
        """Verifica y ejecuta replicación si es necesario"""
        pass


class HealthCheckProtocol(ABC):
    """Protocolo para health checks"""

    @abstractmethod
    async def check_health(self) -> dict:
        """Realiza health check"""
        pass

    @abstractmethod
    async def get_system_stats(self) -> dict:
        """Obtiene estadísticas del sistema"""
        pass
