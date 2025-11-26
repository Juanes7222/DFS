"""Modelos de datos compartidos para el DFS - Versión refactorizada"""

from datetime import datetime
from enum import Enum
from typing import List, Optional, Dict
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, ConfigDict


class NodeState(str, Enum):
    """Estado de un nodo"""

    ACTIVE = "active"
    INACTIVE = "inactive"
    DRAINING = "draining"
    FAILED = "failed"


class ChunkState(str, Enum):
    """Estado de una réplica de chunk"""

    PENDING = "pending"
    COMMITTED = "committed"
    CORRUPTED = "corrupted"
    DELETED = "deleted"


class ReplicaInfo(BaseModel):
    """Información de una réplica de chunk"""

    model_config = ConfigDict(from_attributes=True)

    node_id: str
    url: str
    state: ChunkState = ChunkState.PENDING
    last_heartbeat: Optional[datetime] = None
    checksum_verified: bool = False


class ChunkEntry(BaseModel):
    """Entrada de chunk en un archivo"""

    model_config = ConfigDict(from_attributes=True)

    chunk_id: UUID = Field(default_factory=uuid4)
    seq_index: int
    size: int
    checksum: Optional[str] = None  # SHA256
    replicas: List[ReplicaInfo] = Field(default_factory=list)


class FileMetadata(BaseModel):
    """Metadatos de un archivo"""

    model_config = ConfigDict(from_attributes=True)

    file_id: UUID = Field(default_factory=uuid4)
    path: str
    size: int
    created_at: datetime = Field(default_factory=datetime.utcnow)
    modified_at: datetime = Field(default_factory=datetime.utcnow)
    chunks: List[ChunkEntry] = Field(default_factory=list)
    is_deleted: bool = False
    deleted_at: Optional[datetime] = None


class NodeInfo(BaseModel):
    """Información de un nodo de almacenamiento"""

    model_config = ConfigDict(from_attributes=True)

    node_id: str
    host: str
    port: int
    rack: Optional[str] = None
    free_space: int  # bytes
    total_space: int  # bytes
    chunk_count: int = 0
    last_heartbeat: datetime = Field(default_factory=datetime.utcnow)
    state: NodeState = NodeState.ACTIVE


class UploadInitRequest(BaseModel):
    """Request para iniciar una subida"""

    model_config = ConfigDict(from_attributes=True)

    path: str
    size: int
    chunk_size: int = 64 * 1024 * 1024  # 64MB default


class ChunkTarget(BaseModel):
    """Target para subir un chunk"""

    model_config = ConfigDict(from_attributes=True)

    chunk_id: UUID
    size: int
    targets: List[str]  # URLs de DataNodes


class UploadInitResponse(BaseModel):
    """Response de upload-init"""

    model_config = ConfigDict(from_attributes=True)

    file_id: UUID
    chunks: List[ChunkTarget]


class ChunkCommitInfo(BaseModel):
    """Información de commit de un chunk"""

    model_config = ConfigDict(from_attributes=True)

    chunk_id: UUID
    checksum: str
    nodes: List[str]  # node_ids donde se escribió


class CommitRequest(BaseModel):
    """Request para confirmar una subida"""

    model_config = ConfigDict(from_attributes=True)

    file_id: UUID
    chunks: List[ChunkCommitInfo]


class HeartbeatRequest(BaseModel):
    """Request de heartbeat de un DataNode"""

    model_config = ConfigDict(from_attributes=True)

    node_id: str
    free_space: int
    total_space: int
    chunk_ids: List[UUID]
    url: Optional[str] = None  # URL pública del DataNode
    zerotier_ip: Optional[str] = None  # IP de ZeroTier
    zerotier_node_id: Optional[str] = None  # ID del nodo en ZeroTier


class LeaseRequest(BaseModel):
    """Request para adquirir un lease"""

    model_config = ConfigDict(from_attributes=True)

    path: str
    operation: str  # "write" | "delete"
    timeout_seconds: int = 300


class LeaseResponse(BaseModel):
    """Response de lease"""

    model_config = ConfigDict(from_attributes=True)

    lease_id: UUID
    path: str
    expires_at: datetime


class HealthResponse(BaseModel):
    """Response de health check"""

    model_config = ConfigDict(from_attributes=True)

    status: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    details: Optional[dict] = None


class SystemStats(BaseModel):
    """Estadísticas del sistema"""

    model_config = ConfigDict(from_attributes=True)

    total_files: int
    total_chunks: int
    total_nodes: int
    active_nodes: int
    total_size: int
    total_space: int
    used_space: int
    free_space: int
    replication_factor: int

class RegisterRequest(BaseModel):
    node_id: str = Field(..., description="UUID persistente del nodo")
    zerotier_node_id: Optional[str] = Field(None, description="ZeroTier member id (opcional)")
    zerotier_ip: Optional[str] = Field(None, description="IP asignada por ZeroTier (opcional)")
    listening_ports: Optional[Dict[str,int]] = {}
    data_port: Optional[int] = Field(None, description="Puerto donde el DataNode sirve chunks (opcional)")
    capacity_gb: Optional[float] = None
    version: Optional[str] = None
    bootstrap_token: Optional[str] = Field(None, description="Token de bootstrap (puede ir en body o en header)")
    lease_ttl: Optional[int] = None