"""
Modelos de datos compartidos para el DFS
"""
from datetime import datetime
from enum import Enum
from typing import List, Optional
from uuid import UUID, uuid4
from pydantic import BaseModel, Field


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
    node_id: str
    url: str
    state: ChunkState = ChunkState.PENDING
    last_heartbeat: Optional[datetime] = None
    checksum_verified: bool = False


class ChunkEntry(BaseModel):
    """Entrada de chunk en un archivo"""
    chunk_id: UUID = Field(default_factory=uuid4)
    seq_index: int
    size: int
    checksum: Optional[str] = None  # SHA256
    replicas: List[ReplicaInfo] = Field(default_factory=list)


class FileMetadata(BaseModel):
    """Metadatos de un archivo"""
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
    path: str
    size: int
    chunk_size: int = 64 * 1024 * 1024  # 64MB default


class ChunkTarget(BaseModel):
    """Target para subir un chunk"""
    chunk_id: UUID
    size: int
    targets: List[str]  # URLs de DataNodes


class UploadInitResponse(BaseModel):
    """Response de upload-init"""
    file_id: UUID
    chunks: List[ChunkTarget]


class ChunkCommitInfo(BaseModel):
    """Información de commit de un chunk"""
    chunk_id: UUID
    checksum: str
    nodes: List[str]  # node_ids donde se escribió


class CommitRequest(BaseModel):
    """Request para confirmar una subida"""
    file_id: UUID
    chunks: List[ChunkCommitInfo]


class HeartbeatRequest(BaseModel):
    """Request de heartbeat de un DataNode"""
    node_id: str
    free_space: int
    total_space: int
    chunk_ids: List[UUID]


class LeaseRequest(BaseModel):
    """Request para adquirir un lease"""
    path: str
    operation: str  # "write" | "delete"
    timeout_seconds: int = 300


class LeaseResponse(BaseModel):
    """Response de lease"""
    lease_id: UUID
    path: str
    expires_at: datetime


class HealthResponse(BaseModel):
    """Response de health check"""
    status: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    details: Optional[dict] = None
