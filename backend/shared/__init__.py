"""
Módulo compartido con modelos, utilidades y protocolos para el DFS
"""

# Modelos
from .models import (
    ChunkEntry,
    ChunkState,
    ChunkTarget,
    ChunkCommitInfo,
    CommitRequest,
    FileMetadata,
    HeartbeatRequest,
    LeaseRequest,
    LeaseResponse,
    NodeInfo,
    NodeState,
    ReplicaInfo,
    UploadInitRequest,
    UploadInitResponse,
    HealthResponse,
    SystemStats,
)

# Utilidades
from .utils import (
    calculate_checksum,
    calculate_file_checksum,
    format_bytes,
    split_into_chunks,
)

# Seguridad
from .security import (
    JWTManager,
    TokenData,
    MTLSConfig,
    jwt_manager,
    verify_jwt_token,
    require_permission,
)

# Protocolos
from .protocols import (
    MetadataStorageProtocol,
    ChunkStorageProtocol,
    ReplicationProtocol,
    HealthCheckProtocol,
)

__all__ = [
    # Models
    "ChunkEntry",
    "ChunkState",
    "ChunkTarget",
    "ChunkCommitInfo",
    "CommitRequest",
    "FileMetadata",
    "HeartbeatRequest",
    "LeaseRequest",
    "LeaseResponse",
    "NodeInfo",
    "NodeState",
    "ReplicaInfo",
    "UploadInitRequest",
    "UploadInitResponse",
    "HealthResponse",
    "SystemStats",
    # Utils
    "calculate_checksum",
    "calculate_file_checksum",
    "format_bytes",
    "split_into_chunks",
    # Security
    "JWTManager",
    "TokenData",
    "MTLSConfig",
    "jwt_manager",
    "verify_jwt_token",
    "require_permission",
    # Protocols
    "MetadataStorageProtocol",
    "ChunkStorageProtocol",
    "ReplicationProtocol",
    "HealthCheckProtocol",
]
