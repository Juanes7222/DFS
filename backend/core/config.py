"""Configuración centralizada para el sistema DFS"""

import os
from pathlib import Path
from dataclasses import dataclass, field
from typing import List


@dataclass
class DFSConfig:
    """Configuración centralizada del sistema DFS."""

    # Metadata Service
    metadata_host: str = os.getenv("DFS_METADATA_HOST", "localhost")
    metadata_port: int = int(os.getenv("DFS_METADATA_PORT", "8000"))

    # DataNode
    datanode_host: str = os.getenv("DFS_DATANODE_HOST", "localhost")
    datanode_port: int = int(os.getenv("DFS_DATANODE_PORT", "8001"))
    storage_path: Path = Path(os.getenv("DFS_STORAGE_PATH", "/tmp/dfs-data"))

    # Chunk Configuration
    chunk_size: int = int(os.getenv("DFS_CHUNK_SIZE", "67108864"))  # 64MB
    replication_factor: int = int(os.getenv("DFS_REPLICATION_FACTOR", "3"))

    # Timeouts
    client_timeout: float = float(os.getenv("DFS_CLIENT_TIMEOUT", "30.0"))
    heartbeat_interval: int = int(os.getenv("DFS_HEARTBEAT_INTERVAL", "10"))
    node_timeout: int = int(os.getenv("DFS_NODE_TIMEOUT", "60"))

    # Database
    db_path: Path = Path(os.getenv("DFS_DB_PATH", "/tmp/dfs-metadata.db"))

    # Security
    jwt_secret_key: str = os.getenv("DFS_JWT_SECRET_KEY", "CHANGE-ME-IN-PRODUCTION")
    enable_mtls: bool = os.getenv("DFS_ENABLE_MTLS", "false").lower() == "true"

    # Monitoring
    enable_metrics: bool = os.getenv("DFS_ENABLE_METRICS", "true").lower() == "true"
    metrics_port: int = int(os.getenv("DFS_METRICS_PORT", "9090"))

    # Logging
    log_level: str = os.getenv("DFS_LOG_LEVEL", "INFO")
    log_format: str = os.getenv("DFS_LOG_FORMAT", "detailed")
    
    heartbeat_interval: int = int(os.getenv("HEARTBEAT_INTERVAL", 30))
    
    # CORS - Orígenes permitidos
    cors_origins: List[str] = field(default_factory=lambda: os.getenv(
        "CORS_ORIGINS",
        "http://localhost:3000,http://localhost:5173,http://127.0.0.1:3000,http://127.0.0.1:5173"
    ).split(","))
    
    # Permitir todos los orígenes en desarrollo (usar con cuidado)
    cors_allow_all: bool = os.getenv("CORS_ALLOW_ALL", "false").lower() == "true"

    @property
    def metadata_url(self) -> str:
        return f"http://{self.metadata_host}:{self.metadata_port}"

    @property
    def datanode_url(self) -> str:
        return f"http://{self.datanode_host}:{self.datanode_port}"


# Configuración global
config = DFSConfig()
