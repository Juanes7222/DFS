"""
DFS - Sistema de Archivos Distribuido

Un sistema de archivos distribuido escalable y tolerante a fallos.
"""

__version__ = "1.0.0"
__author__ = "DFS Team"
__email__ = "team@dfs.example.com"

# Exportaciones principales
from core.config import config, DFSConfig
from core.exceptions import (
    DFSError,
    DFSClientError,
    DFSMetadataError,
    DFSStorageError,
    DFSNodeUnavailableError,
    DFSChunkNotFoundError,
    DFSLeaseConflictError,
    DFSSecurityError,
    DFSConfigurationError,
)

__all__ = [
    # Configuración
    "config",
    "DFSConfig",
    
    # Excepciones
    "DFSError",
    "DFSClientError", 
    "DFSMetadataError",
    "DFSStorageError",
    "DFSNodeUnavailableError",
    "DFSChunkNotFoundError",
    "DFSLeaseConflictError",
    "DFSSecurityError",
    "DFSConfigurationError",
]