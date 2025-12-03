"""
Contexto compartido para servicios del Metadata Service
Este módulo mantiene referencias globales a los servicios inicializados
"""

from typing import Optional
import httpx

from shared.protocols import MetadataStorageBase
from metadata.replicator import ReplicationManager
from metadata.leases import LeaseManager

# Variables globales para servicios
storage: Optional[MetadataStorageBase] = None
replicator: Optional[ReplicationManager] = None
lease_manager: Optional[LeaseManager] = None

# Cliente HTTP compartido con connection pooling
_http_client: Optional[httpx.AsyncClient] = None


def set_storage(instance: Optional[MetadataStorageBase]) -> None:
    """Establece la instancia de storage"""
    global storage
    storage = instance


def set_replicator(instance: Optional[ReplicationManager]) -> None:
    """Establece la instancia de replicator"""
    global replicator
    replicator = instance


def set_lease_manager(instance: Optional[LeaseManager]) -> None:
    """Establece la instancia de lease manager"""
    global lease_manager
    lease_manager = instance


def get_storage() -> Optional[MetadataStorageBase]:
    """Obtiene la instancia de storage"""
    return storage


def get_replicator() -> Optional[ReplicationManager]:
    """Obtiene la instancia de replicator"""
    return replicator


def get_lease_manager() -> Optional[LeaseManager]:
    """Obtiene la instancia de lease manager"""
    return lease_manager


def get_http_client() -> httpx.AsyncClient:
    """Obtiene el cliente HTTP compartido con connection pooling"""
    global _http_client
    if _http_client is None:
        # Configuración optimizada para alto rendimiento
        limits = httpx.Limits(
            max_keepalive_connections=20,  # Conexiones keep-alive
            max_connections=50,  # Máximo de conexiones totales
            keepalive_expiry=30.0  # Mantener conexiones 30s
        )
        _http_client = httpx.AsyncClient(
            timeout=300.0,  # Timeout de 5 minutos
            limits=limits,
            follow_redirects=True
        )
    return _http_client


async def close_http_client() -> None:
    """Cierra el cliente HTTP compartido"""
    global _http_client
    if _http_client is not None:
        await _http_client.aclose()
        _http_client = None
