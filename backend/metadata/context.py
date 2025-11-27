"""
Contexto compartido para servicios del Metadata Service
Este módulo mantiene referencias globales a los servicios inicializados
"""

from typing import Optional

from shared.protocols import MetadataStorageBase
from metadata.replicator import ReplicationManager
from metadata.leases import LeaseManager

# Variables globales para servicios
storage: Optional[MetadataStorageBase] = None
replicator: Optional[ReplicationManager] = None
lease_manager: Optional[LeaseManager] = None


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
