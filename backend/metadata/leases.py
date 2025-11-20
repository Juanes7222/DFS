from __future__ import annotations

"""
Gestión de leases para operaciones concurrentes - Versión completa
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, List
from uuid import UUID

from backend.core.exceptions import DFSLeaseConflictError
from backend.shared.models import (
    LeaseResponse,
)  # asumes que LeaseResponse existe y está tipado

logger = logging.getLogger(__name__)


class LeaseManager:
    """
    Gestiona leases para operaciones concurrentes.
    Proporciona exclusividad mutua para operaciones en archivos.
    """

    def __init__(self, storage):
        self.storage = storage
        # `LeaseInfo` es una forward reference pero `from __future__ import annotations`
        # permite usarla directamente aquí.
        self.local_leases: Dict[str, LeaseInfo] = {}
        self.lock = asyncio.Lock()

    async def acquire_lease(
        self,
        path: str,
        operation: str,
        client_id: Optional[str] = None,
        timeout_seconds: int = 300,
    ) -> LeaseResponse:
        """
        Adquiere un lease para una operación.
        """
        async with self.lock:
            # Verificar lease local primero (para eficiencia)
            if path in self.local_leases:
                lease_info = self.local_leases[path]
                if lease_info.is_valid() and not lease_info.is_expired():
                    raise DFSLeaseConflictError(
                        f"Lease activo existente para {path} (expira: {lease_info.expires_at})"
                    )
                else:
                    # Remover lease expirado
                    del self.local_leases[path]

            # Intentar adquirir lease en el storage
            lease_response = await self.storage.acquire_lease(
                path=path, operation=operation, timeout_seconds=timeout_seconds
            )

            if not lease_response:
                raise DFSLeaseConflictError(f"No se pudo adquirir lease para {path}")

            # Registrar lease localmente
            lease_info = LeaseInfo(
                lease_id=lease_response.lease_id,
                path=path,
                operation=operation,
                client_id=client_id,
                expires_at=lease_response.expires_at,
            )

            self.local_leases[path] = lease_info
            logger.info(f"Lease adquirido: {path} (ID: {lease_response.lease_id})")

            return lease_response

    async def release_lease(self, lease_id: UUID, path: Optional[str] = None) -> bool:
        """
        Libera un lease.
        """
        async with self.lock:
            # Liberar en el storage
            success = await self.storage.release_lease(lease_id)

            # Limpiar localmente
            if path and path in self.local_leases:
                if self.local_leases[path].lease_id == lease_id:
                    del self.local_leases[path]
                    logger.info(f"Lease local liberado: {path}")

            if success:
                logger.info(f"Lease liberado: {lease_id}")
            else:
                logger.warning(f"Lease no encontrado al liberar: {lease_id}")

            return success

    async def validate_lease(self, lease_id: UUID, path: str) -> bool:
        """
        Valida si un lease es aún válido.
        """
        async with self.lock:
            # Verificar localmente primero
            if path in self.local_leases:
                lease_info = self.local_leases[path]
                if (
                    lease_info.lease_id == lease_id
                    and lease_info.is_valid()
                    and not lease_info.is_expired()
                ):
                    return True

            # Si no está localmente o es inválido, verificar con el storage
            # En un sistema real, aquí verificarías con el storage distribuido
            # Por simplicidad, asumimos que si no está localmente, es inválido
            return False

    async def renew_lease(
        self, lease_id: UUID, path: str, timeout_seconds: int = 300
    ) -> bool:
        """
        Renueva un lease existente.
        """
        async with self.lock:
            # Verificar que el lease exista y sea válido
            if not await self.validate_lease(lease_id, path):
                return False

            # Liberar el lease existente
            await self.storage.release_lease(lease_id)

            # Adquirir nuevo lease
            try:
                lease_info = self.local_leases[path]
                new_lease = await self.acquire_lease(
                    path=path,
                    operation=lease_info.operation,
                    client_id=lease_info.client_id,
                    timeout_seconds=timeout_seconds,
                )

                logger.info(f"Lease renovado: {path} (nuevo ID: {new_lease.lease_id})")
                return True

            except DFSLeaseConflictError:
                logger.error(f"No se pudo renovar lease para {path}")
                return False

    async def get_active_leases(self) -> List[LeaseInfo]:
        """
        Obtiene la lista de leases activos.
        """
        async with self.lock:
            now = datetime.utcnow()
            active_leases: List[LeaseInfo] = []
            expired_paths: List[str] = []

            for path, lease_info in list(self.local_leases.items()):
                if lease_info.is_valid() and not lease_info.is_expired(now):
                    active_leases.append(lease_info)
                else:
                    # recolectamos para borrar fuera del bucle de iteración
                    expired_paths.append(path)

            for p in expired_paths:
                del self.local_leases[p]

            return active_leases

    async def cleanup_expired_leases(self):
        """Limpia leases expirados localmente."""
        async with self.lock:
            now = datetime.utcnow()
            expired_paths = [
                p for p, li in self.local_leases.items() if li.is_expired(now)
            ]

            for path in expired_paths:
                del self.local_leases[path]
                logger.debug(f"Lease local expirado limpiado: {path}")

    def get_lease_stats(self) -> Dict:
        """Obtiene estadísticas de leases."""
        now = datetime.utcnow()
        total_leases = len(self.local_leases)
        active_leases = sum(
            1
            for lease_info in self.local_leases.values()
            if not lease_info.is_expired(now)
        )
        expired_leases = total_leases - active_leases

        return {
            "total_leases": total_leases,
            "active_leases": active_leases,
            "expired_leases": expired_leases,
        }


class LeaseInfo:
    """Información de un lease local."""

    def __init__(
        self,
        lease_id: UUID,
        path: str,
        operation: str,
        client_id: Optional[str] = None,
        expires_at: Optional[datetime] = None,
        timeout_seconds: int = 300,
    ):
        self.lease_id = lease_id
        self.path = path
        self.operation = operation
        self.client_id = client_id
        # expires_at puede ser pasado (datetime) o None -> calculamos a partir de timeout
        self.expires_at: datetime = expires_at or (
            datetime.utcnow() + timedelta(seconds=timeout_seconds)
        )
        self.created_at: datetime = datetime.utcnow()

    def is_valid(self) -> bool:
        """Verifica si el lease es válido."""
        return self.expires_at > datetime.utcnow()

    def is_expired(self, now: Optional[datetime] = None) -> bool:
        """Verifica si el lease ha expirado."""
        if now is None:
            now = datetime.utcnow()
        return self.expires_at <= now

    def time_remaining(self) -> float:
        """Obtiene el tiempo restante del lease en segundos."""
        now = datetime.utcnow()
        if self.is_expired(now):
            return 0.0
        return (self.expires_at - now).total_seconds()

    def to_dict(self) -> Dict:
        """Convierte a diccionario."""
        return {
            "lease_id": str(self.lease_id),
            "path": self.path,
            "operation": self.operation,
            "client_id": self.client_id,
            "expires_at": self.expires_at.isoformat(),
            "created_at": self.created_at.isoformat(),
            "time_remaining": self.time_remaining(),
        }
