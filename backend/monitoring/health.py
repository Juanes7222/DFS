import logging
from datetime import datetime
from typing import Dict, Any, Optional

import httpx

from core.config import config
from shared.models import HealthResponse

logger = logging.getLogger(__name__)


class HealthChecker:
    """
    Sistema de verificación de salud para componentes del DFS.
    """

    def __init__(self, storage=None, replicator=None, lease_manager=None):
        self.storage = storage
        self.replicator = replicator
        self.lease_manager = lease_manager
        self.health_cache: Dict[str, Any] = {}
        self.cache_ttl = 30  # segundos
        self.last_update: Optional[datetime] = None

    async def check_health(self) -> HealthResponse:
        """
        Realiza un health check completo del sistema.

        Returns:
            HealthResponse con el estado del sistema
        """
        try:
            checks = await self._perform_health_checks()
            overall_status = self._determine_overall_status(checks)
            details = await self._get_system_details()

            health_response = HealthResponse(
                status=overall_status,
                details={
                    "checks": checks,
                    "system": details,
                    "timestamp": datetime.utcnow().isoformat(),
                },
            )

            # Actualizar cache
            self.health_cache = {
                "status": overall_status,
                "checks": checks,
                "system": details,
                "timestamp": datetime.utcnow(),
            }

            return health_response

        except Exception as e:
            logger.error(f"Error en health check: {e}")
            return HealthResponse(
                status="unhealthy",
                details={"error": str(e), "timestamp": datetime.utcnow().isoformat()},
            )

    async def _perform_health_checks(self) -> Dict[str, Any]:
        """Realiza checks de salud individuales"""
        checks = {}

        # Check de storage
        checks["storage"] = await self._check_storage_health()

        # Check de replicación
        checks["replication"] = await self._check_replication_health()

        # Check de leases
        checks["leases"] = await self._check_leases_health()

        # Check de nodos
        checks["nodes"] = await self._check_nodes_health()

        # Check de conectividad
        checks["connectivity"] = await self._check_connectivity()

        return checks

    async def _check_storage_health(self) -> Dict[str, Any]:
        """Verifica la salud del storage"""
        if not self.storage:
            return {"status": "unknown", "reason": "Storage no configurado"}

        try:
            # Verifica que podemos acceder al storage
            nodes = await self.storage.get_active_nodes()
            files = await self.storage.list_files(limit=1)

            return {
                "status": "healthy",
                "active_nodes": len(nodes),
                "total_files": len(files),
                "storage_type": "sqlite",  # Podría ser dinámico
            }
        except Exception as e:
            return {"status": "unhealthy", "error": str(e), "storage_type": "sqlite"}

    async def _check_replication_health(self) -> Dict[str, Any]:
        """Verifica la salud del sistema de replicación"""
        if not self.replicator:
            return {"status": "unknown", "reason": "Replicator no configurado"}

        try:
            stats = self.replicator.get_stats()
            active_nodes = await self.storage.get_active_nodes() if self.storage else []

            replication_health = "healthy"
            if len(active_nodes) < config.replication_factor:
                replication_health = "degraded"
            elif stats.get("failed_replications", 0) > stats.get(
                "successful_replications", 0
            ):
                replication_health = "unhealthy"

            return {
                "status": replication_health,
                "replication_factor": config.replication_factor,
                "active_nodes": len(active_nodes),
                "replication_stats": stats,
            }
        except Exception as e:
            return {"status": "unhealthy", "error": str(e)}

    async def _check_leases_health(self) -> Dict[str, Any]:
        """Verifica la salud del sistema de leases"""
        if not self.lease_manager:
            return {"status": "unknown", "reason": "Lease manager no configurado"}

        try:
            stats = self.lease_manager.get_lease_stats()
            active_leases = await self.lease_manager.get_active_leases()

            return {
                "status": "healthy",
                "active_leases": len(active_leases),
                "lease_stats": stats,
            }
        except Exception as e:
            return {"status": "unhealthy", "error": str(e)}

    async def _check_nodes_health(self) -> Dict[str, Any]:
        """Verifica la salud de los nodos del cluster"""
        if not self.storage:
            return {"status": "unknown", "reason": "Storage no configurado"}

        try:
            all_nodes = await self.storage.list_nodes()
            active_nodes = await self.storage.get_active_nodes()

            # Verificar capacidad
            total_capacity = sum(node.total_space for node in active_nodes)
            free_capacity = sum(node.free_space for node in active_nodes)
            used_capacity = total_capacity - free_capacity

            node_health = "healthy"
            if len(active_nodes) < config.replication_factor:
                node_health = "degraded"
            elif len(active_nodes) == 0:
                node_health = "unhealthy"

            # Verificar nodos individuales
            node_details = []
            for node in active_nodes:
                node_details.append(
                    {
                        "node_id": node.node_id,
                        "free_space": node.free_space,
                        "chunk_count": node.chunk_count,
                        "last_heartbeat": node.last_heartbeat.isoformat(),
                    }
                )

            return {
                "status": node_health,
                "total_nodes": len(all_nodes),
                "active_nodes": len(active_nodes),
                "total_capacity": total_capacity,
                "used_capacity": used_capacity,
                "free_capacity": free_capacity,
                "node_details": node_details,
            }
        except Exception as e:
            return {"status": "unhealthy", "error": str(e)}

    async def _check_connectivity(self) -> Dict[str, Any]:
        """Verifica la conectividad básica"""
        checks = {}

        # Check de puertos locales (simulado)
        checks["local_ports"] = {"status": "healthy", "details": "Puertos locales OK"}

        # Check de DNS (simulado)
        checks["dns"] = {"status": "healthy", "details": "DNS funcionando"}

        overall_status = "healthy"
        for check_name, check_result in checks.items():
            if check_result["status"] != "healthy":
                overall_status = "degraded"
                break

        return {"status": overall_status, "checks": checks}

    async def _get_system_details(self) -> Dict[str, Any]:
        """Obtiene detalles del sistema"""
        if not self.storage:
            return {"error": "Storage no disponible para detalles del sistema"}

        try:
            stats = await self.storage.get_system_stats()

            return {
                "version": "1.0.0",
                "replication_factor": config.replication_factor,
                "chunk_size": config.chunk_size,
                "stats": stats,
            }
        except Exception as e:
            return {"error": f"Error obteniendo detalles del sistema: {e}"}

    def _determine_overall_status(self, checks: Dict[str, Any]) -> str:
        """Determina el estado general basado en los checks individuales"""
        status_priority = {"unhealthy": 3, "degraded": 2, "healthy": 1, "unknown": 0}

        worst_status = "healthy"
        for check_name, check_result in checks.items():
            check_status = check_result.get("status", "unknown")
            if status_priority[check_status] > status_priority[worst_status]:
                worst_status = check_status

        return worst_status

    async def get_cached_health(self) -> Dict[str, Any]:
        """Obtiene el health check desde la cache si está fresca"""
        if (
            self.last_update
            and (datetime.utcnow() - self.last_update).total_seconds() < self.cache_ttl
            and self.health_cache
        ):
            return self.health_cache

        # Si la cache está expirada, hacer un nuevo check
        health_response = await self.check_health()
        return health_response.model_dump()

    async def check_datanode_health(self, storage) -> Dict[str, Any]:
        """
        Health check específico para DataNode.

        Args:
            storage: Storage del DataNode

        Returns:
            Dict con información de salud del DataNode
        """
        try:
            storage_info = storage.get_storage_info()
            stored_chunks = await storage.get_stored_chunks()

            # Verificar integridad de chunks (muestra)
            chunk_integrity = "unknown"
            if stored_chunks:
                sample_chunk = stored_chunks[0]
                chunk_integrity = (
                    "healthy"
                    if await storage.verify_chunk_integrity(sample_chunk)
                    else "degraded"
                )

            return {
                "status": "healthy",
                "storage": storage_info,
                "chunks": {"total": len(stored_chunks), "integrity": chunk_integrity},
                "timestamp": datetime.utcnow().isoformat(),
            }

        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat(),
            }


# Health checks externos para servicios dependientes
async def check_external_service_health(
    url: str, timeout: float = 5.0
) -> Dict[str, Any]:
    """
    Verifica la salud de un servicio externo.

    Args:
        url: URL del servicio a verificar
        timeout: Timeout en segundos

    Returns:
        Dict con información de salud
    """
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.get(f"{url}/health")

            if response.status_code == 200:
                data = response.json()
                return {
                    "status": data.get("status", "unknown"),
                    "response_time": response.elapsed.total_seconds(),
                    "details": data.get("details", {}),
                }
            else:
                return {
                    "status": "unhealthy",
                    "error": f"HTTP {response.status_code}",
                    "response_time": response.elapsed.total_seconds(),
                }

    except Exception as e:
        return {"status": "unhealthy", "error": str(e), "response_time": None}


async def check_metadata_service_health(
    metadata_url: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Verifica la salud del Metadata Service.

    Args:
        metadata_url: URL del Metadata Service

    Returns:
        Dict con información de salud
    """
    url = metadata_url or config.metadata_url
    return await check_external_service_health(url)


async def check_datanode_health(datanode_url: str) -> Dict[str, Any]:
    """
    Verifica la salud de un DataNode específico.

    Args:
        datanode_url: URL del DataNode

    Returns:
        Dict con información de salud
    """
    return await check_external_service_health(datanode_url)


# Utilidades para health checks
def is_healthy(health_data: Dict[str, Any]) -> bool:
    """Verifica si los datos de salud indican un estado saludable"""
    return health_data.get("status") == "healthy"


def get_health_summary(health_data: Dict[str, Any]) -> str:
    """Obtiene un resumen legible del estado de salud"""
    status = health_data.get("status", "unknown")
    checks = health_data.get("details", {}).get("checks", {})

    healthy_checks = sum(
        1 for check in checks.values() if check.get("status") == "healthy"
    )
    total_checks = len(checks)

    return f"{status.upper()} ({healthy_checks}/{total_checks} checks passed)"
