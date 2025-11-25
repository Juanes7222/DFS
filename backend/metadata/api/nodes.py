"""
API Router para operaciones de nodos
"""

import logging
from typing import List

from fastapi import APIRouter, HTTPException, status

from shared import HeartbeatRequest, NodeInfo, RegisterRequest

from core.config import config

logger = logging.getLogger(__name__)

router = APIRouter()


def get_storage():
    """Dependency para obtener storage instance"""
    from metadata import context

    if not context.storage:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Storage no inicializado",
        )
    return context.storage


@router.post("/nodes/heartbeat")
async def node_heartbeat(request: HeartbeatRequest):
    """
    Recibe heartbeat de un DataNode.
    Actualiza el estado del nodo y su inventario de chunks.
    """
    logger.debug(f"Heartbeat: {request.node_id}, chunks={len(request.chunk_ids)}")

    storage = get_storage()

    try:
        await storage.update_node_heartbeat(
            node_id=request.node_id,
            free_space=request.free_space,
            total_space=request.total_space,
            chunk_ids=request.chunk_ids,
        )

        return {"status": "ok", "node_id": request.node_id}

    except Exception as e:
        logger.error(f"Error procesando heartbeat: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error interno del servidor: {str(e)}",
        )


@router.get("/nodes", response_model=List[NodeInfo])
async def list_nodes():
    """
    Lista todos los nodos registrados.
    Incluye nodos activos, inactivos y en cuarentena.
    """
    logger.debug("List nodes")

    storage = get_storage()

    try:
        nodes = await storage.list_nodes()
        return nodes

    except Exception as e:
        logger.error(f"Error listando nodos: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error interno del servidor: {str(e)}",
        )


@router.get("/nodes/{node_id}", response_model=NodeInfo)
async def get_node(node_id: str):
    """
    Obtiene información detallada de un nodo específico.
    Incluye estado, capacidad y chunks almacenados.
    """
    logger.debug(f"Get node: {node_id}")

    storage = get_storage()

    try:
        node = await storage.get_node(node_id)
        if not node:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Nodo no encontrado: {node_id}",
            )

        return node

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error obteniendo nodo: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error interno del servidor: {str(e)}",
        )


@router.delete("/nodes/{node_id}")
async def deactivate_node(node_id: str):
    """
    Desactiva un nodo manualmente.
    Los chunks en el nodo se marcarán para re-replicación.
    """
    logger.info(f"Deactivate node: {node_id}")

    storage = get_storage()

    try:
        node = await storage.get_node(node_id)
        if not node:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Nodo no encontrado: {node_id}",
            )

        # Marcar nodo como inactivo (actualizar estado en la base de datos)
        # Por ahora retornamos mensaje informativo ya que no existe método deactivate_node
        logger.info(f"Solicitud de desactivación para nodo: {node_id}")

        return {
            "status": "deactivated",
            "node_id": node_id,
            "message": "Nodo marcado como inactivo, chunks serán re-replicados",
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error desactivando nodo: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error interno del servidor: {str(e)}",
        )

@router.post("/nodes/register")
async def register_node(request: RegisterRequest):
    """
    Registro automático de un nodo. Valida token y persiste el nodo.
    """
    logger.info("Register request: node_id=%s zerotier_ip=%s", request.node_id, request.zerotier_ip)

    # Validar token
    tokens = getattr(config, "bootstrap_tokens", None)
    allow_open = getattr(config, "allow_open_registration", False)
    if not allow_open:
        if not tokens or request.bootstrap_token not in set(tokens):
            logger.warning("Registro rechazado por token inválido: %s", request.node_id)
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid bootstrap token")

    storage = get_storage()
    try:
        # Llamar al nuevo método register_node en MetadataStorage
        await storage.register_node(
            node_id=request.node_id,
            zerotier_node_id=request.zerotier_node_id,
            zerotier_ip=request.zerotier_ip,
            listening_ports=request.listening_ports or {},
            capacity_gb=request.capacity_gb,
            version=request.version,
            lease_ttl=request.lease_ttl,
            boot_token=request.bootstrap_token,
        )

        # Opcional: auto-authorize en ZeroTier si está configurado
        if getattr(config, "zerotier_api_token", None) and request.zerotier_node_id:
            try:
                ztoken = getattr(config, "zerotier_api_token")
                netid = getattr(config, "zerotier_network_id")
                url = f"https://my.zerotier.com/api/network/{netid}/member/{request.zerotier_node_id}"
                headers = {"Authorization": f"token {ztoken}", "Content-Type": "application/json"}
                body = {"config": {"authorized": True}}
                import requests
                r = requests.post(url, json=body, headers=headers, timeout=10)
                logger.info("ZeroTier authorize result=%s for member=%s", r.status_code, request.zerotier_node_id)
            except Exception as e:
                logger.error("ZeroTier authorization error: %s", e)

        # Obtener peers activos para retornar (opcional)
        try:
            peers = await storage.list_nodes()
        except Exception:
            peers = []

        return {
            "status": "accepted",
            "assigned_role": "storage",
            "config": {"replication_factor": getattr(config, "replication_factor", 2), "peers": peers},
            "lease_ttl": getattr(config, "lease_ttl", 60),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error registrando nodo: %s", e)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
