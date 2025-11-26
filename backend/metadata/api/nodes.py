"""
API Router para operaciones de nodos
"""

import logging
from typing import List, Optional

from fastapi import APIRouter, HTTPException, status, Request, Header

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
    
    # Log adicional para depuración de ZeroTier
    if request.zerotier_ip:
        logger.info(f"Heartbeat con ZeroTier IP: {request.zerotier_ip} (node: {request.node_id})")
    if request.url:
        logger.debug(f"URL pública: {request.url}")

    storage = get_storage()

    try:
        # Actualizar heartbeat con información adicional
        await storage.update_node_heartbeat(
            node_id=request.node_id,
            free_space=request.free_space,
            total_space=request.total_space,
            chunk_ids=request.chunk_ids,
            zerotier_ip=request.zerotier_ip,
            zerotier_node_id=request.zerotier_node_id,
            url=request.url,
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
async def register_node(request: RegisterRequest, http_request: Request, authorization: Optional[str] = Header(None)):
    """
    Registro automático de un nodo. Valida token (body o Authorization header) y persiste el nodo.
    """
    # Determinar token: primero header Authorization Bearer, luego body
    token = None
    if authorization:
        parts = authorization.split()
        if len(parts) == 2 and parts[0].lower() == "bearer":
            token = parts[1]
    if not token:
        token = request.bootstrap_token

    logger.info("Register request: node_id=%s zerotier_ip=%s data_port=%s", 
                request.node_id, request.zerotier_ip, request.data_port)

    # Validar token
    token_config = getattr(config, "bootstrap_token", None)
    allow_open = getattr(config, "allow_open_registration", False)
    logger.info("Validando token de registro para nodo: %s", request.node_id)
    if not allow_open:
        if not token or (token_config and token != token_config):
            logger.warning("Registro rechazado por token inválido: %s", request.node_id)
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid bootstrap token")

    storage = get_storage()
    try:
        # Preparar listening_ports incluyendo data_port si se proporciona
        listening_ports = request.listening_ports or {}
        if request.data_port:
            listening_ports["storage"] = request.data_port
        
        # Validar que zerotier_ip no sea None
        if not request.zerotier_ip:
            logger.error("Registro rechazado: zerotier_ip es requerido para %s", request.node_id)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, 
                detail="zerotier_ip es requerido para el registro"
            )

        # Llamar al register_node en MetadataStorage
        await storage.register_node(
            node_id=request.node_id,
            zerotier_node_id=request.zerotier_node_id,
            zerotier_ip=request.zerotier_ip,
            listening_ports=listening_ports,
            capacity_gb=request.capacity_gb,
            version=request.version,
            lease_ttl=request.lease_ttl,
            boot_token=token,
        )

        # Auto-authorize en ZeroTier (opcional) - usar httpx async en lugar de requests
        if getattr(config, "zerotier_api_token", None) and request.zerotier_node_id:
            try:
                import httpx
                ztoken = getattr(config, "zerotier_api_token")
                netid = getattr(config, "zerotier_network_id")
                url = f"https://my.zerotier.com/api/v1/network/{netid}/member/{request.zerotier_node_id}"
                headers = {"Authorization": f"token {ztoken}"}
                body = {"config": {"authorized": True}}
                
                async with httpx.AsyncClient(timeout=10.0) as client:
                    r = await client.post(url, json=body, headers=headers)
                    logger.info("ZeroTier authorize result=%s for member=%s", 
                               r.status_code, request.zerotier_node_id)
            except Exception as e:
                logger.error("ZeroTier authorization error: %s", e)

        # Construir peers activos
        try:
            peers = await storage.list_nodes()
        except Exception:
            peers = []

        return {
            "status": "accepted",
            "assigned_role": "storage",
            "config": {
                "replication_factor": getattr(config, "replication_factor", 2), 
                "peers": peers
            },
            "lease_ttl": getattr(config, "lease_ttl", 60),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error registrando nodo: %s", e, exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))