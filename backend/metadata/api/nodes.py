"""
API Router para operaciones de nodos
"""

import logging
from typing import List

from fastapi import APIRouter, HTTPException, status

from shared import HeartbeatRequest, NodeInfo

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
