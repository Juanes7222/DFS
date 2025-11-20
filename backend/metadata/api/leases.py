"""
API Router para operaciones de leases
"""

import logging
from typing import Optional
from uuid import UUID

from fastapi import APIRouter, HTTPException, status, Query

from shared import LeaseRequest, LeaseResponse

logger = logging.getLogger(__name__)

router = APIRouter()


def get_lease_manager():
    """Dependency para obtener lease manager instance"""
    from metadata import context
    if not context.lease_manager:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Lease manager no inicializado",
        )
    return context.lease_manager


@router.post("/leases/acquire", response_model=LeaseResponse)
async def acquire_lease(request: LeaseRequest):
    """
    Adquiere un lease exclusivo para una operación sobre un archivo.
    Los leases previenen escrituras concurrentes al mismo archivo.
    """
    logger.debug(f"Acquire lease: {request.path}, operation={request.operation}")
    
    lease_mgr = get_lease_manager()
    
    try:
        lease = await lease_mgr.acquire_lease(
            path=request.path,
            operation=request.operation,
            timeout_seconds=request.timeout_seconds,
        )
        
        if not lease:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"No se pudo adquirir lease para {request.path}",
            )
        
        logger.debug(f"Lease adquirido: {lease.lease_id} para {request.path}")
        return lease
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error adquiriendo lease: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error interno del servidor: {str(e)}",
        )


@router.post("/leases/release")
async def release_lease(lease_id: UUID, path: Optional[str] = None):
    """
    Libera un lease previamente adquirido.
    Permite que otros clientes adquieran leases sobre el mismo archivo.
    """
    logger.debug(f"Release lease: {lease_id}")
    
    lease_mgr = get_lease_manager()
    
    try:
        success = await lease_mgr.release_lease(lease_id, path)
        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Lease no encontrado: {lease_id}",
            )
        
        logger.debug(f"Lease liberado: {lease_id}")
        return {"status": "released", "lease_id": str(lease_id)}
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error liberando lease: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error interno del servidor: {str(e)}",
        )


@router.post("/leases/renew")
async def renew_lease(
    lease_id: UUID,
    path: str,
    extension_seconds: int = Query(60, description="Segundos de extensión")
):
    """
    Renueva un lease existente, extendiendo su tiempo de vida.
    Útil para operaciones de larga duración.
    """
    logger.debug(f"Renew lease: {lease_id}, extension={extension_seconds}s")
    
    lease_mgr = get_lease_manager()
    
    try:
        success = await lease_mgr.renew_lease(lease_id, path, extension_seconds)
        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Lease no encontrado o expirado: {lease_id}",
            )
        
        logger.debug(f"Lease renovado: {lease_id}")
        return {
            "status": "renewed",
            "lease_id": str(lease_id),
            "extension_seconds": extension_seconds,
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error renovando lease: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error interno del servidor: {str(e)}",
        )


@router.get("/leases/stats")
async def get_lease_stats():
    """
    Obtiene estadísticas sobre el uso de leases en el sistema.
    Incluye leases activos, expirados y tasa de renovación.
    """
    logger.debug("Get lease stats")
    
    lease_mgr = get_lease_manager()
    
    try:
        stats = lease_mgr.get_lease_stats()
        return stats
    
    except Exception as e:
        logger.error(f"Error obteniendo stats de leases: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error interno del servidor: {str(e)}",
        )
