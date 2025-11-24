import logging
from typing import List, Optional

from fastapi import APIRouter, HTTPException, status, Query

from core.config import config
from monitoring.metrics import (
    record_upload_operation,
    record_download_operation,
    record_delete_operation,
)
from shared import (
    CommitRequest,
    FileMetadata,
    UploadInitRequest,
    UploadInitResponse,
    format_bytes,
)

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


def get_lease_manager():
    """Dependency para obtener lease manager instance"""
    from metadata import context

    return context.lease_manager


@router.post("/files/upload-init", response_model=UploadInitResponse)
async def upload_init(request: UploadInitRequest):
    """
    Inicia una subida de archivo.
    Devuelve un plan de chunks con targets para cada réplica.
    """
    logger.info(f"Upload init: {request.path}, size: {format_bytes(request.size)}")

    storage = get_storage()
    
    try:
        # Calcular número de chunks
        num_chunks = (request.size + request.chunk_size - 1) // request.chunk_size

        # Obtener nodos activos
        nodes = await storage.get_active_nodes()
        logger.info(f"Nodos activos disponibles: {len(nodes)}")
        if len(nodes) < config.replication_factor:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Nodos insuficientes: {len(nodes)} < {config.replication_factor}",
            )

        logger.info(f"Planificando {num_chunks} chunks para {request.path}")

        # Crear plan de chunks con estrategia round-robin
        chunks = []
        for i in range(num_chunks):
            chunk_size = min(request.chunk_size, request.size - i * request.chunk_size)

            # Seleccionar nodos para réplicas
            target_nodes = []
            for j in range(config.replication_factor):
                node_idx = (i * config.replication_factor + j) % len(nodes)
                node = nodes[node_idx]
                target_nodes.append(f"http://{node.host}:{node.port}")

            chunk_target = await storage.create_chunk_plan(chunk_size, target_nodes)
            chunks.append(chunk_target)

        # Crear metadata de archivo
        file_metadata = await storage.create_file_metadata(
            path=request.path, size=request.size, chunks=chunks
        )

        logger.info(
            f"Upload init exitoso: {request.path} (ID: {file_metadata.file_id})"
        )

        return UploadInitResponse(file_id=file_metadata.file_id, chunks=chunks)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error en upload-init: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error interno del servidor: {str(e)}",
        )


@router.post("/files/commit")
async def commit_upload(request: CommitRequest):
    """
    Confirma que los chunks han sido subidos correctamente.
    Valida que se hayan creado las réplicas necesarias.
    """
    logger.info(
        f"Commit upload: file_id={request.file_id}, chunks={len(request.chunks)}"
    )

    storage = get_storage()

    try:
        # Validar replicación de chunks
        under_replicated_chunks = []
        for chunk_info in request.chunks:
            if len(chunk_info.nodes) < config.replication_factor:
                under_replicated_chunks.append(
                    {
                        "chunk_id": chunk_info.chunk_id,
                        "current_replicas": len(chunk_info.nodes),
                        "expected_replicas": config.replication_factor,
                    }
                )

        if under_replicated_chunks:
            logger.warning(
                f"Chunks con replicación insuficiente: {under_replicated_chunks}"
            )

        # Commit file metadata
        success = await storage.commit_file(request.file_id, request.chunks)

        if not success:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No se pudo confirmar la subida",
            )

        # Registrar métrica
        record_upload_operation(True)

        # Stats
        total_replicas = sum(len(c.nodes) for c in request.chunks)
        logger.info(
            f"Commit exitoso: {len(request.chunks)} chunks, {total_replicas} réplicas"
        )

        return {
            "status": "committed",
            "file_id": str(request.file_id),
            "chunks": len(request.chunks),
            "total_replicas": total_replicas,
            "under_replicated_chunks": under_replicated_chunks,
        }

    except HTTPException:
        record_upload_operation(False)
        raise
    except Exception as e:
        logger.error(f"Error en commit: {e}")
        record_upload_operation(False)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error interno del servidor: {str(e)}",
        )


@router.get("/files/{path:path}", response_model=FileMetadata)
async def get_file_metadata(path: str):
    """Obtiene metadata de un archivo"""
    logger.debug(f"Get file metadata: {path}")

    storage = get_storage()

    try:
        file_metadata = await storage.get_file_by_path(path)
        if not file_metadata:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Archivo no encontrado: {path}",
            )

        record_download_operation(True)
        return file_metadata

    except HTTPException:
        record_download_operation(False)
        raise
    except Exception as e:
        logger.error(f"Error obteniendo metadata: {e}")
        record_download_operation(False)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error interno del servidor: {str(e)}",
        )


@router.get("/files", response_model=List[FileMetadata])
async def list_files(
    prefix: Optional[str] = Query(None, description="Filtrar por prefijo"),
    limit: int = Query(100, description="Límite de resultados", ge=1, le=1000),
    offset: int = Query(0, description="Offset para paginación", ge=0),
):
    """Lista archivos con paginación y filtros"""
    logger.debug(f"List files: prefix={prefix}, limit={limit}, offset={offset}")

    storage = get_storage()

    try:
        files = await storage.list_files(prefix=prefix, limit=limit, offset=offset)
        return files
    except Exception as e:
        logger.error(f"Error listando archivos: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error interno del servidor: {str(e)}",
        )


@router.delete("/files/{path:path}")
async def delete_file(
    path: str, permanent: bool = Query(False, description="Eliminar permanentemente")
):
    """Elimina un archivo (soft-delete por default)"""
    logger.info(f"elete file: {path}, permanent={permanent}")

    storage = get_storage()
    lease_mgr = get_lease_manager()

    try:
        # Adquirir lease para operación de eliminación
        lease = None
        if lease_mgr:
            lease = await lease_mgr.acquire_lease(
                path=path, operation="delete", timeout_seconds=300
            )

        try:
            success = await storage.delete_file(path, permanent=permanent)
            if not success:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Archivo no encontrado: {path}",
                )

            record_delete_operation(True)

            action = (
                "eliminado permanentemente" if permanent else "marcado como eliminado"
            )
            logger.info(f"Archivo {action}: {path}")

            return {
                "status": "deleted",
                "path": path,
                "permanent": permanent,
                "action": action,
            }

        finally:
            # Libera lease
            if lease and lease_mgr:
                await lease_mgr.release_lease(lease.lease_id, path)

    except HTTPException:
        record_delete_operation(False)
        raise
    except Exception as e:
        logger.error(f"Error eliminando archivo: {e}")
        record_delete_operation(False)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error interno del servidor: {str(e)}",
        )
