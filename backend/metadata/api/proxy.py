"""
API Router para operaciones de proxy de chunks.
Permite que clientes sin acceso a ZeroTier usen el DFS.
"""
import logging
from uuid import UUID
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException, UploadFile, File, Query, status
from fastapi.responses import StreamingResponse

from metadata import context

logger = logging.getLogger(__name__)

router = APIRouter()


@router.put(
    "/chunks/{chunk_id}",
    status_code=status.HTTP_201_CREATED,
    summary="Upload chunk via proxy",
    description="Permite subir chunks a través del metadata service como proxy"
)
async def proxy_upload_chunk(
    chunk_id: UUID,
    file: UploadFile = File(..., description="Chunk file (max 100MB)"),
    target_nodes: str = Query(..., description="Node IDs separados por comas")
):
    """
    Proxy upload: Cliente → Metadata → DataNodes
    
    El cliente envía el chunk al metadata service, que lo distribuye
    a los DataNodes en ZeroTier usando pipeline replication.
    """
    storage = context.get_storage()
    if not storage:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Storage no disponible"
        )
    
    try:
        # Parsear nodos destino
        node_ids = [nid.strip() for nid in target_nodes.split(",")]
        logger.info(f"Distribuyendo chunk {chunk_id} a {len(node_ids)} nodos: {node_ids}")
        
        # Obtener información de los nodos
        nodes_info = []
        for node_id in node_ids:
            node = await storage.get_node(node_id)
            if not node:
                logger.warning(f"Nodo {node_id} no encontrado")
                continue
            nodes_info.append(node)
        
        if not nodes_info:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No se encontraron nodos destino disponibles"
            )
        
        # Pipeline replication: enviar al primer nodo con cadena de replicación
        primary_node = nodes_info[0]
        replication_chain = nodes_info[1:]
        
        # Construir URL con pipeline replication
        primary_url = f"http://{primary_node.host}:{primary_node.port}"
        upload_url = f"{primary_url}/api/v1/chunks/{chunk_id}"
        
        logger.info(f"Streaming chunk a nodo primario: {primary_node.node_id[:20]}... ({primary_url})")
        
        # Streaming chunked: leer por bloques en lugar de cargar todo
        async def chunk_iterator():
            """Lee y envía el archivo en bloques de 1MB"""
            total_bytes = 0
            while True:
                block = await file.read(1024 * 1024)  # 1MB por bloque
                if not block:
                    break
                total_bytes += len(block)
                yield block
            logger.info(f"Chunk {chunk_id} streaming completado: {total_bytes} bytes")
        
        params = {}
        if replication_chain:
            # Construir cadena de nodos para replicación pipeline
            chain_nodes = "|".join([f"{n.host}:{n.port}" for n in replication_chain])
            params["replicate_to"] = chain_nodes
            logger.info(f"Pipeline replication chain: {chain_nodes}")
        
        # Usar cliente HTTP compartido (connection pooling)
        client = context.get_http_client()
        
        try:
            # Streaming con chunks (no carga todo en memoria)
            from io import BytesIO
            
            # Para httpx con files, necesitamos BytesIO o similar
            # Como workaround temporal, leemos el chunk completo
            # TODO: Implementar streaming verdadero con custom content provider
            chunk_data = await file.read()
            chunk_size = len(chunk_data)
            
            files_payload = {
                "file": (f"chunk_{chunk_id}", BytesIO(chunk_data), "application/octet-stream")
            }
            
            response = await client.put(
                upload_url,
                files=files_payload,
                params=params
            )
            
            if response.status_code not in (200, 201):
                logger.error(
                    f"Error subiendo chunk: Status {response.status_code}, "
                    f"Body: {response.text[:300]}"
                )
                raise HTTPException(
                    status_code=status.HTTP_502_BAD_GATEWAY,
                    detail=f"Error enviando chunk a DataNode: {response.status_code} - {response.text[:100]}"
                )
            
            result = response.json()
            uploaded_nodes = result.get("nodes", [primary_node.node_id])
            
            logger.info(
                f"Chunk {chunk_id} distribuido exitosamente a {len(uploaded_nodes)} nodos"
            )
            
            return {
                "status": "success",
                "chunk_id": str(chunk_id),
                "size": chunk_size,
                "nodes": uploaded_nodes
            }
            
        except httpx.TimeoutException:
            logger.error(f"Timeout enviando chunk a {primary_node.node_id}")
            raise HTTPException(
                status_code=status.HTTP_504_GATEWAY_TIMEOUT,
                detail="Timeout conectando con DataNode"
            )
        except httpx.ConnectError as e:
            logger.error(f"Error de conexión a DataNode: {e}")
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"No se pudo conectar al DataNode: {str(e)}"
            )
                
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error en proxy upload: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error interno: {str(e)}"
        )


@router.get(
    "/chunks/{chunk_id}",
    summary="Download chunk via proxy",
    description="Permite descargar chunks a través del metadata service como proxy"
)
async def proxy_download_chunk(
    chunk_id: UUID,
    file_path: str = Query(..., description="Path del archivo para verificar réplicas")
):
    """
    Proxy download: Cliente → Metadata → DataNode → Cliente
    
    El metadata service busca el chunk en los DataNodes disponibles
    y lo retorna al cliente via streaming.
    """
    storage = context.get_storage()
    if not storage:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Storage no disponible"
        )
    
    try:
        # Obtener metadata del archivo para encontrar réplicas
        file_metadata = await storage.get_file_by_path(file_path)
        if not file_metadata:
            logger.warning(f"Archivo no encontrado en metadata: {file_path}")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Archivo no encontrado: {file_path}"
            )
        
        # Buscar el chunk en la metadata
        chunk_entry = None
        for chunk in file_metadata.chunks:
            if str(chunk.chunk_id) == str(chunk_id):
                chunk_entry = chunk
                break
        
        if not chunk_entry:
            logger.warning(f"Chunk {chunk_id} no encontrado en archivo {file_path}")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Chunk {chunk_id} no encontrado en archivo {file_path}"
            )
        
        if not chunk_entry.replicas:
            logger.warning(f"No hay réplicas para chunk {chunk_id}")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No hay réplicas disponibles para chunk {chunk_id}"
            )
        
        logger.info(
            f"Descargando chunk {chunk_id} desde {len(chunk_entry.replicas)} réplicas disponibles"
        )
        
        # Usar cliente HTTP compartido (connection pooling)
        client = context.get_http_client()
        
        # Intentar descargar desde cada réplica
        for replica in chunk_entry.replicas:
            try:
                download_url = f"{replica.url}/api/v1/chunks/{chunk_id}"
                logger.info(f"Intentando descargar desde: {replica.node_id[:20]}...")
                
                response = await client.get(download_url)
                
                if response.status_code == 200:
                    logger.info(
                        f"Chunk descargado exitosamente desde {replica.node_id[:20]}... "
                        f"(descomprimido: {response.headers.get('X-Decompressed', 'unknown')})"
                    )
                    
                    # Retornar como streaming response
                    headers = {
                        "X-Chunk-ID": str(chunk_id),
                        "X-Node-ID": replica.node_id,
                        "Content-Length": str(len(response.content))
                    }
                    
                    if "X-Checksum" in response.headers:
                        headers["X-Checksum"] = response.headers["X-Checksum"]
                    
                    async def chunk_generator():
                        yield response.content
                    
                    return StreamingResponse(
                        chunk_generator(),
                        media_type="application/octet-stream",
                        headers=headers
                    )
                    
            except httpx.TimeoutException:
                logger.warning(f"Timeout descargando desde {replica.node_id[:20]}...")
                continue
            except httpx.ConnectError:
                logger.warning(f"No se pudo conectar a {replica.node_id[:20]}...")
                continue
            except Exception as e:
                logger.warning(f"Error descargando desde {replica.node_id[:20]}...: {e}")
                continue
        
        # Si llegamos aquí, ninguna réplica funcionó
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="No se pudo descargar el chunk desde ninguna réplica disponible"
        )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error en proxy download: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error interno: {str(e)}"
        )
