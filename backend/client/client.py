"""
DFS Client Library - Versión refactorizada completa
"""

import asyncio
import logging
from pathlib import Path
from typing import List, Optional, Callable
from uuid import UUID

import httpx

from core.config import config
from core.exceptions import (
    DFSClientError, DFSMetadataError, DFSNodeUnavailableError, DFSChunkNotFoundError
)
from shared.models import (
    ChunkCommitInfo, CommitRequest, FileMetadata, 
    NodeInfo, UploadInitRequest, UploadInitResponse
)
from shared.utils import calculate_checksum, split_into_chunks

logger = logging.getLogger(__name__)


class DFSClient:
    """Cliente refactorizado para interactuar con el DFS."""
    
    def __init__(
        self, 
        metadata_service_url: Optional[str] = None,
        timeout: Optional[float] = None,
        chunk_size: Optional[int] = None
    ):
        self.metadata_service_url = metadata_service_url or config.metadata_url
        self.timeout = timeout or config.client_timeout
        self.chunk_size = chunk_size or config.chunk_size
        
    async def upload(
        self, 
        local_path: str, 
        remote_path: str, 
        progress_callback: Optional[Callable[[float], None]] = None
    ) -> bool:
        """Sube un archivo al DFS con manejo robusto de errores."""
        logger.info(f"Iniciando upload: {local_path} -> {remote_path}")
        
        file_path = Path(local_path)
        if not file_path.exists():
            raise DFSClientError(f"Archivo no encontrado: {local_path}")
            
        try:
            return await self._upload_file(file_path, remote_path, progress_callback)
        except Exception as e:
            logger.error(f"Error en upload: {e}")
            raise DFSClientError(f"Upload falló: {e}") from e
    
    async def _upload_file(
        self, 
        file_path: Path, 
        remote_path: str, 
        progress_callback: Optional[Callable[[float], None]]
    ) -> bool:
        """Lógica interna de upload."""
        file_size = file_path.stat().st_size
        
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            # 1. Iniciar upload
            upload_plan = await self._init_upload(client, remote_path, file_size)
            file_id = upload_plan.file_id
            
            # 2. Subir chunks
            chunk_commits = await self._upload_chunks(
                client, file_path, upload_plan.chunks, progress_callback
            )
            
            # 3. Commit
            return await self._commit_upload(client, file_id, chunk_commits)
    
    async def _init_upload(
        self, 
        client: httpx.AsyncClient, 
        remote_path: str, 
        file_size: int
    ) -> UploadInitResponse:
        """Inicializa el upload con el metadata service."""
        init_request = UploadInitRequest(
            path=remote_path, 
            size=file_size, 
            chunk_size=self.chunk_size
        )
        
        try:
            response = await client.post(
                f"{self.metadata_service_url}/api/v1/files/upload-init",
                json=init_request.model_dump(mode='json')
            )
            response.raise_for_status()
            
            return UploadInitResponse(**response.json())
            
        except httpx.HTTPStatusError as e:
            raise DFSMetadataError(f"Error en upload-init: {e.response.text}")
        except httpx.RequestError as e:
            raise DFSMetadataError(f"Error de conexión: {e}")
    
    async def _upload_chunks(
        self,
        client: httpx.AsyncClient,
        file_path: Path,
        chunks_plan: List,
        progress_callback: Optional[Callable[[float], None]]
    ) -> List[ChunkCommitInfo]:
        """Sube chunks con pipeline replication."""
        chunk_commits = []
        
        for chunk_index, chunk_data in split_into_chunks(str(file_path), self.chunk_size):
            if chunk_index >= len(chunks_plan):
                raise DFSClientError(f"Chunk index fuera de rango: {chunk_index}")
            
            chunk_plan = chunks_plan[chunk_index]
            chunk_id = chunk_plan.chunk_id
            
            if not chunk_plan.targets:
                raise DFSClientError(f"No hay targets para chunk {chunk_index}")
            
            # Calcular checksum
            checksum = calculate_checksum(chunk_data)
            
            # Pipeline replication
            uploaded_nodes = await self._upload_chunk_with_replication(
                client, chunk_id, chunk_data, chunk_plan.targets
            )
            
            chunk_commits.append(ChunkCommitInfo(
                chunk_id=chunk_id,
                checksum=checksum,
                nodes=uploaded_nodes
            ))
            
            if progress_callback:
                progress = (chunk_index + 1) / len(chunks_plan) * 100
                progress_callback(progress)
        
        return chunk_commits
    
    async def _upload_chunk_with_replication(
        self,
        client: httpx.AsyncClient,
        chunk_id: UUID,
        chunk_data: bytes,
        targets: List[str]
    ) -> List[str]:
        """Sube un chunk con pipeline replication."""
        primary_target = targets[0]
        replication_chain = targets[1:]
        
        logger.info(f"Subiendo chunk {chunk_id} a {primary_target}")
        
        if replication_chain:
            logger.info(f"Cadena de replicación: {' -> '.join(replication_chain)}")
        
        try:
            files = {'file': ('chunk', chunk_data, 'application/octet-stream')}
            params = {}
            
            if replication_chain:
                params['replicate_to'] = '|'.join(replication_chain)
            
            response = await client.put(
                f"{primary_target}/api/v1/chunks/{chunk_id}",
                files=files,
                params=params,
                timeout=120.0
            )
            response.raise_for_status()
            
            result = response.json()
            uploaded_nodes = result.get('nodes', [self._extract_node_id(primary_target)])
            
            logger.info(f"Chunk {chunk_id} replicado a {len(uploaded_nodes)} nodos")
            return uploaded_nodes
            
        except httpx.RequestError as e:
            raise DFSNodeUnavailableError(f"Error subiendo chunk a {primary_target}: {e}")
    
    async def _commit_upload(
        self,
        client: httpx.AsyncClient,
        file_id: UUID,
        chunk_commits: List[ChunkCommitInfo]
    ) -> bool:
        """Confirma la subida del archivo."""
        logger.info(f"Enviando commit con {len(chunk_commits)} chunks")
        
        commit_request = CommitRequest(
            file_id=file_id,
            chunks=chunk_commits
        )
        
        try:
            response = await client.post(
                f"{self.metadata_service_url}/api/v1/files/commit",
                json=commit_request.model_dump(mode='json')
            )
            response.raise_for_status()
            
            logger.info(f"Commit exitoso para file_id={file_id}")
            return True
            
        except httpx.HTTPStatusError as e:
            logger.error(f"Error en commit: {e.response.text}")
            return False
        except httpx.RequestError as e:
            logger.error(f"Error de conexión en commit: {e}")
            return False
    
    async def download(
        self, 
        remote_path: str, 
        local_path: str, 
        progress_callback: Optional[Callable[[float], None]] = None
    ) -> bool:
        """Descarga un archivo del DFS."""
        logger.info(f"Descargando {remote_path} -> {local_path}")
        
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                # 1. Obtener metadata
                file_metadata = await self._get_file_metadata(client, remote_path)
                
                # 2. Descargar chunks
                return await self._download_chunks(
                    client, file_metadata, local_path, progress_callback
                )
                
        except Exception as e:
            logger.error(f"Error en download: {e}")
            raise DFSClientError(f"Download falló: {e}") from e
    
    async def _get_file_metadata(
        self, 
        client: httpx.AsyncClient, 
        remote_path: str
    ) -> FileMetadata:
        """Obtiene metadata del archivo."""
        try:
            response = await client.get(
                f"{self.metadata_service_url}/api/v1/files/{remote_path}"
            )
            response.raise_for_status()
            
            return FileMetadata(**response.json())
            
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise DFSClientError(f"Archivo no encontrado: {remote_path}")
            raise DFSMetadataError(f"Error obteniendo metadata: {e.response.text}")
        except httpx.RequestError as e:
            raise DFSMetadataError(f"Error de conexión: {e}")
    
    async def _download_chunks(
        self,
        client: httpx.AsyncClient,
        file_metadata: FileMetadata,
        local_path: str,
        progress_callback: Optional[Callable[[float], None]]
    ) -> bool:
        """Descarga y combina los chunks."""
        output_path = Path(local_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'wb') as output_file:
            for chunk_index, chunk in enumerate(file_metadata.chunks):
                chunk_data = await self._download_chunk(client, chunk)
                output_file.write(chunk_data)
                
                if progress_callback:
                    progress = (chunk_index + 1) / len(file_metadata.chunks) * 100
                    progress_callback(progress)
        
        logger.info(f"Download completado: {local_path}")
        return True
    
    async def _download_chunk(
        self, 
        client: httpx.AsyncClient, 
        chunk
    ) -> bytes:
        """Descarga un chunk individual."""
        # Intentar cada réplica hasta encontrar una disponible
        for replica in chunk.replicas:
            try:
                response = await client.get(
                    f"{replica.url}/api/v1/chunks/{chunk.chunk_id}",
                    timeout=60.0
                )
                
                if response.status_code == 200:
                    chunk_data = response.content
                    
                    # Verificar checksum
                    if chunk.checksum:
                        calculated = calculate_checksum(chunk_data)
                        if calculated != chunk.checksum:
                            logger.warning(f"Checksum mismatch en chunk {chunk.chunk_id}")
                            continue
                    
                    logger.debug(f"Chunk descargado de {replica.url}")
                    return chunk_data
                    
            except Exception as e:
                logger.warning(f"Error descargando chunk de {replica.url}: {e}")
                continue
        
        raise DFSChunkNotFoundError(f"No se pudo descargar chunk {chunk.chunk_id}")
    
    async def list_files(
        self, 
        prefix: Optional[str] = None, 
        limit: int = 100
    ) -> List[FileMetadata]:
        """Lista archivos en el DFS."""
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                params = {}
                if prefix:
                    params['prefix'] = prefix
                if limit:
                    params['limit'] = limit
                
                response = await client.get(
                    f"{self.metadata_service_url}/api/v1/files",
                    params=params
                )
                response.raise_for_status()
                
                files = [FileMetadata(**f) for f in response.json()]
                return files
                
        except httpx.HTTPStatusError as e:
            raise DFSMetadataError(f"Error listando archivos: {e.response.text}")
        except httpx.RequestError as e:
            raise DFSMetadataError(f"Error de conexión: {e}")
    
    async def delete(self, remote_path: str, permanent: bool = False) -> bool:
        """Elimina un archivo del DFS."""
        logger.info(f"Eliminando {remote_path} (permanent={permanent})")
        
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.delete(
                    f"{self.metadata_service_url}/api/v1/files/{remote_path}",
                    params={"permanent": permanent}
                )
                
                if response.status_code == 200:
                    logger.info(f"Archivo eliminado: {remote_path}")
                    return True
                elif response.status_code == 404:
                    raise DFSClientError(f"Archivo no encontrado: {remote_path}")
                else:
                    logger.error(f"Error eliminando archivo: {response.text}")
                    return False
                    
        except httpx.RequestError as e:
            raise DFSMetadataError(f"Error de conexión: {e}")
    
    async def get_nodes(self) -> List[NodeInfo]:
        """Obtiene lista de nodos."""
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(
                    f"{self.metadata_service_url}/api/v1/nodes"
                )
                response.raise_for_status()
                
                nodes = [NodeInfo(**n) for n in response.json()]
                return nodes
                
        except httpx.HTTPStatusError as e:
            raise DFSMetadataError(f"Error obteniendo nodos: {e.response.text}")
        except httpx.RequestError as e:
            raise DFSMetadataError(f"Error de conexión: {e}")
    
    async def health(self) -> dict:
        """Verifica el estado del servicio."""
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(
                    f"{self.metadata_service_url}/api/v1/health"
                )
                
                if response.status_code == 200:
                    return response.json()
                else:
                    return {"status": "error", "details": response.text}
                    
        except httpx.RequestError as e:
            return {"status": "error", "details": str(e)}
    
    def _extract_node_id(self, url: str) -> str:
        """Extrae node_id de una URL."""
        from urllib.parse import urlparse
        parsed = urlparse(url)
        return f"node-{parsed.hostname}-{parsed.port}"