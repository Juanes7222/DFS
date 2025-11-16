"""
DFS Client Library - Librería Python para interactuar con el DFS
"""
import asyncio
import logging
from pathlib import Path
from typing import List, Optional
from uuid import UUID

import httpx

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from shared import (
    ChunkCommitInfo,
    CommitRequest,
    FileMetadata,
    NodeInfo,
    UploadInitRequest,
    calculate_checksum,
    split_into_chunks,
)

logger = logging.getLogger(__name__)


class DFSClient:
    """
    Cliente para interactuar con el DFS.
    Maneja upload, download, list, delete de archivos.
    """
    
    def __init__(self, metadata_service_url: str = "http://localhost:8000", timeout: float = 30.0):
        self.metadata_service_url = metadata_service_url
        self.timeout = timeout
        self.chunk_size = 64 * 1024 * 1024  # 64MB
    
    async def upload(self, local_path: str, remote_path: str, progress_callback=None) -> bool:
        """
        Sube un archivo al DFS.
        
        Args:
            local_path: Ruta local del archivo
            remote_path: Ruta remota en el DFS
            progress_callback: Función callback para progreso (opcional)
        
        Returns:
            True si la subida fue exitosa
        """
        logger.info(f"Subiendo {local_path} -> {remote_path}")
        
        file_path = Path(local_path)
        if not file_path.exists():
            raise FileNotFoundError(f"Archivo no encontrado: {local_path}")
        
        file_size = file_path.stat().st_size
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            # 1. Iniciar upload
            init_request = UploadInitRequest(
                path=remote_path,
                size=file_size,
                chunk_size=self.chunk_size
            )
            
            logger.info(f"Enviando upload-init request: path={remote_path}, size={file_size}")
            
            response = await client.post(
                f"{self.metadata_service_url}/api/v1/files/upload-init",
                json=init_request.model_dump(mode='json')
            )
            
            if response.status_code != 200:
                logger.error(f"Error en upload-init: {response.status_code} - {response.text}")
                return False
            
            upload_plan = response.json()
            file_id = UUID(upload_plan['file_id'])
            chunks_plan = upload_plan['chunks']
            
            logger.info(f"Plan de upload recibido: file_id={file_id}, {len(chunks_plan)} chunks")
            
            # 2. Subir chunks con pipeline replication
            chunk_commits = []
            
            for chunk_index, chunk_data in split_into_chunks(str(file_path), self.chunk_size):
                if chunk_index >= len(chunks_plan):
                    logger.error(f"Chunk index fuera de rango: {chunk_index}")
                    return False
                
                chunk_plan = chunks_plan[chunk_index]
                chunk_id = UUID(chunk_plan['chunk_id'])
                targets = chunk_plan['targets']
                
                if not targets:
                    logger.error(f"No hay targets para chunk {chunk_index}")
                    return False
                
                # Calcular checksum
                checksum = calculate_checksum(chunk_data)
                
                # Pipeline replication: subir solo al primer nodo
                # El primer nodo replicará al segundo, el segundo al tercero, etc.
                primary_target = targets[0]
                replication_chain = targets[1:]
                
                logger.info(f"Subiendo chunk {chunk_index}/{len(chunks_plan)} (size={len(chunk_data)}) a {primary_target}")
                if replication_chain:
                    logger.info(f"  Cadena de replicación: {' -> '.join(replication_chain)}")
                
                try:
                    # Crear form data
                    files = {'file': ('chunk', chunk_data, 'application/octet-stream')}
                    
                    # Pasar la cadena completa de replicación
                    params = {}
                    if replication_chain:
                        params['replicate_to'] = '|'.join(replication_chain)
                    
                    response = await client.put(
                        f"{primary_target}/api/v1/chunks/{chunk_id}",
                        files=files,
                        params=params,
                        timeout=120.0  # Más tiempo para pipeline completo
                    )
                    
                    if response.status_code == 200:
                        result = response.json()
                        
                        # El resultado incluye todos los nodos que almacenaron
                        uploaded_nodes = result.get('nodes', [self._extract_node_id(primary_target)])
                        
                        logger.info(f"Chunk {chunk_index} replicado a {len(uploaded_nodes)} nodos: {uploaded_nodes}")
                        
                        chunk_commits.append(ChunkCommitInfo(
                            chunk_id=chunk_id,
                            checksum=checksum,
                            nodes=uploaded_nodes
                        ))
                    else:
                        logger.error(f"Error subiendo chunk {chunk_index}: {response.status_code} - {response.text}")
                        return False
                
                except Exception as e:
                    logger.error(f"Excepción subiendo chunk {chunk_index}: {e}", exc_info=True)
                    return False
                
                if progress_callback:
                    progress = (chunk_index + 1) / len(chunks_plan) * 100
                    progress_callback(progress)
            
            # 3. Commit
            logger.info(f"Enviando commit con {len(chunk_commits)} chunks")
            
            commit_request = CommitRequest(
                file_id=file_id,
                chunks=chunk_commits
            )
            
            response = await client.post(
                f"{self.metadata_service_url}/api/v1/files/commit",
                json=commit_request.model_dump(mode='json')
            )
            
            if response.status_code != 200:
                logger.error(f"Error en commit: {response.status_code} - {response.text}")
                return False
            
            result = response.json()
            logger.info(f"Commit exitoso: {result}")
            logger.info(f"Upload completado: {remote_path}")
            return True
    
    async def download(self, remote_path: str, local_path: str, progress_callback=None) -> bool:
        """
        Descarga un archivo del DFS.
        
        Args:
            remote_path: Ruta remota en el DFS
            local_path: Ruta local donde guardar
            progress_callback: Función callback para progreso (opcional)
        
        Returns:
            True si la descarga fue exitosa
        """
        logger.info(f"Descargando {remote_path} -> {local_path}")
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            # 1. Obtener metadata
            response = await client.get(
                f"{self.metadata_service_url}/api/v1/files/{remote_path}"
            )
            
            if response.status_code != 200:
                logger.error(f"Error obteniendo metadata: {response.text}")
                return False
            
            file_metadata = FileMetadata(**response.json())
            logger.info(f"Descargando {len(file_metadata.chunks)} chunks")
            
            # 2. Descargar chunks
            output_path = Path(local_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'wb') as output_file:
                for chunk_index, chunk in enumerate(file_metadata.chunks):
                    # Seleccionar réplica (primera disponible)
                    chunk_data = None
                    
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
                                        logger.error(f"Checksum mismatch en chunk {chunk_index}")
                                        chunk_data = None
                                        continue
                                
                                logger.debug(f"Chunk {chunk_index} descargado de {replica.url}")
                                break
                        
                        except Exception as e:
                            logger.warning(f"Error descargando chunk {chunk_index} de {replica.url}: {e}")
                    
                    if chunk_data is None:
                        logger.error(f"No se pudo descargar chunk {chunk_index}")
                        return False
                    
                    output_file.write(chunk_data)
                    
                    if progress_callback:
                        progress = (chunk_index + 1) / len(file_metadata.chunks) * 100
                        progress_callback(progress)
            
            logger.info(f"Download completado: {local_path}")
            return True
    
    async def list_files(self, prefix: Optional[str] = None) -> List[FileMetadata]:
        """
        Lista archivos en el DFS.
        
        Args:
            prefix: Prefijo opcional para filtrar
        
        Returns:
            Lista de FileMetadata
        """
        async with httpx.AsyncClient(timeout=10.0) as client:
            params = {}
            if prefix:
                params['prefix'] = prefix
            
            response = await client.get(
                f"{self.metadata_service_url}/api/v1/files",
                params=params
            )
            
            if response.status_code != 200:
                logger.error(f"Error listando archivos: {response.text}")
                return []
            
            files = [FileMetadata(**f) for f in response.json()]
            return files
    
    async def delete(self, remote_path: str, permanent: bool = False) -> bool:
        """
        Elimina un archivo del DFS.
        
        Args:
            remote_path: Ruta remota en el DFS
            permanent: Si True, elimina permanentemente
        
        Returns:
            True si la eliminación fue exitosa
        """
        logger.info(f"Eliminando {remote_path} (permanent={permanent})")
        
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.delete(
                f"{self.metadata_service_url}/api/v1/files/{remote_path}",
                params={"permanent": permanent}
            )
            
            if response.status_code != 200:
                logger.error(f"Error eliminando archivo: {response.text}")
                return False
            
            logger.info(f"Archivo eliminado: {remote_path}")
            return True
    
    async def get_nodes(self) -> List[NodeInfo]:
        """
        Obtiene lista de nodos.
        
        Returns:
            Lista de NodeInfo
        """
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(
                f"{self.metadata_service_url}/api/v1/nodes"
            )
            
            if response.status_code != 200:
                logger.error(f"Error obteniendo nodos: {response.text}")
                return []
            
            nodes = [NodeInfo(**n) for n in response.json()]
            return nodes
    
    async def health(self) -> dict:
        """
        Verifica el estado del servicio.
        
        Returns:
            Dict con información de health
        """
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(
                f"{self.metadata_service_url}/api/v1/health"
            )
            
            if response.status_code != 200:
                return {"status": "error", "details": response.text}
            
            return response.json()
    
    def _extract_node_id(self, url: str) -> str:
        """Extrae node_id de una URL"""
        # En un sistema real, esto sería más robusto
        # Por ahora, generamos un node_id basado en la URL
        from urllib.parse import urlparse
        parsed = urlparse(url)
        return f"node-{parsed.hostname}-{parsed.port}"
