"""
Storage backend con etcd para Alta Disponibilidad
"""
import json
import logging
from datetime import datetime
from typing import List, Optional
from uuid import UUID

try:
    import etcd3
    ETCD_AVAILABLE = True
except ImportError:
    ETCD_AVAILABLE = False
    logging.warning("etcd3 no está instalado. Instalar con: pip install etcd3")

import sys
sys.path.append('/home/ubuntu/dfs-system')
from shared import FileMetadata, NodeInfo, ChunkTarget

logger = logging.getLogger(__name__)


class EtcdMetadataStorage:
    """
    Backend de almacenamiento usando etcd para metadata distribuida.
    Proporciona consistencia fuerte y alta disponibilidad.
    """
    
    def __init__(self, endpoints: List[str] = None):
        """
        Inicializa conexión con etcd cluster.
        
        Args:
            endpoints: Lista de endpoints etcd (ej: ["localhost:2379"])
        """
        if not ETCD_AVAILABLE:
            raise RuntimeError("etcd3 no está instalado")
        
        self.endpoints = endpoints or ["localhost:2379"]
        
        # Parsear primer endpoint
        host, port = self.endpoints[0].split(":")
        port = int(port)
        
        # Conectar a etcd
        self.client = etcd3.client(host=host, port=port)
        
        # Prefijos para diferentes tipos de datos
        self.PREFIX_FILES = "/dfs/files/"
        self.PREFIX_NODES = "/dfs/nodes/"
        self.PREFIX_CHUNKS = "/dfs/chunks/"
        self.PREFIX_LEASES = "/dfs/leases/"
        
        logger.info(f"Conectado a etcd: {self.endpoints}")
    
    async def initialize(self):
        """Inicializa el storage (no-op para etcd)"""
        # Verificar conectividad
        try:
            self.client.status()
            logger.info("etcd storage inicializado correctamente")
        except Exception as e:
            logger.error(f"Error conectando a etcd: {e}")
            raise
    
    async def close(self):
        """Cierra conexión con etcd"""
        try:
            self.client.close()
            logger.info("Conexión con etcd cerrada")
        except Exception as e:
            logger.error(f"Error cerrando conexión con etcd: {e}")
    
    # ========================================================================
    # FILES
    # ========================================================================
    
    async def create_file_metadata(self, path: str, size: int, chunks: List[ChunkTarget]) -> FileMetadata:
        """Crea metadata de un archivo"""
        file_metadata = FileMetadata(
            path=path,
            size=size,
            chunks=chunks,
            created_at=datetime.utcnow(),
            modified_at=datetime.utcnow(),
            is_deleted=False
        )
        
        key = self.PREFIX_FILES + path
        value = file_metadata.model_dump_json()
        
        # Usar transacción para evitar sobrescribir
        success = self.client.transaction(
            compare=[self.client.transactions.version(key) == 0],
            success=[self.client.transactions.put(key, value)],
            failure=[]
        )
        
        if not success:
            raise ValueError(f"Archivo ya existe: {path}")
        
        logger.info(f"Metadata creada: {path}")
        return file_metadata
    
    async def get_file_metadata(self, path: str) -> Optional[FileMetadata]:
        """Obtiene metadata de un archivo"""
        key = self.PREFIX_FILES + path
        value, metadata = self.client.get(key)
        
        if value is None:
            return None
        
        data = json.loads(value.decode('utf-8'))
        return FileMetadata(**data)
    
    async def list_files(self, prefix: str = "", limit: int = 1000, offset: int = 0) -> List[FileMetadata]:
        """Lista archivos con filtro opcional"""
        search_prefix = self.PREFIX_FILES + prefix
        
        files = []
        for value, metadata in self.client.get_prefix(search_prefix):
            data = json.loads(value.decode('utf-8'))
            file_metadata = FileMetadata(**data)
            
            # Filtrar archivos eliminados
            if not file_metadata.is_deleted:
                files.append(file_metadata)
        
        # Ordenar por path
        files.sort(key=lambda f: f.path)
        
        # Aplicar paginación
        return files[offset:offset + limit]
    
    async def update_file_metadata(self, path: str, **updates) -> FileMetadata:
        """Actualiza metadata de un archivo"""
        file_metadata = await self.get_file_metadata(path)
        if not file_metadata:
            raise ValueError(f"Archivo no encontrado: {path}")
        
        # Aplicar actualizaciones
        for key, value in updates.items():
            if hasattr(file_metadata, key):
                setattr(file_metadata, key, value)
        
        file_metadata.modified_at = datetime.utcnow()
        
        # Guardar
        key = self.PREFIX_FILES + path
        value = file_metadata.model_dump_json()
        self.client.put(key, value)
        
        logger.info(f"Metadata actualizada: {path}")
        return file_metadata
    
    async def delete_file_metadata(self, path: str, permanent: bool = False) -> bool:
        """Elimina metadata de un archivo"""
        if permanent:
            # Eliminación permanente
            key = self.PREFIX_FILES + path
            deleted = self.client.delete(key)
            logger.info(f"Metadata eliminada permanentemente: {path}")
            return deleted
        else:
            # Soft delete
            await self.update_file_metadata(path, is_deleted=True)
            logger.info(f"Archivo marcado como eliminado: {path}")
            return True
    
    # ========================================================================
    # NODES
    # ========================================================================
    
    async def register_node(self, node_info: NodeInfo) -> NodeInfo:
        """Registra o actualiza un nodo"""
        key = self.PREFIX_NODES + node_info.node_id
        value = node_info.model_dump_json()
        
        # Usar lease para auto-expiración (heartbeat)
        lease = self.client.lease(ttl=60)  # 60 segundos
        self.client.put(key, value, lease=lease)
        
        logger.info(f"Nodo registrado: {node_info.node_id}")
        return node_info
    
    async def get_node(self, node_id: str) -> Optional[NodeInfo]:
        """Obtiene información de un nodo"""
        key = self.PREFIX_NODES + node_id
        value, metadata = self.client.get(key)
        
        if value is None:
            return None
        
        data = json.loads(value.decode('utf-8'))
        return NodeInfo(**data)
    
    async def get_all_nodes(self) -> List[NodeInfo]:
        """Obtiene todos los nodos registrados"""
        nodes = []
        for value, metadata in self.client.get_prefix(self.PREFIX_NODES):
            data = json.loads(value.decode('utf-8'))
            nodes.append(NodeInfo(**data))
        
        return nodes
    
    async def get_active_nodes(self, timeout_seconds: int = 30) -> List[NodeInfo]:
        """Obtiene nodos activos (con heartbeat reciente)"""
        # En etcd, los nodos con lease expirado ya no existen
        # Así que todos los nodos en etcd son "activos"
        return await self.get_all_nodes()
    
    async def update_node_heartbeat(self, node_id: str, free_space: int, total_space: int, chunk_ids: List[UUID]) -> NodeInfo:
        """Actualiza heartbeat de un nodo"""
        node_info = await self.get_node(node_id)
        
        if node_info:
            # Actualizar nodo existente
            node_info.free_space = free_space
            node_info.total_space = total_space
            node_info.chunk_count = len(chunk_ids)
            node_info.last_heartbeat = datetime.utcnow()
            node_info.state = "active"
        else:
            # Crear nuevo nodo
            # Extraer host y port del node_id (formato: node-host-port)
            parts = node_id.split('-')
            host = parts[1] if len(parts) > 1 else "unknown"
            port = int(parts[2]) if len(parts) > 2 else 8001
            
            node_info = NodeInfo(
                node_id=node_id,
                host=host,
                port=port,
                free_space=free_space,
                total_space=total_space,
                chunk_count=len(chunk_ids),
                last_heartbeat=datetime.utcnow(),
                state="active"
            )
        
        # Registrar con lease
        await self.register_node(node_info)
        
        # Actualizar índice de chunks por nodo
        for chunk_id in chunk_ids:
            chunk_key = f"{self.PREFIX_CHUNKS}{chunk_id}/nodes/{node_id}"
            self.client.put(chunk_key, "1")
        
        return node_info
    
    # ========================================================================
    # CHUNKS
    # ========================================================================
    
    async def create_chunk_plan(self, size: int, targets: List[str]) -> ChunkTarget:
        """Crea un plan para un chunk"""
        from uuid import uuid4
        
        chunk_target = ChunkTarget(
            chunk_id=uuid4(),
            size=size,
            targets=targets
        )
        
        # Guardar plan en etcd
        key = f"{self.PREFIX_CHUNKS}{chunk_target.chunk_id}/plan"
        value = chunk_target.model_dump_json()
        self.client.put(key, value)
        
        return chunk_target
    
    async def get_chunks_by_node(self, node_id: str) -> List[UUID]:
        """Obtiene lista de chunks almacenados en un nodo"""
        prefix = f"{self.PREFIX_CHUNKS}"
        chunk_ids = []
        
        for key, value in self.client.get_prefix(prefix):
            key_str = key.decode('utf-8')
            if f"/nodes/{node_id}" in key_str:
                # Extraer chunk_id del key
                chunk_id_str = key_str.split('/')[3]
                try:
                    chunk_ids.append(UUID(chunk_id_str))
                except ValueError:
                    pass
        
        return chunk_ids
    
    async def get_under_replicated_chunks(self, replication_factor: int) -> List[tuple]:
        """Obtiene chunks que necesitan más réplicas"""
        under_replicated = []
        
        # Obtener todos los archivos
        files = await self.list_files()
        
        for file in files:
            for chunk in file.chunks:
                # Contar réplicas activas
                active_replicas = [r for r in chunk.replicas if r.state == "active"]
                
                if len(active_replicas) < replication_factor:
                    under_replicated.append((
                        chunk.chunk_id,
                        file.path,
                        len(active_replicas),
                        active_replicas
                    ))
        
        return under_replicated
    
    # ========================================================================
    # LEASES
    # ========================================================================
    
    async def acquire_lease(self, path: str, client_id: str, timeout: int = 60) -> Optional[str]:
        """Adquiere un lease exclusivo para un path"""
        from uuid import uuid4
        
        lease_id = str(uuid4())
        key = self.PREFIX_LEASES + path
        value = json.dumps({
            "lease_id": lease_id,
            "client_id": client_id,
            "path": path,
            "acquired_at": datetime.utcnow().isoformat()
        })
        
        # Usar transacción para garantizar exclusividad
        lease = self.client.lease(ttl=timeout)
        success = self.client.transaction(
            compare=[self.client.transactions.version(key) == 0],
            success=[self.client.transactions.put(key, value, lease=lease)],
            failure=[]
        )
        
        if success:
            logger.info(f"Lease adquirido: {path} por {client_id}")
            return lease_id
        else:
            logger.warning(f"Lease no disponible: {path}")
            return None
    
    async def release_lease(self, lease_id: str) -> bool:
        """Libera un lease"""
        # Buscar el lease por ID
        for key, value in self.client.get_prefix(self.PREFIX_LEASES):
            data = json.loads(value.decode('utf-8'))
            if data.get("lease_id") == lease_id:
                self.client.delete(key)
                logger.info(f"Lease liberado: {lease_id}")
                return True
        
        return False
    
    # ========================================================================
    # STATS
    # ========================================================================
    
    async def get_stats(self) -> dict:
        """Obtiene estadísticas del sistema"""
        files = await self.list_files()
        nodes = await self.get_all_nodes()
        
        total_size = sum(f.size for f in files)
        total_chunks = sum(len(f.chunks) for f in files)
        
        total_space = sum(n.total_space for n in nodes)
        free_space = sum(n.free_space for n in nodes)
        used_space = total_space - free_space
        
        return {
            "total_files": len(files),
            "total_chunks": total_chunks,
            "total_nodes": len(nodes),
            "active_nodes": len([n for n in nodes if n.state == "active"]),
            "total_size": total_size,
            "total_space": total_space,
            "used_space": used_space,
            "free_space": free_space
        }
