"""
Storage backend para metadata (SQLite para MVP)
"""
import asyncio
import json
import logging
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional
from uuid import UUID, uuid4

import sys
sys.path.append(str(Path(__file__).parent.parent))

from shared import (
    ChunkEntry,
    ChunkState,
    ChunkTarget,
    ChunkCommitInfo,
    FileMetadata,
    LeaseResponse,
    NodeInfo,
    NodeState,
    ReplicaInfo,
)

logger = logging.getLogger(__name__)


class MetadataStorage:
    """
    Storage backend para metadata usando SQLite.
    En producción, esto se reemplazaría con etcd o similar.
    """
    
    def __init__(self, db_path: Optional[str] = None):
        if db_path is None:
            self.db_path = str(Path(__file__).parent / "tmp" / "dfs_metadata.db")
        else:
            self.db_path = db_path
        
        # Ensure the directory for the database file exists
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn: Optional[sqlite3.Connection] = None
        self.lock = asyncio.Lock()
    
    async def initialize(self):
        """Inicializa la base de datos"""
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        
        # Crear tablas
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS files (
                file_id TEXT PRIMARY KEY,
                path TEXT UNIQUE NOT NULL,
                size INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                modified_at TEXT NOT NULL,
                is_deleted INTEGER DEFAULT 0,
                deleted_at TEXT,
                chunks_json TEXT NOT NULL
            )
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS nodes (
                node_id TEXT PRIMARY KEY,
                host TEXT NOT NULL,
                port INTEGER NOT NULL,
                rack TEXT,
                free_space INTEGER NOT NULL,
                total_space INTEGER NOT NULL,
                chunk_count INTEGER DEFAULT 0,
                last_heartbeat TEXT NOT NULL,
                state TEXT NOT NULL
            )
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS leases (
                lease_id TEXT PRIMARY KEY,
                path TEXT NOT NULL,
                operation TEXT NOT NULL,
                expires_at TEXT NOT NULL
            )
        """)
        
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_files_path ON files(path)
        """)
        
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_files_deleted ON files(is_deleted)
        """)
        
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_leases_path ON leases(path)
        """)
        
        self.conn.commit()
    
    async def close(self):
        """Cierra la conexión"""
        if self.conn:
            self.conn.close()
    
    # ========================================================================
    # FILES
    # ========================================================================
    
    async def create_file_metadata(
        self,
        path: str,
        size: int,
        chunks: List[ChunkTarget]
    ) -> FileMetadata:
        """Crea metadata de archivo con chunks sin réplicas (se llenan en commit)"""
        async with self.lock:
            file_id = uuid4()
            now = datetime.utcnow().isoformat()
            
            # Convertir chunks a ChunkEntry sin réplicas aún
            chunk_entries = []
            for i, chunk_target in enumerate(chunks):
                chunk_entry = ChunkEntry(
                    chunk_id=chunk_target.chunk_id,
                    seq_index=i,
                    size=chunk_target.size,
                    replicas=[]  # Se llenarán en commit con nodos reales
                )
                chunk_entries.append(chunk_entry)
            
            file_metadata = FileMetadata(
                file_id=file_id,
                path=path,
                size=size,
                created_at=datetime.utcnow(),
                modified_at=datetime.utcnow(),
                chunks=chunk_entries
            )
            
            chunks_json = json.dumps([c.model_dump(mode='json') for c in chunk_entries])
            
            self.conn.execute(
                """
                INSERT INTO files (file_id, path, size, created_at, modified_at, chunks_json)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (str(file_id), path, size, now, now, chunks_json)
            )
            self.conn.commit()
            
            return file_metadata
    
    async def create_chunk_plan(
        self,
        chunk_size: int,
        target_nodes: List[str]
    ) -> ChunkTarget:
        """Crea un plan de chunk con targets"""
        return ChunkTarget(
            chunk_id=uuid4(),
            size=chunk_size,
            targets=target_nodes
        )
    
    async def commit_file(
        self,
        file_id: UUID,
        chunks: List[ChunkCommitInfo]
    ) -> bool:
        """Confirma la subida de un archivo y crea las réplicas"""
        async with self.lock:
            # Obtener archivo
            row = self.conn.execute(
                "SELECT chunks_json FROM files WHERE file_id = ?",
                (str(file_id),)
            ).fetchone()
            
            if not row:
                logger.error(f"Archivo no encontrado para commit: {file_id}")
                return False
            
            # Cargar chunks existentes
            chunk_entries = [ChunkEntry(**c) for c in json.loads(row['chunks_json'])]
            chunk_map = {str(c.chunk_id): c for c in chunk_entries}
            
            # Actualizar chunks con checksums y crear réplicas
            for commit_info in chunks:
                chunk_id_str = str(commit_info.chunk_id)
                if chunk_id_str in chunk_map:
                    chunk = chunk_map[chunk_id_str]
                    chunk.checksum = commit_info.checksum
                    
                    # Crear réplicas basadas en los nodos reales
                    chunk.replicas = []
                    for node_id in commit_info.nodes:
                        # Construir URL del nodo desde el node_id
                        # Formato node_id: "node-host-port"
                        parts = node_id.split('-')
                        if len(parts) >= 3:
                            host = parts[1]
                            port = parts[2]
                            node_url = f"http://{host}:{port}"
                        else:
                            node_url = f"http://unknown/{node_id}"
                        
                        replica = ReplicaInfo(
                            node_id=node_id,
                            url=node_url,
                            state=ChunkState.COMMITTED,
                            last_heartbeat=datetime.utcnow(),
                            checksum_verified=True
                        )
                        chunk.replicas.append(replica)
                    
                    logger.info(f"Chunk {chunk_id_str}: {len(chunk.replicas)} réplicas confirmadas")
                else:
                    logger.warning(f"Chunk {chunk_id_str} no encontrado en metadata")
            
            # Guardar
            chunks_json = json.dumps([c.model_dump(mode='json') for c in chunk_entries])
            now = datetime.utcnow().isoformat()
            
            self.conn.execute(
                "UPDATE files SET chunks_json = ?, modified_at = ? WHERE file_id = ?",
                (chunks_json, now, str(file_id))
            )
            self.conn.commit()
            
            logger.info(f"Commit exitoso para file_id={file_id}, {len(chunks)} chunks")
            return True
    
    async def get_file_by_path(self, path: str) -> Optional[FileMetadata]:
        """Obtiene metadata de archivo por path"""
        async with self.lock:
            row = self.conn.execute(
                """
                SELECT * FROM files 
                WHERE path = ? AND is_deleted = 0
                """,
                (path,)
            ).fetchone()
            
            if not row:
                return None
            
            chunks = [ChunkEntry(**c) for c in json.loads(row['chunks_json'])]
            
            return FileMetadata(
                file_id=UUID(row['file_id']),
                path=row['path'],
                size=row['size'],
                created_at=datetime.fromisoformat(row['created_at']),
                modified_at=datetime.fromisoformat(row['modified_at']),
                chunks=chunks,
                is_deleted=bool(row['is_deleted'])
            )
    
    async def list_files(
        self,
        prefix: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[FileMetadata]:
        """Lista archivos"""
        async with self.lock:
            query = "SELECT * FROM files WHERE is_deleted = 0"
            params = []
            
            if prefix:
                query += " AND path LIKE ?"
                params.append(f"{prefix}%")
            
            query += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
            params.extend([limit, offset])
            
            rows = self.conn.execute(query, params).fetchall()
            
            files = []
            for row in rows:
                chunks = [ChunkEntry(**c) for c in json.loads(row['chunks_json'])]
                files.append(FileMetadata(
                    file_id=UUID(row['file_id']),
                    path=row['path'],
                    size=row['size'],
                    created_at=datetime.fromisoformat(row['created_at']),
                    modified_at=datetime.fromisoformat(row['modified_at']),
                    chunks=chunks,
                    is_deleted=bool(row['is_deleted'])
                ))
            
            return files
    
    async def delete_file(self, path: str, permanent: bool = False) -> bool:
        """Elimina un archivo"""
        async with self.lock:
            if permanent:
                result = self.conn.execute(
                    "DELETE FROM files WHERE path = ?",
                    (path,)
                )
            else:
                now = datetime.utcnow().isoformat()
                result = self.conn.execute(
                    """
                    UPDATE files 
                    SET is_deleted = 1, deleted_at = ? 
                    WHERE path = ? AND is_deleted = 0
                    """,
                    (now, path)
                )
            
            self.conn.commit()
            return result.rowcount > 0
    
    # ========================================================================
    # NODES
    # ========================================================================
    
    async def update_node_heartbeat(
        self,
        node_id: str,
        free_space: int,
        total_space: int,
        chunk_ids: List[UUID]
    ):
        """Actualiza heartbeat de un nodo"""
        async with self.lock:
            now = datetime.utcnow().isoformat()
            
            # Verificar si el nodo existe
            row = self.conn.execute(
                "SELECT node_id FROM nodes WHERE node_id = ?",
                (node_id,)
            ).fetchone()
            
            if row:
                # Actualizar
                self.conn.execute(
                    """
                    UPDATE nodes 
                    SET free_space = ?, total_space = ?, chunk_count = ?, 
                        last_heartbeat = ?, state = ?
                    WHERE node_id = ?
                    """,
                    (free_space, total_space, len(chunk_ids), now, NodeState.ACTIVE.value, node_id)
                )
            else:
                # Insertar nuevo nodo
                # Extraer host y port del node_id (formato: "node-host-port")
                parts = node_id.split('-')
                host = parts[1] if len(parts) > 1 else "localhost"
                port = int(parts[2]) if len(parts) > 2 else 8001
                
                self.conn.execute(
                    """
                    INSERT INTO nodes 
                    (node_id, host, port, free_space, total_space, chunk_count, last_heartbeat, state)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (node_id, host, port, free_space, total_space, len(chunk_ids), now, NodeState.ACTIVE.value)
                )
            
            self.conn.commit()
    
    async def get_node(self, node_id: str) -> Optional[NodeInfo]:
        """Obtiene información de un nodo"""
        async with self.lock:
            row = self.conn.execute(
                "SELECT * FROM nodes WHERE node_id = ?",
                (node_id,)
            ).fetchone()
            
            if not row:
                return None
            
            return NodeInfo(
                node_id=row['node_id'],
                host=row['host'],
                port=row['port'],
                rack=row['rack'],
                free_space=row['free_space'],
                total_space=row['total_space'],
                chunk_count=row['chunk_count'],
                last_heartbeat=datetime.fromisoformat(row['last_heartbeat']),
                state=NodeState(row['state'])
            )
    
    async def list_nodes(self) -> List[NodeInfo]:
        """Lista todos los nodos"""
        async with self.lock:
            rows = self.conn.execute("SELECT * FROM nodes ORDER BY node_id").fetchall()
            
            nodes = []
            for row in rows:
                nodes.append(NodeInfo(
                    node_id=row['node_id'],
                    host=row['host'],
                    port=row['port'],
                    rack=row['rack'],
                    free_space=row['free_space'],
                    total_space=row['total_space'],
                    chunk_count=row['chunk_count'],
                    last_heartbeat=datetime.fromisoformat(row['last_heartbeat']),
                    state=NodeState(row['state'])
                ))
            
            return nodes
    
    async def get_active_nodes(self) -> List[NodeInfo]:
        """Obtiene nodos activos"""
        async with self.lock:
            # Considerar activos los nodos con heartbeat reciente (< 60s)
            threshold = (datetime.utcnow() - timedelta(seconds=60)).isoformat()
            
            rows = self.conn.execute(
                """
                SELECT * FROM nodes 
                WHERE state = ? AND last_heartbeat > ?
                ORDER BY free_space DESC
                """,
                (NodeState.ACTIVE.value, threshold)
            ).fetchall()
            
            nodes = []
            for row in rows:
                nodes.append(NodeInfo(
                    node_id=row['node_id'],
                    host=row['host'],
                    port=row['port'],
                    rack=row['rack'],
                    free_space=row['free_space'],
                    total_space=row['total_space'],
                    chunk_count=row['chunk_count'],
                    last_heartbeat=datetime.fromisoformat(row['last_heartbeat']),
                    state=NodeState(row['state'])
                ))
            
            return nodes
    
    # ========================================================================
    # LEASES
    # ========================================================================
    
    async def acquire_lease(
        self,
        path: str,
        operation: str,
        timeout_seconds: int
    ) -> Optional[LeaseResponse]:
        """Adquiere un lease"""
        async with self.lock:
            # Verificar si ya existe un lease activo
            now = datetime.utcnow()
            
            row = self.conn.execute(
                """
                SELECT lease_id FROM leases 
                WHERE path = ? AND expires_at > ?
                """,
                (path, now.isoformat())
            ).fetchone()
            
            if row:
                return None  # Ya existe un lease activo
            
            # Crear nuevo lease
            lease_id = uuid4()
            expires_at = now + timedelta(seconds=timeout_seconds)
            
            self.conn.execute(
                """
                INSERT INTO leases (lease_id, path, operation, expires_at)
                VALUES (?, ?, ?, ?)
                """,
                (str(lease_id), path, operation, expires_at.isoformat())
            )
            self.conn.commit()
            
            return LeaseResponse(
                lease_id=lease_id,
                path=path,
                expires_at=expires_at
            )
    
    async def release_lease(self, lease_id: UUID) -> bool:
        """Libera un lease"""
        async with self.lock:
            result = self.conn.execute(
                "DELETE FROM leases WHERE lease_id = ?",
                (str(lease_id),)
            )
            self.conn.commit()
            return result.rowcount > 0
    
    async def cleanup_expired_leases(self):
        """Limpia leases expirados"""
        async with self.lock:
            now = datetime.utcnow().isoformat()
            self.conn.execute(
                "DELETE FROM leases WHERE expires_at <= ?",
                (now,)
            )
            self.conn.commit()
