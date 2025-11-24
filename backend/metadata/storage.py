"""
Storage backend para metadata - Versión refactorizada completa
"""

import asyncio
import json
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional, Tuple
from uuid import UUID, uuid4

from core.config import config
from core.exceptions import DFSMetadataError
from shared.models import (
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
from shared.protocols import MetadataStorageProtocol

logger = logging.getLogger(__name__)


class MetadataStorage(MetadataStorageProtocol):
    """
    Storage backend para metadata usando SQLite.
    """

    def __init__(self, db_path: Optional[str] = None):
        resolved = db_path or getattr(config, "db_path", None)
        if not resolved:
            raise DFSMetadataError(
                "db_path no está configurado (config.db_path faltante)."
            )

        self.db_path: str = resolved
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn: Optional[sqlite3.Connection] = None
        self.lock = asyncio.Lock()

    @property
    def _conn(self) -> sqlite3.Connection:
        """Accesor que devuelve la conexión no-Optional o lanza error."""
        if self.conn is None:
            raise DFSMetadataError(
                "Conexión a la base de datos no inicializada. Llamar a initialize()."
            )
        return self.conn

    async def initialize(self) -> None:
        """Inicializa la base de datos."""
        try:
            self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self.conn.row_factory = sqlite3.Row
            await self._create_tables()
            logger.info(f"Metadata storage inicializado: {self.db_path}")
        except Exception as e:
            raise DFSMetadataError(f"Error inicializando storage: {e}")

    async def _create_tables(self) -> None:
        """Crea las tablas necesarias."""
        tables = [
            """
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
            """,
            """
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
            """,
            """
            CREATE TABLE IF NOT EXISTS leases (
                lease_id TEXT PRIMARY KEY,
                path TEXT NOT NULL,
                operation TEXT NOT NULL,
                client_id TEXT,
                expires_at TEXT NOT NULL
            )
            """,
        ]

        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_files_path ON files(path)",
            "CREATE INDEX IF NOT EXISTS idx_files_deleted ON files(is_deleted)",
            "CREATE INDEX IF NOT EXISTS idx_nodes_state ON nodes(state)",
            "CREATE INDEX IF NOT EXISTS idx_nodes_heartbeat ON nodes(last_heartbeat)",
            "CREATE INDEX IF NOT EXISTS idx_leases_path ON leases(path)",
            "CREATE INDEX IF NOT EXISTS idx_leases_expires ON leases(expires_at)",
        ]

        conn = self._conn
        for table_sql in tables:
            conn.execute(table_sql)

        for index_sql in indexes:
            conn.execute(index_sql)

        conn.commit()

    async def close(self) -> None:
        """Cierra la conexión."""
        if self.conn:
            self.conn.close()
            self.conn = None
            logger.info("Conexión de metadata storage cerrada")

    async def create_file_metadata(
        self, path: str, size: int, chunks: List[ChunkTarget]
    ) -> FileMetadata:
        """Crea metadata de archivo."""
        async with self.lock:
            file_id = uuid4()
            now = datetime.now(timezone.utc)

            # Convertir chunks a ChunkEntry sin réplicas aún
            chunk_entries: List[ChunkEntry] = []
            for i, chunk_target in enumerate(chunks):
                chunk_entry = ChunkEntry(
                    chunk_id=chunk_target.chunk_id,
                    seq_index=i,
                    size=chunk_target.size,
                    replicas=[],  # Se llenarán en commit
                )
                chunk_entries.append(chunk_entry)

            file_metadata = FileMetadata(
                file_id=file_id,
                path=path,
                size=size,
                created_at=now,
                modified_at=now,
                chunks=chunk_entries,
            )

            chunks_json = json.dumps([c.model_dump(mode="json") for c in chunk_entries])

            try:
                conn = self._conn
                conn.execute(
                    """
                    INSERT INTO files (file_id, path, size, created_at, modified_at, chunks_json)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(file_id),
                        path,
                        size,
                        now.isoformat(),
                        now.isoformat(),
                        chunks_json,
                    ),
                )
                conn.commit()

                logger.info(f"Metadata creada: {path} (ID: {file_id})")
                return file_metadata

            except sqlite3.IntegrityError:
                raise DFSMetadataError(f"Archivo ya existe: {path}")
            except Exception as e:
                raise DFSMetadataError(f"Error creando metadata: {e}")

    async def create_chunk_plan(
        self, chunk_size: int, target_nodes: List[str]
    ) -> ChunkTarget:
        """Crea un plan de chunk con targets."""
        return ChunkTarget(chunk_id=uuid4(), size=chunk_size, targets=target_nodes)

    async def commit_file(self, file_id: UUID, chunks: List[ChunkCommitInfo]) -> bool:
        """Confirma la subida de un archivo."""
        async with self.lock:
            try:
                conn = self._conn
                # Obtener archivo
                row = conn.execute(
                    "SELECT chunks_json FROM files WHERE file_id = ?", (str(file_id),)
                ).fetchone()

                if not row:
                    logger.error(f"Archivo no encontrado para commit: {file_id}")
                    return False

                # Cargar chunks existentes
                chunk_entries = [
                    ChunkEntry(**c) for c in json.loads(row["chunks_json"])
                ]
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
                            replica = ReplicaInfo(
                                node_id=node_id,
                                url=self._node_id_to_url(node_id),
                                state=ChunkState.COMMITTED,
                                last_heartbeat=datetime.now(timezone.utc),
                                checksum_verified=True,
                            )
                            chunk.replicas.append(replica)

                        logger.debug(
                            f"Chunk {chunk_id_str}: {len(chunk.replicas)} réplicas"
                        )
                    else:
                        logger.warning(
                            f"Chunk {chunk_id_str} no encontrado en metadata"
                        )

                # Guardar
                chunks_json = json.dumps(
                    [c.model_dump(mode="json") for c in chunk_entries]
                )
                now = datetime.now(timezone.utc).isoformat()

                conn.execute(
                    "UPDATE files SET chunks_json = ?, modified_at = ? WHERE file_id = ?",
                    (chunks_json, now, str(file_id)),
                )
                conn.commit()

                logger.info(
                    f"Commit exitoso para file_id={file_id}, {len(chunks)} chunks"
                )
                return True

            except Exception as e:
                logger.error(f"Error en commit: {e}")
                return False

    async def get_file_by_path(self, path: str) -> Optional[FileMetadata]:
        """Obtiene metadata de archivo por path."""
        async with self.lock:
            row = self._conn.execute(
                "SELECT * FROM files WHERE path = ? AND is_deleted = 0", (path,)
            ).fetchone()

            if not row:
                return None

            return self._row_to_file_metadata(row)

    async def list_files(
        self, prefix: Optional[str] = None, limit: int = 100, offset: int = 0
    ) -> List[FileMetadata]:
        """Lista archivos."""
        async with self.lock:
            query = "SELECT * FROM files WHERE is_deleted = 0"
            params: List = []

            if prefix:
                query += " AND path LIKE ?"
                params.append(f"{prefix}%")

            query += " ORDER BY path LIMIT ? OFFSET ?"
            params.extend([limit, offset])

            rows = self._conn.execute(query, params).fetchall()
            return [self._row_to_file_metadata(row) for row in rows]

    async def delete_file(self, path: str, permanent: bool = False) -> bool:
        """Elimina un archivo."""
        async with self.lock:
            try:
                conn = self._conn
                if permanent:
                    result = conn.execute("DELETE FROM files WHERE path = ?", (path,))
                else:
                    now = datetime.now(timezone.utc).isoformat()
                    result = conn.execute(
                        "UPDATE files SET is_deleted = 1, deleted_at = ? WHERE path = ? AND is_deleted = 0",
                        (now, path),
                    )

                conn.commit()
                success = result.rowcount > 0

                if success:
                    action = "eliminado" if permanent else "marcado como eliminado"
                    logger.info(f"Archivo {action}: {path}")

                return success

            except Exception as e:
                logger.error(f"Error eliminando archivo {path}: {e}")
                return False

    async def update_node_heartbeat(
        self, node_id: str, free_space: int, total_space: int, chunk_ids: List[UUID]
    ) -> None:
        """Actualiza heartbeat de un nodo."""
        async with self.lock:
            now = datetime.now(timezone.utc)
            threshold = now - timedelta(seconds=config.node_timeout)

            conn = self._conn
            # Verificar si el nodo existe
            row = conn.execute(
                "SELECT node_id FROM nodes WHERE node_id = ?", (node_id,)
            ).fetchone()

            if row:
                # Actualizar nodo existente
                conn.execute(
                    """
                    UPDATE nodes 
                    SET free_space = ?, total_space = ?, chunk_count = ?, 
                        last_heartbeat = ?, state = ?
                    WHERE node_id = ?
                    """,
                    (
                        free_space,
                        total_space,
                        len(chunk_ids),
                        now.isoformat(),
                        NodeState.ACTIVE.value,
                        node_id,
                    ),
                )
            else:
                # Insertar nuevo nodo
                host, port = self._parse_node_id(node_id)

                conn.execute(
                    """
                    INSERT INTO nodes 
                    (node_id, host, port, free_space, total_space, chunk_count, last_heartbeat, state)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        node_id,
                        host,
                        port,
                        free_space,
                        total_space,
                        len(chunk_ids),
                        now.isoformat(),
                        NodeState.ACTIVE.value,
                    ),
                )

            # Marcar nodos inactivos
            conn.execute(
                "UPDATE nodes SET state = ? WHERE last_heartbeat < ?",
                (NodeState.INACTIVE.value, threshold.isoformat()),
            )

            conn.commit()
            logger.debug(f"Heartbeat actualizado: {node_id}")

    async def get_node(self, node_id: str) -> Optional[NodeInfo]:
        """Obtiene información de un nodo."""
        # Lectura simple sin lock
        row = self._conn.execute(
            "SELECT * FROM nodes WHERE node_id = ?", (node_id,)
        ).fetchone()

        if not row:
            return None

        return self._row_to_node_info(row)

    async def list_nodes(self) -> List[NodeInfo]:
        """Lista todos los nodos."""
        # Lectura simple sin lock (SQLite soporta lecturas concurrentes)
        rows = self._conn.execute(
            "SELECT * FROM nodes ORDER BY last_heartbeat DESC"
        ).fetchall()

        return [self._row_to_node_info(row) for row in rows]

    async def get_active_nodes(self) -> List[NodeInfo]:
        """Obtiene nodos activos."""
        # Lectura simple sin lock
        threshold = (
            datetime.now(timezone.utc) - timedelta(seconds=config.node_timeout)
        ).isoformat()

        rows = self._conn.execute(
            """
            SELECT * FROM nodes 
            WHERE state = ? AND last_heartbeat > ?
            ORDER BY free_space DESC
            """,
            (NodeState.ACTIVE.value, threshold),
        ).fetchall()
        
        logger.info(f"Active nodes found: {rows}")

        return [self._row_to_node_info(row) for row in rows]

    async def acquire_lease(
        self, path: str, operation: str, timeout_seconds: int
    ) -> Optional[LeaseResponse]:
        """Adquiere un lease."""
        async with self.lock:
            await self.cleanup_expired_leases()

            now = datetime.now(timezone.utc)

            # Verificar si ya existe un lease activo
            row = self._conn.execute(
                "SELECT lease_id FROM leases WHERE path = ? AND expires_at > ?",
                (path, now.isoformat()),
            ).fetchone()

            if row:
                return None  # Ya existe un lease activo

            # Crear nuevo lease
            lease_id = uuid4()
            expires_at = now + timedelta(seconds=timeout_seconds)

            self._conn.execute(
                """
                INSERT INTO leases (lease_id, path, operation, expires_at)
                VALUES (?, ?, ?, ?)
                """,
                (str(lease_id), path, operation, expires_at.isoformat()),
            )
            self._conn.commit()

            logger.info(f"Lease adquirido: {path} (ID: {lease_id})")
            return LeaseResponse(lease_id=lease_id, path=path, expires_at=expires_at)

    async def release_lease(self, lease_id: UUID) -> bool:
        """Libera un lease."""
        async with self.lock:
            result = self._conn.execute(
                "DELETE FROM leases WHERE lease_id = ?", (str(lease_id),)
            )
            self._conn.commit()

            success = result.rowcount > 0
            if success:
                logger.info(f"Lease liberado: {lease_id}")

            return success

    async def cleanup_expired_leases(self) -> None:
        """Limpia leases expirados."""
        async with self.lock:
            now = datetime.now(timezone.utc).isoformat()
            result = self._conn.execute(
                "DELETE FROM leases WHERE expires_at <= ?", (now,)
            )

            if result.rowcount > 0:
                logger.debug(f"Limpiados {result.rowcount} leases expirados")

    def _row_to_file_metadata(self, row) -> FileMetadata:
        """Convierte una fila de la BD a FileMetadata."""
        chunks = [ChunkEntry(**c) for c in json.loads(row["chunks_json"])]

        return FileMetadata(
            file_id=UUID(row["file_id"]),
            path=row["path"],
            size=row["size"],
            created_at=datetime.fromisoformat(row["created_at"]),
            modified_at=datetime.fromisoformat(row["modified_at"]),
            chunks=chunks,
            is_deleted=bool(row["is_deleted"]),
            deleted_at=datetime.fromisoformat(row["deleted_at"])
            if row["deleted_at"]
            else None,
        )

    def _row_to_node_info(self, row) -> NodeInfo:
        """Convierte una fila de la BD a NodeInfo."""
        return NodeInfo(
            node_id=row["node_id"],
            host=row["host"],
            port=row["port"],
            rack=row["rack"],
            free_space=row["free_space"],
            total_space=row["total_space"],
            chunk_count=row["chunk_count"],
            last_heartbeat=datetime.fromisoformat(row["last_heartbeat"]),
            state=NodeState(row["state"]),
        )

    def _node_id_to_url(self, node_id: str) -> str:
        """Convierte node_id a URL."""
        host, port = self._parse_node_id(node_id)
        return f"http://{host}:{port}"

    def _parse_node_id(self, node_id: str) -> Tuple[str, int]:
        """Parsea node_id para extraer host y puerto."""
        parts = node_id.split("-")
        if len(parts) >= 3:
            host = parts[1]
            try:
                port = int(parts[2])
                return host, port
            except ValueError:
                pass
        return "localhost", 8001

    async def get_system_stats(self) -> dict:
        """Obtiene estadísticas del sistema."""
        async with self.lock:
            # Estadísticas de archivos
            files_row = self._conn.execute(
                "SELECT COUNT(*) as count, SUM(size) as total_size FROM files WHERE is_deleted = 0"
            ).fetchone()

            # Estadísticas de chunks
            total_chunks = 0
            total_size = files_row["total_size"] or 0

            files = await self.list_files(limit=10000)
            for file in files:
                total_chunks += len(file.chunks)

            # Estadísticas de nodos
            nodes = await self.list_nodes()
            active_nodes = [n for n in nodes if n.state == NodeState.ACTIVE]

            total_space = sum(n.total_space for n in active_nodes)
            free_space = sum(n.free_space for n in active_nodes)
            used_space = total_space - free_space

            return {
                "total_files": files_row["count"],
                "total_chunks": total_chunks,
                "total_size": total_size,
                "total_nodes": len(nodes),
                "active_nodes": len(active_nodes),
                "total_space": total_space,
                "used_space": used_space,
                "free_space": free_space,
            }
