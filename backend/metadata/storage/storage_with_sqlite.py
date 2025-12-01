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
from shared.protocols import MetadataStorageBase

logger = logging.getLogger(__name__)


class SQLiteMetadataStorage(MetadataStorageBase):
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
        """Accesor que devuelve la conexión no-Optional o lanza error"""
        if self.conn is None:
            raise DFSMetadataError(
                "Conexión a la base de datos no inicializada. Llamar a initialize()."
            )
        return self.conn

    async def initialize(self) -> None:
        """Inicializa la base de datos"""
        try:
            self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self.conn.row_factory = sqlite3.Row
            await self._create_tables()
            logger.info(f"Metadata storage inicializado: {self.db_path}")
        except Exception as e:
            raise DFSMetadataError(f"Error inicializando storage: {e}")

    async def _create_tables(self) -> None:
        """Crea las tablas necesarias (ahora con campos extendidos para nodos)."""
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
                state TEXT NOT NULL,
                -- Nuevas columnas para registro automático / ZeroTier
                zerotier_node_id TEXT,
                zerotier_ip TEXT,
                lease_ttl INTEGER DEFAULT 60,
                boot_token TEXT,
                version TEXT
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

        # Ejecutar migración ligera: si la tabla nodes existía sin las columnas nuevas,
        # las agregamos con ALTER TABLE (SQLite permite ADD COLUMN).
        self._migrate_node_table_if_needed()

    def _migrate_node_table_if_needed(self) -> None:
        """
        Añade columnas a la tabla nodes si faltan (migración segura para DB existentes).
        Esto se ejecuta después de crear tablas.
        """
        conn = self._conn
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(nodes)")
        existing = [row["name"] for row in cur.fetchall()]

        # Lista de (column_sql, column_name) para agregar si falta
        needed = [
            ("zerotier_node_id TEXT", "zerotier_node_id"),
            ("zerotier_ip TEXT", "zerotier_ip"),
            ("lease_ttl INTEGER DEFAULT 60", "lease_ttl"),
            ("boot_token TEXT", "boot_token"),
            ("version TEXT", "version"),
        ]
        for col_sql, col_name in needed:
            if col_name not in existing:
                logger.info("Migración: agregando columna %s a nodes", col_name)
                conn.execute(f"ALTER TABLE nodes ADD COLUMN {col_sql}")
        conn.commit()


    async def close(self) -> None:
        """Cierra la conexión"""
        if self.conn:
            self.conn.close()
            self.conn = None
            logger.info("Conexión de metadata storage cerrada")

    async def create_file_metadata(
        self, path: str, size: int, chunks: List[ChunkTarget]
    ) -> FileMetadata:
        """Crea metadata de archivo"""
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
        """Crea un plan de chunk con targets"""
        return ChunkTarget(chunk_id=uuid4(), size=chunk_size, targets=target_nodes)

    async def commit_file(self, file_id: UUID, chunks: List[ChunkCommitInfo]) -> bool:
        """Confirma la subida de un archivo"""
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
        """Obtiene metadata de archivo por path"""
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
        """Lista los archivos"""
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
        """Elimina un archivo"""
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
    
    async def register_node(
        self,
        node_id: str,
        zerotier_node_id: Optional[str],
        zerotier_ip: str,
        listening_ports: Optional[dict] = None,
        capacity_gb: Optional[float] = None,
        version: Optional[str] = None,
        lease_ttl: Optional[int] = None,
        boot_token: Optional[str] = None,
        rack: Optional[str] = None,
    ) -> None:
        """
        Inserta o actualiza la fila de 'nodes' cuando un nodo se registra por primera vez.
        - node_id: id del nodo (UUID o string persistente)
        - zerotier_node_id: member id de ZeroTier (opcional)
        - zerotier_ip: IP asignada por ZeroTier (string)
        - listening_ports: dict con puertos, ej. {"storage":8001}
        - capacity_gb: capacidad total reportada por el nodo
        - version: versión del agente
        - lease_ttl: ttl para heartbeats (opcional)
        - boot_token: token de bootstrap usado (si quieres guardarlo)
        - rack: etiqueta física/ lógica (opcional)
        """
        async with self.lock:
            now = datetime.now(timezone.utc)
            conn = self._conn

            # Determine host/port from provided data: prefer zerotier_ip and storage port
            host = zerotier_ip or "unknown"
            port = 8001
            if listening_ports and isinstance(listening_ports, dict):
                try:
                    port = int(listening_ports.get("storage", port))
                except Exception:
                    port = port

            # Normalize capacity fields
            total_space = int((capacity_gb or 0) * (1024**3)) if capacity_gb is not None else 0
            free_space = total_space  # al registro inicial asumimos libre = total o 0 según preferencia
            # Intentamos detectar si ya existe
            row = conn.execute("SELECT node_id FROM nodes WHERE node_id = ?", (node_id,)).fetchone()

            if row:
                # Update existing
                conn.execute(
                    """
                    UPDATE nodes SET
                        zerotier_node_id = ?,
                        zerotier_ip = ?,
                        host = ?,
                        port = ?,
                        rack = ?,
                        total_space = ?,
                        free_space = ?,
                        version = ?,
                        boot_token = ?,
                        lease_ttl = ?,
                        last_heartbeat = ?,
                        state = ?
                    WHERE node_id = ?
                    """,
                    (
                        zerotier_node_id,
                        zerotier_ip,
                        host,
                        port,
                        rack,
                        total_space,
                        free_space,
                        version,
                        boot_token,
                        int(lease_ttl) if lease_ttl is not None else getattr(config, "lease_ttl", 60),
                        now.isoformat(),
                        NodeState.ACTIVE.value,
                        node_id,
                    ),
                )
            else:
                # Insert new
                conn.execute(
                    """
                    INSERT INTO nodes
                    (node_id, zerotier_node_id, zerotier_ip, host, port, rack, free_space, total_space, chunk_count, last_heartbeat, state, lease_ttl, version, boot_token)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        node_id,
                        zerotier_node_id,
                        zerotier_ip,
                        host,
                        port,
                        rack,
                        free_space,
                        total_space,
                        0,
                        now.isoformat(),
                        NodeState.ACTIVE.value,
                        int(lease_ttl) if lease_ttl is not None else getattr(config, "lease_ttl", 60),
                        version,
                        boot_token,
                    ),
                )

            # After register, optionally mark stale nodes as INACTIVE by heartbeat threshold (existing logic)
            threshold = now - timedelta(seconds=config.node_timeout)
            conn.execute(
                "UPDATE nodes SET state = ? WHERE last_heartbeat < ?",
                (NodeState.INACTIVE.value, threshold.isoformat()),
            )

            conn.commit()
            logger.info("Node registrado/actualizado: %s (%s)", node_id, zerotier_ip)


    async def update_node_heartbeat(
        self, 
        node_id: str, 
        free_space: int, 
        total_space: int, 
        chunk_ids: List[UUID],
        zerotier_ip: Optional[str] = None,
        zerotier_node_id: Optional[str] = None,
        url: Optional[str] = None,
    ) -> None:
        """Actualiza heartbeat de un nodo con información adicional de ZeroTier"""
        async with self.lock:
            now = datetime.now(timezone.utc)
            threshold = now - timedelta(seconds=config.node_timeout)

            logger.info(f"Heartbeat de {node_id}: reportando {len(chunk_ids)} chunks")
            if len(chunk_ids) > 0:
                logger.debug(f"Chunks reportados: {[str(c) for c in chunk_ids[:5]]}{'...' if len(chunk_ids) > 5 else ''}")

            conn = self._conn
            # Verifica si el nodo existe
            row = conn.execute(
                "SELECT node_id FROM nodes WHERE node_id = ?", (node_id,)
            ).fetchone()

            if row:
                # Preparar campos para actualización
                update_fields = [
                    "free_space = ?",
                    "total_space = ?",
                    "chunk_count = ?",
                    "last_heartbeat = ?",
                    "state = ?"
                ]
                update_values = [
                    free_space,
                    total_space,
                    len(chunk_ids),
                    now.isoformat(),
                    NodeState.ACTIVE.value,
                ]
                
                # Agregar campos opcionales si están presentes
                # SIEMPRE actualizar host si hay zerotier_ip válida
                if zerotier_ip and zerotier_ip.strip() and zerotier_ip != "0.0.0.0":
                    update_fields.append("zerotier_ip = ?")
                    update_values.append(zerotier_ip)
                    update_fields.append("host = ?")
                    update_values.append(zerotier_ip)
                    logger.info(f"Actualizando host de {node_id} a {zerotier_ip}")
                else:
                    logger.debug(f"Heartbeat sin ZeroTier IP válida para {node_id}")
                
                if zerotier_node_id and zerotier_node_id.strip():
                    update_fields.append("zerotier_node_id = ?")
                    update_values.append(zerotier_node_id)
                
                if url:
                    # Extraer puerto de la URL si está presente
                    try:
                        port_from_url = int(url.split(":")[-1].split("/")[0])
                        update_fields.append("port = ?")
                        update_values.append(port_from_url)
                    except (ValueError, IndexError):
                        pass
                
                update_values.append(node_id)
                
                query = f"UPDATE nodes SET {', '.join(update_fields)} WHERE node_id = ?"
                conn.execute(query, tuple(update_values))
                
            else:
                # Inserta un nuevo nodo
                host = zerotier_ip if zerotier_ip else "0.0.0.0"
                port = 8001  # Puerto por defecto
                
                # Intentar extraer puerto de la URL
                if url:
                    try:
                        port = int(url.split(":")[-1].split("/")[0])
                    except (ValueError, IndexError):
                        pass

                conn.execute(
                    """
                    INSERT INTO nodes 
                    (node_id, host, port, zerotier_ip, zerotier_node_id, free_space, total_space, chunk_count, last_heartbeat, state)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        node_id,
                        host,
                        port,
                        zerotier_ip,
                        zerotier_node_id,
                        free_space,
                        total_space,
                        len(chunk_ids),
                        now.isoformat(),
                        NodeState.ACTIVE.value,
                    ),
                )

            # Marca los nodos inactivos
            conn.execute(
                "UPDATE nodes SET state = ? WHERE last_heartbeat < ?",
                (NodeState.INACTIVE.value, threshold.isoformat()),
            )

            # Actualizar réplicas basadas en los chunks reportados
            if chunk_ids:
                await self._update_replicas_from_heartbeat(node_id, chunk_ids, url or f"http://{zerotier_ip or '0.0.0.0'}:{8001}")

            conn.commit()
            logger.debug(f"Heartbeat actualizado: {node_id} (ZT IP: {zerotier_ip})")
    
    async def _update_replicas_from_heartbeat(self, node_id: str, chunk_ids: List[UUID], node_url: str) -> None:
        """
        Actualiza las réplicas de los chunks basándose en lo reportado por el heartbeat.
        Este método es la "fuente de verdad" - sincroniza el estado real del nodo:
        - Marca como committed los chunks que el nodo SÍ tiene
        - ELIMINA las réplicas de chunks que el nodo YA NO tiene
        """
        conn = self._conn
        
        # Obtener todos los archivos que no están eliminados
        rows = conn.execute(
            "SELECT file_id, chunks_json FROM files WHERE is_deleted = 0"
        ).fetchall()
        
        chunk_ids_str = {str(c) for c in chunk_ids}
        updated_files = 0
        replicas_added = 0
        replicas_removed = 0
        
        for row in rows:
            file_id = row["file_id"]
            chunks_data = json.loads(row["chunks_json"])
            file_modified = False
            
            for chunk in chunks_data:
                chunk_id = chunk.get("chunk_id")
                replicas = chunk.get("replicas", [])
                
                # Si este nodo reporta tener este chunk
                if chunk_id in chunk_ids_str:
                    # Buscar si ya existe una réplica para este nodo
                    replica_exists = False
                    for replica in replicas:
                        if replica.get("node_id") == node_id:
                            # Actualizar réplica existente
                            if replica.get("state") != "committed":
                                replica["state"] = "committed"
                                file_modified = True
                            if replica.get("url") != node_url:
                                replica["url"] = node_url
                                file_modified = True
                            replica_exists = True
                            break
                    
                    # Si no existe, agregarla
                    if not replica_exists:
                        replicas.append({
                            "node_id": node_id,
                            "url": node_url,
                            "state": "committed",
                            "checksum_verified": False
                        })
                        chunk["replicas"] = replicas
                        file_modified = True
                        replicas_added += 1
                        logger.debug(f"Agregada réplica de chunk {chunk_id} en nodo {node_id}")
                
                else:
                    # El nodo NO reporta tener este chunk
                    # Eliminar la réplica si existía (el nodo ya no tiene el chunk)
                    original_count = len(replicas)
                    replicas = [r for r in replicas if r.get("node_id") != node_id]
                    
                    if len(replicas) < original_count:
                        chunk["replicas"] = replicas
                        file_modified = True
                        replicas_removed += 1
                        logger.warning(
                            f"ELIMINADA réplica de chunk {chunk_id} de nodo {node_id} "
                            f"(no reportada en heartbeat - posible pérdida de datos)"
                        )
            
            # Actualizar el archivo si se modificó
            if file_modified:
                conn.execute(
                    "UPDATE files SET chunks_json = ?, modified_at = ? WHERE file_id = ?",
                    (json.dumps(chunks_data), datetime.now(timezone.utc).isoformat(), file_id)
                )
                updated_files += 1
        
        if updated_files > 0 or replicas_added > 0 or replicas_removed > 0:
            logger.info(
                f"Sincronización de réplicas desde heartbeat de {node_id}: "
                f"{updated_files} archivos actualizados, "
                f"+{replicas_added} réplicas agregadas, "
                f"-{replicas_removed} réplicas eliminadas"
            )

    async def get_node(self, node_id: str) -> Optional[NodeInfo]:
        """Obtiene información de un nodo"""
        # Lectura simple sin lock
        row = self._conn.execute(
            "SELECT * FROM nodes WHERE node_id = ?", (node_id,)
        ).fetchone()

        if not row:
            return None

        return self._row_to_node_info(row)

    async def list_nodes(self) -> List[NodeInfo]:
        """Lista todos los nodos"""
        # Lectura simple sin lock (SQLite soporta lecturas concurrentes)
        rows = self._conn.execute(
            "SELECT * FROM nodes ORDER BY last_heartbeat DESC"
        ).fetchall()

        return [self._row_to_node_info(row) for row in rows]

    async def get_active_nodes(self) -> List[NodeInfo]:
        """Obtiene nodos activos"""
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
        """Adquiere un lease"""
        async with self.lock:
            await self.cleanup_expired_leases()

            now = datetime.now(timezone.utc)

            # Verifica si ya existe un lease activo
            row = self._conn.execute(
                "SELECT lease_id FROM leases WHERE path = ? AND expires_at > ?",
                (path, now.isoformat()),
            ).fetchone()

            if row:
                return None  # Ya existe un lease activo

            # Crea un nuevo lease
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
        """Libera un lease"""
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
        """Limpia leases expirados"""
        async with self.lock:
            now = datetime.now(timezone.utc).isoformat()
            result = self._conn.execute(
                "DELETE FROM leases WHERE expires_at <= ?", (now,)
            )

            if result.rowcount > 0:
                logger.debug(f"Limpiados {result.rowcount} leases expirados")

    def _row_to_file_metadata(self, row) -> FileMetadata:
        """Convierte una fila de la BD a FileMetadata"""
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
        """Convierte una fila de la BD a NodeInfo"""
        # Preferir zerotier_ip sobre host si está disponible
        row = dict(row)
        host = row.get("zerotier_ip") or row["host"]
        
        return NodeInfo(
            node_id=row["node_id"],
            host=host,  # Usar ZeroTier IP si está disponible
            port=row["port"],
            rack=row["rack"],
            free_space=row["free_space"],
            total_space=row["total_space"],
            chunk_count=row["chunk_count"],
            last_heartbeat=datetime.fromisoformat(row["last_heartbeat"]),
            state=NodeState(row["state"]),
        )

    def _node_id_to_url(self, node_id: str) -> str:
        """Convierte node_id a URL"""
        host, port = self._parse_node_id(node_id)
        return f"http://{host}:{port}"

    def _parse_node_id(self, node_id: str) -> Tuple[str, int]:
        """Parsea node_id para extraer host y puerto"""
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
        """Obtiene estadísticas del sistema"""
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
