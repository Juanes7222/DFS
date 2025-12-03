import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple
from uuid import UUID, uuid4

import asyncpg

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

logging.basicConfig(
    level=getattr(logging, config.log_level.upper()),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


class PostgresMetadataStorage(MetadataStorageBase):
    """
    Storage backend para metadata usando PostgreSQL (Neon)
    """

    def __init__(self, connection_string: Optional[str] = None):
        self.connection_string = connection_string or getattr(
            config, "postgres_url", None
        )
        if not self.connection_string:
            raise DFSMetadataError(
                "connection_string no está configurado (config.postgres_url faltante)."
            )
        
        self._pool: Optional[asyncpg.Pool] = None
        self.lock = asyncio.Lock()
        
    @property
    def pool(self) -> asyncpg.Pool:
        """Accesor que devuelve el pool no-Optional o lanza error"""
        if self._pool is None:
            raise DFSMetadataError(
                "Pool de conexiones no inicializado. Llamar a initialize()."
            )
        return self._pool

    async def initialize(self) -> None:
        """Inicializa el pool de conexiones"""
        try:
            self._pool = await asyncpg.create_pool(
                self.connection_string,
                min_size=2,
                max_size=10,
                command_timeout=60,
            )
            await self._create_tables()
            logger.info("Metadata storage (PostgreSQL) inicializado")
        except Exception as e:
            raise DFSMetadataError(f"Error inicializando storage: {e}")

    async def _create_tables(self) -> None:
        """Crea las tablas necesarias en PostgreSQL"""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS files (
                    file_id UUID PRIMARY KEY,
                    path TEXT UNIQUE NOT NULL,
                    size BIGINT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL,
                    modified_at TIMESTAMPTZ NOT NULL,
                    is_deleted BOOLEAN DEFAULT FALSE,
                    deleted_at TIMESTAMPTZ,
                    chunks_json JSONB NOT NULL,
                    compressed BOOLEAN DEFAULT FALSE,
                    original_size BIGINT
                )
                """
            )

            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS nodes (
                    node_id TEXT PRIMARY KEY,
                    host TEXT NOT NULL,
                    port INTEGER NOT NULL,
                    rack TEXT,
                    free_space BIGINT NOT NULL,
                    total_space BIGINT NOT NULL,
                    chunk_count INTEGER DEFAULT 0,
                    last_heartbeat TIMESTAMPTZ NOT NULL,
                    state TEXT NOT NULL,
                    zerotier_node_id TEXT,
                    zerotier_ip TEXT,
                    lease_ttl INTEGER DEFAULT 60,
                    boot_token TEXT,
                    version TEXT
                )
                """
            )

            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS leases (
                    lease_id UUID PRIMARY KEY,
                    path TEXT NOT NULL,
                    operation TEXT NOT NULL,
                    client_id TEXT,
                    expires_at TIMESTAMPTZ NOT NULL
                )
                """
            )

            # Crear índices
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_files_path ON files(path)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_files_deleted ON files(is_deleted)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_nodes_state ON nodes(state)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_nodes_heartbeat ON nodes(last_heartbeat)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_leases_path ON leases(path)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_leases_expires ON leases(expires_at)"
            )

    async def close(self) -> None:
        """Cierra el pool de conexiones"""
        if self.pool:
            await self.pool.close()
            self._pool = None
            logger.info("Conexión de metadata storage cerrada")

    async def create_file_metadata(
        self, path: str, size: int, chunks: List[ChunkTarget],
        compressed: bool = False, original_size: Optional[int] = None
    ) -> FileMetadata:
        """Crea metadata de archivo"""
        async with self.lock:
            file_id = uuid4()
            now = datetime.now(timezone.utc)

            chunk_entries: List[ChunkEntry] = []
            for i, chunk_target in enumerate(chunks):
                chunk_entry = ChunkEntry(
                    chunk_id=chunk_target.chunk_id,
                    seq_index=i,
                    size=chunk_target.size,
                    replicas=[],
                )
                chunk_entries.append(chunk_entry)

            file_metadata = FileMetadata(
                file_id=file_id,
                path=path,
                size=size,
                created_at=now,
                modified_at=now,
                chunks=chunk_entries,
                compressed=compressed,
                original_size=original_size,
            )

            chunks_json = json.dumps([c.model_dump(mode="json") for c in chunk_entries])

            try:
                async with self.pool.acquire() as conn:
                    await conn.execute(
                        """
                        INSERT INTO files (file_id, path, size, created_at, modified_at, chunks_json, compressed, original_size)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                        """,
                        file_id,
                        path,
                        size,
                        now,
                        now,
                        chunks_json,
                        compressed,
                        original_size,
                    )

                logger.info(f"Metadata creada: {path} (ID: {file_id})")
                return file_metadata

            except asyncpg.UniqueViolationError:
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
                async with self.pool.acquire() as conn:
                    row = await conn.fetchrow(
                        "SELECT chunks_json FROM files WHERE file_id = $1", file_id
                    )

                    if not row:
                        logger.error(f"Archivo no encontrado para commit: {file_id}")
                        return False

                    chunk_entries = [
                        ChunkEntry(**c) for c in json.loads(row["chunks_json"])
                    ]
                    chunk_map = {str(c.chunk_id): c for c in chunk_entries}

                    for commit_info in chunks:
                        chunk_id_str = str(commit_info.chunk_id)
                        if chunk_id_str in chunk_map:
                            chunk = chunk_map[chunk_id_str]
                            chunk.checksum = commit_info.checksum

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

                    chunks_json = json.dumps(
                        [c.model_dump(mode="json") for c in chunk_entries]
                    )
                    now = datetime.now(timezone.utc)

                    await conn.execute(
                        "UPDATE files SET chunks_json = $1, modified_at = $2 WHERE file_id = $3",
                        chunks_json,
                        now,
                        file_id,
                    )

                logger.info(
                    f"Commit exitoso para file_id={file_id}, {len(chunks)} chunks"
                )
                return True

            except Exception as e:
                logger.error(f"Error en commit: {e}")
                return False

    async def get_file_by_path(self, path: str) -> Optional[FileMetadata]:
        """Obtiene metadata de archivo por path"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM files WHERE path = $1 AND is_deleted = FALSE", path
            )

            if not row:
                return None

            return self._row_to_file_metadata(row)

    async def list_files(
        self, prefix: Optional[str] = None, limit: int = 100, offset: int = 0
    ) -> List[FileMetadata]:
        """Lista los archivos"""
        async with self.pool.acquire() as conn:
            if prefix:
                rows = await conn.fetch(
                    """
                    SELECT * FROM files 
                    WHERE is_deleted = FALSE AND path LIKE $1
                    ORDER BY path LIMIT $2 OFFSET $3
                    """,
                    f"{prefix}%",
                    limit,
                    offset,
                )
            else:
                rows = await conn.fetch(
                    """
                    SELECT * FROM files 
                    WHERE is_deleted = FALSE 
                    ORDER BY path LIMIT $1 OFFSET $2
                    """,
                    limit,
                    offset,
                )

            return [self._row_to_file_metadata(row) for row in rows]

    async def delete_file(self, path: str, permanent: bool = False) -> bool:
        """Elimina un archivo"""
        logger.info(f"delete_file called - path: {path}, permanent: {permanent}")
        
        async with self.lock:
            try:
                logger.info(f"Acquired lock for delete operation: {path}")
                async with self.pool.acquire() as conn:
                    logger.info(f"Database connection acquired for: {path}")
                    
                    if permanent:
                        # Eliminación permanente
                        logger.info(f"Executing permanent DELETE for: {path}")
                        result = await conn.execute(
                            "DELETE FROM files WHERE path = $1", path
                        )
                    else:
                        # Soft delete
                        logger.info(f"Executing soft delete UPDATE for: {path}")
                        now = datetime.now(timezone.utc)
                        result = await conn.execute(
                            """
                            UPDATE files 
                            SET is_deleted = TRUE, deleted_at = $1 
                            WHERE path = $2 AND is_deleted = FALSE
                            """,
                            now,
                            path,
                        )

                    logger.info(f"Query executed. Result: {result}")
                    
                    # PostgreSQL devuelve algo como "UPDATE 1" o "DELETE 1"
                    # Extraemos el número de filas afectadas
                    rows_affected = 0
                    if result:
                        try:
                            rows_affected = int(result.split()[-1])
                            logger.info(f"Parsed rows_affected: {rows_affected}")
                        except (ValueError, IndexError) as parse_error:
                            logger.warning(f"No se pudo parsear resultado: {result}, error: {parse_error}")
                    
                    success = rows_affected > 0

                    if success:
                        action = "eliminado permanentemente" if permanent else "marcado como eliminado"
                        logger.info(f"Archivo {action}: {path}")
                    else:
                        logger.warning(f"Archivo no encontrado o ya eliminado: {path}, rows_affected={rows_affected}")

                    return success

            except Exception as e:
                logger.error(f"Error eliminando archivo {path}: {e}", exc_info=True)
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
        """Registra o actualiza un nodo"""
        async with self.lock:
            now = datetime.now(timezone.utc)

            host = zerotier_ip or "unknown"
            port = 8001
            if listening_ports and isinstance(listening_ports, dict):
                try:
                    port = int(listening_ports.get("storage", port))
                except Exception:
                    pass

            total_space = (
                int((capacity_gb or 0) * (1024**3)) if capacity_gb is not None else 0
            )
            free_space = total_space

            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT node_id FROM nodes WHERE node_id = $1", node_id
                )

                if row:
                    await conn.execute(
                        """
                        UPDATE nodes SET
                            zerotier_node_id = $1,
                            zerotier_ip = $2,
                            host = $3,
                            port = $4,
                            rack = $5,
                            total_space = $6,
                            free_space = $7,
                            version = $8,
                            boot_token = $9,
                            lease_ttl = $10,
                            last_heartbeat = $11,
                            state = $12
                        WHERE node_id = $13
                        """,
                        zerotier_node_id,
                        zerotier_ip,
                        host,
                        port,
                        rack,
                        total_space,
                        free_space,
                        version,
                        boot_token,
                        int(lease_ttl)
                        if lease_ttl is not None
                        else getattr(config, "lease_ttl", 60),
                        now,
                        NodeState.ACTIVE.value,
                        node_id,
                    )
                else:
                    await conn.execute(
                        """
                        INSERT INTO nodes
                        (node_id, zerotier_node_id, zerotier_ip, host, port, rack, 
                         free_space, total_space, chunk_count, last_heartbeat, state, 
                         lease_ttl, version, boot_token)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                        """,
                        node_id,
                        zerotier_node_id,
                        zerotier_ip,
                        host,
                        port,
                        rack,
                        free_space,
                        total_space,
                        0,
                        now,
                        NodeState.ACTIVE.value,
                        int(lease_ttl)
                        if lease_ttl is not None
                        else getattr(config, "lease_ttl", 60),
                        version,
                        boot_token,
                    )

                threshold = now - timedelta(seconds=config.node_timeout)
                await conn.execute(
                    "UPDATE nodes SET state = $1 WHERE last_heartbeat < $2",
                    NodeState.INACTIVE.value,
                    threshold,
                )

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
        """Actualiza heartbeat de un nodo"""
        async with self.lock:
            now = datetime.now(timezone.utc)
            threshold = now - timedelta(seconds=config.node_timeout)

            logger.info(f"Heartbeat de {node_id}: reportando {len(chunk_ids)} chunks")

            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT node_id FROM nodes WHERE node_id = $1", node_id
                )

                if row:
                    query_parts = [
                        "UPDATE nodes SET",
                        "free_space = $1,",
                        "total_space = $2,",
                        "chunk_count = $3,",
                        "last_heartbeat = $4,",
                        "state = $5",
                    ]
                    params = [
                        free_space,
                        total_space,
                        len(chunk_ids),
                        now,
                        NodeState.ACTIVE.value,
                    ]
                    param_count = 6

                    if zerotier_ip and zerotier_ip.strip() and zerotier_ip != "0.0.0.0":
                        query_parts.insert(-1, f"zerotier_ip = ${param_count},")
                        params.append(zerotier_ip)
                        param_count += 1
                        query_parts.insert(-1, f"host = ${param_count},")
                        params.append(zerotier_ip)
                        param_count += 1

                    if zerotier_node_id and zerotier_node_id.strip():
                        query_parts.insert(-1, f"zerotier_node_id = ${param_count},")
                        params.append(zerotier_node_id)
                        param_count += 1

                    if url:
                        try:
                            port_from_url = int(url.split(":")[-1].split("/")[0])
                            query_parts.insert(-1, f"port = ${param_count},")
                            params.append(port_from_url)
                            param_count += 1
                        except (ValueError, IndexError):
                            pass

                    query_parts.append(f"WHERE node_id = ${param_count}")
                    params.append(node_id)

                    query = " ".join(query_parts)
                    await conn.execute(query, *params)

                else:
                    host = zerotier_ip if zerotier_ip else "0.0.0.0"
                    port = 8001

                    if url:
                        try:
                            port = int(url.split(":")[-1].split("/")[0])
                        except (ValueError, IndexError):
                            pass

                    await conn.execute(
                        """
                        INSERT INTO nodes 
                        (node_id, host, port, zerotier_ip, zerotier_node_id, 
                         free_space, total_space, chunk_count, last_heartbeat, state)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                        """,
                        node_id,
                        host,
                        port,
                        zerotier_ip,
                        zerotier_node_id,
                        free_space,
                        total_space,
                        len(chunk_ids),
                        now,
                        NodeState.ACTIVE.value,
                    )

                await conn.execute(
                    "UPDATE nodes SET state = $1 WHERE last_heartbeat < $2",
                    NodeState.INACTIVE.value,
                    threshold,
                )

                # IMPORTANTE: Sincronizar réplicas SIEMPRE, incluso con chunk_ids vacío
                # Esto permite eliminar réplicas cuando un nodo reporta 0 chunks
                await self._update_replicas_from_heartbeat(
                    node_id,
                    chunk_ids,
                    url or f"http://{zerotier_ip or '0.0.0.0'}:{8001}",
                )

            logger.debug(f"Heartbeat actualizado: {node_id} (ZT IP: {zerotier_ip})")

    async def _update_replicas_from_heartbeat(
        self, node_id: str, chunk_ids: List[UUID], node_url: str
    ) -> None:
        """Actualiza las réplicas de los chunks basándose en el heartbeat"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT file_id, chunks_json FROM files WHERE is_deleted = FALSE"
            )

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

                    if chunk_id in chunk_ids_str:
                        replica_exists = False
                        for replica in replicas:
                            if replica.get("node_id") == node_id:
                                if replica.get("state") != "committed":
                                    replica["state"] = "committed"
                                    file_modified = True
                                if replica.get("url") != node_url:
                                    replica["url"] = node_url
                                    file_modified = True
                                replica_exists = True
                                break

                        if not replica_exists:
                            replicas.append(
                                {
                                    "node_id": node_id,
                                    "url": node_url,
                                    "state": "committed",
                                    "checksum_verified": False,
                                }
                            )
                            chunk["replicas"] = replicas
                            file_modified = True
                            replicas_added += 1

                    else:
                        original_count = len(replicas)
                        replicas = [r for r in replicas if r.get("node_id") != node_id]

                        if len(replicas) < original_count:
                            chunk["replicas"] = replicas
                            file_modified = True
                            replicas_removed += 1

                if file_modified:
                    await conn.execute(
                        "UPDATE files SET chunks_json = $1, modified_at = $2 WHERE file_id = $3",
                        json.dumps(chunks_data),
                        datetime.now(timezone.utc),
                        file_id,
                    )
                    updated_files += 1

            if updated_files > 0 or replicas_added > 0 or replicas_removed > 0:
                logger.info(
                    f"Sincronización de réplicas desde heartbeat de {node_id}: "
                    f"{updated_files} archivos actualizados, "
                    f"+{replicas_added} réplicas agregadas, "
                    f"-{replicas_removed} réplicas eliminadas"
                )
            
            if replicas_removed > 0:
                logger.warning(
                    f"Nodo {node_id} perdió {replicas_removed} réplicas "
                    f"(reportó {len(chunk_ids)} chunks). Re-replicación activada."
                )

    async def get_node(self, node_id: str) -> Optional[NodeInfo]:
        """Obtiene información de un nodo"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM nodes WHERE node_id = $1", node_id)

            if not row:
                return None

            return self._row_to_node_info(row)

    async def list_nodes(self) -> List[NodeInfo]:
        """Lista todos los nodos"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM nodes ORDER BY last_heartbeat DESC")
            return [self._row_to_node_info(row) for row in rows]

    async def get_active_nodes(self) -> List[NodeInfo]:
        """Obtiene nodos activos"""
        threshold = datetime.now(timezone.utc) - timedelta(seconds=config.node_timeout)

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT * FROM nodes 
                WHERE state = $1 AND last_heartbeat > $2
                ORDER BY free_space DESC
                """,
                NodeState.ACTIVE.value,
                threshold,
            )

            return [self._row_to_node_info(row) for row in rows]

    async def acquire_lease(
        self, path: str, operation: str, timeout_seconds: int
    ) -> Optional[LeaseResponse]:
        """Adquiere un lease"""
        async with self.lock:
            await self._cleanup_expired_leases_internal()

            now = datetime.now(timezone.utc)

            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT lease_id FROM leases WHERE path = $1 AND expires_at > $2",
                    path,
                    now,
                )

                if row:
                    return None

                lease_id = uuid4()
                expires_at = now + timedelta(seconds=timeout_seconds)

                await conn.execute(
                    """
                    INSERT INTO leases (lease_id, path, operation, expires_at)
                    VALUES ($1, $2, $3, $4)
                    """,
                    lease_id,
                    path,
                    operation,
                    expires_at,
                )

                logger.info(f"Lease adquirido: {path} (ID: {lease_id})")
                return LeaseResponse(
                    lease_id=lease_id, path=path, expires_at=expires_at
                )

    async def release_lease(self, lease_id: UUID) -> bool:
        """Libera un lease"""
        async with self.lock:
            async with self.pool.acquire() as conn:
                result = await conn.execute(
                    "DELETE FROM leases WHERE lease_id = $1", lease_id
                )

                success = result.split()[-1] != "0"
                if success:
                    logger.info(f"Lease liberado: {lease_id}")

                return success

    async def cleanup_expired_leases(self) -> None:
        """Limpia leases expirados (API pública con lock)"""
        async with self.lock:
            await self._cleanup_expired_leases_internal()

    async def _cleanup_expired_leases_internal(self) -> None:
        """Limpia leases expirados (versión interna sin lock)"""
        now = datetime.now(timezone.utc)
        async with self.pool.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM leases WHERE expires_at <= $1", now
            )

            count = result.split()[-1]
            if count != "0":
                logger.debug(f"Limpiados {count} leases expirados")

    async def get_system_stats(self) -> dict:
        """Obtiene estadísticas del sistema"""
        async with self.pool.acquire() as conn:
            files_row = await conn.fetchrow(
                "SELECT COUNT(*) as count, SUM(size) as total_size FROM files WHERE is_deleted = FALSE"
            )

            total_size = files_row["total_size"] or 0
            files = await self.list_files(limit=10000)
            total_chunks = sum(len(file.chunks) for file in files)

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

    def _row_to_file_metadata(self, row) -> FileMetadata:
        """Convierte una fila de la BD a FileMetadata"""
        chunks = [ChunkEntry(**c) for c in json.loads(row["chunks_json"])]

        return FileMetadata(
            file_id=row["file_id"],
            path=row["path"],
            size=row["size"],
            created_at=row["created_at"],
            modified_at=row["modified_at"],
            chunks=chunks,
            is_deleted=row["is_deleted"],
            deleted_at=row["deleted_at"] if row["deleted_at"] else None,
            compressed=row.get("compressed", False),
            original_size=row.get("original_size"),
        )

    def _row_to_node_info(self, row) -> NodeInfo:
        """Convierte una fila de la BD a NodeInfo"""
        host = row.get("zerotier_ip") or row["host"]

        return NodeInfo(
            node_id=row["node_id"],
            host=host,
            port=row["port"],
            rack=row["rack"],
            free_space=row["free_space"],
            total_space=row["total_space"],
            chunk_count=row["chunk_count"],
            last_heartbeat=row["last_heartbeat"],
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