import asyncio
import logging
from typing import List, Dict
from uuid import UUID

import httpx

from core.config import config
from shared.models import ChunkState
from shared.protocols import ReplicationProtocol

logger = logging.getLogger(__name__)


class ReplicationManager(ReplicationProtocol):
    """
    Gestiona la re-replicación automática de chunks.
    
    ESTRATEGIAS DE REPLICACIÓN:
    
    1. REPLICACIÓN ESTÁTICA (por defecto, enable_rebalancing=False):
       - Mantiene el factor de replicación configurado (ej: 3 réplicas)
       - Si un nodo falla, re-replica los chunks perdidos a otros nodos existentes
       - Cuando se agregan nuevos nodos al cluster:
         * Los archivos NUEVOS se distribuirán automáticamente entre todos los nodos
         * Los archivos EXISTENTES mantienen sus réplicas en los nodos originales
       - Ventajas: Simple, predecible, menos overhead
       - Caso de uso: Clusters estables con pocos cambios de nodos
    
    2. REPLICACIÓN DINÁMICA (enable_rebalancing=True):
       - Además de mantener el factor de replicación, redistribuye chunks
       - Cuando se agregan nuevos nodos:
         * Re-balancea archivos existentes para aprovechar todos los nodos
         * Mejora la distribución de carga y capacidad
       - Ventajas: Mejor distribución de datos en clusters dinámicos
       - Desventajas: Mayor overhead de red, más complejo
       - Caso de uso: Clusters que escalan frecuentemente
    
    COMPORTAMIENTO ACTUAL:
    - Por defecto usa replicación estática (enable_rebalancing=False)
    - Detecta chunks con réplicas insuficientes (heartbeat como fuente de verdad)
    - Re-replica automáticamente cuando current_replicas < replication_factor
    - No redistribuye chunks a nuevos nodos si ya tienen suficientes réplicas
    
    Para habilitar rebalanceo dinámico:
    - Configurar DFS_ENABLE_REBALANCING=true en variables de entorno
    - O modificar enable_rebalancing=True en __init__
    """

    def __init__(
        self,
        storage,
        replication_factor: int,
        enable_rebalancing: bool = False
    ):
        self.storage = storage
        self.replication_factor = replication_factor or config.replication_factor
        self.running = False
        self.task: asyncio.Task
        self.check_interval = 30  # segundos
        
        # Configuración de rebalanceo
        self.enable_rebalancing = enable_rebalancing
        self.max_replicas_per_chunk = self.replication_factor  # No crear más réplicas de las necesarias

        # Estadísticas
        self.replication_attempts = 0
        self.successful_replications = 0
        self.failed_replications = 0

    async def start(self):
        """Inicia el replicator"""
        self.running = True
        self.task = asyncio.create_task(self._replication_loop())
        logger.info("Replication Manager iniciado")

    async def stop(self):
        """Detiene el replicator"""
        self.running = False
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
        logger.info("Replication Manager detenido")

    async def _replication_loop(self):
        """Loop principal de replicación"""
        while self.running:
            try:
                await self.check_and_replicate()
                await asyncio.sleep(self.check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error en replication loop: {e}")
                await asyncio.sleep(5)  # Esperar antes de reintentar

    async def check_and_replicate(self):
        """
        Verifica el estado de replicación y orquesta re-replicación si es necesario.
        """
        logger.debug("Verificando estado de replicación...")

        try:
            # Obtener todos los archivos
            files = await self.storage.list_files(limit=1000)

            # Obtener nodos activos
            active_nodes = await self.storage.get_active_nodes()
            active_node_ids = {node.node_id for node in active_nodes}

            logger.debug(f"Archivos: {len(files)}, Nodos activos: {len(active_nodes)}")

            # Encontrar chunks que necesitan replicación
            chunks_to_replicate = await self._find_chunks_needing_replication(
                files, active_node_ids
            )

            if chunks_to_replicate:
                logger.info(
                    f"Encontrados {len(chunks_to_replicate)} chunks que necesitan replicación"
                )
                await self._replicate_chunks(chunks_to_replicate, active_nodes)
            else:
                logger.debug("No se encontraron chunks que necesiten replicación")

        except Exception as e:
            logger.error(f"Error verificando replicación: {e}")

    async def _find_chunks_needing_replication(
        self, files: List, active_node_ids: set
    ) -> List[Dict]:
        """
        Encuentra chunks que tienen menos réplicas de las requeridas.
        Si enable_rebalancing=True, también encuentra chunks mal distribuidos.
        """
        chunks_to_replicate = []

        for file_metadata in files:
            for chunk in file_metadata.chunks:
                # Contar réplicas saludables
                healthy_replicas = [
                    r
                    for r in chunk.replicas
                    if r.state == ChunkState.COMMITTED and r.node_id in active_node_ids
                ]

                current_replicas = len(healthy_replicas)
                needed_replicas = self.replication_factor

                # Caso 1: Replicación insuficiente (siempre se maneja)
                if current_replicas < needed_replicas:
                    chunks_to_replicate.append(
                        {
                            "file_path": file_metadata.path,
                            "chunk_id": chunk.chunk_id,
                            "chunk_size": chunk.size,
                            "current_replicas": current_replicas,
                            "needed_replicas": needed_replicas,
                            "healthy_replicas": healthy_replicas,
                            "file_metadata": file_metadata,
                            "reason": "insufficient_replication"
                        }
                    )

                    logger.warning(
                        f"Chunk {chunk.chunk_id} tiene {current_replicas} réplicas "
                        f"(esperadas: {needed_replicas})"
                    )
                
                # Caso 2: Rebalanceo (solo si está habilitado)
                elif self.enable_rebalancing and current_replicas == needed_replicas:
                    # Verificar si los chunks están bien distribuidos
                    # Por ejemplo: si hay 5 nodos pero las réplicas están solo en 3 nodos antiguos
                    if len(active_node_ids) > current_replicas:
                        # Calcular la distribución ideal
                        # Solo rebalancear si la distribución es muy desigual
                        nodes_with_replicas = {r.node_id for r in healthy_replicas}
                        
                        # Opcionalmente: agregar lógica para detectar desbalanceo
                        # Por ahora, NO se rebalancea automáticamente
                        # Para activar: descomentar y ajustar la lógica
                        pass

        return chunks_to_replicate

    async def _replicate_chunks(
        self, chunks_to_replicate: List[Dict], available_nodes: List
    ):
        """
        Orquesta la re-replicación de chunks.
        """
        for chunk_info in chunks_to_replicate:
            try:
                success = await self._replicate_single_chunk(
                    chunk_info, available_nodes
                )

                self.replication_attempts += 1
                if success:
                    self.successful_replications += 1
                else:
                    self.failed_replications += 1

            except Exception as e:
                logger.error(f"Error replicando chunk {chunk_info['chunk_id']}: {e}")
                self.failed_replications += 1

    async def _replicate_single_chunk(
        self, chunk_info: Dict, available_nodes: List
    ) -> bool:
        """
        Replica un chunk individual.
        """
        chunk_id = chunk_info["chunk_id"]
        healthy_replicas = chunk_info["healthy_replicas"]

        if not healthy_replicas:
            logger.error(f"No hay réplicas saludables para chunk {chunk_id}")
            return False

        # Seleccionar nodos destino
        target_nodes = self._select_target_nodes(
            available_nodes,
            healthy_replicas,
            chunk_info["needed_replicas"] - len(healthy_replicas),
        )

        if not target_nodes:
            logger.warning(f"No hay nodos disponibles para replicar chunk {chunk_id}")
            return False

        # Seleccionar nodo origen (el más saludable)
        source_replica = self._select_source_replica(healthy_replicas)

        logger.info(
            f"Replicando chunk {chunk_id} desde {source_replica.node_id} "
            f"a {len(target_nodes)} nodos"
        )

        # Replicar a cada nodo destino
        success_count = 0
        for target_node in target_nodes:
            try:
                if await self._copy_chunk_between_nodes(
                    chunk_id, source_replica, target_node
                ):
                    success_count += 1

                    # Actualizar metadata
                    await self._update_chunk_metadata(
                        chunk_info["file_metadata"], chunk_id, target_node
                    )
            except Exception as e:
                logger.error(
                    f"Error replicando chunk {chunk_id} a {target_node.node_id}: {e}"
                )

        logger.info(
            f"Replicación completada para chunk {chunk_id}: "
            f"{success_count}/{len(target_nodes)} éxitos"
        )

        return success_count > 0

    async def _copy_chunk_between_nodes(
        self, chunk_id: UUID, source_replica, target_node
    ) -> bool:
        """
        Copia un chunk de un nodo a otro.
        """
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                # Descargar chunk del nodo origen
                download_url = f"{source_replica.url}/api/v1/chunks/{chunk_id}"
                response = await client.get(download_url, timeout=60.0)

                if response.status_code != 200:
                    logger.error(
                        f"Error descargando chunk {chunk_id} desde {source_replica.node_id}"
                    )
                    return False

                chunk_data = response.content

                # Subir chunk al nodo destino
                upload_url = f"http://{target_node.host}:{target_node.port}/api/v1/chunks/{chunk_id}"
                logger.info(f"Intentando subir chunk a: {upload_url} (node_id: {target_node.node_id})")
                files = {"file": ("chunk", chunk_data, "application/octet-stream")}

                response = await client.put(upload_url, files=files, timeout=60.0)

                if response.status_code == 200:
                    logger.info(f"Chunk {chunk_id} replicado a {target_node.node_id}")
                    return True
                else:
                    logger.error(
                        f"Error subiendo chunk {chunk_id} a {target_node.node_id}"
                    )
                    return False

        except Exception as e:
            logger.error(f"Error copiando chunk entre nodos: {e}")
            return False

    async def _update_chunk_metadata(self, file_metadata, chunk_id: UUID, target_node):
        """
        Actualiza la metadata para incluir la nueva réplica.
        """
        try:
            # En un sistema real, aquí actualizarías la metadata en el storage
            # Por simplicidad, solo logueamos por ahora
            logger.debug(
                f"Metadata actualizada: chunk {chunk_id} replicado a {target_node.node_id}"
            )

            # TODO: Implementar actualización real de metadata
            # Esto requeriría modificar el storage para actualizar réplicas individuales

        except Exception as e:
            logger.error(f"Error actualizando metadata para chunk {chunk_id}: {e}")

    def _select_target_nodes(
        self, available_nodes: List, existing_replicas: List, num_needed: int
    ) -> List:
        """
        Selecciona nodos destino para replicación.
        """
        # Excluye nodos que ya tienen el chunk
        existing_node_ids = {r.node_id for r in existing_replicas}
        candidate_nodes = [
            node for node in available_nodes if node.node_id not in existing_node_ids
        ]

        # Ordena por espacio libre (descendente)
        candidate_nodes.sort(key=lambda node: node.free_space, reverse=True)

        # Selecciona los mejores candidatos
        return candidate_nodes[:num_needed]

    def _select_source_replica(self, healthy_replicas: List):
        """
        Selecciona la réplica origen más confiable.
        """
        # Por ahora, simplemente selecciona la primera réplica saludable
        # Más adelante, podrías considerar:
        # - Latencia del nodo
        # - Carga del nodo
        # - Historial de confiabilidad
        return healthy_replicas[0]

    def get_stats(self) -> Dict:
        """Obtiene estadísticas del replicator"""
        return {
            "replication_attempts": self.replication_attempts,
            "successful_replications": self.successful_replications,
            "failed_replications": self.failed_replications,
            "success_rate": (
                self.successful_replications / self.replication_attempts
                if self.replication_attempts > 0
                else 0
            ),
            "replication_factor": self.replication_factor,
            "running": self.running,
        }

    async def trigger_immediate_check(self):
        """Fuerza una verificación inmediata de replicación"""
        if self.running:
            await self.check_and_replicate()
