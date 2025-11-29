import asyncio
import logging
from collections import defaultdict
from typing import List, Dict, Set, Optional
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
        self.rebalancing_strategy = "hybrid"  # Estrategia de rebalanceo: "variance", "load", "rack_aware", "hybrid"
        self.variance_threshold = 0.3  # Umbral para rebalanceo basado en varianza
        self.max_rebalance_per_cycle = 50  # Limitar rebalanceos por ciclo
        
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
        logger.info("Verificando estado de replicación...")

        try:
            # Obtener todos los archivos
            files = await self.storage.list_files(limit=1000)

            # Obtener nodos activos
            active_nodes = await self.storage.get_active_nodes()
            active_node_ids = {node.node_id for node in active_nodes}

            logger.info(f"Archivos: {len(files)}, Nodos activos: {len(active_nodes)}")

            # Encontrar chunks que necesitan replicación
            chunks_to_replicate = await self._find_chunks_needing_replication(
                files, active_node_ids, active_nodes
            )

            if chunks_to_replicate:
                logger.warning(
                    f"Encontrados {len(chunks_to_replicate)} chunks que necesitan replicación"
                )
                await self._replicate_chunks(chunks_to_replicate, active_nodes)
            else:
                logger.info("No se encontraron chunks que necesiten replicación")

        except Exception as e:
            logger.error(f"Error verificando replicación: {e}")

    async def _find_chunks_needing_replication(
        self, files: List, active_node_ids: Set[str], active_nodes: List
    ) -> List[Dict]:
        """
        Encuentra chunks que necesitan replicación o rebalanceo.
        """
        chunks_to_replicate = []

        for file_metadata in files:
            for chunk in file_metadata.chunks:
                healthy_replicas = [
                    r
                    for r in chunk.replicas
                    if r.state == ChunkState.COMMITTED and r.node_id in active_node_ids
                ]

                current_replicas = len(healthy_replicas)
                needed_replicas = self.replication_factor

                # Caso 1: Replicación insuficiente (prioridad alta)
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
                            "reason": "insufficient_replication",
                            "priority": 1  # Alta prioridad
                        }
                    )
                    logger.warning(
                        f"Chunk {chunk.chunk_id} tiene {current_replicas} réplicas "
                        f"(esperadas: {needed_replicas})"
                    )
                
                # Caso 2: Rebalanceo (prioridad baja)
                elif self.enable_rebalancing and current_replicas == needed_replicas:
                    should_rebalance = False
                    rebalance_reason = None
                    
                    # Aplicar estrategia de rebalanceo seleccionada
                    if self.rebalancing_strategy == "variance":
                        should_rebalance, rebalance_reason = self._check_variance_rebalance(
                            healthy_replicas, active_nodes
                        )
                    elif self.rebalancing_strategy == "load":
                        should_rebalance, rebalance_reason = self._check_load_rebalance(
                            healthy_replicas, active_nodes
                        )
                    elif self.rebalancing_strategy == "rack_aware":
                        should_rebalance, rebalance_reason = self._check_rack_aware_rebalance(
                            healthy_replicas, active_nodes
                        )
                    elif self.rebalancing_strategy == "hybrid":
                        should_rebalance, rebalance_reason = self._check_hybrid_rebalance(
                            healthy_replicas, active_nodes
                        )
                    
                    if should_rebalance:
                        chunks_to_replicate.append(
                            {
                                "file_path": file_metadata.path,
                                "chunk_id": chunk.chunk_id,
                                "chunk_size": chunk.size,
                                "current_replicas": current_replicas,
                                "needed_replicas": needed_replicas,
                                "healthy_replicas": healthy_replicas,
                                "file_metadata": file_metadata,
                                "reason": f"rebalance_{rebalance_reason}",
                                "priority": 2  # Baja prioridad
                            }
                        )

        # Ordenar por prioridad (replicación antes que rebalanceo)
        chunks_to_replicate.sort(key=lambda x: x["priority"])
        
        # Limitar rebalanceos por ciclo
        rebalance_chunks = [c for c in chunks_to_replicate if c["priority"] == 2]
        if len(rebalance_chunks) > self.max_rebalance_per_cycle:
            # Mantener todos los de replicación + límite de rebalanceo
            replication_chunks = [c for c in chunks_to_replicate if c["priority"] == 1]
            limited_rebalance = rebalance_chunks[:self.max_rebalance_per_cycle]
            chunks_to_replicate = replication_chunks + limited_rebalance
            logger.info(
                f"Limitando rebalanceo: {len(rebalance_chunks)} -> {self.max_rebalance_per_cycle}"
            )

        return chunks_to_replicate
    
    def _check_variance_rebalance(
        self, healthy_replicas: List, active_nodes: List
    ) -> tuple[bool, Optional[str]]:
        """
        ESTRATEGIA 1: Rebalanceo basado en varianza de distribución.
        
        Detecta cuando los chunks están concentrados en pocos nodos mientras
        otros nodos tienen poca o ninguna carga.
        
        Ejemplo:
        - 5 nodos activos, pero todas las réplicas están en 2 nodos antiguos
        - Distribución ideal: chunks repartidos uniformemente
        """
        if len(active_nodes) <= len(healthy_replicas):
            return False, None
        
        # Calcular distribución actual de chunks por nodo
        node_chunk_counts = defaultdict(int)
        for node in active_nodes:
            node_chunk_counts[node.node_id] = node.chunk_count
        
        # Calcular distribución ideal y varianza
        total_chunks = sum(node_chunk_counts.values())
        if total_chunks == 0:
            return False, None
        
        avg_chunks_per_node = total_chunks / len(active_nodes)
        variance = sum(
            abs(count - avg_chunks_per_node) for count in node_chunk_counts.values()
        ) / len(active_nodes)
        
        normalized_variance = variance / avg_chunks_per_node if avg_chunks_per_node > 0 else 0
        
        # Verificar si este chunk contribuye al desbalanceo
        nodes_with_this_chunk = {r.node_id for r in healthy_replicas}
        nodes_without_this_chunk = set(node_chunk_counts.keys()) - nodes_with_this_chunk
        
        if normalized_variance > self.variance_threshold and nodes_without_this_chunk:
            # Verificar si mover una réplica mejoraría la distribución
            max_loaded_node = max(
                (nid for nid in nodes_with_this_chunk),
                key=lambda nid: node_chunk_counts[nid]
            )
            min_loaded_node = min(
                (nid for nid in nodes_without_this_chunk),
                key=lambda nid: node_chunk_counts[nid]
            )
            
            load_diff = node_chunk_counts[max_loaded_node] - node_chunk_counts[min_loaded_node]
            
            if load_diff > 2:  # Solo si la diferencia es significativa
                return True, f"variance_{normalized_variance:.2f}"
        
        return False, None

    def _check_load_rebalance(
        self, healthy_replicas: List, active_nodes: List
    ) -> tuple[bool, Optional[str]]:
        """
        ESTRATEGIA 2: Rebalanceo basado en carga de nodos.
        
        Mueve réplicas de nodos sobrecargados a nodos con más capacidad.
        Considera tanto el número de chunks como el espacio disponible.
        """
        if len(active_nodes) <= len(healthy_replicas):
            return False, None
        
        # Crear mapa de nodos con sus métricas
        node_metrics = {}
        for node in active_nodes:
            if node.total_space > 0:
                usage_ratio = 1 - (node.free_space / node.total_space)
            else:
                usage_ratio = 1.0
            
            node_metrics[node.node_id] = {
                "chunk_count": node.chunk_count,
                "usage_ratio": usage_ratio,
                "free_space": node.free_space,
                "load_score": (node.chunk_count / 100) + usage_ratio  # Score combinado
            }
        
        # Identificar nodos con réplicas
        nodes_with_replicas = {r.node_id for r in healthy_replicas}
        
        # Encontrar nodo más cargado con réplica
        overloaded_nodes = [
            nid for nid in nodes_with_replicas
            if node_metrics[nid]["load_score"] > 1.5  # Umbral de sobrecarga
        ]
        
        # Encontrar nodos con baja carga sin réplica
        underloaded_nodes = [
            nid for nid in node_metrics.keys()
            if nid not in nodes_with_replicas and node_metrics[nid]["load_score"] < 0.8
        ]
        
        if overloaded_nodes and underloaded_nodes:
            max_load = max(node_metrics[nid]["load_score"] for nid in overloaded_nodes)
            min_load = min(node_metrics[nid]["load_score"] for nid in underloaded_nodes)
            
            if max_load - min_load > 0.5:  # Diferencia significativa
                return True, f"load_diff_{(max_load - min_load):.2f}"
        
        return False, None

    def _check_rack_aware_rebalance(
        self, healthy_replicas: List, active_nodes: List
    ) -> tuple[bool, Optional[str]]:
        """
        ESTRATEGIA 3: Rebalanceo consciente de racks/zonas.
        
        Asegura que las réplicas estén distribuidas en diferentes racks
        para tolerancia a fallos de rack completo.
        """
        if len(active_nodes) <= len(healthy_replicas):
            return False, None
        
        # Agrupar nodos por rack
        racks = defaultdict(list)
        for node in active_nodes:
            rack = node.rack or "default"
            racks[rack].append(node.node_id)
        
        if len(racks) <= 1:
            return False, None  # No hay múltiples racks
        
        # Verificar distribución de réplicas por rack
        replica_racks = defaultdict(int)
        for replica in healthy_replicas:
            for rack, nodes in racks.items():
                if replica.node_id in nodes:
                    replica_racks[rack] += 1
                    break
        
        # Verificar si hay racks sin réplicas
        empty_racks = [rack for rack in racks.keys() if rack not in replica_racks]
        
        # Verificar si hay racks con múltiples réplicas
        overloaded_racks = [
            rack for rack, count in replica_racks.items() if count > 1
        ]
        
        if empty_racks and overloaded_racks:
            return True, f"rack_distribution"
        
        return False, None

    def _check_hybrid_rebalance(
        self, healthy_replicas: List, active_nodes: List
    ) -> tuple[bool, Optional[str]]:
        """
        ESTRATEGIA 4: Híbrida (combina múltiples factores).
        
        Considera varianza, carga y racks simultáneamente.
        Usa un sistema de puntuación para decidir.
        """
        variance_check, variance_reason = self._check_variance_rebalance(
            healthy_replicas, active_nodes
        )
        load_check, load_reason = self._check_load_rebalance(
            healthy_replicas, active_nodes
        )
        rack_check, rack_reason = self._check_rack_aware_rebalance(
            healthy_replicas, active_nodes
        )
        
        # Sistema de puntuación
        score = 0
        reasons = []
        
        if variance_check:
            score += 2
            reasons.append(variance_reason)
        
        if load_check:
            score += 3  # Mayor peso a carga
            reasons.append(load_reason)
        
        if rack_check:
            score += 4  # Mayor peso a diversidad de racks
            reasons.append(rack_reason)
        
        # Umbral de decisión
        if score >= 3:  # Requiere al menos 2 factores o rack diversity
            return True, "+".join(reasons)
        
        return False, None
    
    def _calculate_node_distribution_score(self, active_nodes: List) -> Dict:
        """
        Calcula scores de distribución para cada nodo.
        Útil para tomar decisiones de rebalanceo.
        """
        scores = {}
        total_chunks = sum(node.chunk_count for node in active_nodes)
        total_space = sum(node.total_space for node in active_nodes)
        
        for node in active_nodes:
            chunk_ratio = node.chunk_count / total_chunks if total_chunks > 0 else 0
            space_ratio = node.total_space / total_space if total_space > 0 else 0
            usage_ratio = 1 - (node.free_space / node.total_space) if node.total_space > 0 else 1
            
            # Score más bajo = mejor candidato para recibir chunks
            score = (chunk_ratio * 0.4) + (usage_ratio * 0.4) + (1 - space_ratio) * 0.2
            
            scores[node.node_id] = {
                "score": score,
                "chunk_count": node.chunk_count,
                "free_space": node.free_space,
                "total_space": node.total_space,
                "rack": node.rack
            }
        
        return scores

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
                logger.info(f"Descargando chunk {chunk_id} desde: {download_url}")
                
                try:
                    response = await client.get(download_url, timeout=60.0)
                    response.raise_for_status()
                except httpx.HTTPStatusError as e:
                    logger.error(
                        f"Error HTTP descargando chunk {chunk_id} desde {source_replica.node_id}: "
                        f"Status {e.response.status_code}, Body: {e.response.text[:200]}"
                    )
                    return False
                except Exception as e:
                    logger.error(f"Error descargando chunk {chunk_id}: {type(e).__name__}: {e}")
                    return False

                chunk_data = response.content
                logger.info(f"Chunk {chunk_id} descargado: {len(chunk_data)} bytes")

                # Subir chunk al nodo destino
                upload_url = f"http://{target_node.host}:{target_node.port}/api/v1/chunks/{chunk_id}"
                logger.info(
                    f"Subiendo chunk {chunk_id} a: {upload_url} "
                    f"(node_id: {target_node.node_id}, size: {len(chunk_data)} bytes)"
                )
                
                # Usar BytesIO para simular un archivo
                from io import BytesIO
                
                files = {
                    "file": (
                        f"chunk_{chunk_id}", 
                        BytesIO(chunk_data), 
                        "application/octet-stream" 
                    )
                }
                
                try:
                    # PUT con multipart/form-data (como espera el endpoint)
                    response = await client.put(
                        upload_url, 
                        files=files, 
                        timeout=60.0
                    )
                    
                    if response.status_code == 201:  # El endpoint retorna 201 CREATED
                        logger.info(f"Chunk {chunk_id} replicado exitosamente a {target_node.node_id}")
                        return True
                    else:
                        logger.error(
                            f"Error subiendo chunk {chunk_id} a {target_node.node_id}: "
                            f"Status {response.status_code}, Body: {response.text[:500]}"
                        )
                        return False
                        
                except httpx.TimeoutException:
                    logger.error(f"Timeout subiendo chunk {chunk_id} a {target_node.node_id}")
                    return False
                except Exception as e:
                    logger.error(
                        f"Error subiendo chunk {chunk_id} a {target_node.node_id}: "
                        f"{type(e).__name__}: {e}"
                    )
                    return False

        except Exception as e:
            logger.error(
                f"Error inesperado copiando chunk {chunk_id} entre nodos: "
                f"{type(e).__name__}: {e}", 
                exc_info=True
            )
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
