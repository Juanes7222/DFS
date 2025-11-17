"""
Replication Manager - Gestiona la re-replicación automática
"""
import asyncio
import logging
from datetime import datetime, timedelta
from typing import List

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from shared import ChunkState, NodeState

logger = logging.getLogger(__name__)


class ReplicationManager:
    """
    Gestiona la re-replicación automática de chunks.
    Detecta pérdida de réplicas y orquesta copias.
    """
    
    def __init__(self, storage, replication_factor: int):
        self.storage = storage
        self.replication_factor = replication_factor
        self.running = False
        self.check_interval = 30  # segundos
    
    async def run(self):
        """Loop principal del replicator"""
        self.running = True
        logger.info("Replication Manager iniciado")
        
        while self.running:
            try:
                await self.check_and_replicate()
                await asyncio.sleep(self.check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error en replication loop: {e}")
                await asyncio.sleep(5)
        
        logger.info("Replication Manager detenido")
    
    def stop(self):
        """Detiene el replicator"""
        self.running = False
    
    async def check_and_replicate(self):
        """
        Verifica el estado de replicación y orquesta re-replicación si es necesario.
        """
        logger.debug("Verificando estado de replicación...")
        
        # Obtener todos los archivos
        files = await self.storage.list_files(limit=1000)
        
        # Obtener nodos activos
        active_nodes = await self.storage.get_active_nodes()
        active_node_ids = {node.node_id for node in active_nodes}
        
        logger.debug(f"Archivos: {len(files)}, Nodos activos: {len(active_nodes)}")
        
        # Verificar cada archivo
        for file_metadata in files:
            for chunk in file_metadata.chunks:
                # Contar réplicas saludables
                healthy_replicas = [
                    r for r in chunk.replicas
                    if r.state == ChunkState.COMMITTED and r.node_id in active_node_ids
                ]
                
                if len(healthy_replicas) < self.replication_factor:
                    logger.warning(
                        f"Chunk {chunk.chunk_id} tiene {len(healthy_replicas)} réplicas "
                        f"(esperadas: {self.replication_factor})"
                    )
                    
                    # En un sistema real, aquí se orquestaría la re-replicación
                    # Por ahora solo logueamos
                    # TODO: Implementar re-replicación real
                    # await self.replicate_chunk(chunk, healthy_replicas, active_nodes)
        
        # Limpiar leases expirados
        await self.storage.cleanup_expired_leases()
    
    async def replicate_chunk(self, chunk, source_replicas, target_nodes):
        """
        Orquesta la re-replicación de un chunk.
        
        En un sistema real:
        1. Seleccionar nodo origen con réplica saludable
        2. Seleccionar nodo destino con espacio disponible
        3. Solicitar al nodo origen que copie el chunk al destino
        4. Actualizar metadata cuando se complete
        """
        # TODO: Implementar lógica de re-replicación
        pass
