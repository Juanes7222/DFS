# Estrategias de Replicación en DFS

## Visión General

El sistema de archivos distribuido (DFS) implementa replicación automática de chunks para garantizar durabilidad y disponibilidad de datos. Este documento explica las dos estrategias de replicación disponibles y cuándo usar cada una.

## Factor de Replicación

El **factor de replicación** (por defecto: 3) define cuántas copias de cada chunk se mantienen en el cluster. Este valor es configurable mediante:

```bash
export DFS_REPLICATION_FACTOR=3
```

## Estrategias de Replicación

### 1. Replicación Estática (Por Defecto)

**Configuración:** `DFS_ENABLE_REBALANCING=false` (valor por defecto)

#### Comportamiento

- **Mantenimiento del Factor:** Mantiene el factor de replicación configurado (ej: 3 réplicas por chunk)
- **Recuperación ante Fallos:** Si un nodo falla, re-replica automáticamente los chunks perdidos a otros nodos existentes
- **Archivos Nuevos:** Se distribuyen automáticamente entre todos los nodos disponibles en el momento de la carga
- **Archivos Existentes:** Al agregar nuevos nodos, los archivos existentes **mantienen** sus réplicas en los nodos originales
- **Sin Rebalanceo:** No redistribuye chunks existentes cuando se agregan nuevos nodos

#### Escenario de Ejemplo

```
Estado inicial (3 nodos):
- Archivo A.txt: Chunks en nodos [1, 2, 3]
- Archivo B.txt: Chunks en nodos [1, 2, 3]

Se agregan nodos 4 y 5:
- Archivo A.txt: Chunks SIGUEN en [1, 2, 3] ✓
- Archivo B.txt: Chunks SIGUEN en [1, 2, 3] ✓
- Archivo C.txt (nuevo): Chunks en [1, 3, 5] ← Usa TODOS los nodos
```

#### Ventajas

-  **Simplicidad:** Comportamiento predecible y fácil de entender
-  **Menor Overhead:** No consume ancho de banda en rebalanceo
-  **Estabilidad:** Minimiza movimiento de datos innecesario
-  **Rápido:** No requiere tiempo de rebalanceo al escalar

#### Cuándo Usar

- Clusters con topología relativamente estable
- Entornos donde los nodos no cambian frecuentemente
- Aplicaciones sensibles al ancho de banda
- Cuando la distribución natural de archivos nuevos es suficiente

### 2. Replicación Dinámica (Rebalanceo)

**Configuración:** `DFS_ENABLE_REBALANCING=true`

#### Comportamiento

- **Mantenimiento del Factor:** Mantiene el factor de replicación configurado
- **Recuperación ante Fallos:** Re-replica chunks perdidos inmediatamente
- **Archivos Nuevos:** Distribuidos entre todos los nodos disponibles
- **Archivos Existentes:** Al agregar nodos, **redistribuye** chunks existentes para aprovechar toda la capacidad
- **Con Rebalanceo:** Mueve réplicas proactivamente para equilibrar la carga

#### Escenario de Ejemplo

```
Estado inicial (3 nodos):
- Archivo A.txt: Chunks en nodos [1, 2, 3]
- Archivo B.txt: Chunks en nodos [1, 2, 3]

Se agregan nodos 4 y 5:
- Archivo A.txt: Chunks en [1, 3, 4] ← Rebalanceado
- Archivo B.txt: Chunks en [2, 4, 5] ← Rebalanceado
- Archivo C.txt (nuevo): Chunks en [1, 3, 5]
```

#### Ventajas

-  **Mejor Distribución:** Aprovecha todos los nodos del cluster
-  **Balance de Carga:** Distribuye I/O entre más nodos
-  **Capacidad Optimizada:** Utiliza todo el espacio disponible
-  **Escalabilidad:** Ideal para clusters que crecen dinámicamente

#### Desventajas

-  **Overhead de Red:** Consume ancho de banda durante rebalanceo
-  **Complejidad:** Más difícil de predecir y debuggear
-  **Tiempo de Convergencia:** Puede tomar tiempo redistribuir datos grandes

#### Cuándo Usar

- Clusters que escalan frecuentemente (autoescalado)
- Entornos cloud con nodos efímeros
- Cuando se necesita máxima utilización de recursos
- Aplicaciones con cargas de trabajo intensivas en I/O

## Sincronización de Réplicas (Heartbeat)

Independientemente de la estrategia de replicación, el sistema usa **heartbeats como fuente de verdad**:

### Proceso de Sincronización

1. **Heartbeat Periódico (cada 30s):**
   - Cada DataNode reporta su inventario de chunks al metadata service
   - Incluye: `chunk_ids[]`, `zerotier_ip`, `zerotier_node_id`, `url`

2. **Actualización de Réplicas:**
   - El metadata service compara el inventario reportado con su estado
   - **Agrega réplicas** para chunks que el nodo reporta tener
   - **Elimina réplicas** para chunks que el nodo ya no reporta
   - Actualiza estado de réplica a "committed"

3. **Detección de Pérdidas:**
   - Si un nodo deja de reportar un chunk → réplica eliminada
   - Si `current_replicas < replication_factor` → se activa re-replicación
   - Logs de advertencia: `" Eliminadas X réplicas del nodo Y"`

### Ejemplo de Logs

```
INFO: Nodo abc-123 reporta 150 chunks
INFO: Réplicas actualizadas para nodo abc-123: +3 agregadas, -1 eliminadas
WARN:  Eliminadas 1 réplicas del nodo abc-123 (no reportadas en heartbeat)
INFO: Chunk def-456 tiene 2/3 réplicas → programando re-replicación
```

## Configuración

### Variables de Entorno

```bash
# Factor de replicación (por defecto: 3)
export DFS_REPLICATION_FACTOR=3

# Habilitar rebalanceo dinámico (por defecto: false)
export DFS_ENABLE_REBALANCING=false

# Intervalo de heartbeat en segundos (por defecto: 30)
export DFS_HEARTBEAT_INTERVAL=30

# Timeout para considerar nodo inactivo (por defecto: 60)
export DFS_NODE_TIMEOUT=60
```

### Cambiar Estrategia en Runtime

La estrategia se configura al iniciar el metadata service. Para cambiarla:

1. Detener el servicio
2. Configurar `DFS_ENABLE_REBALANCING=true` o `false`
3. Reiniciar el servicio

```bash
# Replicación estática (recomendado)
export DFS_ENABLE_REBALANCING=false
python -m metadata.server

# Replicación dinámica
export DFS_ENABLE_REBALANCING=true
python -m metadata.server
```

## Monitoreo y Métricas

### Métricas Disponibles

```python
# Endpoint: GET /metrics
{
    "replication_attempts": 45,
    "successful_replications": 42,
    "failed_replications": 3,
    "under_replicated_chunks": 2
}
```

### Logs Importantes

```bash
# Inicio del servicio
INFO: Replication Manager inicializado (rebalancing=deshabilitado)

# Sincronización de réplicas
INFO: Réplicas actualizadas para nodo X: +10 agregadas, -0 eliminadas

# Re-replicación
INFO: Chunk abc-123 necesita re-replicación: 2/3 réplicas
INFO: Re-replicación exitosa: chunk abc-123 → nodo def-456

# Rebalanceo (solo si está habilitado)
INFO: Chunk xyz-789 necesita rebalanceo: distribuyendo a nuevos nodos
```

## Casos de Uso y Recomendaciones

### Usar Replicación Estática Si:

-  Tu cluster tiene 3-10 nodos relativamente estables
-  Agregas nodos raramente (< 1 vez por mes)
-  Prefieres simplicidad sobre optimización extrema
-  Tu red tiene ancho de banda limitado
-  Tus archivos son grandes (> 1GB)

### Usar Replicación Dinámica Si:

-  Cluster con autoescalado automático (cloud)
-  Agregas/quitas nodos frecuentemente (> 1 vez por semana)
-  Necesitas máxima utilización de recursos
-  Tienes ancho de banda abundante
-  Cargas de trabajo con alta concurrencia de lecturas

## Comparación con Otros Sistemas

| Sistema | Estrategia por Defecto | Rebalanceo Automático |
|---------|------------------------|----------------------|
| **DFS** | Estática | Opcional (configurable) |
| HDFS | Estática | Comando manual (`hdfs balancer`) |
| Ceph | Dinámica | Automático (CRUSH) |
| GlusterFS | Estática | Comando manual (`gluster rebalance`) |
| MinIO | Estática | No soportado |

## Preguntas Frecuentes

### ¿Por qué estática es el valor por defecto?

La mayoría de deployments tienen topologías relativamente estables. La replicación estática es más simple, predecible y evita overhead innecesario. Los archivos nuevos se distribuyen naturalmente entre todos los nodos disponibles.

### ¿Qué pasa si un nodo falla?

En **ambas estrategias**, el sistema detecta la pérdida de réplicas mediante heartbeats y re-replica automáticamente a otros nodos para mantener el factor de replicación.

## Arquitectura Técnica

### Pipeline de Replicación

```
┌─────────────────┐
│ Cliente carga   │
│ archivo nuevo   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Metadata elige  │
│ nodos destino   │ ← Usa TODOS los nodos disponibles
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Pipeline        │
│ replication     │ ← PUT /chunks/{id}?replicate_to=...
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Chunks en nodos │
│ [1, 3, 5]       │
└─────────────────┘
```

### Ciclo de Re-replicación

```
┌─────────────────┐
│ Heartbeat cada  │
│ 30 segundos     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Metadata compara│
│ replicas vs     │ ← current < needed?
│ inventario real │
└────────┬────────┘
         │
         ▼ (Si faltan)
┌─────────────────┐
│ ReplicationMgr  │
│ programa copia  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Copia chunk     │
│ nodo A → nodo B │
└─────────────────┘
```

