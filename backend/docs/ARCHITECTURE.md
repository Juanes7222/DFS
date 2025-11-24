# Arquitectura del Sistema de Archivos Distribuido (DFS)

Este documento describe en detalle la arquitectura, componentes y flujos de datos del Sistema de Archivos Distribuido.

## Visión General

El DFS implementa una arquitectura maestro-esclavo (master-slave) donde un Metadata Service centralizado coordina las operaciones mientras múltiples DataNodes manejan el almacenamiento físico de los datos. Los clientes interactúan directamente con ambos componentes: consultan metadata del maestro y transfieren datos directamente hacia/desde los DataNodes.

Esta arquitectura se inspira en sistemas distribuidos probados como Google File System (GFS) y Hadoop Distributed File System (HDFS), adaptada para un entorno más ligero basado en Python y contenedores.

## Componentes del Sistema

### Metadata Service (Master)

El Metadata Service es el componente central que mantiene todo el estado del sistema. Su responsabilidad principal es mantener un mapeo consistente entre rutas de archivos lógicas y la ubicación física de sus chunks en los DataNodes.

**Responsabilidades Principales:**

El servicio mantiene un namespace jerárquico similar a un filesystem tradicional, donde cada archivo tiene una ruta única y metadata asociada. Cuando un archivo se crea, el servicio genera un identificador único (UUID) y lo asocia con la ruta proporcionada por el cliente.

Para cada archivo, el servicio almacena la lista ordenada de chunks que lo componen. Cada chunk tiene su propio UUID, tamaño, checksum SHA256, y la lista de réplicas con sus ubicaciones. Esta información es crítica para la reconstrucción correcta del archivo durante la descarga.

El servicio implementa un sistema de leases para coordinar escrituras concurrentes. Cuando un cliente desea escribir un archivo, debe adquirir un lease exclusivo que garantiza que no habrá conflictos con otros clientes. Los leases tienen un timeout configurable y se liberan automáticamente si el cliente no los renueva.

**Algoritmo de Placement:**

El placement de réplicas es crucial para la durabilidad y disponibilidad. El algoritmo actual implementa una estrategia round-robin simple que distribuye las réplicas uniformemente entre los nodos disponibles. Para cada chunk, el servicio selecciona tres nodos diferentes (asumiendo replication_factor=3) rotando a través de la lista de nodos activos.

En una implementación más sofisticada, el algoritmo consideraría:
- Rack awareness para evitar colocar todas las réplicas en el mismo rack
- Balanceo de carga basado en espacio libre y carga actual
- Localidad de red para minimizar latencia
- Historial de confiabilidad de cada nodo

**Monitoreo de Nodos:**

El servicio mantiene un registro de todos los DataNodes en el cluster. Cada nodo envía heartbeats periódicos cada 10 segundos que incluyen:
- Espacio libre y capacidad total
- Lista completa de chunk IDs almacenados
- Métricas de rendimiento (opcional)

Si un nodo deja de enviar heartbeats por más de 30 segundos, el servicio lo marca como inactivo y programa re-replicación de todos sus chunks a otros nodos sanos.

**Persistencia:**

La metadata se persiste en una base de datos SQLite local. Cada operación que modifica el estado (crear archivo, commit chunks, eliminar archivo, actualizar nodo) se escribe inmediatamente a disco para garantizar durabilidad.

Para producción, se recomienda reemplazar SQLite con etcd, que proporciona:
- Consenso distribuido mediante Raft
- Alta disponibilidad con múltiples réplicas
- Consistencia fuerte
- Watch API para notificaciones de cambios

**API REST:**

El servicio expone una API REST completa documentada con OpenAPI. Los endpoints principales son:

- `POST /api/v1/files/upload-init`: Inicia un upload y devuelve el plan de chunks
- `POST /api/v1/files/commit`: Confirma que los chunks fueron subidos
- `GET /api/v1/files/{path}`: Obtiene metadata de un archivo
- `GET /api/v1/files`: Lista archivos con filtros opcionales
- `DELETE /api/v1/files/{path}`: Elimina un archivo (soft-delete)
- `POST /api/v1/leases/acquire`: Adquiere un lease exclusivo
- `POST /api/v1/leases/release`: Libera un lease
- `GET /api/v1/nodes`: Lista todos los nodos
- `POST /api/v1/nodes/heartbeat`: Recibe heartbeat de un DataNode
- `GET /api/v1/health`: Health check del servicio

### DataNode (Storage Node)

Los DataNodes son los trabajadores del sistema, responsables del almacenamiento físico y la entrega de chunks. Cada DataNode opera de manera independiente y se comunica con el Metadata Service solo para heartbeats y coordinación.

**Almacenamiento de Chunks:**

Los chunks se almacenan como archivos binarios en el filesystem local. Cada chunk tiene dos archivos asociados:
- `{chunk_id}.chunk`: Los datos binarios del chunk
- `{chunk_id}.checksum`: El checksum SHA256 en formato hexadecimal

Esta estructura simple permite operaciones eficientes de lectura/escritura y facilita debugging y recuperación manual si es necesario.

**Verificación de Integridad:**

Cada vez que un chunk se escribe, el DataNode calcula su checksum SHA256 y lo almacena. Durante la lectura, el DataNode recalcula el checksum y lo compara con el almacenado para detectar corrupción.

Adicionalmente, el DataNode puede ejecutar scrubbing en background: un proceso que periódicamente verifica todos los chunks almacenados para detectar corrupción silenciosa causada por bit rot o fallos de hardware.

**Heartbeats:**

Los heartbeats son el mecanismo principal de coordinación entre DataNodes y el Metadata Service. Cada 10 segundos, el DataNode envía un heartbeat que incluye:

```json
{
  "node_id": "node-datanode1-8001",
  "free_space": 50000000000,
  "total_space": 100000000000,
  "chunk_ids": ["uuid1", "uuid2", "uuid3", ...]
}
```

El Metadata Service usa esta información para:
- Detectar nodos caídos
- Tomar decisiones de placement basadas en espacio disponible
- Identificar chunks que necesitan re-replicación
- Validar que los chunks reportados coinciden con la metadata

**API REST:**

Los DataNodes exponen una API simple para operaciones de chunks:

- `PUT /api/v1/chunks/{chunk_id}`: Almacena un chunk
- `GET /api/v1/chunks/{chunk_id}`: Recupera un chunk
- `DELETE /api/v1/chunks/{chunk_id}`: Elimina un chunk
- `GET /api/v1/health`: Health check

Estas APIs usan streaming HTTP para transferir datos de manera eficiente sin cargar chunks completos en memoria.

### Cliente (CLI y Librería)

El cliente encapsula toda la lógica compleja de interacción con el DFS, proporcionando una interfaz simple para los usuarios.

**Upload Flow:**

1. El cliente lee el archivo local y calcula su tamaño total
2. Llama a `POST /api/v1/files/upload-init` con el path y tamaño
3. El Metadata Service devuelve un plan de chunks con targets para cada réplica
4. El cliente divide el archivo en chunks según el plan
5. Para cada chunk:
   - Calcula el checksum SHA256
   - Sube el chunk en paralelo a todos los targets designados
   - Registra qué nodos confirmaron la escritura
6. Una vez todos los chunks están subidos, llama a `POST /api/v1/files/commit` con la lista de chunks y sus checksums
7. El Metadata Service valida y confirma el archivo

**Download Flow:**

1. El cliente llama a `GET /api/v1/files/{path}` para obtener metadata
2. El Metadata Service devuelve la lista de chunks con sus réplicas
3. Para cada chunk:
   - Selecciona una réplica (preferiblemente la más cercana o menos cargada)
   - Descarga el chunk desde el DataNode
   - Verifica el checksum
   - Si falla, reintenta con otra réplica
4. Reconstruye el archivo concatenando los chunks en orden
5. Verifica el checksum global (opcional)

**Manejo de Errores:**

El cliente implementa reintentos con backoff exponencial para manejar fallos transitorios:
- Timeouts de red
- Nodos temporalmente no disponibles
- Errores 5xx del servidor

Si un chunk no puede ser subido a suficientes réplicas, el cliente puede solicitar al Metadata Service nodos alternativos.

### Frontend Web

El frontend proporciona una interfaz visual para gestionar el DFS y monitorear el estado del cluster.

**Componentes Principales:**

**Dashboard**: Muestra métricas clave del cluster en tiempo real:
- Número de nodos activos/inactivos
- Uso total de almacenamiento
- Número de archivos y chunks
- Factor de replicación promedio
- Gráficos de throughput y latencia

**Gestión de Archivos**: Permite operaciones CRUD en archivos:
- Listar archivos con búsqueda y filtrado
- Upload de archivos con barra de progreso
- Download de archivos
- Eliminación con confirmación
- Vista detallada mostrando chunks y réplicas

**Vista de Nodos**: Muestra información de cada DataNode:
- Estado (activo/inactivo)
- Espacio libre/usado con gráfico
- Número de chunks almacenados
- Último heartbeat
- Métricas de rendimiento

**Integración con Backend:**

El frontend se comunica exclusivamente con el Metadata Service a través de su API REST. No interactúa directamente con los DataNodes, excepto durante uploads/downloads donde el Metadata Service proporciona las URLs de los DataNodes.

## Flujos de Datos Detallados

### Flujo de Upload Completo

```
Cliente                    Metadata Service              DataNode1/2/3
   |                              |                            |
   |---(1) upload-init----------->|                            |
   |      {path, size}            |                            |
   |                              |                            |
   |<--(2) plan------------------ |                            |
   |      {file_id, chunks[]}     |                            |
   |                              |                            |
   |---(3) PUT chunk1-------------|--------------------------->|
   |      {chunk_data}            |                            |
   |                              |                            |
   |<--(4) {checksum}-------------|----------------------------|
   |                              |                            |
   |---(5) commit---------------->|                            |
   |      {file_id, chunks[]}     |                            |
   |                              |                            |
   |                              |---(6) validate checksums   |
   |                              |                            |
   |<--(7) success----------------|                            |
```

**Detalles de cada paso:**

1. El cliente solicita iniciar un upload proporcionando el path de destino y el tamaño del archivo
2. El Metadata Service calcula cuántos chunks se necesitan, selecciona nodos para cada réplica, y devuelve el plan
3. El cliente sube cada chunk a los DataNodes designados (en paralelo)
4. Cada DataNode confirma la escritura y devuelve el checksum calculado
5. El cliente envía commit con la lista de chunks, checksums y nodos que confirmaron
6. El Metadata Service valida que hay suficientes réplicas y los checksums coinciden
7. El Metadata Service confirma el archivo y actualiza su metadata

### Flujo de Download Completo

```
Cliente                    Metadata Service              DataNode1/2/3
   |                              |                            |
   |---(1) GET file metadata----->|                            |
   |      {path}                  |                            |
   |                              |                            |
   |<--(2) metadata---------------|                            |
   |      {chunks[], replicas[]}  |                            |
   |                              |                            |
   |---(3) GET chunk1-------------|--------------------------->|
   |                              |                            |
   |<--(4) chunk_data-------------|----------------------------|
   |                              |                            |
   |---(5) verify checksum        |                            |
   |                              |                            |
   |---(6) reconstruct file       |                            |
```

**Detalles de cada paso:**

1. El cliente solicita metadata del archivo
2. El Metadata Service devuelve la lista de chunks con todas sus réplicas
3. El cliente descarga cada chunk desde una réplica (seleccionada por proximidad o carga)
4. El DataNode envía el chunk via streaming HTTP
5. El cliente verifica el checksum; si falla, reintenta con otra réplica
6. El cliente concatena los chunks en orden para reconstruir el archivo original

### Flujo de Re-replicación

```
Metadata Service           DataNode1 (source)         DataNode4 (target)
   |                              |                            |
   |---(1) detect under-replication                           |
   |      (chunk X has only 2 replicas)                       |
   |                              |                            |
   |---(2) select source & target                             |
   |      (copy from DN1 to DN4)  |                            |
   |                              |                            |
   |---(3) GET chunk X----------->|                            |
   |                              |                            |
   |<--(4) chunk_data-------------|                            |
   |                              |                            |
   |---(5) PUT chunk X------------|--------------------------->|
   |                              |                            |
   |<--(6) success----------------|----------------------------|
   |                              |                            |
   |---(7) update metadata        |                            |
   |      (add DN4 as replica)    |                            |
```

**Detalles de cada paso:**

1. El ReplicationManager detecta que un chunk tiene menos réplicas que el factor configurado
2. Selecciona un DataNode source que tiene el chunk y un target con espacio disponible
3. Descarga el chunk desde el source
4. Recibe los datos del chunk
5. Sube el chunk al target
6. El target confirma la escritura
7. Actualiza la metadata agregando el target a la lista de réplicas

## Modelo de Datos

### FileMetadata

```python
{
  "file_id": "uuid",
  "path": "/path/to/file.txt",
  "size": 134217728,  # bytes
  "created_at": "2024-01-01T00:00:00Z",
  "modified_at": "2024-01-01T00:00:00Z",
  "is_deleted": false,
  "chunks": [
    {
      "chunk_id": "uuid",
      "seq_index": 0,
      "size": 67108864,
      "checksum": "sha256_hex",
      "replicas": [
        {
          "node_id": "node-datanode1-8001",
          "url": "http://datanode1:8001",
          "state": "active",
          "last_heartbeat": "2024-01-01T00:00:00Z",
          "checksum_verified": true
        },
        ...
      ]
    },
    ...
  ]
}
```

### NodeInfo

```python
{
  "node_id": "node-datanode1-8001",
  "host": "datanode1",
  "port": 8001,
  "rack": "rack1",  # opcional
  "free_space": 50000000000,
  "total_space": 100000000000,
  "chunk_count": 1234,
  "last_heartbeat": "2024-01-01T00:00:00Z",
  "state": "active"  # active, inactive, dead
}
```

## Consideraciones de Escalabilidad

### Metadata Service

El Metadata Service es el cuello de botella principal del sistema. Para escalar:

**Sharding Horizontal**: Particionar el namespace por prefijo de path. Por ejemplo:
- Master1 maneja /a-m/*
- Master2 maneja /n-z/*

**Replicación con Consenso**: Usar etcd o Raft para replicar metadata en múltiples nodos con failover automático.

**Caching**: Implementar cache de metadata frecuentemente accedida en memoria o Redis.

### DataNodes

Los DataNodes escalan linealmente. Agregar más nodos aumenta proporcionalmente:
- Capacidad de almacenamiento
- Throughput de lectura/escritura
- Tolerancia a fallos

### Red

El ancho de banda de red puede convertirse en cuello de botella. Optimizaciones:
- Usar gRPC con streaming para transferencias más eficientes
- Implementar compresión de datos en tránsito
- Considerar RDMA para clusters de alto rendimiento

## Consideraciones de Seguridad

### Autenticación

Implementar autenticación JWT para APIs:
- Clientes deben autenticarse con el Metadata Service
- Tokens JWT incluyen permisos y expiración
- DataNodes validan tokens antes de servir chunks

### Autorización

Implementar ACLs (Access Control Lists) por archivo:
- Permisos de lectura/escritura/ejecución
- Ownership y grupos
- Validación en cada operación

### Encriptación

**En Tránsito**: Usar mTLS entre todos los componentes:
- Cliente ↔ Metadata Service
- Cliente ↔ DataNode
- Metadata Service ↔ DataNode

**En Reposo**: Encriptar chunks en disco:
- Usar AES-256-GCM
- Claves gestionadas por KMS (Key Management Service)
- Rotación periódica de claves

## Consideraciones de Confiabilidad

### Durabilidad de Datos

Con replication_factor=3 y asumiendo probabilidad de fallo de nodo de 1% anual:
- Probabilidad de perder un chunk: 0.01^3 = 0.000001 (1 en 1 millón)
- Para 1 millón de chunks: esperamos perder 1 chunk por año

Para mayor durabilidad, aumentar replication_factor o implementar erasure coding.

### Disponibilidad

Con 3 réplicas, el sistema puede tolerar la pérdida de 2 nodos y seguir sirviendo todos los datos.

Disponibilidad estimada: 99.99% (asumiendo nodos con 99% uptime individual)

### Recuperación ante Desastres

Estrategias recomendadas:
- Backups periódicos de metadata a almacenamiento externo
- Replicación geográfica de chunks críticos
- Procedimientos documentados de recuperación
- Simulacros regulares de disaster recovery

## Métricas y Monitoreo

### Métricas Clave

**Metadata Service:**
- Latencia de operaciones (p50, p95, p99)
- Throughput de requests (req/s)
- Tamaño de metadata en memoria
- Número de leases activos
- Replication lag (chunks bajo factor)

**DataNodes:**
- Throughput de lectura/escritura (MB/s)
- IOPS (operaciones de I/O por segundo)
- Uso de disco (%)
- Latencia de operaciones
- Tasa de errores de checksum

**Sistema:**
- Número de archivos totales
- Capacidad total/usada/libre
- Número de nodos activos
- Distribución de tamaños de archivos
- Tasa de crecimiento de datos

### Alertas Recomendadas

- Nodo inactivo por más de 1 minuto
- Uso de disco > 85%
- Replication lag > 100 chunks
- Tasa de errores > 1%
- Latencia p99 > 1 segundo

## Referencias

Este diseño se inspira en sistemas distribuidos ampliamente probados:

- Google File System (GFS): Arquitectura maestro-esclavo, chunks grandes, replicación
- Hadoop Distributed File System (HDFS): Implementación open-source de conceptos de GFS
- Ceph: Sistema de almacenamiento distribuido con CRUSH algorithm para placement

---

**Autor**: Manus AI  
**Versión**: 1.0.0  
**Última Actualización**: 2024
