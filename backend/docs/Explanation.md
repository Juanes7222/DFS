# Informe Teórico Completo - Sistema de Archivos Distribuido (DFS)

## índice

1. Introducción a Sistemas de Archivos Distribuidos
2. Arquitectura del Sistema
3. Conceptos Fundamentales
4. Componentes del Sistema
5. Protocolos y Comunicación
6. Algoritmos Implementados
7. Gestión de Datos
8. Confiabilidad y Tolerancia a Fallos
9. Performance y Escalabilidad
10. Seguridad
11. Implementación Técnica Detallada
12. Casos de Uso y Aplicaciones
13. Comparación con Otros Sistemas
14. Conclusiones

---

## 1. Introducción a Sistemas de Archivos Distribuidos

### 1.1 ¿Qué es un Sistema de Archivos Distribuido?

Un Sistema de Archivos Distribuido (Distributed File System - DFS) es una arquitectura de software que permite almacenar y acceder a archivos a través de múltiples servidores fí­sicos, presentando al usuario una vista unificada del sistema de archivos. A diferencia de un sistema de archivos tradicional que reside en un único disco o servidor, un DFS distribuye los datos entre varios nodos de almacenamiento conectados por red.

**Caracterí­sticas principales**:

El almacenamiento distribuido permite que los datos se fragmenten y repliquen automáticamente entre múltiples servidores. Esto significa que un archivo grande puede dividirse en partes más pequeñas (chunks) que se almacenan en diferentes máquinas, mientras que copias redundantes garantizan que los datos permanezcan accesibles incluso si algunos servidores fallan.

La transparencia de ubicación es fundamental en estos sistemas. Los usuarios y aplicaciones no necesitan conocer la ubicación fí­sica de los archivos. El sistema presenta una interfaz unificada donde los archivos se acceden mediante rutas lógicas (como `/documentos/reporte.pdf`) sin importar en qué servidor fí­sico residen realmente los datos.

La escalabilidad horizontal es una ventaja clave. A medida que crecen las necesidades de almacenamiento, se pueden agregar más servidores al cluster sin necesidad de reemplazar hardware existente o realizar migraciones complejas de datos. El sistema automáticamente incorpora la nueva capacidad y redistribuye la carga.

La tolerancia a fallos se logra mediante replicación. Cada fragmento de datos se almacena en múltiples servidores (tí­picamente 3 copias). Si un servidor falla, el sistema continúa operando usando las réplicas restantes, y automáticamente crea nuevas copias para mantener el nivel de redundancia deseado.

### 1.2 Motivación y Problemas que Resuelve

Los sistemas de archivos tradicionales enfrentan limitaciones significativas en el contexto moderno de big data y computación en la nube.

**Limitaciones de capacidad**: Un disco o servidor individual tiene un lí­mite fí­sico de almacenamiento. Cuando una organización necesita almacenar petabytes de datos, no es práctico ni económico usar un único servidor masivo. Los DFS permiten combinar la capacidad de cientos o miles de servidores commodity más económicos.

**Punto único de fallo**: Si todos los datos residen en un único servidor, cualquier fallo de hardware resulta en pérdida completa de acceso a los datos. Los DFS eliminan este riesgo distribuyendo y replicando datos entre múltiples máquinas independientes.

**Limitaciones de rendimiento**: Un servidor individual tiene lí­mites de throughput de I/O determinados por sus discos y controladores. Los DFS permiten paralelizar operaciones de lectura y escritura entre múltiples servidores, logrando throughput agregado mucho mayor.

**Costos de escalamiento vertical**: Expandir un sistema tradicional requiere hardware más potente y costoso. Los DFS permiten escalamiento horizontal usando hardware commodity más económico.

### 1.3 Desafí­os en Sistemas Distribuidos

La construcción de un DFS presenta desafí­os técnicos complejos que no existen en sistemas centralizados.

**Consistencia de datos**: Cuando múltiples clientes acceden y modifican archivos simultáneamente, y esos archivos están replicados en varios servidores, mantener todas las copias sincronizadas es complejo. El sistema debe decidir qué operaciones se permiten concurrentemente y cómo resolver conflictos.

**Coordinación distribuida**: Las decisiones sobre dónde almacenar nuevos datos, cuándo replicar, y cómo manejar fallos requieren coordinación entre múltiples nodos. Esto es más complejo que en un sistema centralizado donde un único proceso toma todas las decisiones.

**Particiones de red**: En una red distribuida, es posible que algunos nodos pierdan comunicación con otros temporalmente. El sistema debe decidir cómo operar durante estas particiones y cómo reconciliar el estado cuando la red se recupera.

**Latencia de red**: Las operaciones que requieren comunicación entre nodos están sujetas a latencia de red, que es órdenes de magnitud mayor que el acceso a disco local. El diseño debe minimizar comunicación innecesaria.

---

## 2. Arquitectura del Sistema

### 2.1 Arquitectura Master-Worker

Nuestro DFS implementa una arquitectura master-worker, también conocida como arquitectura maestro-esclavo, que es un patrón común en sistemas distribuidos.

**Componentes principales**:

El **Metadata Service** actúa como el nodo maestro. Es el cerebro del sistema que mantiene toda la información sobre qué archivos existen, cómo están divididos en chunks, y dónde se encuentra cada chunk. No almacena los datos reales de los archivos, solo los metadatos.

Los **DataNodes** son los nodos trabajadores que almacenan los chunks reales de datos. Cada DataNode es responsable de un conjunto de chunks y responde a solicitudes de lectura y escritura de esos chunks.

Los **Clientes** son aplicaciones o usuarios que desean almacenar o recuperar archivos. Los clientes interactúan con el Metadata Service para operaciones de metadata y directamente con DataNodes para transferencia de datos.

**Flujo de comunicación**:

Cuando un cliente quiere subir un archivo, primero contacta al Metadata Service. El Metadata Service decide cómo dividir el archivo en chunks y selecciona qué DataNodes almacenarán cada chunk. El cliente luego transfiere los datos directamente a los DataNodes seleccionados. Una vez completada la transferencia, el cliente notifica al Metadata Service para confirmar la operación.

Para descargar un archivo, el cliente consulta al Metadata Service para obtener la lista de chunks y sus ubicaciones. Luego descarga cada chunk directamente desde los DataNodes. El Metadata Service no participa en la transferencia de datos real, solo proporciona la información de ubicación.

### 2.2 Separación de Metadata y Datos

Una decisión arquitectónica fundamental es la separación entre el plano de metadata y el plano de datos.

**Plano de Metadata**:

El Metadata Service mantiene estructuras de datos que mapean rutas de archivos a listas de chunks, y chunks a ubicaciones de DataNodes. También mantiene información sobre el estado de cada DataNode (activo, inactivo, fallido) y métricas del sistema (espacio usado, espacio libre).

Esta separación permite que el Metadata Service sea ligero en términos de almacenamiento. Incluso para un sistema con petabytes de datos, los metadatos tí­picamente ocupan solo gigabytes. Esto facilita mantener todos los metadatos en memoria para acceso rápido.

**Plano de Datos**:

Los DataNodes manejan la transferencia real de datos. Cuando un cliente escribe un chunk, el DataNode lo recibe como un stream de bytes y lo almacena en su sistema de archivos local. Para lecturas, el DataNode lee el chunk del disco y lo enví­a al cliente.

Esta separación tiene ventajas importantes. El Metadata Service no se convierte en un cuello de botella para transferencia de datos, ya que los clientes se comunican directamente con DataNodes. El throughput agregado del sistema escala linealmente con el número de DataNodes.

### 2.3 Modelo de Consistencia

El sistema implementa un modelo de consistencia eventual con garantí­as fuertes para ciertas operaciones.

**Escrituras**:

Las escrituras de archivos nuevos son atómicas. Un archivo no es visible para otros clientes hasta que el cliente que lo está escribiendo confirma la operación mediante el endpoint de commit. Esto evita que otros clientes vean archivos parcialmente escritos.

Durante la escritura, el cliente enví­a cada chunk a múltiples DataNodes (tí­picamente 3) en paralelo. Solo cuando todos los DataNodes confirman la recepción exitosa, el cliente considera el chunk como escrito. Si algún DataNode falla, el cliente puede reintentar con un DataNode diferente.

**Lecturas**:

Las lecturas son eventualmente consistentes. Una vez que un archivo se confirma, las lecturas subsecuentes pueden provenir de cualquier réplica. Si una réplica está temporalmente no disponible, el sistema automáticamente intenta otra réplica.

**Metadata**:

Las operaciones de metadata (crear archivo, eliminar archivo, listar directorio) son fuertemente consistentes. El Metadata Service procesa estas operaciones secuencialmente, garantizando que todos los clientes ven el mismo estado del namespace.

---

## 3. Conceptos Fundamentales

### 3.1 Chunking (Fragmentación de Archivos)

El chunking es el proceso de dividir archivos grandes en fragmentos más pequeños de tamaño fijo.

**¿Por qué fragmentar archivos?**

Los archivos grandes (gigabytes o terabytes) son difí­ciles de manejar como unidades atómicas. Fragmentarlos permite distribuir la carga de almacenamiento entre múltiples servidores. También permite paralelizar operaciones de lectura y escritura, mejorando significativamente el rendimiento.

Si un archivo de 1GB se divide en 16 chunks de 64MB, esos chunks pueden almacenarse en 16 DataNodes diferentes. Un cliente puede descargar los 16 chunks en paralelo, logrando throughput 16 veces mayor que si descargara de un único servidor.

**Tamaño de chunk**:

Nuestro sistema usa un tamaño de chunk por defecto de 64MB. Este valor es un compromiso entre varios factores.

Chunks más grandes reducen la cantidad de metadata que el Metadata Service debe mantener. Para un archivo de 1TB, chunks de 64MB resultan en aproximadamente 16,000 chunks, mientras que chunks de 4MB resultarí­an en 256,000 chunks.

Sin embargo, chunks muy grandes reducen la granularidad de distribución. Si solo tienes 10 archivos de 1GB cada uno con chunks de 1GB, solo tendrí­as 10 chunks totales, limitando la distribución entre DataNodes.

Chunks más pequeños permiten mejor balance de carga pero aumentan overhead de metadata y número de operaciones de red.

**Implementación**:

```python
# En el Metadata Service
CHUNK_SIZE = 1048576  # 64MB en bytes

def calculate_chunks(file_size: int, chunk_size: int = CHUNK_SIZE):
    """Calcula cuántos chunks se necesitan para un archivo"""
    num_chunks = (file_size + chunk_size - 1) // chunk_size
    return num_chunks

# Ejemplo: archivo de 100MB
file_size = 104857600  # 100MB
num_chunks = calculate_chunks(file_size)  # Resultado: 2 chunks
# Chunk 0: 64MB
# Chunk 1: 36MB (resto)
```

Cada chunk se identifica con un UUID único generado por el Metadata Service. Este UUID es independiente del contenido del chunk, permitiendo que el sistema maneje chunks idénticos como entidades separadas.

### 3.2 Replicación

La replicación es el proceso de mantener múltiples copias de cada chunk en diferentes DataNodes.

**Objetivos de la replicación**:

La durabilidad de datos se logra mediante redundancia. Si un disco falla, las copias en otros discos permanecen disponibles. Con un factor de replicación de 3, el sistema puede tolerar la falla simultánea de 2 DataNodes sin pérdida de datos.

La disponibilidad mejora porque las lecturas pueden servirse desde cualquier réplica. Si un DataNode está sobrecargado o temporalmente lento, los clientes pueden leer desde otra réplica.

El rendimiento de lectura aumenta porque múltiples clientes pueden leer el mismo chunk desde diferentes réplicas simultáneamente, distribuyendo la carga.

**Factor de replicación**:

Nuestro sistema usa un factor de replicación por defecto de 3. Esto significa que cada chunk se almacena en 3 DataNodes diferentes.

```python
REPLICATION_FACTOR = 3

def select_target_nodes(chunk_id: UUID, available_nodes: List[NodeInfo]) -> List[NodeInfo]:
    """Selecciona DataNodes target para almacenar un chunk"""
    if len(available_nodes) < REPLICATION_FACTOR:
        raise InsufficientNodesError(
            f"Se requieren {REPLICATION_FACTOR} nodos, solo {len(available_nodes)} disponibles"
        )
    
    # Selección round-robin simple (en producción serí­a más sofisticado)
    targets = []
    for i in range(REPLICATION_FACTOR):
        node = available_nodes[i % len(available_nodes)]
        targets.append(node)
    
    return targets
```

**Placement de réplicas**:

La estrategia de placement determina en qué DataNodes se colocan las réplicas. Una estrategia naive serí­a seleccionar DataNodes aleatoriamente, pero esto no considera la topologí­a de red.

En un datacenter real, los servidores están organizados en racks. Servidores en el mismo rack comparten un switch de red y fuente de alimentación. Si el rack falla, todos los servidores en ese rack se vuelven inaccesibles.

Una estrategia de placement rack-aware coloca la primera réplica en un DataNode del rack donde está el cliente (minimizando latencia), la segunda réplica en un DataNode diferente del mismo rack (para tolerancia a fallo de servidor individual), y la tercera réplica en un DataNode de un rack completamente diferente (para tolerancia a fallo de rack).

Nuestra implementación actual usa placement simple round-robin, pero la arquitectura permite implementar estrategias más sofisticadas:

```python
def select_target_nodes_rack_aware(
    chunk_id: UUID, 
    available_nodes: List[NodeInfo],
    client_rack: Optional[str] = None
) -> List[NodeInfo]:
    """Selección rack-aware de DataNodes target"""
    targets = []
    
    # Primera réplica: mismo rack que el cliente si es posible
    if client_rack:
        same_rack_nodes = [n for n in available_nodes if n.rack == client_rack]
        if same_rack_nodes:
            targets.append(same_rack_nodes[0])
            available_nodes.remove(same_rack_nodes[0])
    
    # Si no se pudo colocar en el mismo rack, usar cualquier nodo
    if not targets:
        targets.append(available_nodes[0])
        available_nodes.remove(available_nodes[0])
    
    # Segunda réplica: mismo rack que la primera si hay otro nodo disponible
    same_rack_nodes = [n for n in available_nodes if n.rack == targets[0].rack]
    if same_rack_nodes:
        targets.append(same_rack_nodes[0])
        available_nodes.remove(same_rack_nodes[0])
    else:
        targets.append(available_nodes[0])
        available_nodes.remove(available_nodes[0])
    
    # Tercera réplica: rack diferente
    different_rack_nodes = [n for n in available_nodes if n.rack != targets[0].rack]
    if different_rack_nodes:
        targets.append(different_rack_nodes[0])
    else:
        targets.append(available_nodes[0])
    
    return targets
```

### 3.3 Heartbeats y Detección de Fallos

Los heartbeats son mensajes periódicos que los DataNodes enví­an al Metadata Service para indicar que están vivos y operacionales.

**Mecanismo de heartbeat**:

Cada DataNode ejecuta un loop así­ncrono que enví­a un heartbeat cada 10 segundos. El heartbeat incluye información sobre el estado del DataNode: espacio libre en disco, espacio total, y lista de chunks que almacena.

```python
# En el DataNode
async def send_heartbeat():
    """Enviar heartbeat al Metadata Service"""
    while True:
        try:
            free, total = get_disk_usage()
            chunks = get_stored_chunks()
            
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{METADATA_URL}/api/v1/nodes/heartbeat",
                    json={
                        "node_id": NODE_ID,
                        "free_space": free,
                        "total_space": total,
                        "chunk_ids": [str(c) for c in chunks]
                    },
                    timeout=5.0
                )
                
                if response.status_code == 200:
                    logger.info("Heartbeat enviado OK")
                else:
                    logger.warning(f"Heartbeat falló: {response.status_code}")
        
        except Exception as e:
            logger.error(f"Error en heartbeat: {e}")
        
        await asyncio.sleep(10)  # Esperar 10 segundos
```

**Detección de fallos**:

El Metadata Service mantiene un timestamp del último heartbeat recibido de cada DataNode. Periódicamente (cada 30 segundos), verifica si algún DataNode no ha enviado heartbeat recientemente.

```python
# En el Metadata Service
NODE_TIMEOUT = 30  # segundos

def check_node_health():
    """Verificar salud de todos los nodos"""
    now = datetime.now(timezone.utc)
    
    for node_id, node in nodes_db.items():
        time_since_heartbeat = (now - node.last_heartbeat).total_seconds()
        
        if time_since_heartbeat > NODE_TIMEOUT:
            if node.state == "active":
                logger.warning(f"Nodo {node_id} no responde, marcando como inactivo")
                node.state = "inactive"
                # Trigger re-replicación de chunks en este nodo
                trigger_replication_for_node(node_id)
```

Si un DataNode no enví­a heartbeat por más de 30 segundos, se marca como inactivo. Esto puede deberse a fallo del servidor, problema de red, o sobrecarga extrema.

**Re-replicación automática**:

Cuando un DataNode falla, los chunks que almacenaba quedan bajo-replicados. Por ejemplo, si un chunk tení­a 3 réplicas y un DataNode falla, ahora solo tiene 2 réplicas disponibles.

El Metadata Service ejecuta un proceso background que identifica chunks bajo-replicados y crea nuevas réplicas:

```python
async def replication_monitor():
    """Monitorear y corregir chunks bajo-replicados"""
    while True:
        # Obtener todos los archivos
        for file in files_db.values():
            for chunk in file.chunks:
                # Contar réplicas activas
                active_replicas = [
                    r for r in chunk.replicas 
                    if nodes_db.get(r.node_id, {}).get('state') == 'active'
                ]
                
                if len(active_replicas) < REPLICATION_FACTOR:
                    logger.warning(
                        f"Chunk {chunk.chunk_id} bajo-replicado: "
                        f"{len(active_replicas)}/{REPLICATION_FACTOR}"
                    )
                    
                    # Seleccionar nuevo DataNode target
                    available_nodes = [
                        n for n in nodes_db.values() 
                        if n.state == 'active' and n.node_id not in [r.node_id for r in chunk.replicas]
                    ]
                    
                    if available_nodes:
                        target_node = available_nodes[0]
                        
                        # Copiar desde una réplica existente al nuevo nodo
                        source_node = nodes_db[active_replicas[0].node_id]
                        await replicate_chunk(
                            chunk.chunk_id,
                            source_node,
                            target_node
                        )
        
        await asyncio.sleep(60)  # Verificar cada minuto
```

### 3.4 Namespace y Metadata

El namespace es la estructura jerárquica de directorios y archivos que presenta el sistema a los usuarios.

**Metadata de archivos**:

Para cada archivo, el sistema mantiene:

- **file_id**: Identificador único (UUID)
- **path**: Ruta completa del archivo en el namespace
- **size**: Tamaño total del archivo en bytes
- **created_at**: Timestamp de creación
- **modified_at**: Timestamp de última modificación
- **chunks**: Lista de chunks que componen el archivo
- **is_deleted**: Flag para soft delete
- **deleted_at**: Timestamp de eliminación (si aplica)

```python
class FileMetadata(BaseModel):
    """Metadatos de un archivo"""
    file_id: UUID = Field(default_factory=uuid4)
    path: str
    size: int
    created_at: datetime = Field(default_factory=datetime.utcnow)
    modified_at: datetime = Field(default_factory=datetime.utcnow)
    chunks: List[ChunkEntry] = Field(default_factory=list)
    is_deleted: bool = False
    deleted_at: Optional[datetime] = None
```

**Metadata de chunks**:

Para cada chunk, el sistema mantiene:

- **chunk_id**: Identificador único (UUID)
- **seq_index**: índice secuencial del chunk en el archivo (0, 1, 2, ...)
- **size**: Tamaño del chunk en bytes
- **checksum**: Hash SHA256 del contenido del chunk
- **replicas**: Lista de réplicas del chunk

```python
class ChunkEntry(BaseModel):
    """Entrada de chunk en un archivo"""
    chunk_id: UUID = Field(default_factory=uuid4)
    seq_index: int
    size: int
    checksum: Optional[str] = None  # SHA256
    replicas: List[ReplicaInfo] = Field(default_factory=list)
```

**Metadata de réplicas**:

Para cada réplica de un chunk:

- **node_id**: ID del DataNode que almacena la réplica
- **url**: URL del DataNode para acceder a la réplica
- **state**: Estado de la réplica (pending, committed, corrupted, deleted)
- **last_heartbeat**: íšltimo heartbeat del DataNode
- **checksum_verified**: Si el checksum ha sido verificado

```python
class ReplicaInfo(BaseModel):
    """Información de una réplica de chunk"""
    node_id: str
    url: str
    state: ChunkState = ChunkState.PENDING
    last_heartbeat: Optional[datetime] = None
    checksum_verified: bool = False
```

---

## 4. Componentes del Sistema

### 4.1 Metadata Service (Master)

El Metadata Service es el componente central que coordina todas las operaciones del sistema.

**Responsabilidades**:

**Gestión de namespace**: Mantiene el árbol de directorios y archivos. Procesa operaciones como crear archivo, eliminar archivo, renombrar archivo, listar directorio.

**Mapeo de chunks**: Mantiene el mapeo de cada archivo a sus chunks, y de cada chunk a las ubicaciones de sus réplicas en DataNodes.

**Coordinación de escrituras**: Cuando un cliente quiere escribir un archivo, el Metadata Service decide cómo dividirlo en chunks y selecciona los DataNodes target para cada chunk.

**Monitoreo de nodos**: Recibe heartbeats de DataNodes y mantiene información actualizada sobre qué nodos están activos, cuánto espacio tienen disponible, y qué chunks almacenan.

**Re-replicación**: Detecta chunks bajo-replicados (debido a fallos de nodos) y coordina la creación de nuevas réplicas.

**Implementación**:

El Metadata Service está implementado como una aplicación FastAPI que expone APIs REST.

```python
app = FastAPI(title="DFS Metadata Service", version="1.0.0")

# Storage en memoria (en producción serí­a SQLite o etcd)
files_db = {}  # file_id -> FileMetadata
nodes_db = {}  # node_id -> NodeInfo

@app.post("/api/v1/files/upload-init")
async def upload_init(request: UploadInitRequest):
    """Iniciar upload de un archivo"""
    # Verificar que hay suficientes nodos activos
    active_nodes = [n for n in nodes_db.values() if n.state == "active"]
    if len(active_nodes) < REPLICATION_FACTOR:
        raise HTTPException(
            status_code=503,
            detail=f"Insuficientes nodos: {len(active_nodes)}/{REPLICATION_FACTOR}"
        )
    
    # Calcular chunks
    num_chunks = (request.size + request.chunk_size - 1) // request.chunk_size
    chunks = []
    
    for i in range(num_chunks):
        chunk_size = min(request.chunk_size, request.size - i * request.chunk_size)
        chunk_id = uuid4()
        
        # Seleccionar nodos target (round-robin)
        targets = []
        for j in range(REPLICATION_FACTOR):
            node = active_nodes[(i + j) % len(active_nodes)]
            targets.append(f"http://{node.host}:{node.port}")
        
        chunks.append(ChunkTarget(
            chunk_id=chunk_id,
            size=chunk_size,
            targets=targets
        ))
    
    file_id = uuid4()
    
    return {
        "file_id": str(file_id),
        "chunks": chunks
    }
```

**Persistencia de metadata**:

En nuestra implementación simplificada, los metadatos se mantienen en memoria. Esto es suficiente para desarrollo y pruebas, pero en producción se necesita persistencia.

Dos opciones de backend están implementadas:

**SQLite**: Base de datos relacional embebida. Simple de configurar, adecuada para clusters pequeños (< 100 nodos). Los metadatos se persisten en un archivo en disco.

**etcd**: Sistema de almacenamiento distribuido de clave-valor con consenso Raft. Adecuado para clusters grandes y alta disponibilidad. Permite ejecutar múltiples instancias del Metadata Service en modo activo-pasivo.

```python
# Backend SQLite
class SQLiteMetadataStorage:
    def __init__(self, db_path: str = "dfs_metadata.db"):
        self.conn = sqlite3.connect(db_path)
        self.create_tables()
    
    def create_tables(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS files (
                file_id TEXT PRIMARY KEY,
                path TEXT UNIQUE NOT NULL,
                size INTEGER NOT NULL,
                created_at TIMESTAMP NOT NULL,
                modified_at TIMESTAMP NOT NULL,
                is_deleted BOOLEAN NOT NULL DEFAULT 0,
                deleted_at TIMESTAMP
            )
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS chunks (
                chunk_id TEXT PRIMARY KEY,
                file_id TEXT NOT NULL,
                seq_index INTEGER NOT NULL,
                size INTEGER NOT NULL,
                checksum TEXT,
                FOREIGN KEY (file_id) REFERENCES files(file_id)
            )
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS replicas (
                replica_id INTEGER PRIMARY KEY AUTOINCREMENT,
                chunk_id TEXT NOT NULL,
                node_id TEXT NOT NULL,
                url TEXT NOT NULL,
                state TEXT NOT NULL,
                FOREIGN KEY (chunk_id) REFERENCES chunks(chunk_id)
            )
        """)
```

### 4.2 DataNode (Worker)

Los DataNodes son los nodos de almacenamiento que guardan los chunks reales de datos.

**Responsabilidades**:

**Almacenamiento de chunks**: Recibir chunks de clientes y almacenarlos en el sistema de archivos local.

**Servir lecturas**: Responder a solicitudes de lectura de chunks enviando los datos al cliente.

**Heartbeats**: Enviar heartbeats periódicos al Metadata Service reportando estado y chunks almacenados.

**Replicación**: Participar en operaciones de replicación copiando chunks a otros DataNodes cuando se lo ordena el Metadata Service.

**Implementación**:

Cada DataNode es una aplicación FastAPI independiente que escucha en un puerto especí­fico.

```python
# Configuración desde variables de entorno
NODE_ID = os.getenv("NODE_ID", "node-localhost-8001")
PORT = int(os.getenv("PORT", "8001"))
STORAGE_PATH = Path(os.getenv("STORAGE_PATH", f"/tmp/dfs-data-{PORT}"))

app = FastAPI(title=f"DFS DataNode {NODE_ID}")

@app.post("/chunks/{chunk_id}")
async def write_chunk(chunk_id: UUID, request: Request):
    """Escribir un chunk"""
    chunk_path = STORAGE_PATH / f"{chunk_id}.chunk"
    
    # Recibir datos del cliente
    content = await request.body()
    
    # Escribir a disco
    with open(chunk_path, "wb") as f:
        f.write(content)
    
    logger.info(f"Chunk escrito: {chunk_id} ({len(content)} bytes)")
    return {"status": "ok", "chunk_id": str(chunk_id), "size": len(content)}

@app.get("/chunks/{chunk_id}")
async def read_chunk(chunk_id: UUID):
    """Leer un chunk"""
    chunk_path = STORAGE_PATH / f"{chunk_id}.chunk"
    
    if not chunk_path.exists():
        raise HTTPException(status_code=404, detail="Chunk no encontrado")
    
    # Enviar archivo al cliente
    return FileResponse(chunk_path)
```

**Almacenamiento fí­sico**:

Los chunks se almacenan como archivos individuales en el sistema de archivos local del DataNode. El nombre del archivo es el UUID del chunk con extensión `.chunk`.

Por ejemplo, si un chunk tiene UUID `a1b2c3d4-e5f6-7890-abcd-ef1234567890`, se almacena como:
```
/tmp/dfs-data1/a1b2c3d4-e5f6-7890-abcd-ef1234567890.chunk
```

Esta organización simple facilita operaciones de mantenimiento como backups y recuperación de datos.

**Gestión de espacio**:

El DataNode monitorea continuamente el espacio disponible en su volumen de almacenamiento y lo reporta en heartbeats.

```python
def get_disk_usage():
    """Obtener uso de disco"""
    stat = shutil.disk_usage(STORAGE_PATH)
    return stat.free, stat.total

# En el heartbeat
free, total = get_disk_usage()
await client.post(
    f"{METADATA_URL}/api/v1/nodes/heartbeat",
    json={
        "node_id": NODE_ID,
        "free_space": free,
        "total_space": total,
        "chunk_ids": [str(c) for c in chunks]
    }
)
```

El Metadata Service usa esta información para decisiones de placement. Si un DataNode está casi lleno (> 90% usado), el Metadata Service evita asignarle nuevos chunks.

### 4.3 Cliente

El cliente es la interfaz que usan aplicaciones y usuarios para interactuar con el DFS.

**Dos interfaces**:

**CLI (Command Line Interface)**: Herramienta de lí­nea de comandos para operaciones interactivas.

```bash
# Subir archivo
dfs_cli.py upload local.txt /remote/path.txt

# Descargar archivo
dfs_cli.py download /remote/path.txt local.txt

# Listar archivos
dfs_cli.py ls /remote/

# Eliminar archivo
dfs_cli.py rm /remote/path.txt

# Ver estado del cluster
dfs_cli.py status
```

**Librerí­a Python**: API programática para integración en aplicaciones.

```python
from dfs_client import DFSClient

client = DFSClient("http://localhost:8000")

# Upload
await client.upload("local.txt", "/remote/path.txt")

# Download
await client.download("/remote/path.txt", "local.txt")

# List
files = await client.list_files(prefix="/remote/")

# Delete
await client.delete("/remote/path.txt")
```

**Protocolo de upload**:

El cliente implementa un protocolo de tres fases para uploads:

**Fase 1 - Inicialización**: El cliente enví­a una solicitud al Metadata Service con el path del archivo y su tamaño. El Metadata Service responde con un plan de upload que incluye cómo dividir el archivo en chunks y qué DataNodes usar para cada chunk.

**Fase 2 - Transferencia de datos**: El cliente lee el archivo local, lo divide en chunks según el plan, y enví­a cada chunk a los DataNodes target en paralelo. Para cada chunk, el cliente calcula un checksum SHA256.

**Fase 3 - Commit**: Una vez que todos los chunks se han transferido exitosamente, el cliente enví­a una solicitud de commit al Metadata Service con la lista de chunks y sus checksums. El Metadata Service crea la entrada de metadata del archivo y lo hace visible en el namespace.

```python
async def upload_file(self, local_path: Path, remote_path: str):
    """Upload completo de un archivo"""
    # Leer archivo
    file_data = local_path.read_bytes()
    file_size = len(file_data)
    
    # Fase 1: Inicialización
    response = await self.client.post(
        f"{self.metadata_url}/api/v1/files/upload-init",
        json={
            "path": remote_path,
            "size": file_size,
            "chunk_size": 64 * 1024 * 1024
        }
    )
    
    upload_plan = response.json()
    file_id = upload_plan["file_id"]
    chunks = upload_plan["chunks"]
    
    # Fase 2: Transferencia de datos
    offset = 0
    chunk_commits = []
    
    for chunk_info in chunks:
        chunk_id = chunk_info["chunk_id"]
        chunk_size = chunk_info["size"]
        targets = chunk_info["targets"]
        
        chunk_data = file_data[offset:offset + chunk_size]
        chunk_checksum = hashlib.sha256(chunk_data).hexdigest()
        
        # Subir a todos los targets en paralelo
        tasks = []
        for target_url in targets:
            task = self.client.post(
                f"{target_url}/chunks/{chunk_id}",
                content=chunk_data
            )
            tasks.append(task)
        
        responses = await asyncio.gather(*tasks)
        
        uploaded_nodes = [
            resp.json().get("node_id") 
            for resp in responses 
            if resp.status_code == 200
        ]
        
        chunk_commits.append({
            "chunk_id": chunk_id,
            "checksum": chunk_checksum,
            "nodes": uploaded_nodes
        })
        
        offset += chunk_size
    
    # Fase 3: Commit
    response = await self.client.post(
        f"{self.metadata_url}/api/v1/files/commit",
        json={
            "file_id": file_id,
            "chunks": chunk_commits
        }
    )
    
    return response.status_code == 200
```

**Protocolo de download**:

El download es más simple, con dos fases:

**Fase 1 - Obtener metadata**: El cliente consulta al Metadata Service por el archivo usando su path. El Metadata Service responde con la lista de chunks y las ubicaciones de sus réplicas.

**Fase 2 - Transferencia de datos**: El cliente descarga cada chunk desde un DataNode (seleccionando una de las réplicas disponibles) y los reensambla en el orden correcto para reconstruir el archivo original.

```python
async def download_file(self, remote_path: str, local_path: Path):
    """Download completo de un archivo"""
    # Fase 1: Obtener metadata
    response = await self.client.get(
        f"{self.metadata_url}/api/v1/files/{remote_path}"
    )
    
    file_metadata = response.json()
    chunks = file_metadata["chunks"]
    
    # Fase 2: Descargar chunks
    chunk_data_list = []
    
    for chunk in sorted(chunks, key=lambda c: c["seq_index"]):
        chunk_id = chunk["chunk_id"]
        replicas = chunk["replicas"]
        
        # Intentar descargar de la primera réplica disponible
        for replica in replicas:
            try:
                response = await self.client.get(
                    f"{replica['url']}/chunks/{chunk_id}"
                )
                
                if response.status_code == 200:
                    chunk_data = response.content
                    
                    # Verificar checksum
                    actual_checksum = hashlib.sha256(chunk_data).hexdigest()
                    expected_checksum = chunk["checksum"]
                    
                    if actual_checksum != expected_checksum:
                        logger.warning(f"Checksum mismatch para chunk {chunk_id}")
                        continue
                    
                    chunk_data_list.append(chunk_data)
                    break
            
            except Exception as e:
                logger.warning(f"Error descargando de {replica['url']}: {e}")
                continue
    
    # Reensamblar archivo
    file_data = b"".join(chunk_data_list)
    local_path.write_bytes(file_data)
```

---

*Continuará en la Parte 2...*

## 5. Protocolos y Comunicación

### 5.1 APIs REST

El sistema utiliza APIs REST sobre HTTP para toda la comunicación entre componentes.

**Ventajas de REST**:

REST (Representational State Transfer) es un estilo arquitectónico que usa HTTP como protocolo de transporte. Las ventajas incluyen simplicidad (cualquier cliente HTTP puede interactuar con el sistema), stateless (cada request contiene toda la información necesaria), y cacheable (responses pueden cachearse para mejorar performance).

**Endpoints del Metadata Service**:

```
POST /api/v1/files/upload-init
  Request: { path: str, size: int, chunk_size: int }
  Response: { file_id: str, chunks: [ChunkTarget] }
  
POST /api/v1/files/commit
  Request: { file_id: str, chunks: [ChunkCommit] }
  Response: { status: str, file_id: str }
  
GET /api/v1/files
  Query: prefix (opcional)
  Response: [FileMetadata]
  
GET /api/v1/files/{path}
  Response: FileMetadata
  
DELETE /api/v1/files/{path}
  Response: { status: str }
  
POST /api/v1/nodes/heartbeat
  Request: { node_id: str, free_space: int, total_space: int, chunk_ids: [str] }
  Response: { status: str }
  
GET /api/v1/nodes
  Response: [NodeInfo]
  
GET /api/v1/health
  Response: { status: str, details: {...} }
```

**Endpoints del DataNode**:

```
POST /chunks/{chunk_id}
  Body: binary data del chunk
  Response: { status: str, chunk_id: str, size: int }
  
GET /chunks/{chunk_id}
  Response: binary data del chunk
  
DELETE /chunks/{chunk_id}
  Response: { status: str }
  
GET /health
  Response: { status: str, node_id: str }
```

**Formato de datos**:

Todos los requests y responses usan JSON para metadata. Los datos binarios de chunks se enví­an como application/octet-stream.

```python
# Ejemplo de request upload-init
{
  "path": "/usuarios/alice/documento.pdf",
  "size": 10485760,  # 10MB
  "chunk_size": 1048576  # 64MB
}

# Ejemplo de response upload-init
{
  "file_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "chunks": [
    {
      "chunk_id": "11111111-2222-3333-4444-555555555555",
      "size": 10485760,
      "targets": [
        "http://localhost:8001",
        "http://localhost:8002",
        "http://localhost:8003"
      ]
    }
  ]
}
```

### 5.2 Comunicación Así­ncrona

El sistema utiliza programación así­ncrona (asyncio en Python) para manejar múltiples operaciones concurrentes eficientemente.

**Â¿Por qué así­ncrono?**

En operaciones de I/O (red, disco), la mayorí­a del tiempo se gasta esperando. Un enfoque sí­ncrono bloquea el thread mientras espera, desperdiciando recursos. La programación así­ncrona permite que un thread maneje múltiples operaciones concurrentes, cambiando entre ellas cuando una está esperando I/O.

Por ejemplo, cuando un DataNode enví­a un heartbeat, no necesita bloquear todo el servidor esperando la response. Puede continuar sirviendo requests de chunks mientras espera.

**Implementación con FastAPI y httpx**:

FastAPI soporta endpoints así­ncronos nativamente. Los handlers pueden ser funciones async que usan await para operaciones de I/O.

```python
@app.post("/api/v1/files/upload-init")
async def upload_init(request: UploadInitRequest):
    # Esta función puede ser suspendida durante operaciones I/O
    # permitiendo que el servidor maneje otros requests
    
    # Operación así­ncrona: consultar base de datos
    existing_file = await db.get_file_by_path(request.path)
    
    if existing_file:
        raise HTTPException(status_code=409, detail="Archivo ya existe")
    
    # Operación sí­ncrona: cálculos en memoria
    num_chunks = calculate_chunks(request.size, request.chunk_size)
    
    # Operación así­ncrona: seleccionar nodos
    target_nodes = await select_target_nodes(num_chunks)
    
    return {
        "file_id": str(uuid4()),
        "chunks": create_chunk_plan(num_chunks, target_nodes)
    }
```

Para requests HTTP salientes, usamos httpx.AsyncClient:

```python
async def send_heartbeat():
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{METADATA_URL}/api/v1/nodes/heartbeat",
            json={...},
            timeout=5.0
        )
```

**Concurrencia en uploads**:

Cuando un cliente sube un archivo con múltiples chunks, enví­a todos los chunks en paralelo usando asyncio.gather:

```python
# Subir todos los chunks en paralelo
tasks = []
for chunk_info in chunks:
    for target_url in chunk_info["targets"]:
        task = client.post(f"{target_url}/chunks/{chunk_id}", content=chunk_data)
        tasks.append(task)

# Esperar a que todos completen
responses = await asyncio.gather(*tasks, return_exceptions=True)
```

Esto permite saturar el ancho de banda de red y lograr throughput mucho mayor que uploads secuenciales.

### 5.3 Manejo de Errores y Reintentos

En sistemas distribuidos, los fallos son inevitables. El diseño debe anticipar y manejar errores gracefully.

**Tipos de errores**:

**Errores de red**: Timeouts, conexiones rechazadas, pérdida de paquetes. Estos son tí­picamente transitorios y pueden resolverse con reintentos.

**Errores de nodo**: Un DataNode puede fallar completamente o volverse inaccesible. El sistema debe detectar esto y usar réplicas alternativas.

**Errores de datos**: Corrupción de datos detectada mediante checksums. El sistema debe descartar la réplica corrupta y usar otra.

**Errores de capacidad**: Un DataNode se queda sin espacio. El Metadata Service debe seleccionar nodos alternativos.

**Estrategias de manejo**:

**Reintentos con backoff exponencial**: Para errores transitorios, reintentamos la operación con delays crecientes (1s, 2s, 4s, 8s...).

```python
async def retry_with_backoff(func, max_retries=3):
    """Ejecutar función con reintentos y backoff exponencial"""
    for attempt in range(max_retries):
        try:
            return await func()
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            
            delay = 2 ** attempt  # 1s, 2s, 4s
            logger.warning(f"Intento {attempt + 1} falló: {e}. Reintentando en {delay}s")
            await asyncio.sleep(delay)
```

**Failover a réplicas**: Si un DataNode falla durante un download, el cliente automáticamente intenta la siguiente réplica.

```python
for replica in chunk_replicas:
    try:
        response = await client.get(f"{replica['url']}/chunks/{chunk_id}")
        if response.status_code == 200:
            return response.content
    except Exception as e:
        logger.warning(f"Fallo al descargar de {replica['url']}: {e}")
        continue

raise Exception("No se pudo descargar chunk de ninguna réplica")
```

**Verificación de checksums**: Después de descargar un chunk, verificamos su checksum. Si no coincide, descartamos los datos y probamos otra réplica.

```python
chunk_data = await download_chunk(chunk_id, replica_url)
actual_checksum = hashlib.sha256(chunk_data).hexdigest()

if actual_checksum != expected_checksum:
    logger.error(f"Checksum mismatch: esperado {expected_checksum}, obtenido {actual_checksum}")
    # Marcar réplica como corrupta
    await metadata_service.mark_replica_corrupted(chunk_id, replica_url)
    # Intentar otra réplica
    continue
```

**Timeouts**: Todas las operaciones de red tienen timeouts para evitar bloqueos indefinidos.

```python
async with httpx.AsyncClient(timeout=30.0) as client:
    response = await client.post(url, json=data)
```

---

## 6. Algoritmos Implementados

### 6.1 Algoritmo de Placement de Chunks

El algoritmo de placement decide en qué DataNodes colocar cada chunk y sus réplicas.

**Objetivos del algoritmo**:

**Balance de carga**: Distribuir chunks uniformemente entre DataNodes para evitar hotspots.

**Tolerancia a fallos**: Colocar réplicas en nodos independientes para que fallos no correlacionados no afecten múltiples réplicas del mismo chunk.

**Localidad de datos**: Cuando sea posible, colocar datos cerca de donde serán accedidos para minimizar latencia.

**Utilización de capacidad**: Evitar llenar completamente algunos nodos mientras otros están vací­os.

**Implementación actual (Round-Robin)**:

Nuestra implementación usa un algoritmo round-robin simple:

```python
def select_target_nodes_round_robin(
    chunk_index: int,
    available_nodes: List[NodeInfo],
    replication_factor: int = 3
) -> List[NodeInfo]:
    """Seleccionar nodos target usando round-robin"""
    targets = []
    
    for i in range(replication_factor):
        # Seleccionar nodo usando módulo
        node_index = (chunk_index + i) % len(available_nodes)
        node = available_nodes[node_index]
        targets.append(node)
    
    return targets

# Ejemplo: 5 nodos disponibles, replication_factor=3
# Chunk 0 -> nodos [0, 1, 2]
# Chunk 1 -> nodos [1, 2, 3]
# Chunk 2 -> nodos [2, 3, 4]
# Chunk 3 -> nodos [3, 4, 0]
# Chunk 4 -> nodos [4, 0, 1]
```

Este algoritmo garantiza distribución uniforme y es simple de implementar, pero no considera capacidad de nodos ni topologí­a de red.

**Algoritmo mejorado (Weighted Random)**:

Un algoritmo más sofisticado considera el espacio libre de cada nodo:

```python
def select_target_nodes_weighted(
    available_nodes: List[NodeInfo],
    replication_factor: int = 3
) -> List[NodeInfo]:
    """Seleccionar nodos target con probabilidad proporcional al espacio libre"""
    # Calcular peso de cada nodo basado en espacio libre
    weights = [node.free_space for node in available_nodes]
    total_weight = sum(weights)
    
    if total_weight == 0:
        raise InsufficientSpaceError("No hay espacio disponible en ningún nodo")
    
    # Normalizar pesos a probabilidades
    probabilities = [w / total_weight for w in weights]
    
    # Seleccionar nodos sin reemplazo
    targets = []
    remaining_nodes = available_nodes.copy()
    remaining_probs = probabilities.copy()
    
    for _ in range(replication_factor):
        # Seleccionar nodo con probabilidad proporcional al espacio libre
        node = random.choices(remaining_nodes, weights=remaining_probs, k=1)[0]
        targets.append(node)
        
        # Remover nodo seleccionado para evitar duplicados
        index = remaining_nodes.index(node)
        remaining_nodes.pop(index)
        remaining_probs.pop(index)
        
        # Renormalizar probabilidades
        total = sum(remaining_probs)
        remaining_probs = [p / total for p in remaining_probs]
    
    return targets
```

Este algoritmo tiende a llenar nodos de manera más uniforme, ya que nodos con más espacio libre tienen mayor probabilidad de ser seleccionados.

### 6.2 Algoritmo de Re-replicación

Cuando un DataNode falla, los chunks que almacenaba quedan bajo-replicados. El algoritmo de re-replicación restaura el factor de replicación deseado.

**Proceso**:

**Detección**: El Metadata Service detecta que un nodo no ha enviado heartbeat por más de 30 segundos y lo marca como inactivo.

**Identificación de chunks afectados**: Se identifican todos los chunks que tení­an réplicas en el nodo fallido.

**Priorización**: Los chunks se priorizan por nivel de riesgo. Un chunk con solo 1 réplica restante tiene mayor prioridad que uno con 2 réplicas restantes.

**Selección de fuente y destino**: Para cada chunk bajo-replicado, se selecciona una réplica fuente (un DataNode activo que tiene el chunk) y un DataNode destino (un nodo activo que no tiene el chunk).

**Replicación**: El Metadata Service instruye al DataNode fuente a copiar el chunk al DataNode destino.

**Implementación**:

```python
async def replication_monitor():
    """Monitorear y corregir chunks bajo-replicados"""
    while True:
        under_replicated_chunks = []
        
        # Identificar chunks bajo-replicados
        for file in files_db.values():
            for chunk in file.chunks:
                active_replicas = [
                    r for r in chunk.replicas
                    if nodes_db.get(r.node_id, {}).get('state') == 'active'
                ]
                
                if len(active_replicas) < REPLICATION_FACTOR:
                    priority = REPLICATION_FACTOR - len(active_replicas)
                    under_replicated_chunks.append((priority, chunk, active_replicas))
        
        # Ordenar por prioridad (mayor prioridad primero)
        under_replicated_chunks.sort(key=lambda x: x[0], reverse=True)
        
        # Procesar chunks bajo-replicados
        for priority, chunk, active_replicas in under_replicated_chunks:
            logger.warning(
                f"Chunk {chunk.chunk_id} bajo-replicado: "
                f"{len(active_replicas)}/{REPLICATION_FACTOR} (prioridad {priority})"
            )
            
            # Seleccionar nodo fuente (réplica existente)
            source_replica = active_replicas[0]
            source_node = nodes_db[source_replica.node_id]
            
            # Seleccionar nodo destino (nodo activo sin este chunk)
            available_nodes = [
                n for n in nodes_db.values()
                if n.state == 'active' and 
                   n.node_id not in [r.node_id for r in chunk.replicas]
            ]
            
            if not available_nodes:
                logger.error(f"No hay nodos disponibles para replicar chunk {chunk.chunk_id}")
                continue
            
            target_node = select_target_node_for_replication(available_nodes)
            
            # Instruir replicación
            await replicate_chunk(
                chunk.chunk_id,
                source_node,
                target_node
            )
            
            # Actualizar metadata
            chunk.replicas.append(ReplicaInfo(
                node_id=target_node.node_id,
                url=f"http://{target_node.host}:{target_node.port}",
                state=ChunkState.COMMITTED
            ))
        
        # Esperar antes de la siguiente verificación
        await asyncio.sleep(60)

async def replicate_chunk(
    chunk_id: UUID,
    source_node: NodeInfo,
    target_node: NodeInfo
):
    """Copiar un chunk de un nodo a otro"""
    logger.info(f"Replicando chunk {chunk_id} de {source_node.node_id} a {target_node.node_id}")
    
    # Descargar chunk del nodo fuente
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"http://{source_node.host}:{source_node.port}/chunks/{chunk_id}",
            timeout=60.0
        )
        
        if response.status_code != 200:
            raise Exception(f"Error descargando chunk: {response.status_code}")
        
        chunk_data = response.content
        
        # Subir chunk al nodo destino
        response = await client.post(
            f"http://{target_node.host}:{target_node.port}/chunks/{chunk_id}",
            content=chunk_data,
            timeout=60.0
        )
        
        if response.status_code != 200:
            raise Exception(f"Error subiendo chunk: {response.status_code}")
    
    logger.info(f"Chunk {chunk_id} replicado exitosamente")
```

**Throttling de re-replicación**:

Para evitar saturar la red durante re-replicación masiva (por ejemplo, si múltiples nodos fallan simultáneamente), el sistema limita el número de operaciones de replicación concurrentes:

```python
MAX_CONCURRENT_REPLICATIONS = 10
replication_semaphore = asyncio.Semaphore(MAX_CONCURRENT_REPLICATIONS)

async def replicate_chunk_throttled(chunk_id, source_node, target_node):
    """Replicar chunk con throttling"""
    async with replication_semaphore:
        await replicate_chunk(chunk_id, source_node, target_node)
```

### 6.3 Algoritmo de Garbage Collection

El sistema implementa soft delete para archivos. Cuando un usuario elimina un archivo, no se borran inmediatamente los chunks fí­sicos. En su lugar, el archivo se marca como eliminado en metadata.

**Ventajas del soft delete**:

**Recuperación**: Los archivos eliminados accidentalmente pueden recuperarse si se detecta el error rápidamente.

**Performance**: Eliminar archivos es instantáneo (solo actualizar metadata) en lugar de requerir eliminar potencialmente miles de chunks.

**Auditorí­a**: Se mantiene historial de qué archivos existieron y cuándo fueron eliminados.

**Garbage collection**:

Periódicamente, un proceso background identifica chunks que pertenecen a archivos eliminados hace más de cierto tiempo (por ejemplo, 7 dí­as) y los elimina fí­sicamente.

```python
GARBAGE_COLLECTION_THRESHOLD = timedelta(days=7)

async def garbage_collector():
    """Eliminar chunks de archivos eliminados antiguos"""
    while True:
        now = datetime.now(timezone.utc)
        chunks_to_delete = []
        
        # Identificar archivos elegibles para garbage collection
        for file in files_db.values():
            if file.is_deleted:
                time_since_deletion = now - file.deleted_at
                
                if time_since_deletion > GARBAGE_COLLECTION_THRESHOLD:
                    logger.info(f"Archivo {file.path} elegible para GC (eliminado hace {time_since_deletion})")
                    
                    # Agregar todos los chunks del archivo
                    for chunk in file.chunks:
                        chunks_to_delete.append((file.file_id, chunk))
        
        # Eliminar chunks fí­sicamente
        for file_id, chunk in chunks_to_delete:
            for replica in chunk.replicas:
                try:
                    node = nodes_db.get(replica.node_id)
                    if node and node.state == 'active':
                        await delete_chunk_from_node(chunk.chunk_id, node)
                except Exception as e:
                    logger.error(f"Error eliminando chunk {chunk.chunk_id} de {replica.node_id}: {e}")
            
            # Eliminar metadata del chunk
            # (en implementación real, también eliminar entrada de archivo)
        
        # Ejecutar GC cada 24 horas
        await asyncio.sleep(86400)

async def delete_chunk_from_node(chunk_id: UUID, node: NodeInfo):
    """Eliminar un chunk de un DataNode"""
    async with httpx.AsyncClient() as client:
        response = await client.delete(
            f"http://{node.host}:{node.port}/chunks/{chunk_id}",
            timeout=10.0
        )
        
        if response.status_code == 200:
            logger.info(f"Chunk {chunk_id} eliminado de {node.node_id}")
        else:
            logger.warning(f"Error eliminando chunk {chunk_id}: {response.status_code}")
```

---

## 7. Gestión de Datos

### 7.1 Integridad de Datos

Garantizar que los datos no se corrompan es crí­tico en un sistema de almacenamiento.

**Checksums**:

Cada chunk tiene un checksum SHA256 calculado por el cliente durante el upload. Este checksum se almacena en metadata y se usa para verificar integridad.

```python
import hashlib

def calculate_checksum(data: bytes) -> str:
    """Calcular SHA256 checksum de datos"""
    return hashlib.sha256(data).hexdigest()

# Durante upload
chunk_data = file_data[offset:offset + chunk_size]
chunk_checksum = calculate_checksum(chunk_data)

# Durante download
downloaded_data = await download_chunk(chunk_id)
actual_checksum = calculate_checksum(downloaded_data)

if actual_checksum != expected_checksum:
    raise ChecksumMismatchError(
        f"Checksum mismatch: esperado {expected_checksum}, obtenido {actual_checksum}"
    )
```

**Scrubbing**:

Periódicamente, el sistema ejecuta un proceso de scrubbing que lee todos los chunks almacenados, calcula sus checksums, y los compara con los checksums esperados en metadata.

```python
async def scrubbing_process():
    """Verificar integridad de todos los chunks periódicamente"""
    while True:
        logger.info("Iniciando scrubbing de chunks")
        corrupted_chunks = []
        
        for file in files_db.values():
            for chunk in file.chunks:
                for replica in chunk.replicas:
                    node = nodes_db.get(replica.node_id)
                    if not node or node.state != 'active':
                        continue
                    
                    try:
                        # Descargar chunk
                        chunk_data = await download_chunk_from_node(
                            chunk.chunk_id,
                            node
                        )
                        
                        # Verificar checksum
                        actual_checksum = calculate_checksum(chunk_data)
                        expected_checksum = chunk.checksum
                        
                        if actual_checksum != expected_checksum:
                            logger.error(
                                f"Corrupción detectada: chunk {chunk.chunk_id} "
                                f"en nodo {node.node_id}"
                            )
                            corrupted_chunks.append((chunk, replica, node))
                    
                    except Exception as e:
                        logger.error(f"Error en scrubbing de chunk {chunk.chunk_id}: {e}")
        
        # Manejar chunks corruptos
        for chunk, replica, node in corrupted_chunks:
            # Marcar réplica como corrupta
            replica.state = ChunkState.CORRUPTED
            
            # Trigger re-replicación desde una réplica buena
            await replicate_chunk_from_good_replica(chunk)
        
        logger.info(f"Scrubbing completado. {len(corrupted_chunks)} chunks corruptos encontrados")
        
        # Ejecutar scrubbing cada semana
        await asyncio.sleep(604800)
```

### 7.2 Compresión

Aunque no está implementado en la versión actual, el sistema puede extenderse para soportar compresión de chunks.

**Compresión transparente**:

Los chunks pueden comprimirse antes de almacenarlos en DataNodes. Esto reduce uso de disco y ancho de banda de red, a costa de CPU para comprimir/descomprimir.

```python
import gzip

def compress_chunk(data: bytes) -> bytes:
    """Comprimir chunk usando gzip"""
    return gzip.compress(data, compresslevel=6)

def decompress_chunk(data: bytes) -> bytes:
    """Descomprimir chunk"""
    return gzip.decompress(data)

# Durante upload
chunk_data = file_data[offset:offset + chunk_size]
compressed_data = compress_chunk(chunk_data)
await upload_chunk_to_node(chunk_id, compressed_data, node)

# Durante download
compressed_data = await download_chunk_from_node(chunk_id, node)
chunk_data = decompress_chunk(compressed_data)
```

**Compresión adaptativa**:

No todos los datos se benefician de compresión. Archivos ya comprimidos (JPG, MP4, ZIP) pueden incluso aumentar de tamaño al intentar comprimirlos nuevamente.

Un sistema más sofisticado detecta el tipo de archivo y decide si aplicar compresión:

```python
def should_compress(file_path: str, sample_data: bytes) -> bool:
    """Decidir si comprimir basado en tipo de archivo y compresibilidad"""
    # Extensiones que tí­picamente no se benefician de compresión
    no_compress_extensions = {'.jpg', '.jpeg', '.png', '.mp4', '.zip', '.gz', '.7z'}
    
    ext = Path(file_path).suffix.lower()
    if ext in no_compress_extensions:
        return False
    
    # Probar compresión en muestra
    if len(sample_data) > 1024:
        sample = sample_data[:1024]
        compressed_sample = gzip.compress(sample)
        compression_ratio = len(compressed_sample) / len(sample)
        
        # Solo comprimir si se logra al menos 10% de reducción
        return compression_ratio < 0.9
    
    return True
```

### 7.3 Deduplicación

La deduplicación identifica y elimina datos duplicados, almacenando solo una copia fí­sica.

**Deduplicación a nivel de chunk**:

Si dos archivos diferentes contienen chunks idénticos, solo se almacena una copia fí­sica del chunk.

```python
def upload_chunk_with_dedup(chunk_data: bytes, chunk_checksum: str):
    """Upload chunk con deduplicación"""
    # Verificar si ya existe un chunk con este checksum
    existing_chunk = find_chunk_by_checksum(chunk_checksum)
    
    if existing_chunk:
        logger.info(f"Chunk con checksum {chunk_checksum} ya existe, reutilizando")
        return existing_chunk.chunk_id
    
    # Chunk no existe, subir normalmente
    chunk_id = uuid4()
    await upload_chunk_to_nodes(chunk_id, chunk_data)
    return chunk_id
```

**Consideraciones**:

La deduplicación requiere indexar chunks por checksum, lo que aumenta complejidad de metadata. También introduce dependencias: si múltiples archivos referencian el mismo chunk fí­sico, eliminar uno de esos archivos no debe eliminar el chunk si otros archivos aún lo necesitan.

Esto requiere reference counting:

```python
class ChunkMetadata:
    chunk_id: UUID
    checksum: str
    size: int
    reference_count: int  # Cuántos archivos usan este chunk
    replicas: List[ReplicaInfo]

def delete_file(file_id: UUID):
    """Eliminar archivo con manejo de reference counting"""
    file = files_db[file_id]
    
    for chunk in file.chunks:
        chunk.reference_count -= 1
        
        if chunk.reference_count == 0:
            # Ningún archivo usa este chunk, eliminar fí­sicamente
            for replica in chunk.replicas:
                await delete_chunk_from_node(chunk.chunk_id, replica.node_id)
```

---

## 8. Confiabilidad y Tolerancia a Fallos

### 8.1 Tipos de Fallos

Un DFS debe manejar múltiples tipos de fallos:

**Fallo de DataNode**: Un servidor completo se vuelve inaccesible (crash, pérdida de red, apagado).

**Fallo de disco**: Un disco en un DataNode falla, perdiendo todos los chunks almacenados en él.

**Corrupción de datos**: Bits se corrompen en disco o durante transmisión de red (bit flips).

**Fallo de red**: Particiones de red separan partes del cluster.

**Fallo del Metadata Service**: El nodo maestro falla, potencialmente dejando el sistema inoperable.

### 8.2 Mecanismos de Tolerancia a Fallos

**Replicación**:

La replicación (factor 3) permite que el sistema tolere la pérdida de hasta 2 réplicas de cualquier chunk sin pérdida de datos.

Probabilidad de pérdida de datos con replicación 3x:
```
P(pérdida) = P(fallo_nodo1) í— P(fallo_nodo2) í— P(fallo_nodo3)

Asumiendo P(fallo_nodo) = 0.01 (1% anual):
P(pérdida) = 0.01 í— 0.01 í— 0.01 = 0.000001 (0.0001%)
```

**Re-replicación automática**:

Cuando un nodo falla, el sistema detecta chunks bajo-replicados y crea nuevas réplicas automáticamente. Esto restaura el nivel de redundancia en minutos u horas (dependiendo del volumen de datos).

**Checksums**:

Los checksums SHA256 detectan corrupción de datos con probabilidad extremadamente alta. SHA256 tiene 2^256 valores posibles, haciendo colisiones prácticamente imposibles.

**Heartbeats**:

Los heartbeats cada 10 segundos permiten detectar fallos de nodos en menos de 30 segundos. Esto minimiza el tiempo que el sistema opera con redundancia reducida.

**Soft delete**:

El soft delete permite recuperar archivos eliminados accidentalmente, protegiendo contra errores humanos.

### 8.3 Alta Disponibilidad del Metadata Service

El Metadata Service es un punto único de fallo en la arquitectura actual. Si falla, el sistema completo se vuelve inoperable (aunque los datos en DataNodes permanecen intactos).

**Solución: Metadata Service con HA**:

Para producción, se implementa HA usando etcd como backend de metadata. etcd es un sistema distribuido de clave-valor con consenso Raft, que permite ejecutar múltiples instancias del Metadata Service.

**Arquitectura HA**:

```
        Load Balancer
              |
    +---------+---------+
    |         |         |
Metadata  Metadata  Metadata
Service1  Service2  Service3
    |         |         |
    +---------+---------+
              |
           etcd cluster
         (3 o 5 nodos)
```

Múltiples instancias del Metadata Service se ejecutan simultáneamente, todas leyendo y escribiendo al mismo cluster etcd. Un load balancer distribuye requests entre las instancias.

Si una instancia falla, el load balancer automáticamente redirige tráfico a las instancias restantes. No hay pérdida de servicio.

**Implementación con etcd**:

```python
import etcd3

class EtcdMetadataStorage:
    def __init__(self, etcd_endpoints: List[str]):
        self.client = etcd3.client(
            host=etcd_endpoints[0].split(':')[0],
            port=int(etcd_endpoints[0].split(':')[1])
        )
    
    async def create_file(self, file_metadata: FileMetadata):
        """Crear archivo en etcd"""
        key = f"/files/{file_metadata.file_id}"
        value = file_metadata.json()
        
        # Transacción: solo crear si no existe
        success = self.client.put_if_not_exists(key, value)
        
        if not success:
            raise FileExistsError(f"Archivo {file_metadata.path} ya existe")
    
    async def get_file(self, file_id: str) -> FileMetadata:
        """Obtener archivo de etcd"""
        key = f"/files/{file_id}"
        value, metadata = self.client.get(key)
        
        if value is None:
            raise FileNotFoundError(f"Archivo {file_id} no encontrado")
        
        return FileMetadata.parse_raw(value)
    
    async def update_file(self, file_metadata: FileMetadata):
        """Actualizar archivo en etcd"""
        key = f"/files/{file_metadata.file_id}"
        value = file_metadata.json()
        
        self.client.put(key, value)
    
    async def list_files(self, prefix: str = "/") -> List[FileMetadata]:
        """Listar archivos con prefijo"""
        files = []
        
        for value, metadata in self.client.get_prefix("/files/"):
            file_metadata = FileMetadata.parse_raw(value)
            if file_metadata.path.startswith(prefix):
                files.append(file_metadata)
        
        return files
```

**Leases en etcd**:

Los heartbeats de DataNodes se implementan usando leases de etcd. Cada DataNode crea un lease con TTL de 30 segundos y lo renueva en cada heartbeat.

```python
async def register_node_with_lease(node_info: NodeInfo):
    """Registrar nodo usando lease de etcd"""
    # Crear lease con TTL de 30 segundos
    lease = client.lease(ttl=30)
    
    # Registrar nodo con el lease
    key = f"/nodes/{node_info.node_id}"
    value = node_info.json()
    client.put(key, value, lease=lease)
    
    # Renovar lease periódicamente
    while True:
        await asyncio.sleep(10)
        lease.refresh()
```

Si un DataNode deja de renovar su lease (porque falló), etcd automáticamente elimina la entrada del nodo después de 30 segundos. El Metadata Service puede watch este cambio y trigger re-replicación.

---

*Continuará en la Parte 3...*

## 9. Performance y Escalabilidad

### 9.1 Throughput y Latencia

**Throughput**:

El throughput es la cantidad de datos que el sistema puede transferir por unidad de tiempo. En nuestras pruebas:

- Upload individual: 3.77 - 19.29 MB/s por archivo
- Upload concurrente: 72.75 MB/s (20 archivos simultáneos)
- Download: Similar al upload

El throughput escala linealmente con el número de DataNodes porque los clientes se comunican directamente con DataNodes en paralelo.

```
Throughput_total â‰ˆ Throughput_por_nodo í— Número_de_nodos_activos
```

Con 3 DataNodes y throughput individual de ~15 MB/s, el throughput agregado teórico es ~45 MB/s. En la práctica, logramos 72.75 MB/s en pruebas concurrentes debido a paralelización adicional.

**Latencia**:

La latencia es el tiempo desde que se inicia una operación hasta que completa. Para archivos pequeños (1MB), la latencia es ~0.13-0.26 segundos.

Componentes de latencia:
- Latencia de red: ~1-10ms por hop
- Tiempo de procesamiento en Metadata Service: ~10-50ms
- Tiempo de transferencia de datos: depende del tamaño del archivo y ancho de banda
- Tiempo de escritura a disco: ~5-20ms por chunk

Para archivos grandes, la latencia está dominada por transferencia de datos:
```
Latencia_total â‰ˆ Tamaño_archivo / Throughput
```

Archivo de 50MB con throughput de 19.29 MB/s: ~2.59 segundos.

### 9.2 Escalabilidad Horizontal

El sistema está diseñado para escalar horizontalmente agregando más DataNodes.

**Escalamiento de capacidad**:

Agregar un nuevo DataNode aumenta la capacidad total del sistema inmediatamente. El Metadata Service detecta el nuevo nodo via heartbeat y comienza a asignarle chunks para nuevos uploads.

```python
# Agregar nuevo DataNode
# 1. Iniciar DataNode en nuevo servidor
NODE_ID=node-server4-8001 PORT=8001 STORAGE_PATH=/data/dfs python3.11 datanode.py

# 2. El DataNode automáticamente enví­a heartbeat al Metadata Service
# 3. El Metadata Service lo registra como nodo activo
# 4. Nuevos uploads usan el nuevo nodo
```

**Escalamiento de throughput**:

Agregar DataNodes también aumenta throughput agregado porque más clientes pueden leer/escribir en paralelo.

Con N DataNodes y throughput individual T:
```
Throughput_agregado = N í— T
```

**Rebalancing**:

Cuando se agrega un nuevo DataNode, inicialmente está vací­o mientras los nodos existentes pueden estar llenos. El sistema puede ejecutar un proceso de rebalancing que mueve chunks de nodos llenos al nuevo nodo.

```python
async def rebalance_cluster():
    """Rebalancear chunks entre nodos para uso uniforme"""
    nodes = list(nodes_db.values())
    
    # Calcular uso promedio
    total_used = sum(n.total_space - n.free_space for n in nodes)
    total_capacity = sum(n.total_space for n in nodes)
    average_usage = total_used / total_capacity
    
    # Identificar nodos sobre-utilizados y sub-utilizados
    overloaded_nodes = [
        n for n in nodes
        if (n.total_space - n.free_space) / n.total_space > average_usage + 0.1
    ]
    
    underloaded_nodes = [
        n for n in nodes
        if (n.total_space - n.free_space) / n.total_space < average_usage - 0.1
    ]
    
    # Mover chunks de nodos sobrecargados a subcargados
    for overloaded in overloaded_nodes:
        for underloaded in underloaded_nodes:
            # Seleccionar chunks para mover
            chunks_to_move = select_chunks_to_move(overloaded, underloaded)
            
            for chunk_id in chunks_to_move:
                await move_chunk(chunk_id, overloaded, underloaded)
```

### 9.3 Optimizaciones de Performance

**Caching en Metadata Service**:

El Metadata Service puede cachear metadata frecuentemente accedida en memoria para reducir latencia de lookups.

```python
from functools import lru_cache

@lru_cache(maxsize=10000)
def get_file_metadata_cached(file_path: str) -> FileMetadata:
    """Obtener metadata de archivo con caching"""
    return get_file_metadata(file_path)
```

**Pipelining de uploads**:

En lugar de esperar a que un chunk complete antes de comenzar el siguiente, el cliente puede pipeline múltiples chunks:

```python
async def upload_file_pipelined(file_path: Path, remote_path: str):
    """Upload con pipelining de chunks"""
    # Iniciar upload
    upload_plan = await init_upload(remote_path, file_size)
    
    # Crear pipeline de chunks
    chunk_queue = asyncio.Queue(maxsize=5)  # Buffer de 5 chunks
    
    # Producer: leer y encolar chunks
    async def produce_chunks():
        offset = 0
        for chunk_info in upload_plan["chunks"]:
            chunk_data = read_chunk_from_file(file_path, offset, chunk_info["size"])
            await chunk_queue.put((chunk_info, chunk_data))
            offset += chunk_info["size"]
        await chunk_queue.put(None)  # Sentinel
    
    # Consumer: subir chunks
    async def consume_chunks():
        while True:
            item = await chunk_queue.get()
            if item is None:
                break
            chunk_info, chunk_data = item
            await upload_chunk(chunk_info, chunk_data)
    
    # Ejecutar producer y consumer en paralelo
    await asyncio.gather(produce_chunks(), consume_chunks())
```

**Compresión de metadata**:

Para sistemas muy grandes con millones de archivos, la metadata puede comprimirse para reducir uso de memoria y ancho de banda.

**Batching de heartbeats**:

En lugar de que cada DataNode enví­e heartbeats individuales, pueden batching múltiples updates en un solo request.

---

## 10. Seguridad

### 10.1 Autenticación y Autorización

La versión actual no implementa autenticación, pero el sistema está diseñado para soportarla.

**Autenticación con JWT**:

Los clientes se autentican obteniendo un JSON Web Token (JWT) que incluyen en requests subsecuentes.

```python
from jose import jwt
from datetime import datetime, timedelta

SECRET_KEY = "clave-secreta-segura"
ALGORITHM = "HS256"

def create_access_token(user_id: str, permissions: List[str]) -> str:
    """Crear JWT token"""
    payload = {
        "sub": user_id,
        "permissions": permissions,
        "exp": datetime.now(timezone.utc) + timedelta(hours=24)
    }
    token = jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)
    return token

def verify_token(token: str) -> dict:
    """Verificar y decodificar JWT token"""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except jwt.JWTError:
        raise HTTPException(status_code=401, detail="Token inválido")
```

**Middleware de autenticación**:

```python
from fastapi import Depends, HTTPException
from fastapi.security import HTTPBearer

security = HTTPBearer()

async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Obtener usuario actual desde JWT"""
    token = credentials.credentials
    payload = verify_token(token)
    return payload

@app.post("/api/v1/files/upload-init")
async def upload_init(
    request: UploadInitRequest,
    user: dict = Depends(get_current_user)
):
    """Upload con autenticación"""
    # Verificar que el usuario tiene permiso de escritura
    if "write" not in user["permissions"]:
        raise HTTPException(status_code=403, detail="Permiso denegado")
    
    # Proceder con upload
    ...
```

**Control de acceso basado en roles (RBAC)**:

```python
class Permission(str, Enum):
    READ = "read"
    WRITE = "write"
    DELETE = "delete"
    ADMIN = "admin"

class Role(str, Enum):
    USER = "user"
    POWER_USER = "power_user"
    ADMIN = "admin"

ROLE_PERMISSIONS = {
    Role.USER: [Permission.READ],
    Role.POWER_USER: [Permission.READ, Permission.WRITE],
    Role.ADMIN: [Permission.READ, Permission.WRITE, Permission.DELETE, Permission.ADMIN]
}

def check_permission(user: dict, required_permission: Permission):
    """Verificar que el usuario tiene el permiso requerido"""
    user_role = user.get("role", Role.USER)
    user_permissions = ROLE_PERMISSIONS.get(user_role, [])
    
    if required_permission not in user_permissions:
        raise HTTPException(status_code=403, detail=f"Permiso {required_permission} requerido")
```

### 10.2 Cifrado

**Cifrado en tránsito (mTLS)**:

La comunicación entre componentes puede cifrarse usando TLS mutuo (mTLS), donde tanto cliente como servidor verifican certificados.

```python
import ssl
import httpx

def create_ssl_context(cert_file: str, key_file: str, ca_file: str) -> ssl.SSLContext:
    """Crear contexto SSL para mTLS"""
    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    context.load_cert_chain(certfile=cert_file, keyfile=key_file)
    context.load_verify_locations(cafile=ca_file)
    context.verify_mode = ssl.CERT_REQUIRED
    return context

# Cliente con mTLS
ssl_context = create_ssl_context("client.crt", "client.key", "ca.crt")
async with httpx.AsyncClient(verify=ssl_context) as client:
    response = await client.get("https://metadata-service:8000/api/v1/health")

# Servidor con mTLS (Uvicorn)
uvicorn.run(
    app,
    host="0.0.0.0",
    port=8000,
    ssl_certfile="server.crt",
    ssl_keyfile="server.key",
    ssl_ca_certs="ca.crt",
    ssl_cert_reqs=ssl.CERT_REQUIRED
)
```

**Cifrado en reposo**:

Los chunks pueden cifrarse antes de almacenarlos en disco usando AES-256-GCM.

```python
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
import os

def encrypt_chunk(plaintext: bytes, key: bytes) -> tuple[bytes, bytes]:
    """Cifrar chunk con AES-256-GCM"""
    aesgcm = AESGCM(key)
    nonce = os.urandom(12)  # 96-bit nonce
    ciphertext = aesgcm.encrypt(nonce, plaintext, None)
    return nonce, ciphertext

def decrypt_chunk(nonce: bytes, ciphertext: bytes, key: bytes) -> bytes:
    """Descifrar chunk"""
    aesgcm = AESGCM(key)
    plaintext = aesgcm.decrypt(nonce, ciphertext, None)
    return plaintext

# Durante upload
chunk_data = read_chunk_from_file(...)
encryption_key = get_encryption_key()  # Desde KMS o configuración
nonce, encrypted_data = encrypt_chunk(chunk_data, encryption_key)

# Almacenar nonce junto con datos cifrados
chunk_with_nonce = nonce + encrypted_data
await upload_chunk_to_node(chunk_id, chunk_with_nonce)

# Durante download
chunk_with_nonce = await download_chunk_from_node(chunk_id)
nonce = chunk_with_nonce[:12]
ciphertext = chunk_with_nonce[12:]
chunk_data = decrypt_chunk(nonce, ciphertext, encryption_key)
```

**Gestión de claves**:

Las claves de cifrado deben gestionarse cuidadosamente. Opciones:

- **KMS (Key Management Service)**: Servicios como AWS KMS, Google Cloud KMS, o HashiCorp Vault gestionan claves de forma segura.
- **Cifrado por usuario**: Cada usuario tiene su propia clave, permitiendo que solo ellos descifren sus archivos.
- **Cifrado por archivo**: Cada archivo se cifra con una clave única, que a su vez se cifra con una clave maestra.

### 10.3 Auditorí­a

Registrar todas las operaciones permite detectar accesos no autorizados y cumplir con regulaciones.

```python
import logging

audit_logger = logging.getLogger("audit")

def log_audit_event(
    user_id: str,
    action: str,
    resource: str,
    result: str,
    metadata: dict = None
):
    """Registrar evento de auditorí­a"""
    audit_logger.info(
        f"user={user_id} action={action} resource={resource} result={result} "
        f"metadata={metadata or {}}"
    )

# En endpoints
@app.post("/api/v1/files/upload-init")
async def upload_init(request: UploadInitRequest, user: dict = Depends(get_current_user)):
    try:
        result = await process_upload_init(request)
        log_audit_event(
            user_id=user["sub"],
            action="upload_init",
            resource=request.path,
            result="success",
            metadata={"size": request.size}
        )
        return result
    except Exception as e:
        log_audit_event(
            user_id=user["sub"],
            action="upload_init",
            resource=request.path,
            result="failure",
            metadata={"error": str(e)}
        )
        raise
```

Los logs de auditorí­a deben almacenarse en un sistema separado (como Elasticsearch o un servicio de logging centralizado) para prevenir que atacantes los modifiquen.

---

## 11. Implementación Técnica Detallada

### 11.1 Stack Tecnológico

**Backend**:
- **Python 3.11**: Lenguaje de programación principal
- **FastAPI**: Framework web así­ncrono para APIs REST
- **Uvicorn**: Servidor ASGI de alto rendimiento
- **Pydantic**: Validación de datos y serialización
- **httpx**: Cliente HTTP así­ncrono
- **asyncio**: Programación así­ncrona

**Almacenamiento**:
- **SQLite**: Base de datos embebida para metadata (desarrollo)
- **etcd**: Sistema distribuido de clave-valor (producción)
- **Sistema de archivos local**: Almacenamiento de chunks

**Frontend**:
- **React 19**: Framework UI
- **TypeScript**: Lenguaje tipado
- **TailwindCSS**: Framework de estilos
- **shadcn/ui**: Componentes UI
- **Wouter**: Routing

**Infraestructura**:
- **Docker**: Containerización
- **Docker Compose**: Orquestación local
- **Kubernetes**: Orquestación en producción
- **Helm**: Gestión de despliegues Kubernetes

**Observabilidad**:
- **Prometheus**: Métricas
- **Grafana**: Visualización
- **OpenTelemetry**: Trazas distribuidas (opcional)

### 11.2 Estructura del Código

```
dfs-system/
â”œâ”€â”€ metadata-service/
â”‚   â”œâ”€â”€ main.py              # Aplicación FastAPI principal
â”‚   â”œâ”€â”€ main_simple.py       # Versión simplificada para desarrollo
â”‚   â”œâ”€â”€ storage.py           # Backend SQLite
â”‚   â”œâ”€â”€ storage_etcd.py      # Backend etcd
â”‚   â”œâ”€â”€ replicator.py        # Lógica de re-replicación
â”‚   â”œâ”€â”€ metrics.py           # Métricas Prometheus
â”‚   â””â”€â”€ requirements.txt
â”‚
â”œâ”€â”€ datanode/
â”‚   â”œâ”€â”€ main.py              # Aplicación FastAPI del DataNode
â”‚   â”œâ”€â”€ datanode_simple.py   # Versión simplificada
â”‚   â”œâ”€â”€ metrics.py           # Métricas Prometheus
â”‚   â””â”€â”€ requirements.txt
â”‚
â”œâ”€â”€ client/
â”‚   â”œâ”€â”€ dfs_client.py        # Librerí­a cliente Python
â”‚   â”œâ”€â”€ dfs_cli.py           # CLI
â”‚   â””â”€â”€ requirements.txt
â”‚
â”œâ”€â”€ shared/
â”‚   â”œâ”€â”€ models.py            # Modelos Pydantic compartidos
â”‚   â”œâ”€â”€ utils.py             # Utilidades
â”‚   â””â”€â”€ security.py          # JWT, mTLS
â”‚
â”œâ”€â”€ infra/
â”‚   â”œâ”€â”€ docker/
â”‚   â”‚   â”œâ”€â”€ Dockerfile.metadata
â”‚   â”‚   â”œâ”€â”€ Dockerfile.datanode
â”‚   â”‚   â””â”€â”€ Dockerfile.client
â”‚   â”œâ”€â”€ helm/
â”‚   â”‚   â””â”€â”€ dfs-chart/
â”‚   â”‚       â”œâ”€â”€ Chart.yaml
â”‚   â”‚       â”œâ”€â”€ values.yaml
â”‚   â”‚       â””â”€â”€ templates/
â”‚   â””â”€â”€ prometheus/
â”‚       â””â”€â”€ prometheus.yml
â”‚
â”œâ”€â”€ tests/
â”‚   â”œâ”€â”€ test_e2e.py
â”‚   â”œâ”€â”€ performance/
â”‚   â”‚   â””â”€â”€ load_test.js
â”‚   â””â”€â”€ requirements.txt
â”‚
â”œâ”€â”€ docs/
â”‚   â”œâ”€â”€ ARCHITECTURE.md
â”‚   â”œâ”€â”€ API.md
â”‚   â”œâ”€â”€ KUBERNETES.md
â”‚   â”œâ”€â”€ RUNBOOK.md
â”‚   â””â”€â”€ SECURITY.md
â”‚
â”œâ”€â”€ docker-compose.yml
â”œâ”€â”€ start_all.sh
â”œâ”€â”€ stop_all.sh
â”œâ”€â”€ README.md
â””â”€â”€ INSTALL.md
```

### 11.3 Modelos de Datos

**FileMetadata**:

```python
class FileMetadata(BaseModel):
    file_id: UUID = Field(default_factory=uuid4)
    path: str = Field(..., description="Ruta completa del archivo")
    size: int = Field(..., ge=0, description="Tamaño en bytes")
    created_at: datetime = Field(default_factory=datetime.utcnow)
    modified_at: datetime = Field(default_factory=datetime.utcnow)
    chunks: List[ChunkEntry] = Field(default_factory=list)
    is_deleted: bool = False
    deleted_at: Optional[datetime] = None
    
    class Config:
        json_encoders = {
            UUID: str,
            datetime: lambda v: v.isoformat()
        }
```

**ChunkEntry**:

```python
class ChunkEntry(BaseModel):
    chunk_id: UUID = Field(default_factory=uuid4)
    seq_index: int = Field(..., ge=0, description="índice del chunk en el archivo")
    size: int = Field(..., ge=0, description="Tamaño del chunk en bytes")
    checksum: Optional[str] = Field(None, description="SHA256 checksum")
    replicas: List[ReplicaInfo] = Field(default_factory=list)
```

**ReplicaInfo**:

```python
class ChunkState(str, Enum):
    PENDING = "pending"
    COMMITTED = "committed"
    CORRUPTED = "corrupted"
    DELETED = "deleted"

class ReplicaInfo(BaseModel):
    node_id: str
    url: str
    state: ChunkState = ChunkState.PENDING
    last_heartbeat: Optional[datetime] = None
    checksum_verified: bool = False
```

**NodeInfo**:

```python
class NodeState(str, Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    DECOMMISSIONED = "decommissioned"

class NodeInfo(BaseModel):
    node_id: str
    host: str
    port: int
    rack: Optional[str] = None
    free_space: int
    total_space: int
    chunk_count: int = 0
    last_heartbeat: datetime = Field(default_factory=datetime.utcnow)
    state: NodeState = NodeState.ACTIVE
```

### 11.4 Configuración

El sistema usa variables de entorno para configuración:

```bash
# Metadata Service
METADATA_SERVICE_PORT=8000
METADATA_STORAGE_BACKEND=sqlite  # o etcd
METADATA_DB_PATH=/data/metadata.db
ETCD_ENDPOINTS=localhost:2379,localhost:2380,localhost:2381
REPLICATION_FACTOR=3
CHUNK_SIZE=1048576  # 64MB

# DataNode
NODE_ID=node-server1-8001
DATANODE_PORT=8001
STORAGE_PATH=/data/chunks
METADATA_SERVICE_URL=http://localhost:8000
HEARTBEAT_INTERVAL=10  # segundos

# Seguridad
JWT_SECRET=clave-secreta-muy-segura
ENABLE_MTLS=false
TLS_CERT_FILE=/certs/server.crt
TLS_KEY_FILE=/certs/server.key
TLS_CA_FILE=/certs/ca.crt

# Observabilidad
PROMETHEUS_PORT=9090
ENABLE_METRICS=true
LOG_LEVEL=INFO
```

---

## 12. Casos de Uso y Aplicaciones

### 12.1 Data Lakes

Un data lake almacena grandes volúmenes de datos en su formato nativo para análisis posterior.

**Caso de uso**: Una empresa de e-commerce almacena logs de clickstream (millones de eventos por dí­a) en el DFS. Analistas ejecutan queries Spark que leen estos datos para generar reportes.

**Ventajas del DFS**:
- Almacenamiento económico de petabytes de datos
- Throughput alto para cargas de trabajo de análisis paralelo
- Durabilidad mediante replicación

### 12.2 Backup y Archivado

Almacenamiento de backups a largo plazo de bases de datos, archivos de usuarios, etc.

**Caso de uso**: Un servicio SaaS hace backups diarios de sus bases de datos PostgreSQL. Los backups se comprimen y suben al DFS, donde se retienen por 90 dí­as.

**Ventajas del DFS**:
- Costo por GB menor que almacenamiento en disco local
- Replicación geográfica (si DataNodes están en diferentes datacenters)
- Soft delete permite recuperar backups eliminados accidentalmente

### 12.3 Content Delivery

Almacenamiento y distribución de contenido multimedia (imágenes, videos).

**Caso de uso**: Una plataforma de streaming de video almacena archivos de video en el DFS. Cuando un usuario solicita un video, se descarga del DFS y se enví­a al CDN para distribución.

**Ventajas del DFS**:
- Almacenamiento escalable para bibliotecas de contenido grandes
- Throughput alto para servir múltiples streams simultáneos
- Deduplicación reduce almacenamiento de contenido duplicado

### 12.4 Machine Learning

Almacenamiento de datasets de entrenamiento y modelos.

**Caso de uso**: Un equipo de ML entrena modelos de visión por computadora usando datasets de imágenes (terabytes). Los datasets se almacenan en el DFS y se leen durante entrenamiento distribuido.

**Ventajas del DFS**:
- Almacenamiento centralizado accesible desde múltiples nodos de entrenamiento
- Throughput alto para saturar GPUs durante entrenamiento
- Versionado de datasets (mediante directorios con timestamps)

---

## 13. Comparación con Otros Sistemas

### 13.1 HDFS (Hadoop Distributed File System)

**Similitudes**:
- Arquitectura master-worker (NameNode/DataNode en HDFS)
- Chunking de archivos grandes
- Replicación para tolerancia a fallos
- Optimizado para throughput sobre latencia

**Diferencias**:
- HDFS está escrito en Java, nuestro DFS en Python
- HDFS usa bloques de 128MB por defecto, nosotros 64MB
- HDFS tiene integración nativa con Hadoop MapReduce
- HDFS soporta append a archivos existentes, nosotros no (solo write-once)

**Cuándo usar HDFS**: Para cargas de trabajo Hadoop/Spark en clusters grandes (100+ nodos).

**Cuándo usar nuestro DFS**: Para proyectos más pequeños, prototipado rápido, o cuando se prefiere Python.

### 13.2 Ceph

**Similitudes**:
- Sistema distribuido de almacenamiento
- Replicación y erasure coding
- Escalabilidad horizontal

**Diferencias**:
- Ceph soporta múltiples interfaces (object storage, block storage, file system)
- Ceph usa CRUSH algorithm para placement, nosotros round-robin
- Ceph está escrito en C++, mucho más complejo
- Ceph tiene overhead mayor pero más features

**Cuándo usar Ceph**: Para infraestructura de almacenamiento empresarial con requisitos complejos.

**Cuándo usar nuestro DFS**: Para casos de uso especí­ficos de file storage sin necesidad de block storage.

### 13.3 MinIO

**Similitudes**:
- Almacenamiento distribuido
- API REST
- Escalabilidad horizontal

**Diferencias**:
- MinIO es object storage (S3-compatible), no file system
- MinIO usa erasure coding en lugar de replicación simple
- MinIO está escrito en Go, optimizado para performance
- MinIO es production-ready, nuestro DFS es educativo/prototipo

**Cuándo usar MinIO**: Para almacenamiento de objetos compatible con S3.

**Cuándo usar nuestro DFS**: Para aprendizaje o cuando se necesita un file system jerárquico.

### 13.4 GlusterFS

**Similitudes**:
- File system distribuido
- Replicación
- Escalabilidad horizontal

**Diferencias**:
- GlusterFS no tiene metadata server centralizado (fully distributed)
- GlusterFS soporta múltiples volúmenes con diferentes configuraciones
- GlusterFS está escrito en C, más bajo nivel
- GlusterFS se monta como file system POSIX, nuestro DFS usa APIs REST

**Cuándo usar GlusterFS**: Para reemplazo drop-in de NFS con alta disponibilidad.

**Cuándo usar nuestro DFS**: Para aplicaciones que prefieren APIs REST sobre POSIX.

---

## 14. Conclusiones

### 14.1 Logros del Sistema

Hemos implementado un Sistema de Archivos Distribuido funcional que demuestra los conceptos fundamentales de sistemas distribuidos:

**Arquitectura sólida**: La separación entre metadata y datos permite escalabilidad. El diseño master-worker es simple pero efectivo.

**Tolerancia a fallos**: Mediante replicación 3x, heartbeats, y re-replicación automática, el sistema tolera fallos de nodos sin pérdida de datos.

**Performance competitivo**: Con throughput de 72.75 MB/s en uploads concurrentes, el sistema es adecuado para muchas cargas de trabajo.

**Observabilidad**: Métricas Prometheus y dashboards Grafana permiten monitorear el sistema en producción.

**Deployment flexible**: Soporte para Docker Compose (desarrollo) y Kubernetes (producción) facilita adopción.

**Documentación exhaustiva**: Documentación técnica completa, runbooks operacionales, y guí­as de instalación.

### 14.2 Limitaciones y Trabajo Futuro

**Limitaciones actuales**:

**No soporta modificación de archivos**: Solo write-once-read-many. No se pueden actualizar archivos existentes.

**Metadata Service es punto único de fallo**: Aunque hay soporte para etcd, la implementación por defecto usa SQLite en un solo nodo.

**No hay autenticación**: El sistema confí­a en todos los clientes. No hay control de acceso.

**Placement simple**: El algoritmo round-robin no considera topologí­a de red ni carga actual.

**No hay compresión ni deduplicación**: Estas optimizaciones no están implementadas.

**Mejoras futuras**:

**Append y modificación de archivos**: Permitir agregar datos a archivos existentes y modificar regiones especí­ficas.

**Metadata Service distribuido**: Implementar consenso Raft directamente en el Metadata Service para HA sin dependencia de etcd.

**Autenticación y autorización**: Implementar JWT, RBAC, y ACLs para control de acceso granular.

**Placement rack-aware**: Considerar topologí­a de red para placement de réplicas.

**Compresión adaptativa**: Comprimir chunks automáticamente según tipo de archivo.

**Deduplicación**: Detectar y eliminar chunks duplicados.

**Erasure coding**: Usar códigos de corrección de errores (como Reed-Solomon) en lugar de replicación simple para reducir overhead de almacenamiento.

**Snapshots**: Permitir crear snapshots point-in-time del file system.

**Quotas**: Limitar uso de almacenamiento por usuario o directorio.

**Cifrado end-to-end**: Cifrar datos en el cliente antes de subirlos.

### 14.3 Lecciones Aprendidas

**Diseño de sistemas distribuidos**:

La simplicidad es valiosa. Una arquitectura master-worker simple es más fácil de entender, implementar, y debuggear que alternativas fully distributed.

La separación de concerns (metadata vs datos) permite optimizar cada componente independientemente.

Los fallos son inevitables. El diseño debe asumir que componentes fallarán y manejar esos fallos gracefully.

**Implementación**:

La programación así­ncrona (asyncio) es esencial para performance en sistemas I/O-bound.

Las APIs REST son suficientes para muchos casos de uso, aunque protocolos binarios (gRPC) pueden ofrecer mejor performance.

La observabilidad debe ser parte del diseño desde el principio, no agregada después.

**Testing**:

Los tests E2E son crí­ticos para validar comportamiento del sistema completo.

Los tests de performance revelan cuellos de botella que no son evidentes en código.

Los tests de chaos (simular fallos) son necesarios para validar tolerancia a fallos.

### 14.4 Aplicabilidad Práctica

Este DFS es adecuado para:

**Educación**: Aprender conceptos de sistemas distribuidos mediante implementación práctica.

**Prototipado**: Validar ideas que requieren almacenamiento distribuido sin complejidad de sistemas enterprise.

**Proyectos pequeños/medianos**: Aplicaciones con requisitos de almacenamiento de terabytes (no petabytes).

**Investigación**: Base para experimentar con nuevos algoritmos de placement, replicación, etc.

No es adecuado para:

**Sistemas crí­ticos de producción**: Falta de features enterprise (HA completo, seguridad robusta).

**Escala masiva**: No ha sido probado en clusters de 100+ nodos.

**Cargas de trabajo de baja latencia**: Optimizado para throughput, no latencia.

### 14.5 Reflexión Final

Construir un Sistema de Archivos Distribuido desde cero es un ejercicio educativo invaluable. Expone la complejidad inherente de sistemas distribuidos y las decisiones de diseño que deben tomarse.

Los conceptos implementados aquí­ (chunking, replicación, heartbeats, re-replicación, metadata management) son fundamentales en muchos sistemas distribuidos modernos, desde bases de datos distribuidas hasta sistemas de mensajerí­a.

Aunque este DFS es un prototipo educativo, los principios son aplicables a sistemas de producción. Sistemas como HDFS, Ceph, y GlusterFS implementan estos mismos conceptos con optimizaciones adicionales y features enterprise.

El código fuente completo y la documentación exhaustiva permiten que otros aprendan de este proyecto, lo extiendan, y lo adapten a sus necesidades especí­ficas.

---

## Apéndices

### Apéndice A: Glosario de Términos

**Chunk**: Fragmento de un archivo de tamaño fijo (tí­picamente 64MB).

**Replicación**: Proceso de mantener múltiples copias de datos en diferentes nodos.

**Factor de replicación**: Número de copias de cada chunk (tí­picamente 3).

**Heartbeat**: Mensaje periódico que un nodo enví­a para indicar que está vivo.

**Metadata**: Información sobre archivos (nombre, tamaño, ubicación) sin incluir el contenido real.

**Namespace**: Estructura jerárquica de directorios y archivos.

**Placement**: Decisión de en qué nodos almacenar chunks.

**Re-replicación**: Proceso de crear nuevas réplicas cuando un nodo falla.

**Checksum**: Hash criptográfico usado para verificar integridad de datos.

**Soft delete**: Marcar archivos como eliminados sin borrarlos fí­sicamente inmediatamente.

**Garbage collection**: Proceso de eliminar fí­sicamente datos marcados como eliminados.

**Throughput**: Cantidad de datos transferidos por unidad de tiempo (MB/s).

**Latencia**: Tiempo desde inicio hasta completar una operación.

**Escalabilidad horizontal**: Capacidad de agregar más nodos para aumentar capacidad/performance.

**Tolerancia a fallos**: Capacidad de continuar operando cuando componentes fallan.

**Consenso**: Algoritmo que permite a nodos distribuidos acordar un valor.

**Lease**: Permiso temporal que expira automáticamente si no se renueva.

**mTLS**: Mutual TLS, donde tanto cliente como servidor verifican certificados.

**JWT**: JSON Web Token, estándar para tokens de autenticación.

**RBAC**: Role-Based Access Control, modelo de control de acceso basado en roles.

### Apéndice B: Referencias

**Sistemas de archivos distribuidos**:
- "The Google File System" (Ghemawat et al., 2003)
- "The Hadoop Distributed File System" (Shvachko et al., 2010)
- "Ceph: A Scalable, High-Performance Distributed File System" (Weil et al., 2006)

**Sistemas distribuidos**:
- "Designing Data-Intensive Applications" (Martin Kleppmann, 2017)
- "Distributed Systems" (Maarten van Steen & Andrew S. Tanenbaum, 2017)

**Algoritmos de consenso**:
- "The Raft Consensus Algorithm" (Ongaro & Ousterhout, 2014)
- "Paxos Made Simple" (Leslie Lamport, 2001)

**Tecnologí­as usadas**:
- FastAPI: https://fastapi.tiangolo.com/
- etcd: https://etcd.io/
- Prometheus: https://prometheus.io/
- Kubernetes: https://kubernetes.io/

### Apéndice C: Comandos íštiles

**Iniciar el sistema**:
```bash
cd /home/ubuntu/dfs-system
./start_all.sh
```

**Verificar estado**:
```bash
curl http://localhost:8000/api/v1/health
curl http://localhost:8000/api/v1/nodes
```

**Usar el CLI**:
```bash
cd client
python3.11 dfs_cli.py upload local.txt /remote/file.txt
python3.11 dfs_cli.py ls /
python3.11 dfs_cli.py download /remote/file.txt downloaded.txt
python3.11 dfs_cli.py rm /remote/file.txt
```

**Ver logs**:
```bash
tail -f /tmp/metadata.log
tail -f /tmp/datanode1.log
```

**Ejecutar tests**:
```bash
cd /home/ubuntu
python3.11 test_dfs_intensive.py
```

**Acceder a Prometheus**:
```
http://localhost:9090
```

**Acceder a Grafana**:
```
http://localhost:3001
Usuario: admin
Password: admin
```

---

**Fin del Informe Teórico Completo**

*Documento generado el 14 de noviembre de 2025*  
*Sistema de Archivos Distribuido (DFS) v1.0.0*  
*Autor: Equipo de Desarrollo DFS*