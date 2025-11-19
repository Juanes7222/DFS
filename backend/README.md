# Sistema de Archivos Distribuido (DFS)

Un sistema de archivos distribuido profesional y productivo construido con Python FastAPI, diseñado para almacenar, replicar y recuperar archivos de manera confiable a través de múltiples nodos.

## Características Principales

El DFS implementa un sistema robusto de almacenamiento distribuido con las siguientes capacidades:

**Almacenamiento Distribuido**: Los archivos se dividen automáticamente en chunks de 64MB y se distribuyen a través de múltiples nodos de almacenamiento, permitiendo el manejo eficiente de archivos de cualquier tamaño.

**Replicación Automática**: Cada chunk se replica en tres nodos diferentes por defecto, garantizando la durabilidad de los datos incluso ante fallos de hardware. El sistema monitorea continuamente el estado de las réplicas y re-replica automáticamente cuando detecta pérdidas.

**Alta Disponibilidad**: El Metadata Service coordina todas las operaciones y mantiene un registro consistente del estado del cluster. Los DataNodes envían heartbeats periódicos para reportar su estado y el inventario de chunks almacenados.

**Observabilidad Integrada**: Métricas Prometheus exportadas desde todos los componentes, con dashboards Grafana preconfigurados para monitoreo en tiempo real del estado del cluster, uso de disco, throughput y latencias.

**APIs REST Completas**: Interfaces OpenAPI documentadas para todas las operaciones, facilitando la integración con sistemas externos y el desarrollo de clientes personalizados.

**Cliente CLI Intuitivo**: Herramienta de línea de comandos que simplifica las operaciones comunes como upload, download, listado y eliminación de archivos.

**Frontend Web Moderno**: Dashboard React con visualización en tiempo real del estado del cluster, gestión de archivos, y vista detallada de nodos y réplicas.

## Arquitectura

El sistema está compuesto por tres componentes principales que trabajan en conjunto:

### Metadata Service (Master)

El Metadata Service actúa como el cerebro del sistema, manteniendo el namespace global y el mapeo de archivos a chunks. Cuando un cliente desea subir un archivo, el Metadata Service calcula cuántos chunks se necesitan, selecciona los nodos de destino para cada réplica, y devuelve un plan de subida al cliente. Durante la descarga, proporciona la ubicación de todas las réplicas de cada chunk, permitiendo al cliente seleccionar las fuentes óptimas.

Este servicio también coordina la re-replicación automática mediante un background worker que monitorea continuamente el factor de replicación de cada chunk y programa copias adicionales cuando detecta que algún chunk está por debajo del umbral configurado.

### DataNode (Nodos de Almacenamiento)

Los DataNodes son responsables del almacenamiento físico de los chunks en disco local. Cada DataNode expone APIs REST para escribir, leer y eliminar chunks, verificando checksums SHA256 en cada operación para detectar corrupción de datos.

Los DataNodes envían heartbeats periódicos al Metadata Service cada 10 segundos, reportando su espacio libre, capacidad total y el inventario completo de chunks almacenados. Esta información permite al Metadata Service tomar decisiones informadas sobre placement de nuevos chunks y detectar nodos que han fallado.

### Cliente (CLI y Librería)

El cliente encapsula toda la lógica de comunicación con el DFS. Durante un upload, el cliente divide el archivo en chunks, calcula checksums, sube cada chunk en paralelo a los nodos designados, y finalmente confirma la operación al Metadata Service. Para descargas, el cliente obtiene la metadata del archivo, descarga todos los chunks en paralelo desde las réplicas disponibles, y reconstruye el archivo original verificando la integridad con checksums.

## Inicio Rápido

### Prerrequisitos

El sistema requiere Docker y Docker Compose instalados en el host. Se recomienda al menos 4GB de RAM y 20GB de espacio en disco para un cluster de desarrollo con tres DataNodes.

### Instalación y Ejecución

Para iniciar el cluster completo con todos los servicios, ejecute el siguiente comando desde el directorio raíz del proyecto:

```bash
cd /home/ubuntu/dfs-system
./start.sh
```

Este script construye las imágenes Docker necesarias y levanta todos los servicios: Metadata Service, tres DataNodes, Prometheus y Grafana. El proceso toma aproximadamente 2-3 minutos en el primer arranque mientras se descargan las imágenes base y se instalan las dependencias.

Alternativamente, puede usar Docker Compose directamente:

```bash
docker-compose up -d
```

### Verificación del Estado

Una vez que los servicios están corriendo, puede verificar el estado del cluster:

```bash
# Verificar que todos los contenedores están corriendo
docker-compose ps

# Ver logs del Metadata Service
docker-compose logs -f metadata

# Ver logs de un DataNode
docker-compose logs -f datanode1

# Verificar health del Metadata Service
curl http://localhost:8000/api/v1/health

# Listar nodos registrados
curl http://localhost:8000/api/v1/nodes
```

### Uso del Cliente CLI

El cliente CLI proporciona comandos intuitivos para interactuar con el DFS:

```bash
# Entrar al contenedor del cliente
docker-compose run --rm client bash

# Subir un archivo
python /app/client/dfs_cli.py upload /local/path/file.txt /dfs/path/file.txt

# Listar archivos
python /app/client/dfs_cli.py ls

# Descargar un archivo
python /app/client/dfs_cli.py download /dfs/path/file.txt /local/path/downloaded.txt

# Ver estado del cluster
python /app/client/dfs_cli.py status

# Listar nodos
python /app/client/dfs_cli.py nodes

# Eliminar un archivo
python /app/client/dfs_cli.py rm /dfs/path/file.txt
```

### Acceso a Interfaces Web

El sistema expone varias interfaces web para monitoreo y gestión:

| Servicio | URL | Credenciales | Descripción |
|----------|-----|--------------|-------------|
| Frontend DFS | http://localhost:3000 | N/A | Dashboard principal con gestión de archivos |
| Metadata Service API | http://localhost:8000/docs | N/A | Documentación OpenAPI interactiva |
| Prometheus | http://localhost:9090 | N/A | Métricas y queries |
| Grafana | http://localhost:3001 | admin/admin | Dashboards de monitoreo |

## Monitoreo y Observabilidad

El sistema exporta métricas detalladas en formato Prometheus desde todos los componentes. Prometheus scrape estas métricas cada 10 segundos y las almacena en su base de datos de series temporales.

### Métricas Clave

**Metadata Service:**
- `dfs_metadata_files_total`: Número total de archivos en el sistema
- `dfs_metadata_nodes_active`: Número de nodos activos
- `dfs_metadata_storage_used_bytes`: Espacio de almacenamiento utilizado
- `dfs_metadata_upload_operations_total`: Total de operaciones de upload
- `dfs_metadata_replication_lag`: Chunks por debajo del factor de replicación

**DataNode:**
- `dfs_datanode_chunks_stored`: Número de chunks almacenados
- `dfs_datanode_disk_free_bytes`: Espacio libre en disco
- `dfs_datanode_bytes_written_total`: Total de bytes escritos
- `dfs_datanode_chunk_write_operations_total`: Total de operaciones de escritura

### Configuración de Grafana

Para configurar dashboards en Grafana:

1. Acceda a http://localhost:3001 (admin/admin)
2. Agregue Prometheus como data source: http://prometheus:9090
3. Importe dashboards preconfigurados o cree los suyos propios
4. Configure alertas basadas en umbrales de métricas

## Operaciones Comunes

### Agregar un Nuevo DataNode

Para escalar el cluster agregando más capacidad de almacenamiento:

```yaml
# Agregar al docker-compose.yml
datanode4:
  build:
    context: .
    dockerfile: infra/docker/Dockerfile.datanode
  container_name: dfs-datanode4
  ports:
    - "8004:8004"
  environment:
    - NODE_ID=node-datanode4-8004
    - STORAGE_PATH=/data
    - METADATA_SERVICE_URL=http://metadata:8000
    - HEARTBEAT_INTERVAL=10
  volumes:
    - datanode4-data:/data
  networks:
    - dfs-network
  command: ["sh", "-c", "PORT=8004 python main.py"]
```

Luego reinicie el servicio:

```bash
docker-compose up -d datanode4
```

El nuevo nodo comenzará a enviar heartbeats automáticamente y estará disponible para almacenar nuevos chunks en cuestión de segundos.

### Backup del Metadata

El Metadata Service almacena su estado en SQLite. Para hacer backup:

```bash
# Copiar la base de datos
docker cp dfs-metadata:/tmp/dfs_metadata.db ./backup/metadata_$(date +%Y%m%d).db

# Restaurar desde backup
docker cp ./backup/metadata_20240101.db dfs-metadata:/tmp/dfs_metadata.db
docker-compose restart metadata
```

### Recuperación ante Fallos

Si un DataNode falla, el sistema automáticamente:

1. Detecta la falla cuando los heartbeats dejan de llegar
2. Marca el nodo como inactivo después de 30 segundos
3. Identifica todos los chunks que están por debajo del factor de replicación
4. Programa re-replicación desde las réplicas sanas a nodos activos
5. Restaura el factor de replicación completo en minutos

No se requiere intervención manual para recuperación ante fallos de nodos individuales.

## Arquitectura de Almacenamiento

Los archivos se almacenan siguiendo esta estructura:

```
/data/
├── <chunk_id_1>.chunk      # Datos del chunk
├── <chunk_id_1>.checksum   # SHA256 checksum
├── <chunk_id_2>.chunk
├── <chunk_id_2>.checksum
└── ...
```

Cada chunk se almacena como un archivo binario independiente junto con su checksum. Esta estructura simple facilita operaciones de backup, migración y debugging.

## Desarrollo y Testing

### Ejecutar Tests

El proyecto incluye tests end-to-end que verifican el funcionamiento completo del sistema:

```bash
cd /home/ubuntu/dfs-system/tests
./run_tests.sh
```

Los tests cubren:
- Upload y download de archivos pequeños y grandes
- Verificación de checksums
- Listado y filtrado de archivos
- Eliminación de archivos
- Operaciones concurrentes
- Resiliencia ante fallos

### Desarrollo Local

Para desarrollo local sin Docker:

```bash
# Instalar dependencias
cd metadata-service
pip install -r requirements.txt

# Iniciar Metadata Service
python main.py

# En otra terminal, iniciar DataNode
cd ../datanode
pip install -r requirements.txt
NODE_ID=node-local-8001 python main.py
```

## Limitaciones Conocidas

**Consistencia Eventual**: El sistema usa SQLite para metadata, lo cual limita la escalabilidad horizontal del Metadata Service. Para producción a gran escala, se recomienda migrar a etcd o una base de datos distribuida.

**Sin Erasure Coding**: Actualmente solo soporta replicación completa. Para optimizar uso de espacio en clusters grandes, considere implementar erasure coding (Reed-Solomon).

**Sin Autenticación**: Las APIs están abiertas sin autenticación. En producción, implemente JWT o mTLS para asegurar las comunicaciones.

**Sin Encriptación en Reposo**: Los chunks se almacenan sin encriptar. Para datos sensibles, integre con un KMS o implemente encriptación a nivel de aplicación.

## Roadmap

Las siguientes características están planificadas para futuras versiones:

**Alta Disponibilidad del Metadata Service**: Integración con etcd para consenso distribuido, permitiendo múltiples instancias del Metadata Service con failover automático.

**Seguridad**: Implementación de mTLS entre componentes, autenticación JWT para APIs, y encriptación en reposo con integración KMS.

**Kubernetes Native**: Helm charts completos y Kubernetes Operator para automatizar despliegue, scaling y operaciones day-2.

**Erasure Coding**: Soporte para Reed-Solomon erasure coding como alternativa a replicación completa, reduciendo overhead de almacenamiento.

**Snapshots y Versionado**: Capacidad de tomar snapshots del filesystem y mantener versiones históricas de archivos.

## Estructura del Proyecto

```
dfs-system/
├── metadata-service/     # Servicio de metadatos (Master)
│   ├── main.py          # Aplicación FastAPI
│   ├── storage.py       # Backend de almacenamiento
│   ├── replicator.py    # Gestor de replicación
│   ├── metrics.py       # Métricas Prometheus
│   └── requirements.txt
├── datanode/            # Nodo de almacenamiento
│   ├── main.py          # Aplicación FastAPI
│   ├── metrics.py       # Métricas Prometheus
│   └── requirements.txt
├── client/              # Cliente CLI y librería Python
│   ├── dfs_cli.py       # CLI
│   ├── dfs_client.py    # Librería
│   └── requirements.txt
├── shared/              # Código compartido
│   ├── models.py        # Modelos de datos
│   └── utils.py         # Utilidades
├── tests/               # Tests E2E
│   ├── test_e2e.py
│   └── run_tests.sh
├── infra/               # Infraestructura
│   ├── docker/          # Dockerfiles
│   ├── prometheus/      # Configuración Prometheus
│   └── helm/            # Helm charts (próximamente)
└── docker-compose.yml   # Orquestación local
```
