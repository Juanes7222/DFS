# Documentación de API - DFS

Esta guía describe todas las APIs REST expuestas por el Sistema de Archivos Distribuido.

## Metadata Service API

Base URL: `http://localhost:8000`

Documentación interactiva OpenAPI disponible en: `http://localhost:8000/docs`

### Autenticación

Actualmente las APIs no requieren autenticación. En producción, se recomienda implementar JWT o mTLS.

### Endpoints de Archivos

#### POST /api/v1/files/upload-init

Inicia el proceso de subida de un archivo.

**Request Body:**

```json
{
  "path": "/path/to/file.txt",
  "size": 134217728,
  "chunk_size": 67108864
}
```

**Response:**

```json
{
  "file_id": "550e8400-e29b-41d4-a716-446655440000",
  "chunks": [
    {
      "chunk_id": "660e8400-e29b-41d4-a716-446655440001",
      "size": 67108864,
      "targets": [
        "http://datanode1:8001",
        "http://datanode2:8002",
        "http://datanode3:8003"
      ]
    },
    {
      "chunk_id": "770e8400-e29b-41d4-a716-446655440002",
      "size": 67108864,
      "targets": [
        "http://datanode2:8002",
        "http://datanode3:8003",
        "http://datanode1:8001"
      ]
    }
  ]
}
```

**Ejemplo con curl:**

```bash
curl -X POST http://localhost:8000/api/v1/files/upload-init \
  -H "Content-Type: application/json" \
  -d '{
    "path": "/test/file.txt",
    "size": 1048576,
    "chunk_size": 67108864
  }'
```

#### POST /api/v1/files/commit

Confirma que los chunks han sido subidos correctamente.

**Request Body:**

```json
{
  "file_id": "550e8400-e29b-41d4-a716-446655440000",
  "chunks": [
    {
      "chunk_id": "660e8400-e29b-41d4-a716-446655440001",
      "checksum": "sha256_hex_string",
      "nodes": [
        "node-datanode1-8001",
        "node-datanode2-8002",
        "node-datanode3-8003"
      ]
    }
  ]
}
```

**Response:**

```json
{
  "status": "committed",
  "file_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

**Ejemplo con curl:**

```bash
curl -X POST http://localhost:8000/api/v1/files/commit \
  -H "Content-Type: application/json" \
  -d '{
    "file_id": "550e8400-e29b-41d4-a716-446655440000",
    "chunks": [...]
  }'
```

#### GET /api/v1/files/{path}

Obtiene metadata de un archivo específico.

**Path Parameters:**
- `path`: Ruta del archivo (URL-encoded)

**Response:**

```json
{
  "file_id": "550e8400-e29b-41d4-a716-446655440000",
  "path": "/test/file.txt",
  "size": 134217728,
  "created_at": "2024-01-01T00:00:00Z",
  "modified_at": "2024-01-01T00:00:00Z",
  "is_deleted": false,
  "chunks": [
    {
      "chunk_id": "660e8400-e29b-41d4-a716-446655440001",
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
        }
      ]
    }
  ]
}
```

**Ejemplo con curl:**

```bash
curl http://localhost:8000/api/v1/files/%2Ftest%2Ffile.txt
```

#### GET /api/v1/files

Lista todos los archivos con filtros opcionales.

**Query Parameters:**
- `prefix` (opcional): Filtrar por prefijo de path
- `limit` (opcional): Número máximo de resultados (default: 1000)
- `offset` (opcional): Offset para paginación (default: 0)

**Response:**

```json
[
  {
    "file_id": "550e8400-e29b-41d4-a716-446655440000",
    "path": "/test/file1.txt",
    "size": 1048576,
    "created_at": "2024-01-01T00:00:00Z",
    "modified_at": "2024-01-01T00:00:00Z",
    "is_deleted": false,
    "chunks": [...]
  },
  {
    "file_id": "660e8400-e29b-41d4-a716-446655440001",
    "path": "/test/file2.txt",
    "size": 2097152,
    "created_at": "2024-01-01T01:00:00Z",
    "modified_at": "2024-01-01T01:00:00Z",
    "is_deleted": false,
    "chunks": [...]
  }
]
```

**Ejemplo con curl:**

```bash
# Listar todos los archivos
curl http://localhost:8000/api/v1/files

# Listar archivos con prefijo
curl "http://localhost:8000/api/v1/files?prefix=/test"

# Paginación
curl "http://localhost:8000/api/v1/files?limit=10&offset=20"
```

#### DELETE /api/v1/files/{path}

Elimina un archivo.

**Path Parameters:**
- `path`: Ruta del archivo (URL-encoded)

**Query Parameters:**
- `permanent` (opcional): Si es `true`, elimina permanentemente. Si es `false`, soft-delete (default: false)

**Response:**

```json
{
  "status": "deleted",
  "path": "/test/file.txt"
}
```

**Ejemplo con curl:**

```bash
# Soft delete
curl -X DELETE http://localhost:8000/api/v1/files/%2Ftest%2Ffile.txt

# Permanent delete
curl -X DELETE "http://localhost:8000/api/v1/files/%2Ftest%2Ffile.txt?permanent=true"
```

### Endpoints de Nodos

#### GET /api/v1/nodes

Lista todos los nodos registrados.

**Response:**

```json
[
  {
    "node_id": "node-datanode1-8001",
    "host": "datanode1",
    "port": 8001,
    "rack": null,
    "free_space": 50000000000,
    "total_space": 100000000000,
    "chunk_count": 1234,
    "last_heartbeat": "2024-01-01T00:00:00Z",
    "state": "active"
  },
  {
    "node_id": "node-datanode2-8002",
    "host": "datanode2",
    "port": 8002,
    "rack": null,
    "free_space": 45000000000,
    "total_space": 100000000000,
    "chunk_count": 1456,
    "last_heartbeat": "2024-01-01T00:00:00Z",
    "state": "active"
  }
]
```

**Ejemplo con curl:**

```bash
curl http://localhost:8000/api/v1/nodes
```

#### POST /api/v1/nodes/heartbeat

Recibe heartbeat de un DataNode (uso interno).

**Request Body:**

```json
{
  "node_id": "node-datanode1-8001",
  "free_space": 50000000000,
  "total_space": 100000000000,
  "chunk_ids": [
    "660e8400-e29b-41d4-a716-446655440001",
    "770e8400-e29b-41d4-a716-446655440002"
  ]
}
```

**Response:**

```json
{
  "status": "ok"
}
```

### Endpoints de Leases

#### POST /api/v1/leases/acquire

Adquiere un lease exclusivo para escritura.

**Request Body:**

```json
{
  "path": "/test/file.txt",
  "client_id": "client-uuid",
  "timeout": 60
}
```

**Response:**

```json
{
  "lease_id": "lease-uuid",
  "path": "/test/file.txt",
  "expires_at": "2024-01-01T00:01:00Z"
}
```

**Ejemplo con curl:**

```bash
curl -X POST http://localhost:8000/api/v1/leases/acquire \
  -H "Content-Type: application/json" \
  -d '{
    "path": "/test/file.txt",
    "client_id": "my-client",
    "timeout": 60
  }'
```

#### POST /api/v1/leases/release

Libera un lease.

**Request Body:**

```json
{
  "lease_id": "lease-uuid"
}
```

**Response:**

```json
{
  "status": "released"
}
```

### Health Check

#### GET /api/v1/health

Verifica el estado de salud del Metadata Service.

**Response:**

```json
{
  "status": "healthy",
  "timestamp": "2024-01-01T00:00:00Z",
  "details": {
    "total_nodes": 3,
    "active_nodes": 3,
    "replication_factor": 3
  }
}
```

**Ejemplo con curl:**

```bash
curl http://localhost:8000/api/v1/health
```

### Métricas

#### GET /metrics

Expone métricas en formato Prometheus.

**Response:** (formato texto Prometheus)

```
# HELP dfs_metadata_files_total Total number of files
# TYPE dfs_metadata_files_total gauge
dfs_metadata_files_total 1234

# HELP dfs_metadata_nodes_active Number of active nodes
# TYPE dfs_metadata_nodes_active gauge
dfs_metadata_nodes_active 3

...
```

**Ejemplo con curl:**

```bash
curl http://localhost:8000/metrics
```

## DataNode API

Base URL: `http://localhost:8001` (puerto varía por nodo)

### Endpoints de Chunks

#### PUT /api/v1/chunks/{chunk_id}

Almacena un chunk.

**Path Parameters:**
- `chunk_id`: UUID del chunk

**Request Body:** Multipart form-data con el archivo

**Response:**

```json
{
  "status": "stored",
  "chunk_id": "660e8400-e29b-41d4-a716-446655440001",
  "size": 67108864,
  "checksum": "sha256_hex"
}
```

**Ejemplo con curl:**

```bash
curl -X PUT http://localhost:8001/api/v1/chunks/660e8400-e29b-41d4-a716-446655440001 \
  -F "file=@/path/to/chunk.bin"
```

#### GET /api/v1/chunks/{chunk_id}

Recupera un chunk.

**Path Parameters:**
- `chunk_id`: UUID del chunk

**Response:** Streaming binario del chunk

**Headers:**
- `X-Chunk-ID`: UUID del chunk
- `X-Checksum`: SHA256 checksum
- `Content-Length`: Tamaño en bytes

**Ejemplo con curl:**

```bash
curl http://localhost:8001/api/v1/chunks/660e8400-e29b-41d4-a716-446655440001 \
  -o chunk.bin
```

#### DELETE /api/v1/chunks/{chunk_id}

Elimina un chunk.

**Path Parameters:**
- `chunk_id`: UUID del chunk

**Response:**

```json
{
  "status": "deleted",
  "chunk_id": "660e8400-e29b-41d4-a716-446655440001"
}
```

**Ejemplo con curl:**

```bash
curl -X DELETE http://localhost:8001/api/v1/chunks/660e8400-e29b-41d4-a716-446655440001
```

### Health Check

#### GET /api/v1/health

Verifica el estado de salud del DataNode.

**Response:**

```json
{
  "status": "healthy",
  "timestamp": "2024-01-01T00:00:00Z",
  "details": {
    "node_id": "node-datanode1-8001",
    "storage_path": "/data",
    "total_space": 100000000000,
    "free_space": 50000000000,
    "used_space": 50000000000,
    "chunk_count": 1234
  }
}
```

**Ejemplo con curl:**

```bash
curl http://localhost:8001/api/v1/health
```

### Métricas

#### GET /metrics

Expone métricas en formato Prometheus.

**Response:** (formato texto Prometheus)

```
# HELP dfs_datanode_chunks_stored Number of chunks stored
# TYPE dfs_datanode_chunks_stored gauge
dfs_datanode_chunks_stored 1234

# HELP dfs_datanode_disk_free_bytes Free disk space in bytes
# TYPE dfs_datanode_disk_free_bytes gauge
dfs_datanode_disk_free_bytes 50000000000

...
```

**Ejemplo con curl:**

```bash
curl http://localhost:8001/metrics
```

## Códigos de Estado HTTP

| Código | Significado | Uso |
|--------|-------------|-----|
| 200 | OK | Operación exitosa |
| 201 | Created | Recurso creado exitosamente |
| 400 | Bad Request | Request inválido (parámetros faltantes/incorrectos) |
| 404 | Not Found | Recurso no encontrado |
| 409 | Conflict | Conflicto (ej: archivo ya existe) |
| 500 | Internal Server Error | Error del servidor |
| 503 | Service Unavailable | Servicio temporalmente no disponible |

## Manejo de Errores

Todos los errores devuelven un objeto JSON con el siguiente formato:

```json
{
  "detail": "Descripción del error"
}
```

Ejemplo:

```json
{
  "detail": "Archivo no encontrado: /test/file.txt"
}
```

## Rate Limiting

Actualmente no hay rate limiting implementado. En producción, se recomienda implementar:

- Límite por IP: 1000 requests/minuto
- Límite por cliente autenticado: 10000 requests/minuto
- Límite de ancho de banda: 100 MB/s por cliente

## Ejemplos de Uso Completo

### Subir un Archivo

```bash
#!/bin/bash
# upload_file.sh

FILE_PATH="/local/path/file.txt"
REMOTE_PATH="/dfs/path/file.txt"
METADATA_URL="http://localhost:8000"

# 1. Obtener tamaño del archivo
FILE_SIZE=$(stat -f%z "$FILE_PATH")

# 2. Iniciar upload
INIT_RESPONSE=$(curl -s -X POST "$METADATA_URL/api/v1/files/upload-init" \
  -H "Content-Type: application/json" \
  -d "{\"path\": \"$REMOTE_PATH\", \"size\": $FILE_SIZE, \"chunk_size\": 67108864}")

FILE_ID=$(echo $INIT_RESPONSE | jq -r '.file_id')
CHUNKS=$(echo $INIT_RESPONSE | jq -c '.chunks[]')

# 3. Subir cada chunk
COMMIT_CHUNKS="[]"
OFFSET=0

for CHUNK in $CHUNKS; do
  CHUNK_ID=$(echo $CHUNK | jq -r '.chunk_id')
  CHUNK_SIZE=$(echo $CHUNK | jq -r '.size')
  TARGETS=$(echo $CHUNK | jq -r '.targets[]')
  
  # Extraer chunk del archivo
  dd if="$FILE_PATH" of="/tmp/chunk_$CHUNK_ID" bs=1 skip=$OFFSET count=$CHUNK_SIZE 2>/dev/null
  
  # Calcular checksum
  CHECKSUM=$(sha256sum "/tmp/chunk_$CHUNK_ID" | awk '{print $1}')
  
  # Subir a cada target
  UPLOADED_NODES="[]"
  for TARGET in $TARGETS; do
    RESPONSE=$(curl -s -X PUT "$TARGET/api/v1/chunks/$CHUNK_ID" \
      -F "file=@/tmp/chunk_$CHUNK_ID")
    
    if [ $? -eq 0 ]; then
      NODE_ID=$(echo $TARGET | sed 's/http:\/\//node-/' | sed 's/:/-/')
      UPLOADED_NODES=$(echo $UPLOADED_NODES | jq ". + [\"$NODE_ID\"]")
    fi
  done
  
  # Agregar a commit
  COMMIT_CHUNKS=$(echo $COMMIT_CHUNKS | jq ". + [{\"chunk_id\": \"$CHUNK_ID\", \"checksum\": \"$CHECKSUM\", \"nodes\": $UPLOADED_NODES}]")
  
  OFFSET=$((OFFSET + CHUNK_SIZE))
  rm "/tmp/chunk_$CHUNK_ID"
done

# 4. Commit
curl -X POST "$METADATA_URL/api/v1/files/commit" \
  -H "Content-Type: application/json" \
  -d "{\"file_id\": \"$FILE_ID\", \"chunks\": $COMMIT_CHUNKS}"

echo "Upload completado: $REMOTE_PATH"
```

### Descargar un Archivo

```bash
#!/bin/bash
# download_file.sh

REMOTE_PATH="/dfs/path/file.txt"
LOCAL_PATH="/local/path/downloaded.txt"
METADATA_URL="http://localhost:8000"

# 1. Obtener metadata
METADATA=$(curl -s "$METADATA_URL/api/v1/files/$(echo $REMOTE_PATH | jq -sRr @uri)")

# 2. Descargar cada chunk
rm -f "$LOCAL_PATH"
CHUNKS=$(echo $METADATA | jq -c '.chunks | sort_by(.seq_index) | .[]')

for CHUNK in $CHUNKS; do
  CHUNK_ID=$(echo $CHUNK | jq -r '.chunk_id')
  REPLICAS=$(echo $CHUNK | jq -r '.replicas[0].url')  # Usar primera réplica
  
  # Descargar chunk
  curl -s "$REPLICAS/api/v1/chunks/$CHUNK_ID" >> "$LOCAL_PATH"
done

echo "Download completado: $LOCAL_PATH"
```

## Librerías Cliente

### Python

```python
from dfs_client import DFSClient

# Crear cliente
client = DFSClient("http://localhost:8000")

# Upload
await client.upload("/local/file.txt", "/dfs/file.txt")

# Download
await client.download("/dfs/file.txt", "/local/downloaded.txt")

# List
files = await client.list_files(prefix="/dfs")

# Delete
await client.delete("/dfs/file.txt")
```

### JavaScript/TypeScript

```typescript
import { DFSClient } from 'dfs-client-js';

// Crear cliente
const client = new DFSClient('http://localhost:8000');

// Upload
await client.upload('/local/file.txt', '/dfs/file.txt');

// Download
await client.download('/dfs/file.txt', '/local/downloaded.txt');

// List
const files = await client.listFiles({ prefix: '/dfs' });

// Delete
await client.delete('/dfs/file.txt');
```

---

**Autor**: Manus AI  
**Versión**: 1.0.0  
**Última Actualización**: 2024
