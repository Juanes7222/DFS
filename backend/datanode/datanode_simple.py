"""
DataNode simplificado con soporte de pipeline replication
"""
import sys
import os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from fastapi import FastAPI, HTTPException, UploadFile, Query
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import httpx
import asyncio
from uuid import UUID
import shutil
import logging
from typing import Optional

from shared import calculate_checksum

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

NODE_ID = os.getenv("NODE_ID", "node-localhost-8001")
PORT = int(os.getenv("PORT", "8001"))
STORAGE_PATH = Path(os.getenv("STORAGE_PATH", f"/tmp/dfs-data-{PORT}"))
METADATA_URL = os.getenv("METADATA_SERVICE_URL", "http://localhost:8000")

STORAGE_PATH.mkdir(parents=True, exist_ok=True)

app = FastAPI(title=f"DFS DataNode {NODE_ID}")

# Agregar CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # En producción, especificar los orígenes permitidos
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_disk_usage():
    stat = shutil.disk_usage(STORAGE_PATH)
    return stat.free, stat.total

def get_stored_chunks():
    if not STORAGE_PATH.exists():
        return []
    return [UUID(f.stem) for f in STORAGE_PATH.glob("*.chunk")]

@app.get("/")
async def root():
    return {"service": "DFS DataNode", "node_id": NODE_ID, "status": "running"}

@app.get("/health")
@app.get("/api/v1/health")
async def health():
    free, total = get_disk_usage()
    chunks = get_stored_chunks()
    return {
        "status": "healthy",
        "node_id": NODE_ID,
        "free_space": free,
        "total_space": total,
        "chunk_count": len(chunks)
    }

@app.put("/api/v1/chunks/{chunk_id}")
async def put_chunk(
    chunk_id: UUID,
    file: UploadFile,
    replicate_to: Optional[str] = Query(None, description="URL del siguiente nodo en el pipeline")
):
    """
    Almacena un chunk con replicación en pipeline.
    """
    logger.info(f"Recibiendo chunk: {chunk_id}, replicate_to: {replicate_to}")
    
    chunk_path = STORAGE_PATH / f"{chunk_id}.chunk"
    checksum_path = STORAGE_PATH / f"{chunk_id}.checksum"
    
    try:
        # Leer chunk
        chunk_data = await file.read()
        
        # Calcular checksum
        checksum = calculate_checksum(chunk_data)
        
        # Guardar chunk localmente PRIMERO
        with open(chunk_path, 'wb') as f:
            f.write(chunk_data)
        
        # Guardar checksum
        with open(checksum_path, 'w') as f:
            f.write(checksum)
        
        logger.info(f"Chunk almacenado localmente: {chunk_id}, size: {len(chunk_data)}, checksum: {checksum}")
        
        # Pipeline replication: replicar al siguiente nodo si está especificado
        replicated_nodes = [NODE_ID]  # Este nodo
        
        if replicate_to:
            try:
                logger.info(f"Replicando chunk {chunk_id} a {replicate_to}")
                async with httpx.AsyncClient(timeout=60.0) as client:
                    files_to_send = {'file': ('chunk', chunk_data, 'application/octet-stream')}
                    
                    # Extraer siguiente nodo si hay más en la cadena
                    next_nodes = replicate_to.split('|')
                    current_target = next_nodes[0]
                    remaining_chain = '|'.join(next_nodes[1:]) if len(next_nodes) > 1 else None
                    
                    params = {}
                    if remaining_chain:
                        params['replicate_to'] = remaining_chain
                    
                    response = await client.put(
                        f"{current_target}/api/v1/chunks/{chunk_id}",
                        files=files_to_send,
                        params=params,
                        timeout=60.0
                    )
                    
                    if response.status_code == 200:
                        result = response.json()
                        # Agregar nodos downstream que confirmaron escritura
                        downstream_nodes = result.get('nodes', [])
                        replicated_nodes.extend(downstream_nodes)
                        logger.info(f"Replicación exitosa, total nodos: {len(replicated_nodes)}")
                    else:
                        logger.error(f"Error replicando a {current_target}: {response.status_code}")
            except Exception as e:
                logger.error(f"Excepción replicando a {replicate_to}: {e}")
        
        return {
            "status": "stored",
            "chunk_id": str(chunk_id),
            "size": len(chunk_data),
            "checksum": checksum,
            "node_id": NODE_ID,
            "nodes": replicated_nodes  # Lista de todos los nodos que almacenaron
        }
    
    except Exception as e:
        logger.error(f"Error almacenando chunk {chunk_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Error almacenando chunk: {str(e)}")

@app.get("/api/v1/chunks/{chunk_id}")
async def get_chunk(chunk_id: UUID):
    """Recupera un chunk"""
    logger.info(f"Sirviendo chunk: {chunk_id}")
    
    chunk_path = STORAGE_PATH / f"{chunk_id}.chunk"
    checksum_path = STORAGE_PATH / f"{chunk_id}.checksum"
    
    if not chunk_path.exists():
        raise HTTPException(status_code=404, detail=f"Chunk no encontrado: {chunk_id}")
    
    # Verificar checksum
    try:
        with open(chunk_path, 'rb') as f:
            chunk_data = f.read()
        
        calculated_checksum = calculate_checksum(chunk_data)
        
        if checksum_path.exists():
            with open(checksum_path, 'r') as f:
                stored_checksum = f.read().strip()
            
            if calculated_checksum != stored_checksum:
                logger.error(f"Checksum mismatch para chunk {chunk_id}")
                raise HTTPException(status_code=500, detail="Checksum verification failed")
        
        # Streaming response
        async def chunk_generator():
            yield chunk_data
        
        return StreamingResponse(
            chunk_generator(),
            media_type="application/octet-stream",
            headers={
                "X-Chunk-ID": str(chunk_id),
                "X-Checksum": calculated_checksum,
                "Content-Length": str(len(chunk_data))
            }
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error sirviendo chunk {chunk_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Error leyendo chunk: {str(e)}")

@app.delete("/api/v1/chunks/{chunk_id}")
async def delete_chunk(chunk_id: UUID):
    """Elimina un chunk"""
    logger.info(f"Eliminando chunk: {chunk_id}")
    
    chunk_path = STORAGE_PATH / f"{chunk_id}.chunk"
    checksum_path = STORAGE_PATH / f"{chunk_id}.checksum"
    
    deleted = False
    
    if chunk_path.exists():
        chunk_path.unlink()
        deleted = True
    
    if checksum_path.exists():
        checksum_path.unlink()
    
    if not deleted:
        raise HTTPException(status_code=404, detail=f"Chunk no encontrado: {chunk_id}")
    
    return {"status": "deleted", "chunk_id": str(chunk_id)}

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
                    logger.debug(f"Heartbeat enviado OK")
                else:
                    logger.warning(f"Heartbeat falló: {response.status_code}")
        
        except Exception as e:
            logger.error(f"Error en heartbeat: {e}")
        
        await asyncio.sleep(10)

@app.on_event("startup")
async def startup():
    logger.info(f"DataNode {NODE_ID} iniciando en puerto {PORT}")
    logger.info(f"Storage: {STORAGE_PATH}")
    asyncio.create_task(send_heartbeat())

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
