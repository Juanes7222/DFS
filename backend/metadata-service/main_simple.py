"""
Metadata Service simplificado
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from datetime import datetime
from typing import List
from uuid import uuid4

from shared.models import (
    FileMetadata, NodeInfo, ChunkTarget, ChunkEntry, ReplicaInfo,
    UploadInitRequest, CommitRequest, HeartbeatRequest, ChunkState
)

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="DFS Metadata Service", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Storage en memoria
files_db = {}
nodes_db = {}

REPLICATION_FACTOR = 3
CHUNK_SIZE = 67108864

@app.get("/api/v1/health")
async def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "details": {
            "total_nodes": len(nodes_db),
            "active_nodes": len([n for n in nodes_db.values() if n.state == "active"]),
            "replication_factor": REPLICATION_FACTOR
        }
    }

@app.get("/api/v1/nodes", response_model=List[NodeInfo])
async def list_nodes():
    return list(nodes_db.values())

@app.post("/api/v1/nodes/heartbeat")
async def receive_heartbeat(request: HeartbeatRequest):
    node_id = request.node_id
    
    if node_id in nodes_db:
        node = nodes_db[node_id]
        node.free_space = request.free_space
        node.total_space = request.total_space
        node.chunk_count = len(request.chunk_ids)
        node.last_heartbeat = datetime.utcnow()
        node.state = "active"
    else:
        parts = node_id.split('-')
        host = parts[1] if len(parts) > 1 else "localhost"
        port = int(parts[2]) if len(parts) > 2 else 8001
        
        node = NodeInfo(
            node_id=node_id,
            host=host,
            port=port,
            free_space=request.free_space,
            total_space=request.total_space,
            chunk_count=len(request.chunk_ids),
            last_heartbeat=datetime.utcnow(),
            state="active"
        )
        nodes_db[node_id] = node
    
    logger.info(f"Heartbeat: {node_id}")
    return {"status": "ok"}

@app.post("/api/v1/files/upload-init")
async def upload_init(request: UploadInitRequest):
    active_nodes = [n for n in nodes_db.values() if n.state == "active"]
    if len(active_nodes) < REPLICATION_FACTOR:
        raise HTTPException(
            status_code=503,
            detail=f"Insuficientes nodos: {len(active_nodes)}/{REPLICATION_FACTOR}"
        )
    
    num_chunks = (request.size + request.chunk_size - 1) // request.chunk_size
    chunks = []
    
    for i in range(num_chunks):
        chunk_size = min(request.chunk_size, request.size - i * request.chunk_size)
        chunk_id = uuid4()
        
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
    logger.info(f"Upload init: {request.path}")
    
    return {
        "file_id": str(file_id),
        "chunks": chunks
    }

@app.post("/api/v1/files/commit")
async def upload_commit(request: CommitRequest):
    chunks = []
    for i, c in enumerate(request.chunks):
        replicas = [
            ReplicaInfo(
                node_id=node_id,
                url=f"http://{node_id}",
                state=ChunkState.COMMITTED,
                checksum_verified=True
            )
            for node_id in c.nodes
        ]
        
        chunks.append(ChunkEntry(
            chunk_id=c.chunk_id,
            seq_index=i,
            size=0,
            checksum=c.checksum,
            replicas=replicas
        ))
    
    file_metadata = FileMetadata(
        file_id=request.file_id,
        path=f"/uploaded/{request.file_id}",
        size=0,
        chunks=chunks,
        is_deleted=False
    )
    
    files_db[str(request.file_id)] = file_metadata
    logger.info(f"Upload commit: {request.file_id}")
    
    return {"status": "committed", "file_id": str(request.file_id)}

@app.get("/api/v1/files")
async def list_files(prefix: str = "", limit: int = 1000, offset: int = 0):
    all_files = [f for f in files_db.values() if not f.is_deleted]
    if prefix:
        all_files = [f for f in all_files if f.path.startswith(prefix)]
    all_files.sort(key=lambda f: f.path)
    return all_files[offset:offset + limit]

@app.get("/api/v1/files/{path:path}")
async def get_file(path: str):
    for file in files_db.values():
        if file.path == f"/{path}":
            return file
    raise HTTPException(status_code=404, detail="Archivo no encontrado")

@app.delete("/api/v1/files/{path:path}")
async def delete_file(path: str, permanent: bool = False):
    for file_id, file in files_db.items():
        if file.path == f"/{path}":
            if permanent:
                del files_db[file_id]
            else:
                file.is_deleted = True
            return {"status": "deleted", "path": f"/{path}"}
    raise HTTPException(status_code=404, detail="Archivo no encontrado")

if __name__ == "__main__":
    logger.info("Iniciando Metadata Service en puerto 8000...")
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
