"""
Tests End-to-End para el DFS
"""
import asyncio
import os
import tempfile
from pathlib import Path

import pytest
import httpx

import sys
sys.path.append('/home/ubuntu/dfs-system')
sys.path.append('/home/ubuntu/dfs-system/client')
from dfs_client import DFSClient
from shared import calculate_checksum


# Configuración
METADATA_URL = os.getenv("METADATA_URL", "http://localhost:8000")


@pytest.fixture
def client():
    """Fixture para el cliente DFS"""
    return DFSClient(METADATA_URL)


@pytest.fixture
def temp_file():
    """Fixture para crear un archivo temporal"""
    with tempfile.NamedTemporaryFile(mode='wb', delete=False) as f:
        # Crear archivo de 10MB
        data = b'x' * (10 * 1024 * 1024)
        f.write(data)
        temp_path = f.name
    
    yield temp_path
    
    # Cleanup
    if os.path.exists(temp_path):
        os.unlink(temp_path)


@pytest.mark.asyncio
async def test_health_check():
    """Test: Health check del Metadata Service"""
    async with httpx.AsyncClient() as http_client:
        response = await http_client.get(f"{METADATA_URL}/api/v1/health")
        assert response.status_code == 200
        
        data = response.json()
        assert 'status' in data
        assert data['status'] in ['healthy', 'degraded']


@pytest.mark.asyncio
async def test_list_nodes(client):
    """Test: Listar nodos"""
    nodes = await client.get_nodes()
    
    # Debe haber al menos 1 nodo
    assert len(nodes) >= 1
    
    # Verificar estructura
    for node in nodes:
        assert node.node_id
        assert node.host
        assert node.port > 0
        assert node.free_space >= 0
        assert node.total_space > 0


@pytest.mark.asyncio
async def test_upload_download(client, temp_file):
    """Test: Upload y download de archivo"""
    remote_path = "/test/file1.bin"
    
    # Upload
    success = await client.upload(temp_file, remote_path)
    assert success, "Upload debe ser exitoso"
    
    # Verificar que el archivo existe
    files = await client.list_files()
    paths = [f.path for f in files]
    assert remote_path in paths, "Archivo debe estar en la lista"
    
    # Download
    with tempfile.NamedTemporaryFile(mode='wb', delete=False) as f:
        download_path = f.name
    
    try:
        success = await client.download(remote_path, download_path)
        assert success, "Download debe ser exitoso"
        
        # Verificar checksums
        with open(temp_file, 'rb') as f:
            original_checksum = calculate_checksum(f.read())
        
        with open(download_path, 'rb') as f:
            downloaded_checksum = calculate_checksum(f.read())
        
        assert original_checksum == downloaded_checksum, "Checksums deben coincidir"
    
    finally:
        if os.path.exists(download_path):
            os.unlink(download_path)
    
    # Cleanup: eliminar archivo
    await client.delete(remote_path, permanent=True)


@pytest.mark.asyncio
async def test_list_files(client, temp_file):
    """Test: Listar archivos con filtros"""
    # Subir algunos archivos
    files_to_upload = [
        ("/test/dir1/file1.txt", temp_file),
        ("/test/dir1/file2.txt", temp_file),
        ("/test/dir2/file3.txt", temp_file),
    ]
    
    for remote_path, local_path in files_to_upload:
        await client.upload(local_path, remote_path)
    
    # Listar todos
    all_files = await client.list_files()
    assert len(all_files) >= 3
    
    # Listar con prefijo
    dir1_files = await client.list_files(prefix="/test/dir1")
    assert len(dir1_files) >= 2
    
    # Cleanup
    for remote_path, _ in files_to_upload:
        await client.delete(remote_path, permanent=True)


@pytest.mark.asyncio
async def test_delete_file(client, temp_file):
    """Test: Eliminar archivo"""
    remote_path = "/test/to_delete.bin"
    
    # Upload
    await client.upload(temp_file, remote_path)
    
    # Verificar que existe
    files = await client.list_files()
    paths = [f.path for f in files]
    assert remote_path in paths
    
    # Soft delete
    success = await client.delete(remote_path, permanent=False)
    assert success
    
    # Verificar que ya no aparece en lista
    files = await client.list_files()
    paths = [f.path for f in files]
    assert remote_path not in paths
    
    # Permanent delete
    await client.delete(remote_path, permanent=True)


@pytest.mark.asyncio
async def test_large_file(client):
    """Test: Subir y descargar archivo grande (>100MB)"""
    # Crear archivo grande
    with tempfile.NamedTemporaryFile(mode='wb', delete=False) as f:
        # 100MB
        chunk = b'x' * (1024 * 1024)  # 1MB
        for _ in range(100):
            f.write(chunk)
        large_file = f.name
    
    remote_path = "/test/large_file.bin"
    
    try:
        # Upload
        success = await client.upload(large_file, remote_path)
        assert success
        
        # Download
        with tempfile.NamedTemporaryFile(mode='wb', delete=False) as f:
            download_path = f.name
        
        try:
            success = await client.download(remote_path, download_path)
            assert success
            
            # Verificar tamaño
            original_size = os.path.getsize(large_file)
            downloaded_size = os.path.getsize(download_path)
            assert original_size == downloaded_size
        
        finally:
            if os.path.exists(download_path):
                os.unlink(download_path)
        
        # Cleanup
        await client.delete(remote_path, permanent=True)
    
    finally:
        if os.path.exists(large_file):
            os.unlink(large_file)


@pytest.mark.asyncio
async def test_concurrent_uploads(client, temp_file):
    """Test: Uploads concurrentes"""
    remote_paths = [f"/test/concurrent_{i}.bin" for i in range(5)]
    
    # Upload concurrente
    tasks = [
        client.upload(temp_file, remote_path)
        for remote_path in remote_paths
    ]
    
    results = await asyncio.gather(*tasks)
    assert all(results), "Todos los uploads deben ser exitosos"
    
    # Verificar
    files = await client.list_files()
    paths = [f.path for f in files]
    
    for remote_path in remote_paths:
        assert remote_path in paths
    
    # Cleanup
    for remote_path in remote_paths:
        await client.delete(remote_path, permanent=True)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
