"""
Tests End-to-End para el DFS - Versión refactorizada
"""

import asyncio
import os
import tempfile
from pathlib import Path

import pytest
import httpx

# Importar desde la nueva estructura
from client import DFSClient
from shared import calculate_checksum
from core.config import config


@pytest.fixture
def client():
    """Fixture para el cliente DFS"""
    return DFSClient()


@pytest.fixture
def temp_file():
    """Fixture para crear un archivo temporal"""
    with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.bin') as f:
        # Crear archivo de 1MB (más rápido para tests)
        data = b'x' * (1 * 1024 * 1024)
        f.write(data)
        temp_path = f.name
    
    yield temp_path
    
    # Cleanup
    if os.path.exists(temp_path):
        os.unlink(temp_path)


@pytest.fixture(autouse=True)
async def cleanup_files(client):
    """Fixture para limpiar archivos después de cada test"""
    # Ejecutar el test
    yield
    
    # Limpiar archivos de test
    try:
        files = await client.list_files(prefix="/test/")
        for file in files:
            await client.delete(file.path, permanent=True)
    except Exception:
        pass  # Ignorar errores de cleanup


@pytest.mark.asyncio
async def test_health_check():
    """Test: Health check del Metadata Service"""
    async with httpx.AsyncClient() as http_client:
        response = await http_client.get(f"{config.metadata_url}/api/v1/health")
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
    files = await client.list_files(prefix="/test/")
    paths = [f.path for f in files]
    assert remote_path in paths, "Archivo debe estar en la lista"
    
    # Download
    with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.bin') as f:
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
        
        # Verificar tamaño
        original_size = os.path.getsize(temp_file)
        downloaded_size = os.path.getsize(download_path)
        assert original_size == downloaded_size, "Tamaños deben coincidir"
    
    finally:
        if os.path.exists(download_path):
            os.unlink(download_path)


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
        success = await client.upload(local_path, remote_path)
        assert success, f"Upload de {remote_path} debe ser exitoso"
    
    # Listar todos los archivos de test
    all_files = await client.list_files(prefix="/test/")
    assert len(all_files) >= 3
    
    # Listar con prefijo específico
    dir1_files = await client.list_files(prefix="/test/dir1")
    assert len(dir1_files) == 2
    
    dir2_files = await client.list_files(prefix="/test/dir2")
    assert len(dir2_files) == 1


@pytest.mark.asyncio
async def test_delete_file(client, temp_file):
    """Test: Eliminar archivo"""
    remote_path = "/test/to_delete.bin"
    
    # Upload
    success = await client.upload(temp_file, remote_path)
    assert success
    
    # Verificar que existe
    files = await client.list_files(prefix="/test/")
    paths = [f.path for f in files]
    assert remote_path in paths
    
    # Soft delete
    success = await client.delete(remote_path, permanent=False)
    assert success
    
    # Verificar que ya no aparece en lista normal
    files = await client.list_files(prefix="/test/")
    paths = [f.path for f in files]
    assert remote_path not in paths
    
    # Permanent delete
    success = await client.delete(remote_path, permanent=True)
    assert success


@pytest.mark.asyncio
async def test_large_file(client):
    """Test: Subir y descargar archivo grande (10MB)"""
    # Crear archivo más grande
    with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.bin') as f:
        # 10MB (más rápido que 100MB para tests)
        chunk = b'x' * (1024 * 1024)  # 1MB
        for _ in range(10):
            f.write(chunk)
        large_file = f.name
    
    remote_path = "/test/large_file.bin"
    
    try:
        # Upload con progreso
        upload_success = False
        def progress_callback(progress):
            nonlocal upload_success
            # Solo verificar que el callback se ejecuta
            assert 0 <= progress <= 100
        
        upload_success = await client.upload(large_file, remote_path, progress_callback)
        assert upload_success
        
        # Download con progreso
        with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.bin') as f:
            download_path = f.name
        
        try:
            download_success = False
            def download_progress(progress):
                nonlocal download_success
                assert 0 <= progress <= 100
            
            download_success = await client.download(remote_path, download_path, download_progress)
            assert download_success
            
            # Verificar tamaño
            original_size = os.path.getsize(large_file)
            downloaded_size = os.path.getsize(download_path)
            assert original_size == downloaded_size
            
            # Verificar contenido
            with open(large_file, 'rb') as f1, open(download_path, 'rb') as f2:
                assert f1.read() == f2.read()
        
        finally:
            if os.path.exists(download_path):
                os.unlink(download_path)
    
    finally:
        if os.path.exists(large_file):
            os.unlink(large_file)


@pytest.mark.asyncio
async def test_concurrent_uploads(client, temp_file):
    """Test: Uploads concurrentes"""
    remote_paths = [f"/test/concurrent_{i}.bin" for i in range(3)]  # Reducido para mayor velocidad
    
    # Upload concurrente
    tasks = [
        client.upload(temp_file, remote_path)
        for remote_path in remote_paths
    ]
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Verificar que no hay excepciones
    for result in results:
        if isinstance(result, Exception):
            raise result
        assert result, "Todos los uploads deben ser exitosos"
    
    # Verificar que todos los archivos existen
    files = await client.list_files(prefix="/test/concurrent")
    paths = [f.path for f in files]
    
    for remote_path in remote_paths:
        assert remote_path in paths


@pytest.mark.asyncio
async def test_file_info(client, temp_file):
    """Test: Obtener información detallada de archivo"""
    remote_path = "/test/info_test.bin"
    
    # Upload
    success = await client.upload(temp_file, remote_path)
    assert success
    
    try:
        # Obtener información
        files = await client.list_files(prefix=remote_path)
        assert len(files) == 1
        
        file_info = files[0]
        assert file_info.path == remote_path
        assert file_info.size > 0
        assert len(file_info.chunks) > 0
        
        # Verificar estructura de chunks
        for chunk in file_info.chunks:
            assert chunk.chunk_id
            assert chunk.size > 0
            assert len(chunk.replicas) > 0
            
            for replica in chunk.replicas:
                assert replica.node_id
                assert replica.url
                assert replica.state.value in ['pending', 'committed']
    
    finally:
        await client.delete(remote_path, permanent=True)


@pytest.mark.asyncio
async def test_system_status(client):
    """Test: Verificar estado del sistema"""
    health = await client.health()
    
    assert 'status' in health
    assert health['status'] in ['healthy', 'degraded', 'error']
    
    if 'details' in health:
        details = health['details']
        if 'total_nodes' in details:
            assert details['total_nodes'] >= 0
        if 'active_nodes' in details:
            assert details['active_nodes'] >= 0


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])