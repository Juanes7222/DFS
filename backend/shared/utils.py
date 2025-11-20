"""
Utilidades compartidas para el DFS
"""
import hashlib
from typing import BinaryIO


def calculate_checksum(data: bytes) -> str:
    """Calcula SHA256 checksum de datos"""
    return hashlib.sha256(data).hexdigest()


def calculate_file_checksum(file_obj: BinaryIO, chunk_size: int = 8192) -> str:
    """Calcula SHA256 checksum de un archivo"""
    sha256 = hashlib.sha256()
    while True:
        chunk = file_obj.read(chunk_size)
        if not chunk:
            break
        sha256.update(chunk)
    return sha256.hexdigest()


def format_bytes(bytes_value: int) -> str:
    """Formatea bytes en formato legible"""
    value = float(bytes_value)  # Convierte a float para evitar error de Pylance
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if value < 1024.0:
            return f"{value:.2f} {unit}"
        value /= 1024.0
    return f"{value:.2f} PB"


def split_into_chunks(file_path: str, chunk_size: int):
    """
    Generador que divide un archivo en chunks
    
    Yields:
        tuple: (chunk_index, chunk_data)
    """
    with open(file_path, 'rb') as f:
        chunk_index = 0
        while True:
            chunk_data = f.read(chunk_size)
            if not chunk_data:
                break
            yield chunk_index, chunk_data
            chunk_index += 1
