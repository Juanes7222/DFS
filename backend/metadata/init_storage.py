import logging
from typing import Optional

from core.config import config
from shared.protocols import MetadataStorageBase
from metadata.storage.storage_with_postgress import PostgresMetadataStorage
from metadata.storage.storage_with_sqlite import SQLiteMetadataStorage

logger = logging.getLogger(__name__)


def create_metadata_storage(
    backend: Optional[str] = None,
    **kwargs
) -> MetadataStorageBase:
    """
    Factory para crear la instancia correcta de MetadataStorage.
    
    Args:
        backend: Tipo de backend ('sqlite' o 'postgres'). 
                 Si no se especifica, se lee de config.metadata_backend
        **kwargs: Argumentos adicionales para el constructor
        
    Returns:
        Instancia de MetadataStorageBase
        
    Ejemplo de uso:
        # Usar SQLite
        storage = create_metadata_storage(backend='sqlite', db_path='./data/metadata.db')
        
        # Usar Postgres/Neon
        storage = create_metadata_storage(
            backend='postgres', 
            connection_string='postgresql://...'
        )
        
        # Desde config
        storage = create_metadata_storage()
    """
    backend_storage: str = backend or getattr(config, 'backend_storage_type', 'sqlite')
    
    if backend_storage.lower() == 'sqlite':
        logger.info("Inicializando MetadataStorage con backend SQLite")
        return SQLiteMetadataStorage(**kwargs)
    
    elif backend_storage.lower() in ('postgres', 'postgresql', 'neon'):
        logger.info("Inicializando MetadataStorage con backend PostgreSQL")
        return PostgresMetadataStorage(**kwargs)
    
    else:
        raise ValueError(
            f"Backend no soportado: {backend}. "
            f"Opciones válidas: 'sqlite', 'postgres'"
        )