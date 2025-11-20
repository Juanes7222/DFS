"""Excepciones personalizadas para el sistema DFS"""


class DFSError(Exception):
    """Excepción base para todos los errores del DFS"""

    pass


class DFSClientError(DFSError):
    """Error del lado del cliente"""

    pass


class DFSMetadataError(DFSError):
    """Error del servicio de metadatos"""

    pass


class DFSStorageError(DFSError):
    """Error de almacenamiento"""

    pass


class DFSNodeUnavailableError(DFSError):
    """Nodo no disponible"""

    pass


class DFSChunkNotFoundError(DFSError):
    """Chunk no encontrado"""

    pass


class DFSLeaseConflictError(DFSError):
    """Conflicto de lease"""

    pass


class DFSSecurityError(DFSError):
    """Error de seguridad"""

    pass


class DFSConfigurationError(DFSError):
    """Error de configuración"""

    pass
