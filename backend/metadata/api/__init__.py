"""
API routers para el Metadata Service
"""

from metadata.api.files import router as file_router
from metadata.api.nodes import router as node_router
from metadata.api.leases import router as lease_router
from metadata.api.system import router as system_router

__all__ = [
    "file_router",
    "node_router",
    "lease_router",
    "system_router",
]
