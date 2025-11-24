"""
Módulo de seguridad para DFS - Versión refactorizada
"""

import os
from datetime import datetime, timedelta, timezone
from typing import Optional, Any, cast, List

from core.config import config
from core.exceptions import DFSSecurityError

try:
    import jwt  # type: ignore
    from jwt import PyJWTError  # type: ignore

    JWT_AVAILABLE = True
except ImportError:
    JWT_AVAILABLE = False
    jwt = None  # type: ignore
    PyJWTError = Exception  # type: ignore

try:
    from fastapi import HTTPException, Security, status, Depends
    from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False

from pydantic import BaseModel, Field


class TokenData(BaseModel):
    """Datos contenidos en un token JWT"""

    username: str
    client_id: str
    permissions: List[str] = Field(default_factory=list)
    expires_at: datetime


class JWTManager:
    """Gestor de tokens JWT para autenticación"""

    def __init__(self, secret_key: Optional[str] = None, algorithm: str = "HS256"):
        if not JWT_AVAILABLE:
            raise DFSSecurityError(
                "PyJWT no está instalado. Instalar con: pip install pyjwt"
            )

        # preferimos que exista una clave en config o pasada explícitamente;
        # si no existe, lanzamos para evitar usar None más adelante
        resolved_secret = secret_key or getattr(config, "jwt_secret_key", None)
        if not resolved_secret:
            raise DFSSecurityError(
                "Secret key para JWT no está configurada (config.jwt_secret_key faltante)."
            )

        self.secret_key: str = resolved_secret
        self.algorithm = algorithm

    def create_token(
        self,
        username: str,
        client_id: str,
        permissions: Optional[List[str]] = None,
        expires_delta: Optional[timedelta] = None,
    ) -> str:
        """
        Crea un token JWT.
        """
        if expires_delta is None:
            expires_delta = timedelta(minutes=60)

        expires_at = datetime.now(timezone.utc) + expires_delta

        payload = {
            "sub": username,
            "client_id": client_id,
            "permissions": permissions or [],
            # PyJWT acepta datetime para exp si usa opcionales, pero para consistencia
            # convertimos a timestamp (segundos)
            "exp": int(expires_at.timestamp()),
            "iat": int(datetime.now(timezone.utc).timestamp()),
        }

        # Aseguramos al analizador que jwt no es None aquí
        token = cast(Any, jwt).encode(
            payload, self.secret_key, algorithm=self.algorithm
        )
        # PyJWT >= 2 devuelve str; si obtiene bytes, convertimos:
        if isinstance(token, bytes):
            token = token.decode("utf-8")
        return token

    def verify_token(self, token: str) -> TokenData:
        """
        Verifica y decodifica un token JWT.
        Devuelve TokenData o lanza DFSSecurityError si inválido.
        """
        try:
            # decode normalmente devuelve el payload dict
            payload = cast(Any, jwt).decode(
                token, self.secret_key, algorithms=[self.algorithm]
            )

            # Validaciones adicionales defensivas:
            if payload is None:
                raise DFSSecurityError("Payload vacío después de decodificar token.")

            sub = payload.get("sub")
            client_id = payload.get("client_id")
            permissions = payload.get("permissions", [])
            exp = payload.get("exp")

            if sub is None or client_id is None or exp is None:
                raise DFSSecurityError("Token incompleto (faltan sub/client_id/exp).")

            # exp puede ser timestamp int
            try:
                expires_at = datetime.fromtimestamp(int(exp))
            except Exception:
                raise DFSSecurityError("Formato inválido en campo 'exp' del token.")

            return TokenData(
                username=str(sub),
                client_id=str(client_id),
                permissions=list(permissions),
                expires_at=expires_at,
            )
        except PyJWTError as e:
            # PyJWTError ya está definido (o es Exception si no está instalado)
            raise DFSSecurityError(f"Token inválido: {e}")

    def has_permission(self, token: str, required_permission: str) -> bool:
        """
        Verifica si un token tiene un permiso específico.
        """
        try:
            token_data = self.verify_token(token)
            return required_permission in token_data.permissions
        except DFSSecurityError:
            return False


# Configuración de seguridad global
jwt_manager = JWTManager()

if FASTAPI_AVAILABLE:
    security = HTTPBearer()

    async def verify_jwt_token(
        credentials: HTTPAuthorizationCredentials = Security(security),
    ) -> TokenData:
        """
        Dependency de FastAPI para verificar tokens JWT.
        """
        token = credentials.credentials
        try:
            return jwt_manager.verify_token(token)
        except DFSSecurityError as e:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=str(e),
                headers={"WWW-Authenticate": "Bearer"},
            )

    def require_permission(permission: str):
        """
        Dependency de FastAPI para verificar permisos.
        """

        async def permission_checker(
            token_data: TokenData = Depends(verify_jwt_token),
        ) -> TokenData:
            if permission not in token_data.permissions:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"Permiso requerido: {permission}",
                )
            return token_data

        return permission_checker


class MTLSConfig:
    """Configuración para mTLS (mutual TLS)"""

    def __init__(
        self,
        ca_cert_path: Optional[str] = None,
        server_cert_path: Optional[str] = None,
        server_key_path: Optional[str] = None,
        client_cert_path: Optional[str] = None,
        client_key_path: Optional[str] = None,
    ):
        self.ca_cert_path = ca_cert_path or os.getenv(
            "DFS_CA_CERT", "/etc/dfs/certs/ca.crt"
        )
        self.server_cert_path = server_cert_path or os.getenv(
            "DFS_SERVER_CERT", "/etc/dfs/certs/server.crt"
        )
        self.server_key_path = server_key_path or os.getenv(
            "DFS_SERVER_KEY", "/etc/dfs/certs/server.key"
        )
        self.client_cert_path = client_cert_path or os.getenv(
            "DFS_CLIENT_CERT", "/etc/dfs/certs/client.crt"
        )
        self.client_key_path = client_key_path or os.getenv(
            "DFS_CLIENT_KEY", "/etc/dfs/certs/client.key"
        )

    def get_server_ssl_context(self):
        """
        Obtiene contexto SSL para servidor.
        """
        import ssl

        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        context.load_cert_chain(self.server_cert_path, self.server_key_path)
        context.load_verify_locations(self.ca_cert_path)
        context.verify_mode = ssl.CERT_REQUIRED

        return context

    def get_client_ssl_context(self):
        """
        Obtiene contexto SSL para cliente.
        """
        import ssl

        context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        context.load_cert_chain(self.client_cert_path, self.client_key_path)
        context.load_verify_locations(self.ca_cert_path)
        context.check_hostname = True
        context.verify_mode = ssl.CERT_REQUIRED

        return context
