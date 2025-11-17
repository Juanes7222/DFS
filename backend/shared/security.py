"""
Módulo de seguridad para DFS
Proporciona autenticación JWT y utilidades para mTLS
"""
import os
from datetime import datetime, timedelta
from typing import Optional

try:
    import jwt
    from jwt import PyJWTError
    JWT_AVAILABLE = True
except ImportError:
    JWT_AVAILABLE = False

from pydantic import BaseModel


# Configuración
JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "CHANGE-ME-IN-PRODUCTION")
JWT_ALGORITHM = "HS256"
JWT_EXPIRATION_MINUTES = int(os.getenv("JWT_EXPIRATION_MINUTES", "60"))


class TokenData(BaseModel):
    """Datos contenidos en un token JWT"""
    username: str
    client_id: str
    permissions: list[str] = []
    expires_at: datetime


class JWTManager:
    """Gestor de tokens JWT para autenticación"""
    
    def __init__(self, secret_key: str = JWT_SECRET_KEY, algorithm: str = JWT_ALGORITHM):
        if not JWT_AVAILABLE:
            raise RuntimeError("PyJWT no está instalado. Instalar con: pip install pyjwt")
        
        self.secret_key = secret_key
        self.algorithm = algorithm
    
    def create_token(
        self,
        username: str,
        client_id: str,
        permissions: list[str] = None,
        expires_delta: Optional[timedelta] = None
    ) -> str:
        """
        Crea un token JWT.
        
        Args:
            username: Nombre de usuario
            client_id: ID del cliente
            permissions: Lista de permisos (ej: ["read", "write"])
            expires_delta: Tiempo de expiración personalizado
        
        Returns:
            Token JWT como string
        """
        if expires_delta is None:
            expires_delta = timedelta(minutes=JWT_EXPIRATION_MINUTES)
        
        expires_at = datetime.utcnow() + expires_delta
        
        payload = {
            "sub": username,
            "client_id": client_id,
            "permissions": permissions or [],
            "exp": expires_at,
            "iat": datetime.utcnow()
        }
        
        token = jwt.encode(payload, self.secret_key, algorithm=self.algorithm)
        return token
    
    def verify_token(self, token: str) -> Optional[TokenData]:
        """
        Verifica y decodifica un token JWT.
        
        Args:
            token: Token JWT a verificar
        
        Returns:
            TokenData si el token es válido, None si no
        """
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
            
            return TokenData(
                username=payload.get("sub"),
                client_id=payload.get("client_id"),
                permissions=payload.get("permissions", []),
                expires_at=datetime.fromtimestamp(payload.get("exp"))
            )
        except PyJWTError as e:
            print(f"Error verificando token: {e}")
            return None
    
    def has_permission(self, token: str, required_permission: str) -> bool:
        """
        Verifica si un token tiene un permiso específico.
        
        Args:
            token: Token JWT
            required_permission: Permiso requerido (ej: "write")
        
        Returns:
            True si tiene el permiso, False si no
        """
        token_data = self.verify_token(token)
        if not token_data:
            return False
        
        return required_permission in token_data.permissions


# Middleware de FastAPI para autenticación JWT
try:
    from fastapi import HTTPException, Security, status
    from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
    
    security = HTTPBearer()
    jwt_manager = JWTManager()
    
    async def verify_jwt_token(credentials: HTTPAuthorizationCredentials = Security(security)) -> TokenData:
        """
        Dependency de FastAPI para verificar tokens JWT.
        
        Uso:
            @app.get("/protected")
            async def protected_route(token_data: TokenData = Depends(verify_jwt_token)):
                return {"user": token_data.username}
        """
        token = credentials.credentials
        token_data = jwt_manager.verify_token(token)
        
        if not token_data:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token inválido o expirado",
                headers={"WWW-Authenticate": "Bearer"},
            )
        
        return token_data
    
    def require_permission(permission: str):
        """
        Dependency de FastAPI para verificar permisos.
        
        Uso:
            @app.post("/files/upload")
            async def upload(token_data: TokenData = Depends(require_permission("write"))):
                ...
        """
        async def permission_checker(token_data: TokenData = Security(verify_jwt_token)) -> TokenData:
            if permission not in token_data.permissions:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"Permiso requerido: {permission}"
                )
            return token_data
        
        return permission_checker

except ImportError:
    # FastAPI no disponible
    pass


# Utilidades para mTLS
class MTLSConfig:
    """Configuración para mTLS (mutual TLS)"""
    
    def __init__(
        self,
        ca_cert_path: str = None,
        server_cert_path: str = None,
        server_key_path: str = None,
        client_cert_path: str = None,
        client_key_path: str = None
    ):
        """
        Inicializa configuración de mTLS.
        
        Args:
            ca_cert_path: Path al certificado CA
            server_cert_path: Path al certificado del servidor
            server_key_path: Path a la clave privada del servidor
            client_cert_path: Path al certificado del cliente
            client_key_path: Path a la clave privada del cliente
        """
        self.ca_cert_path = ca_cert_path or os.getenv("DFS_CA_CERT", "/etc/dfs/certs/ca.crt")
        self.server_cert_path = server_cert_path or os.getenv("DFS_SERVER_CERT", "/etc/dfs/certs/server.crt")
        self.server_key_path = server_key_path or os.getenv("DFS_SERVER_KEY", "/etc/dfs/certs/server.key")
        self.client_cert_path = client_cert_path or os.getenv("DFS_CLIENT_CERT", "/etc/dfs/certs/client.crt")
        self.client_key_path = client_key_path or os.getenv("DFS_CLIENT_KEY", "/etc/dfs/certs/client.key")
    
    def get_server_ssl_context(self):
        """
        Obtiene contexto SSL para servidor.
        Requiere que el cliente presente un certificado válido.
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
        Presenta certificado al servidor.
        """
        import ssl
        
        context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        context.load_cert_chain(self.client_cert_path, self.client_key_path)
        context.load_verify_locations(self.ca_cert_path)
        context.check_hostname = True
        context.verify_mode = ssl.CERT_REQUIRED
        
        return context
    
    def get_uvicorn_ssl_config(self) -> dict:
        """
        Obtiene configuración SSL para Uvicorn.
        
        Uso:
            mtls = MTLSConfig()
            uvicorn.run(app, **mtls.get_uvicorn_ssl_config())
        """
        return {
            "ssl_keyfile": self.server_key_path,
            "ssl_certfile": self.server_cert_path,
            "ssl_ca_certs": self.ca_cert_path,
            "ssl_cert_reqs": 2  # ssl.CERT_REQUIRED
        }
    
    def get_httpx_ssl_config(self) -> dict:
        """
        Obtiene configuración SSL para httpx client.
        
        Uso:
            mtls = MTLSConfig()
            async with httpx.AsyncClient(**mtls.get_httpx_ssl_config()) as client:
                response = await client.get("https://...")
        """
        import ssl
        
        return {
            "verify": self.ca_cert_path,
            "cert": (self.client_cert_path, self.client_key_path)
        }


# Script para generar certificados de desarrollo
GENERATE_CERTS_SCRIPT = """#!/bin/bash
# Script para generar certificados autofirmados para desarrollo
# NO USAR EN PRODUCCIÓN - usar certificados de una CA real

set -e

CERTS_DIR=${1:-./certs}
mkdir -p $CERTS_DIR

echo "Generando certificados en $CERTS_DIR"

# Generar CA
openssl genrsa -out $CERTS_DIR/ca.key 4096
openssl req -new -x509 -days 365 -key $CERTS_DIR/ca.key -out $CERTS_DIR/ca.crt \\
  -subj "/C=US/ST=State/L=City/O=DFS/CN=DFS-CA"

# Generar certificado del servidor
openssl genrsa -out $CERTS_DIR/server.key 4096
openssl req -new -key $CERTS_DIR/server.key -out $CERTS_DIR/server.csr \\
  -subj "/C=US/ST=State/L=City/O=DFS/CN=dfs-metadata"
openssl x509 -req -days 365 -in $CERTS_DIR/server.csr \\
  -CA $CERTS_DIR/ca.crt -CAkey $CERTS_DIR/ca.key -CAcreateserial \\
  -out $CERTS_DIR/server.crt

# Generar certificado del cliente
openssl genrsa -out $CERTS_DIR/client.key 4096
openssl req -new -key $CERTS_DIR/client.key -out $CERTS_DIR/client.csr \\
  -subj "/C=US/ST=State/L=City/O=DFS/CN=dfs-client"
openssl x509 -req -days 365 -in $CERTS_DIR/client.csr \\
  -CA $CERTS_DIR/ca.crt -CAkey $CERTS_DIR/ca.key -CAcreateserial \\
  -out $CERTS_DIR/client.crt

# Limpiar archivos temporales
rm $CERTS_DIR/*.csr
rm $CERTS_DIR/*.srl

echo "Certificados generados exitosamente en $CERTS_DIR"
echo "Archivos:"
ls -lh $CERTS_DIR/
"""


def save_generate_certs_script(path: str = "./generate_certs.sh"):
    """Guarda el script de generación de certificados"""
    with open(path, 'w') as f:
        f.write(GENERATE_CERTS_SCRIPT)
    
    os.chmod(path, 0o755)
    print(f"Script guardado en: {path}")
    print("Ejecutar con: ./generate_certs.sh [directorio]")


if __name__ == "__main__":
    # Ejemplo de uso
    if JWT_AVAILABLE:
        # Crear token
        manager = JWTManager()
        token = manager.create_token(
            username="admin",
            client_id="client-123",
            permissions=["read", "write", "delete"]
        )
        print(f"Token generado: {token}")
        
        # Verificar token
        token_data = manager.verify_token(token)
        if token_data:
            print(f"Token válido para: {token_data.username}")
            print(f"Permisos: {token_data.permissions}")
        
        # Verificar permiso
        has_write = manager.has_permission(token, "write")
        print(f"Tiene permiso 'write': {has_write}")
    
    # Guardar script de generación de certificados
    save_generate_certs_script()
