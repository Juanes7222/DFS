import requests
import uuid
import json
import time
import sys
import logging
import ctypes
import subprocess
import platform
from pathlib import Path
from typing import Optional
from core.config import config
from core.exceptions import RegistrationError

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('datanode_registration.log')
    ]
)
logger = logging.getLogger(__name__)

# Constantes
METADATA_URL = config.metadata_url
NODE_ID_FILE = Path("node_id.txt")
ZEROTIER_NETWORK_ID = config.zerotier_network_id
BOOTSTRAP_TOKEN = config.bootstrap_token
MAX_RETRIES = 10
RETRY_DELAY = 5
HEARTBEAT_INTERVAL = 60


def is_admin():
    """Verifica si el script está corriendo con permisos de administrador."""
    try:
        return ctypes.windll.shell32.IsUserAnAdmin()
    except:
        return False


def request_admin_privileges():
    """Solicita permisos de administrador y reinicia el script."""
    if sys.platform != 'win32':
        return True  # En Linux/Mac, continuar normal
    
    if not is_admin():
        logger.info("Este script requiere permisos de administrador.")
        logger.info("Solicitando elevación de privilegios...")
        
        try:
            # Reiniciar con permisos de admin
            ctypes.windll.shell32.ShellExecuteW(
                None,
                "runas",
                sys.executable,
                f'-m datanode.agent',
                None,
                1
            )
            sys.exit(0)
        except Exception as e:
            logger.error(f"Error al solicitar permisos: {e}")
            logger.error("Por favor, ejecuta este script como administrador.")
            sys.exit(1)
    
    return True


def get_node_id() -> str:
    """
    Genera o recupera un node_id persistente.
    
    Returns:
        str: UUID único del nodo
    """
    try:
        if NODE_ID_FILE.exists():
            node_id = NODE_ID_FILE.read_text().strip()
            if node_id and len(node_id) == 36:  # Validar formato UUID
                logger.info(f"Node ID recuperado: {node_id}")
                return node_id
            else:
                logger.warning("Node ID inválido encontrado, generando uno nuevo")
        
        node_id = str(uuid.uuid4())
        NODE_ID_FILE.write_text(node_id)
        logger.info(f"Nuevo Node ID generado: {node_id}")
        return node_id
        
    except Exception as e:
        logger.error(f"Error manejando node_id: {e}")
        raise

def get_zerotier_ip() -> Optional[str]:
    """
    Obtiene la IP ZeroTier del nodo local.
    
    Intenta múltiples métodos para obtener la IP:
    1. CLI de ZeroTier
    2. Archivo de configuración de ZeroTier
    3. API local de ZeroTier
    
    Returns:
        Optional[str]: Dirección IP de ZeroTier o None si no está disponible
    """
    if not ZEROTIER_NETWORK_ID:
        logger.warning("ZEROTIER_NETWORK_ID no configurado")
        return None
    
    # Método 1: Usar CLI de ZeroTier (más confiable)
    try:
        zt_ip = get_zerotier_ip_from_cli()
        if zt_ip:
            logger.info(f"ZeroTier IP obtenida del CLI: {zt_ip}")
            return zt_ip
    except Exception as e:
        logger.error(f"Error obteniendo IP desde CLI de ZeroTier: {e}")
    
    # Método 2: Leer desde archivo de configuración
    config_path = Path(f"/var/lib/zerotier-one/networks.d/{ZEROTIER_NETWORK_ID}.conf")
    
    try:
        if config_path.exists():
            data = json.loads(config_path.read_text())
            assigned = data.get("assignedAddresses", [])
            
            if assigned:
                zt_ip = assigned[0].split("/")[0]
                logger.info(f"ZeroTier IP obtenida del archivo de configuración: {zt_ip}")
                return zt_ip
            else:
                logger.warning("No hay direcciones asignadas en el archivo de configuración")
    except json.JSONDecodeError as e:
        logger.error(f"Error parseando configuración de ZeroTier: {e}")
    except Exception as e:
        logger.error(f"Error leyendo configuración de ZeroTier: {e}")
    
    # Método 3: Intentar con API local de ZeroTier
    try:
        zt_ip = get_zerotier_ip_from_api()
        if zt_ip:
            logger.info(f"ZeroTier IP obtenida de la API local: {zt_ip}")
            return zt_ip
    except Exception as e:
        logger.error(f"Error obteniendo IP desde API de ZeroTier: {e}")
    
    logger.error("No se pudo obtener la IP de ZeroTier por ningún método")
    return None


def get_zerotier_node_id_from_cli() -> Optional[str]:
    """
    Obtiene el ZeroTier Node ID del sistema local.
    
    Returns:
        Optional[str]: ZeroTier Node ID o None
    """
    try:
        system = platform.system()
        
        if system == "Windows":
            result = subprocess.run(
                "zerotier-cli info",
                capture_output=True,
                text=True,
                shell=True
            )
        else:
            result = subprocess.run(
                ["sudo", "zerotier-cli", "info"],
                capture_output=True,
                text=True
            )
        
        if result.returncode != 0:
            logger.error(f"Error ejecutando zerotier-cli info: {result.stderr}")
            return None
        
        # El formato es: 200 info <node_id> <version> ONLINE
        parts = result.stdout.strip().split()
        if len(parts) >= 3 and parts[0] == "200":
            node_id = parts[2]
            logger.info(f"ZeroTier Node ID encontrado: {node_id}")
            return node_id
        
        logger.warning("No se pudo parsear el ZeroTier Node ID")
        return None
        
    except Exception as e:
        logger.error(f"Error en get_zerotier_node_id_from_cli: {e}")
        return None


def get_zerotier_ip_from_cli() -> Optional[str]:
    """
    Obtiene la IP de ZeroTier usando el CLI.
    
    Returns:
        Optional[str]: Dirección IP o None
    """
    try:
        system = platform.system()
        
        if system == "Windows":
            result = subprocess.run(
                "zerotier-cli listnetworks",
                capture_output=True,
                text=True,
                shell=True
            )
        else:
            result = subprocess.run(
                ["sudo", "zerotier-cli", "listnetworks"],
                capture_output=True,
                text=True
            )
        
        if result.returncode != 0:
            logger.error(f"Error ejecutando zerotier-cli: {result.stderr}")
            return None
        
        # Parsear la salida para encontrar nuestra red
        for line in result.stdout.splitlines():
            if ZEROTIER_NETWORK_ID is None:
                continue
            if ZEROTIER_NETWORK_ID in line:
                # El formato es: 200 listnetworks <netid> <name> <mac> <status> <type> <dev> <ips>
                parts = line.split()
                if len(parts) >= 8:
                    # Las IPs están al final, pueden ser múltiples separadas por comas
                    ips = parts[7].split(',')
                    for ip in ips:
                        ip = ip.strip()
                        # Filtrar solo IPs IPv4 (las de ZeroTier suelen ser del rango específico)
                        if '.' in ip and '/' in ip:
                            # Remover la máscara de subred
                            clean_ip = ip.split('/')[0]
                            logger.info(f"IP encontrada en CLI: {clean_ip}")
                            return clean_ip
        
        logger.warning(f"Red {ZEROTIER_NETWORK_ID} no encontrada en listnetworks")
        return None
        
    except Exception as e:
        logger.error(f"Error en get_zerotier_ip_from_cli: {e}")
        return None


def get_zerotier_ip_from_api() -> Optional[str]:
    """
    Obtiene la IP de ZeroTier usando la API local.
    
    Returns:
        Optional[str]: Dirección IP o None
    """
    try:
        # Rutas del token según el sistema operativo
        system = platform.system()
        if system == "Windows":
            token_path = Path(r"C:\ProgramData\ZeroTier\One\authtoken.secret")
        elif system == "Darwin":
            token_path = Path("/Library/Application Support/ZeroTier/One/authtoken.secret")
        else:  # Linux
            token_path = Path("/var/lib/zerotier-one/authtoken.secret")
        
        if not token_path.exists():
            logger.warning(f"Token file no encontrado: {token_path}")
            return None
        
        token = token_path.read_text().strip()
        headers = {"X-ZT1-Auth": token}
        
        response = requests.get(
            f"http://127.0.0.1:9993/network/{ZEROTIER_NETWORK_ID}",
            headers=headers,
            timeout=3
        )
        
        if response.status_code == 200:
            data = response.json()
            assigned = data.get("assignedAddresses", [])
            if assigned:
                # Tomar la primera IP y remover la máscara de subred
                ip = assigned[0].split("/")[0]
                logger.info(f"IP encontrada en API: {ip}")
                return ip
        else:
            logger.warning(f"API response status: {response.status_code}")
        
        return None
        
    except Exception as e:
        logger.error(f"Error en get_zerotier_ip_from_api: {e}")
        return None

def validate_config() -> None:
    """
    Valida que la configuración necesaria esté presente.
    
    Raises:
        RegistrationError: Si falta configuración crítica
    """
    if not METADATA_URL:
        raise RegistrationError("METADATA_URL no está configurado")
    
    if not config.data_port:
        raise RegistrationError("data_port no está configurado")
    
    if not BOOTSTRAP_TOKEN:
        raise RegistrationError("BOOTSTRAP_TOKEN no está configurado")
    
    logger.info("Configuración validada correctamente")


def register_to_master(node_id: str, zt_ip: Optional[str], data_port: int) -> bool:
    """
    Registra el nodo ante el Metadata Service.
    
    Args:
        node_id: Identificador único del nodo
        zt_ip: Dirección IP de ZeroTier (opcional)
        data_port: Puerto del servicio de datos
        
    Returns:
        bool: True si el registro fue exitoso
    """
    url = f"{METADATA_URL}/api/v1/nodes/register"  # Cambiar la ruta también
    print(METADATA_URL)
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {BOOTSTRAP_TOKEN}"  # Usar formato Bearer
    }
    
    payload = {
        "node_id": node_id,
        "zerotier_ip": zt_ip,  # Cambiar a zerotier_ip
        "data_port": data_port
    }
    
    try:
        logger.info(f"Intentando registrar nodo en {url}")
        logger.debug(f"Payload: {payload}")
        
        response = requests.post(url, json=payload, headers=headers, timeout=10)
        response.raise_for_status()
        
        result = response.json()
        logger.info(f"Nodo registrado exitosamente: {result}")
        return True
        
    except requests.exceptions.Timeout:
        logger.error("Timeout al intentar conectar con el Metadata Service")
        return False
    except requests.exceptions.ConnectionError:
        logger.error(f"No se pudo conectar al Metadata Service en {METADATA_URL}")
        return False
    except requests.exceptions.HTTPError as e:
        logger.error(f"Error HTTP al registrar nodo: {e.response.status_code} - {e.response.text}")
        return False
    except Exception as e:
        logger.error(f"Error inesperado registrando nodo: {e}", exc_info=True)
        return False


def send_heartbeat(node_id: str) -> bool:
    """
    Envía un heartbeat al Metadata Service para mantener el nodo activo.
    
    Args:
        node_id: Identificador único del nodo
        
    Returns:
        bool: True si el heartbeat fue exitoso
    """
    url = f"{METADATA_URL}/api/v1/nodes/{node_id}/heartbeat"
    
    try:
        response = requests.post(url, timeout=5)
        response.raise_for_status()
        logger.debug("Heartbeat enviado exitosamente")
        return True
    except Exception as e:
        logger.warning(f"Error enviando heartbeat: {e}")
        return False


def register_with_retry(node_id: str, zt_ip: Optional[str], data_port: int) -> None:
    """
    Intenta registrar el nodo con reintentos.
    
    Args:
        node_id: Identificador único del nodo
        zt_ip: Dirección IP de ZeroTier
        data_port: Puerto del servicio de datos
        
    Raises:
        RegistrationError: Si no se pudo registrar después de todos los intentos
    """
    for attempt in range(1, MAX_RETRIES + 1):
        logger.info(f"Intento de registro {attempt}/{MAX_RETRIES}")
        
        if register_to_master(node_id, zt_ip, data_port):
            logger.info("Registro completado exitosamente")
            return
        
        if attempt < MAX_RETRIES:
            logger.warning(f"Reintentando en {RETRY_DELAY} segundos...")
            time.sleep(RETRY_DELAY)
    
    raise RegistrationError(f"No se pudo registrar después de {MAX_RETRIES} intentos")


def run_heartbeat_loop(node_id: str) -> None:
    """
    Ejecuta un loop de heartbeat indefinido.
    
    Args:
        node_id: Identificador único del nodo
    """
    logger.info(f"Iniciando loop de heartbeat (intervalo: {HEARTBEAT_INTERVAL}s)")
    
    while True:
        try:
            time.sleep(HEARTBEAT_INTERVAL)
            send_heartbeat(node_id)
        except KeyboardInterrupt:
            logger.info("Heartbeat detenido por el usuario")
            break
        except Exception as e:
            logger.error(f"Error en loop de heartbeat: {e}", exc_info=True)


def main():
    """Función principal de registro del DataNode."""
    try:
        logger.info("Iniciando proceso de registro del DataNode")
        
        request_admin_privileges()
        
        # Validar configuración
        validate_config()
        
        # Obtener o generar node_id
        node_id = get_node_id()
        
        # Obtener ZeroTier IP
        zt_ip = get_zerotier_ip()
        if zt_ip:
            logger.info(f"ZeroTier IP detectada: {zt_ip}")
        else:
            logger.warning("No se detectó IP de ZeroTier, usando None")
        
        # Obtener puerto de datos
        data_port = config.data_port
        logger.info(f"Puerto de datos configurado: {data_port}")
        
        # Registrar con reintentos
        register_with_retry(node_id, zt_ip, data_port)
        
        logger.info("DataNode registrado y listo para operar")
        
        # Iniciar loop de heartbeat (opcional, comentar si no es necesario)
        # run_heartbeat_loop(node_id)
        
    except RegistrationError as e:
        logger.error(f"Error de registro: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("Proceso interrumpido por el usuario")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error inesperado: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()