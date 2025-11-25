import requests
import uuid
import json
import time
import os
from core.config import config

ZEROTIER_NETWORK_ID = config.zerotier_network_id  # ejemplo: "8056c2e21c000001"
METADATA_SERVICE_IP = config.metadata_service_ip  # ejemplo: "10.147.17.23"
METADATA_SERVICE_PORT = config.metadata_service_port
NODE_ID_FILE = "node_id.txt"


def get_node_id():
    """Genera o recupera un node_id persistente."""
    if os.path.exists(NODE_ID_FILE):
        with open(NODE_ID_FILE, "r") as f:
            return f.read().strip()

    node_id = str(uuid.uuid4())
    with open(NODE_ID_FILE, "w") as f:
        f.write(node_id)
    return node_id


def get_zerotier_ip():
    """Obtiene la IP del cliente ZeroTier para esta máquina."""
    path = f"/var/lib/zerotier-one/networks.d/{ZEROTIER_NETWORK_ID}.conf"

    if not os.path.exists(path):
        return None

    with open(path, "r") as f:
        data = json.load(f)

    assigned_ips = data.get("assignedAddresses", [])
    if not assigned_ips:
        return None

    return assigned_ips[0].split("/")[0]


def register_to_master(node_id):
    """Envía un POST al Metadata Service para registrar el nodo."""
    url = f"http://{METADATA_SERVICE_IP}:{METADATA_SERVICE_PORT}/api/v1/nodes/register"

    payload = {"node_id": node_id}

    try:
        r = requests.post(url, json=payload, timeout=5)
        r.raise_for_status()
        print("Nodo registrado exitosamente:", r.json())
        return True

    except Exception as e:
        print("Error registrando nodo:", e)
        return False


def main():
    print("Iniciando registro automático del DataNode...")

    node_id = get_node_id()
    print("Node ID local:", node_id)

    # Esperar a que ZeroTier asigne IP
    ip = None
    while ip is None:
        print("Esperando a que ZeroTier asigne una IP...")
        time.sleep(3)
        ip = get_zerotier_ip()

    print("ZeroTier IP:", ip)

    # Intentar registro hasta que funcione
    while True:
        ok = register_to_master(node_id)
        if ok:
            break

        print("Reintentando en 5 segundos...")
        time.sleep(5)

    print("Registro completado. El DataNode está listo.")


if __name__ == "__main__":
    main()
