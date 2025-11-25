import requests
import subprocess
import platform
import os
from dotenv import load_dotenv


def get_zerotier_networks(api_token):
    url = "https://my.zerotier.com/api/v1/network"
    headers = {"Authorization": f"Bearer {api_token}"}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        return None


def print_networks_info(networks):
    if networks is None:
        print("Error al obtener las redes de ZeroTier.")
        return

    for network in networks:
        print(f"Network ID: {network.get('id')}")
        print(f"Name: {network.get('name')}")
        print(f"Description: {network.get('description')}")
        print(f"Status: {network.get('status')}")
        print()


def join_network_local(network_id):
    """Une el nodo local a una red ZeroTier usando el CLI"""
    try:
        if platform.system() == "Windows":
            result = subprocess.run(
                ["zerotier-cli", "join", network_id],
                capture_output=True,
                text=True,
                shell=True
            )
        else:
            result = subprocess.run(
                ["sudo", "zerotier-cli", "join", network_id],
                capture_output=True,
                text=True
            )
        
        if result.returncode == 0:
            print(f"Nodo unido exitosamente a la red {network_id}")
            print(result.stdout)
        else:
            print(f"Error al unirse a la red: {result.stderr}")
    except FileNotFoundError:
        print("ZeroTier CLI no encontrado. Asegúrate de que ZeroTier esté instalado.")


def get_node_id():
    """Obtiene el ID del nodo local"""
    try:
        if platform.system() == "Windows":
            result = subprocess.run(
                ["zerotier-cli", "info"],
                capture_output=True,
                text=True
            )
        else:
            result = subprocess.run(
                ["sudo", "zerotier-cli", "info"],
                capture_output=True,
                text=True
            )
        
        if result.returncode == 0:
            node_id = result.stdout.split()[2]
            return node_id
        else:
            return None
    except Exception as e:
        print(f"Error obteniendo node ID: {e}")
        return None


def authorize_member(api_token, network_id, member_id, member_name=None):
    """Autoriza un miembro en la red desde ZeroTier Central"""
    url = f"https://my.zerotier.com/api/v1/network/{network_id}/member/{member_id}"
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }
    payload = {"authorized": True}
    if member_name:
        payload["name"] = member_name
    
    response = requests.post(url, json=payload, headers=headers)
    if response.status_code == 200:
        print(f"Miembro {member_id} autorizado exitosamente en la red {network_id}")
    else:
        print(f"Error autorizando miembro: {response.status_code} - {response.text}")


if __name__ == "__main__":
    load_dotenv()
    
    api_token = os.getenv("ZEROTIER_API_TOKEN")
    network_id = os.getenv("NETWORK_ID")
    
    # Ver información de las redes
    networks = get_zerotier_networks(api_token)
    print_networks_info(networks)
    
    # Unir el nodo local a la red
    print(f"\nUniendo nodo local a la red {network_id}...")
    join_network_local(network_id)
    
    # Obtener el ID del nodo local
    node_id = get_node_id()
    if node_id:
        print(f"\nID del nodo local: {node_id}")
        
        # Autorizar el nodo en ZeroTier Central
        print(f"\nAutorizando nodo en ZeroTier Central...")
        authorize_member(api_token, network_id, node_id, "DFS")
    else:
        print("\nNo se pudo obtener el ID del nodo. Autoriza manualmente desde ZeroTier Central.")