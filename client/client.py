import os
import json
import requests
import grpc
from dataNode.protos import dataNode_pb2
from dataNode.protos import dataNode_pb2_grpc

"""
Cliente para el sistema distribuido DFS.
Permite registrar usuarios, autenticarse, listar archivos, subir/descargar archivos por bloques,
crear/eliminar directorios y archivos, y manejar errores de DataNode y NameNode.
"""

# Mapear nombres de contenedores a localhost con los puertos expuestos
HOST_MAP = {
    "datanode1": "localhost:50051",
    "datanode2": "localhost:50052",
    "datanode3": "localhost:50053",
}


# Configuración global
NAMENODE_URL = "http://localhost:8000"
# tamaño de bloque
from block_config import BLOCK_SIZE_MB, BLOCK_SIZE
#token JWT en memoria
TOKEN = None

# Funciones REST para interactuar con el NameNode
def register_user(username, password):
    r = requests.post(f"{NAMENODE_URL}/register", json={
        "username": username,
        "password": password
    })
    resp = r.json()
    if r.status_code == 200 and 'msg' in resp:
        print(resp['msg'])
    else:
        print("Error al registrar usuario.")

def login(username, password):
    global TOKEN
    r = requests.post(f"{NAMENODE_URL}/login", data={
        "username": username,
        "password": password
    })
    if r.status_code == 200:
        TOKEN = r.json()["access_token"]
        print("Login exitoso")
    else:
        print("Error de login.")

def list_files(path=None):
    headers = {"Authorization": f"Bearer {TOKEN}"}
    params = {}
    if path:
        params["path"] = path
    r = requests.get(f"{NAMENODE_URL}/ls", headers=headers, params=params)
    resp = r.json()
    if r.status_code == 200 and 'files' in resp:
        if resp['files']:
            print("Archivos:")
            for f in resp['files']:
                print(f"  - {f}")
        else:
            print("No hay archivos ni carpetas.")
    else:
        print("Error al listar archivos.")

def remove_file(filename):
    headers = {"Authorization": f"Bearer {TOKEN}"}
    r = requests.delete(f"{NAMENODE_URL}/rm/{filename}", headers=headers)
    resp = r.json()
    if r.status_code == 200 and 'msg' in resp:
        print(resp['msg'])
    else:
        print(f"No se pudo eliminar '{filename}'.")

def make_dir(dirname):
    headers = {"Authorization": f"Bearer {TOKEN}"}
    r = requests.post(f"{NAMENODE_URL}/mkdir", json={"dirname": dirname}, headers=headers)
    resp = r.json()
    if 'results' in resp:
        errores = [r['datanode'] for r in resp['results'] if not r['status'] == 'Directory created']
        if not errores:
            print(f"Directorio '{dirname}' creado")
        else:
            print(f"Directorio '{dirname}' creado. Algunos DataNodes tuvieron errores:")
            for nodo in errores:
                print(f"  - {nodo}")
    elif 'msg' in resp:
        print(resp['msg'])
    else:
        print(f"No se pudo crear el directorio '{dirname}'.")

def remove_dir(dirname):
    headers = {"Authorization": f"Bearer {TOKEN}"}
    r = requests.delete(f"{NAMENODE_URL}/rmdir/{dirname}", headers=headers)
    resp = r.json()
    if 'details' in resp:
        errores = [d for d in resp['details'] if 'Error' in d or 'not found' in d]
        if not errores:
            print(f"Directorio '{dirname}' eliminado")
        else:
            print(f"Directorio '{dirname}' eliminado. Algunos DataNodes tuvieron errores:")
            for d in errores:
                print(f"  - {d}")
    elif 'msg' in resp:
        print(resp['msg'])
    else:
        print(f"No se pudo eliminar el directorio '{dirname}'.")

def get_metadata(filename):
    headers = {"Authorization": f"Bearer {TOKEN}"}
    r = requests.get(f"{NAMENODE_URL}/get_metadata/{filename}", headers=headers)
    if r.status_code == 404:
        print(f"El archivo '{filename}' no existe.")
        return None
    resp = r.json()
    if 'block_location' in resp and 'blocks' in resp['block_location']:
        print(f"Bloques de '{filename}':")
        for b in resp['block_location']['blocks']:
            print(f"  - {b['id']} en {b['datanode']}")
    else:
        print(f"No hay metadata para '{filename}'.")
    return resp

def put_metadata(filename, size_mb):
    headers = {"Authorization": f"Bearer {TOKEN}"}
    r = requests.post(f"{NAMENODE_URL}/put_metadata",
                      json={"filename": filename, "size_mb": size_mb},
                      headers=headers)
    try:
        return r.json()
    except Exception:
        print("Respuesta no válida del servidor.")
        return {}

# Funciones gRPC para interactuar con los DataNodes
def store_block(host, port, block_id, data):
    with grpc.insecure_channel(f"{host}:{port}") as channel:
        stub = dataNode_pb2_grpc.DataNodeServiceStub(channel)
        request = dataNode_pb2.BlockRequest(block_id=block_id, data=data)
        response = stub.StoreBlock(request)
        return response.status

def get_block(host, port, block_id):
    try:
        with grpc.insecure_channel(
            f"{host}:{port}",
            options=[
                ("grpc.max_send_message_length", 4 * 1024 * 1024 + 1024),
                ("grpc.max_receive_message_length", 4 * 1024 * 1024 + 1024),
            ]
        ) as channel:
            stub = dataNode_pb2_grpc.DataNodeServiceStub(channel)
            request = dataNode_pb2.BlockRequest(block_id=block_id)
            response = stub.GetBlock(request)
            if response.status == "OK":
                return response.data
            else:
                print(f"[ERROR] Bloque '{block_id}' perdido en nodo {host}:{port} ({response.status})")
                return None
    except grpc.RpcError as e:
        print(f"[ERROR] DataNode caído o inaccesible: {host}:{port} para el bloque {block_id}")
        return None

# Comandos CLI: operaciones de usuario
def put_file(filepath, dfs_path=None):
    if dfs_path is None:
        filename = os.path.basename(filepath)
    else:
        if dfs_path.endswith("/"):
            filename = dfs_path + os.path.basename(filepath)
        elif '.' not in os.path.basename(dfs_path):
            filename = dfs_path + '/' + os.path.basename(filepath)
        else:
            filename = dfs_path
    size_mb = BLOCK_SIZE_MB * ((os.path.getsize(filepath) + BLOCK_SIZE - 1) // BLOCK_SIZE)

    resp = put_metadata(filename, size_mb)
    assignments = resp["metadata"]

    # Dividir archivo en bloques
    with open(filepath, "rb") as f:
        for i, assignment in enumerate(assignments):
            block_data = f.read(BLOCK_SIZE)
            raw_host, raw_port = assignment["datanode"].split(":")
            mapped = HOST_MAP.get(raw_host, f"{raw_host}:{raw_port}")
            host, port = mapped.split(":")
            status = store_block(host, port, assignment["id"], block_data)
            print(f"Bloque {i} enviado a {host}:{port} → {status}")

def get_file(filename, output_path):
    # Preguntar al NameNode
    meta = get_metadata(filename)
    if not meta:
        print(f"[ERROR] No se puede descargar '{filename}': archivo no existe.")
        return
    assignments = meta["block_location"]["blocks"]

    # Descargar bloques y reconstruir archivo
    with open(output_path, "wb") as f:
        all_ok = True
        for block in assignments:
            raw_host, raw_port = block["datanode"].split(":")
            mapped = HOST_MAP.get(raw_host, f"{raw_host}:{raw_port}")
            host, port = mapped.split(":")
            data = get_block(host, port, block["id"])
            if data:
                f.write(data)
                print(f"[INFO] Bloque {block['id']} recuperado de {host}:{port}")
            else:
                print(f"[ERROR] Bloque {block['id']} perdido o nodo inaccesible: {host}:{port}")
                all_ok = False
    if all_ok:
        print(f"[INFO] Archivo reconstruido en {output_path}")
    else:
        print("[ERROR] No se pudieron recuperar todos los bloques.")

# Main CLI: entrada principal y modo interactivo
if __name__ == "__main__":
    """
    CLI principal. Permite ejecutar comandos por argumentos o en modo interactivo tras login.
    Comandos disponibles:
      - register <usuario> <contraseña>
      - login <usuario> <contraseña>
      - ls
      - put <archivo>
      - get <nombre> <destino>
      - rm <archivo>
      - mkdir <dir>
      - rmdir <dir>
      - exit
    """
    import sys
    args = sys.argv[1:]
    if len(args) >= 3 and args[0] == "login":
        login(args[1], args[2])
        print("Modo consola interactiva. Escribe 'exit' para salir o 'help' para ver comandos.")
        while True:
            try:
                cmdline = input("$ ").strip()
            except (EOFError, KeyboardInterrupt):
                print("\nSaliendo...")
                break
            if cmdline == "exit":
                print("Saliendo...")
                break
            if cmdline == "help":
                print("Comandos disponibles: ls, put <archivo>, get <nombre> <destino>, rm <archivo>, mkdir <dir>, rmdir <dir>, exit")
                continue
            parts = cmdline.split()
            if not parts:
                continue
            cmd = parts[0]
            if cmd == "ls":
                # ls [directorio]
                if len(parts) == 2:
                    list_files(parts[1])
                else:
                    list_files()
            elif cmd == "put" and len(parts) == 2:
                # put <archivo> (sube a raíz)
                put_file(parts[1])
            elif cmd == "put" and len(parts) == 3:
                # put <archivo> <ruta_destino>
                put_file(parts[1], parts[2])
            elif cmd == "get" and len(parts) == 3:
                get_file(parts[1], parts[2])
            elif cmd == "rm" and len(parts) == 2:
                remove_file(parts[1])
            elif cmd == "mkdir" and len(parts) == 2:
                make_dir(parts[1])
            elif cmd == "rmdir" and len(parts) == 2:
                remove_dir(parts[1])
            else:
                print("Comandos disponibles: ls, put <archivo>, get <nombre> <destino>, rm <archivo>, mkdir <dir>, rmdir <dir>, exit")
    else:
        i = 0
        while i < len(args):
            cmd = args[i]
            if cmd == "register":
                register_user(args[i+1], args[i+2])
                i += 3
            elif cmd == "login":
                login(args[i+1], args[i+2])
                i += 3
            elif cmd == "ls":
                # ls [directorio]
                if i+1 < len(args) and not args[i+1].startswith("-"):
                    list_files(args[i+1])
                    i += 2
                else:
                    list_files()
                    i += 1
            elif cmd == "put":
                # put <archivo> [ruta_destino]
                if i+2 < len(args) and not args[i+2].startswith("-"):
                    put_file(args[i+1], args[i+2])
                    i += 3
                else:
                    put_file(args[i+1])
                    i += 2
            elif cmd == "get":
                get_file(args[i+1], args[i+2])
                i += 3
            elif cmd == "rm":
                remove_file(args[i+1])
                i += 2
            elif cmd == "mkdir":
                make_dir(args[i+1])
                i += 2
            elif cmd == "rmdir":
                remove_dir(args[i+1])
                i += 2
            else:
                print("Comandos disponibles: register, login, ls, put, get, rm, mkdir, rmdir")
                i += 1