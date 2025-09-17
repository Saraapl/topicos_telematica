import grpc
from concurrent import futures
import time
import os
import requests
import shutil
from dataNode.protos import dataNode_pb2
from dataNode.protos import dataNode_pb2_grpc

import sys, os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from block_config import BLOCK_SIZE_MB, BLOCK_SIZE

"""
DataNode: servidor de almacenamiento distribuido para bloques de archivos.
Expone un servicio gRPC para almacenar, recuperar y eliminar bloques.
Envía heartbeats y se registra en el NameNode.
"""

# Configuración global
DATANODE_HOST = "0.0.0.0"
NAMENODE_URL = os.getenv("NAMENODE_URL", "http://namenode:8000")
# Intervalo de heartbeat en segundos
HEARTBEAT_INTERVAL = 10
# Carpeta local para bloques
STORAGE_DIR = "dataNode/storage/blocks"
os.makedirs(STORAGE_DIR, exist_ok=True)

# Servicio gRPC principal
class DataNodeService(dataNode_pb2_grpc.DataNodeServiceServicer):
    def DeleteDir(self, request, context):
        dir_path = os.path.join(STORAGE_DIR, request.dir_name)
        try:
            if os.path.exists(dir_path) and os.path.isdir(dir_path):
                shutil.rmtree(dir_path)
                print(f"[INFO] Carpeta eliminada: {dir_path}")
                return dataNode_pb2.BlockReply(
                    block_id=request.dir_name,
                    status="Directory deleted"
                )
            else:
                print(f"[ERROR] Carpeta {dir_path} no existe para eliminar")
                return dataNode_pb2.BlockReply(
                    block_id=request.dir_name,
                    status="Directory not found"
                )
        except Exception as e:
            print(f"[ERROR] No se pudo eliminar carpeta: {dir_path} → {e}")
            return dataNode_pb2.BlockReply(
                block_id=request.dir_name,
                status=f"Error: {e}"
            )

    def MakeDir(self, request, context):
        dir_path = os.path.join(STORAGE_DIR, request.dir_name)
        try:
            os.makedirs(dir_path, exist_ok=True)
            print(f"[INFO] Carpeta creada: {dir_path}")
            return dataNode_pb2.BlockReply(
                block_id=request.dir_name,
                status="Directory created"
            )
        except Exception as e:
            print(f"[ERROR] No se pudo crear carpeta: {dir_path} → {e}")
            return dataNode_pb2.BlockReply(
                block_id=request.dir_name,
                status=f"Error: {e}"
            )

    def StoreBlock(self, request, context):
        block_path = os.path.join(STORAGE_DIR, request.block_id)
        os.makedirs(os.path.dirname(block_path), exist_ok=True)
        print(f"[DataNode] Guardando bloque: {request.block_id} en {block_path} (size: {len(request.data)} bytes)")
        with open(block_path, "wb") as f:
            f.write(request.data)
        return dataNode_pb2.BlockReply(
            block_id=request.block_id,
            status="Block stored successfully"
        )

    def GetBlock(self, request, context):
        block_path = os.path.join(STORAGE_DIR, request.block_id)
        if not os.path.exists(block_path):
            return dataNode_pb2.BlockReply(
                block_id=request.block_id,
                status="Block not found"
            )
        with open(block_path, "rb") as f:
            data = f.read()
        return dataNode_pb2.BlockReply(
            block_id=request.block_id,
            data=data,
            status="OK"
        )

    def DeleteBlock(self, request, context):
        block_path = os.path.join(STORAGE_DIR, request.block_id)
        if os.path.exists(block_path):
            os.remove(block_path)
            print(f"[INFO] Bloque {request.block_id} eliminado")
            return dataNode_pb2.BlockReply(
                block_id=request.block_id,
                status="Block deleted"
            )
        else:
            print(f"[ERROR] Bloque {request.block_id} no existe para eliminar")
            return dataNode_pb2.BlockReply(
                block_id=request.block_id,
                status="Block not found"
            )

# Registro y heartbeat en NameNode
def register_with_namenode(max_retries=10, delay=3):
    for attempt in range(max_retries):
        try:
            response = requests.post(f"{NAMENODE_URL}/register_datanode", json={
                "host": os.getenv("DATANODE_NAME", "datanode"),
                "port": DATANODE_PORT
            })
            response.raise_for_status()
            print(f"[INFO] Registrado con NameNode en intento {attempt+1}: {response.json()}")
            return response.json()
        except Exception as e:
            print(f"[WARN] Falló el registro con NameNode (intento {attempt+1}/{max_retries}): {e}")
            time.sleep(delay)

    raise RuntimeError("No se pudo registrar con el NameNode después de varios intentos")

def send_heartbeat(datanode_id):
    requests.post(f"{NAMENODE_URL}/heartbeat/{datanode_id}")

def serve(port):
    # gRPC server
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10),
        options=[
            ("grpc.max_send_message_length", 64 * 1024 * 1024),
            ("grpc.max_receive_message_length", 64 * 1024 * 1024),
        ]
    )
    dataNode_pb2_grpc.add_DataNodeServiceServicer_to_server(DataNodeService(), server)
    server.add_insecure_port(f"{DATANODE_HOST}:{port}")
    server.start()
    print(f"DataNode listening on {DATANODE_HOST}:{port}")

    # Registro en el NameNode
    global DATANODE_PORT
    DATANODE_PORT = port
    info = register_with_namenode()
    datanode_id = info.get("id")

    # Enviar heartbeats periódicamente
    while True:
        send_heartbeat(datanode_id)
        time.sleep(HEARTBEAT_INTERVAL)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        port = int(sys.argv[1])
    else:
        port = 50051
    serve(port)