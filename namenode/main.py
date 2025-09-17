from fastapi import FastAPI, HTTPException, Depends, status, Request, Body, Query
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel
from typing import List
from jose import JWTError, jwt
import time
from datetime import datetime
import sqlite3
from .db import DB_PATH
from block_config import BLOCK_SIZE_MB, BLOCK_SIZE
import json
import grpc
from dataNode.protos import dataNode_pb2, dataNode_pb2_grpc

app = FastAPI()


"""
NameNode: servidor principal del sistema distribuido DFS.
Expone una API REST para autenticación, gestión de archivos, directorios y metadatos.
Coordina la asignación de bloques y DataNodes, y gestiona el registro y heartbeat de DataNodes.
"""

# Clave secreta y configuración JWT
SECRET_KEY = "supersecretkey"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

# Modelos de datos
class UserRegister(BaseModel):
    username: str
    password: str

class FileMetadata(BaseModel):
    filename: str
    size_mb: int

# Autenticación y utilidades JWT
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/login")

def create_access_token(data: dict, expires_delta: int = ACCESS_TOKEN_EXPIRE_MINUTES):
    to_encode = data.copy()
    expire = int(time.time()) + expires_delta * 60
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def verify_token(token: str):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username = payload.get("sub")
        if username is None:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")
        return username
    except JWTError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

# Endpoints API REST
@app.post("/register")
def register(user: UserRegister):
    print(f"[INFO] Registro de usuario: {user.username}")

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT username FROM users WHERE username=?", (user.username,))
    if c.fetchone():
        conn.close()
        print(f"[ERROR] Usuario ya existe: {user.username}")
        raise HTTPException(status_code=400, detail="User already exists")
    c.execute("INSERT INTO users (username, password) VALUES (?, ?)", (user.username, user.password))
    conn.commit()
    conn.close()
    print(f"[INFO] Usuario registrado: {user.username}")
    return {"msg": "User registered"}


@app.post("/login")
def login(form_data: OAuth2PasswordRequestForm = Depends()):
    username = form_data.username
    password = form_data.password
    print(f"[INFO] Login intento: {username}")

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT password FROM users WHERE username=?", (username,))
    row = c.fetchone()
    conn.close()
    if not row or row[0] != password:
        print(f"[ERROR] Login fallido para usuario: {username}")
        raise HTTPException(status_code=400, detail="Incorrect username or password")
    token = create_access_token({"sub": username})
    print(f"[INFO] Login exitoso: {username}")
    return {"access_token": token, "token_type": "bearer"}


@app.get("/ls")
def list_files(token: str = Depends(oauth2_scheme), path: str = Query(None)):
    username = verify_token(token)
    print(f"[INFO] Listando archivos para usuario: {username} en path: {path}")

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    if path is None or path == "":
        # Listar raíz: solo los elementos de primer nivel
        c.execute("SELECT filename FROM files WHERE username=?", (username,))
        all_files = [row[0] for row in c.fetchall()]
        files = []
        for f in all_files:
            rel = f.rstrip("/")
            if "/" not in rel:
                files.append(f)
            elif f.endswith("/") and rel.count("/") == 1:
                files.append(f)
        conn.close()
        print(f"[INFO] Archivos encontrados: {files}")
        return {"files": files}
    else:
        # Listar contenido de un subdirectorio
        prefix = path.rstrip("/") + "/"
        c.execute("SELECT filename FROM files WHERE username=? AND filename LIKE ?", (username, prefix + "%"))
        all_files = [row[0] for row in c.fetchall()]
        files = []
        for f in all_files:
            rel = f[len(prefix):].rstrip("/")
            if rel and "/" not in rel:
                files.append(rel)
        conn.close()
        print(f"[INFO] Archivos encontrados en {path}: {files}")
        return {"files": files}


@app.post("/register_datanode")
async def register_datanode(request: Request):
    data = await request.json()
    host, port = data["host"], data["port"]
    print(f"[INFO] Registro de DataNode: {host}:{port}")

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO datanodes (host, port, last_heartbeat)
        VALUES (?, ?, ?)
    """, (host, port, datetime.now()))
    datanode_id = cur.lastrowid
    conn.commit()
    conn.close()

    return {"id": datanode_id, "msg": f"DataNode {host}:{port} registrado"}


@app.post("/heartbeat/{datanode_id}")
def heartbeat(datanode_id: int):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        UPDATE datanodes
        SET last_heartbeat = ?
        WHERE id = ?
    """, (datetime.now(), datanode_id))
    conn.commit()
    conn.close()
    return {"msg": f"DataNode {datanode_id} vivo"}


@app.get("/datanodes")
def list_datanodes():
    print("[INFO] Listando DataNodes registrados")
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT id, host, port, last_heartbeat FROM datanodes")
    nodes = [{"id": row[0], "host": row[1], "port": row[2], "last_heartbeat": row[3]} for row in cur.fetchall()]
    conn.close()
    return {"datanodes": nodes}


@app.post("/put_metadata")
def put_metadata(meta: FileMetadata, token: str = Depends(oauth2_scheme)):
    username = verify_token(token)
    print(f"[INFO] Subiendo metadata para archivo: {meta.filename}, tamaño: {meta.size_mb}MB, usuario: {username}")

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Consultar datanodes activos (heartbeat)
    cur.execute("""
        SELECT id, host, port, last_heartbeat
        FROM datanodes
    """)
    all_nodes = cur.fetchall()

    now = datetime.now()
    active_nodes = [
        {"id": row[0], "host": row[1], "port": row[2]}
        for row in all_nodes
        if row[3] and (now - datetime.fromisoformat(row[3])).total_seconds() < 30
    ]

    if not active_nodes:
        conn.close()
        raise HTTPException(status_code=500, detail="No active DataNodes available")

    # Calcular número de bloques
    file_size = meta.size_mb
    num_blocks = (file_size * 1024 * 1024 + BLOCK_SIZE - 1) // BLOCK_SIZE

    # Asignar bloques a DataNodes (round-robin)
    assignments = []
    for i in range(num_blocks):
        node = active_nodes[i % len(active_nodes)]
        assignments.append({
            "id": f"{meta.filename}_block{i}",
            "datanode": f"{node['host']}:{node['port']}"
        })

    metadata_json = json.dumps({"blocks": assignments})

    cur.execute("""
        INSERT INTO files (username, filename, metadata, block_location)
        VALUES (?, ?, ?, ?)
    """, (username, meta.filename, metadata_json, metadata_json))
    conn.commit()
    conn.close()

    return {"msg": "Metadata uploaded", "metadata": assignments}


@app.get("/get_metadata/{filename:path}")
def get_metadata(filename: str, token: str = Depends(oauth2_scheme)):
    username = verify_token(token)
    print(f"[INFO] Obteniendo metadata para archivo: {filename} para usuario: {username}")

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("SELECT metadata FROM files WHERE username=? AND filename=?", (username, filename))
    row = c.fetchone()

    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="File not found")
    return {"filename": filename, "block_location": json.loads(row[0])}


@app.delete("/rm/{filename:path}")
def remove_file(filename: str, token: str = Depends(oauth2_scheme)):
    username = verify_token(token)
    print(f"[INFO] Eliminando archivo: {filename} para usuario: {username}")

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Obtener metadata para saber los bloques y datanodes
    cur.execute("SELECT metadata FROM files WHERE username=? AND filename=?", (username, filename))
    row = cur.fetchone()
    if not row:
        print(f"[ERROR] Archivo '{filename}' no existe para usuario: {username}")
        conn.close()
        raise HTTPException(status_code=404, detail=f"File '{filename}' not found")
    metadata = json.loads(row[0])
    blocks = metadata.get("blocks", [])
    for block in blocks:
        datanode = block["datanode"]
        block_id = block["id"]
        host, port = datanode.split(":")
        try:
            with grpc.insecure_channel(f"{host}:{port}") as channel:
                stub = dataNode_pb2_grpc.DataNodeServiceStub(channel)
                request = dataNode_pb2.BlockRequest(block_id=block_id)
                response = stub.DeleteBlock(request)
                print(f"[INFO] Bloque {block_id} eliminado en DataNode {datanode}: {response.status}")
        except Exception as e:
            print(f"[ERROR] Error eliminando bloque {block_id} en {datanode}: {e}")

    cur.execute("DELETE FROM files WHERE username=? AND filename=?", (username, filename))
    conn.commit()
    conn.close()
    print(f"[INFO] Archivo '{filename}' y sus bloques eliminados")
    return {"msg": f"Archivo '{filename}' y sus bloques eliminados"}


@app.post("/mkdir")
def make_dir(dirname: str = Body(..., embed=True), token: str = Depends(oauth2_scheme)):
    username = verify_token(token)
    print(f"[INFO] Creando directorio: {dirname} para usuario: {username}")

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("INSERT INTO files (username, filename, metadata, block_location) VALUES (?, ?, ?, ?)", (username, dirname + "/", "{}", "{}"))
    conn.commit()

    # Crear directorio en todos los DataNodes activos
    cur.execute("SELECT host, port, last_heartbeat FROM datanodes")
    all_nodes = cur.fetchall()
    now = datetime.now()
    active_nodes = [row for row in all_nodes if row[2] and (now - datetime.fromisoformat(row[2])).total_seconds() < 30]
    results = []
    for node in active_nodes:
        host, port = node[0], node[1]
        try:
            with grpc.insecure_channel(f"{host}:{port}") as channel:
                stub = dataNode_pb2_grpc.DataNodeServiceStub(channel)
                request = dataNode_pb2.DirRequest(dir_name=dirname)
                response = stub.MakeDir(request)
                results.append({"datanode": f"{host}:{port}", "status": response.status})
        except Exception as e:
            results.append({"datanode": f"{host}:{port}", "status": f"Error: {e}"})
    conn.close()
    return {"msg": f"Directorio '{dirname}' creado", "results": results}


@app.delete("/rmdir/{dirname}")
def remove_dir(dirname: str, token: str = Depends(oauth2_scheme)):
    username = verify_token(token)
    print(f"[INFO] Eliminando directorio: {dirname} para usuario: {username}")

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    prefix = dirname.rstrip("/") + "/"

    # Eliminar directorio en todos los DataNodes
    cur.execute("SELECT host, port, last_heartbeat FROM datanodes")
    all_nodes = cur.fetchall()
    now = datetime.now()
    active_nodes = [row for row in all_nodes if row[2] and (now - datetime.fromisoformat(row[2])).total_seconds() < 30]
    results = []
    for node in active_nodes:
        host, port = node[0], node[1]
        try:
            with grpc.insecure_channel(f"{host}:{port}") as channel:
                stub = dataNode_pb2_grpc.DataNodeServiceStub(channel)
                from dataNode.protos import dataNode_pb2  # Importar aquí para evitar problemas de import
                request = dataNode_pb2.DirRequest(dir_name=dirname)
                response = stub.DeleteDir(request)
                results.append({"datanode": f"{host}:{port}", "status": response.status})
        except Exception as e:
            results.append({"datanode": f"{host}:{port}", "status": f"Error: {e}"})

    cur.execute("DELETE FROM files WHERE username=? AND filename LIKE ?", (username, prefix + "%"))
    conn.commit()
    conn.close()

    # Resumir resultados por estado
    summary = {}
    for r in results:
        status = r["status"]
        summary.setdefault(status, []).append(r["datanode"])
    resumen = []
    for status, nodes in summary.items():
        resumen.append(f"{status}: {', '.join(nodes)}")
    return {
        "msg": f"Directorio '{dirname}' y su contenido eliminados",
        "details": resumen
    }