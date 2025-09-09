"""
Cliente de comunicación para GridDFS
Maneja la comunicación con NameNode y DataNodes
"""
import os
import base64
import logging
import requests
from typing import List, Dict, Any, Optional
from shared.config import config
from shared.rabbitmq_config import RabbitMQManager
from shared.models import RequestBlockMessage
from nanoid import generate

logger = logging.getLogger(__name__)

class GridDFSClient:
    def __init__(self, namenode_url: str = None, auth_token: str = None):
        self.namenode_url = namenode_url or config.NAMENODE_URL
        self.auth_token = auth_token
        self.rabbitmq = None
        self.client_id = generate()
        
        # Headers por defecto para requests
        self.headers = {
            "Content-Type": "application/json"
        }
        if self.auth_token:
            self.headers["Authorization"] = f"Bearer {self.auth_token}"
    
    def set_auth_token(self, token: str):
        """Configurar token de autenticación"""
        self.auth_token = token
        self.headers["Authorization"] = f"Bearer {token}"
    
    def login(self, username: str, password: str) -> Dict[str, Any]:
        """Autenticar con el NameNode"""
        try:
            response = requests.post(
                f"{self.namenode_url}/auth/login",
                json={"username": username, "password": password}
            )
            response.raise_for_status()
            
            token_data = response.json()
            self.set_auth_token(token_data["access_token"])
            
            return token_data
            
        except requests.RequestException as e:
            logger.error(f"Error en login: {e}")
            raise
    
    def register(self, username: str, password: str) -> Dict[str, Any]:
        """Registrar nuevo usuario"""
        try:
            response = requests.post(
                f"{self.namenode_url}/auth/register",
                json={"username": username, "password": password}
            )
            response.raise_for_status()
            return response.json()
            
        except requests.RequestException as e:
            logger.error(f"Error en registro: {e}")
            raise
    
    def upload_file(self, local_path: str, remote_path: str) -> Dict[str, Any]:
        """Subir archivo al sistema distribuido"""
        try:
            if not os.path.exists(local_path):
                raise FileNotFoundError(f"Archivo local no encontrado: {local_path}")
            
            # Crear plan de upload
            with open(local_path, 'rb') as f:
                files = {'file': (os.path.basename(local_path), f)}
                data = {'file_path': remote_path}
                
                response = requests.post(
                    f"{self.namenode_url}/files/upload/plan",
                    headers={"Authorization": f"Bearer {self.auth_token}"},
                    files=files,
                    data=data
                )
                response.raise_for_status()
            
            upload_plan = response.json()
            logger.info(f"Upload plan creado: {upload_plan['upload_id']}")
            
            return upload_plan
            
        except requests.RequestException as e:
            logger.error(f"Error subiendo archivo: {e}")
            raise
    
    def download_file(self, remote_path: str, local_path: str) -> bool:
        """Descargar archivo del sistema distribuido"""
        try:
            # Obtener plan de descarga
            response = requests.get(
                f"{self.namenode_url}/files/download/plan",
                headers=self.headers,
                params={"file_path": remote_path}
            )
            response.raise_for_status()
            
            download_plan = response.json()
            file_info = download_plan["file_info"]
            blocks = download_plan["blocks"]
            
            logger.info(f"Descargando archivo: {file_info['filename']} ({file_info['file_size']} bytes)")
            
            # Conectar a RabbitMQ para solicitar bloques
            self.rabbitmq = RabbitMQManager()
            if not self.rabbitmq.connect():
                raise Exception("No se pudo conectar a RabbitMQ")
            
            # Configurar cola de respuesta temporal
            response_queue = self.rabbitmq.setup_client_queue(self.client_id)
            
            # Reconstruir archivo bloque por bloque
            with open(local_path, 'wb') as output_file:
                for block in sorted(blocks, key=lambda x: x["block_index"]):
                    block_data = self._download_block(block, response_queue)
                    if block_data:
                        output_file.write(block_data)
                    else:
                        raise Exception(f"No se pudo descargar bloque {block['block_id']}")
            
            logger.info(f"Archivo descargado exitosamente a: {local_path}")
            return True
            
        except requests.RequestException as e:
            logger.error(f"Error descargando archivo: {e}")
            raise
        except Exception as e:
            logger.error(f"Error en descarga: {e}")
            raise
        finally:
            if self.rabbitmq:
                self.rabbitmq.disconnect()
    
    def _download_block(self, block_info: Dict[str, Any], response_queue: str) -> Optional[bytes]:
        """Descargar un bloque específico de los DataNodes"""
        block_id = block_info["block_id"]
        locations = block_info["locations"]
        
        if not locations:
            logger.error(f"No hay ubicaciones disponibles para bloque {block_id}")
            return None
        
        # Intentar descargar desde diferentes DataNodes
        for location in locations:
            try:
                datanode_id = location["datanode_id"]
                
                # Solicitar bloque al DataNode
                request_message = RequestBlockMessage(
                    block_id=block_id,
                    client_id=self.client_id,
                    response_queue=response_queue
                )
                
                self.rabbitmq.publish_direct(
                    f"block.request.{datanode_id}",
                    request_message.dict()
                )
                
                # Esperar respuesta
                response = self.rabbitmq.get_message(response_queue, timeout=30)
                
                if response and response.get("status") == "success":
                    block_data_b64 = response.get("block_data")
                    if block_data_b64:
                        block_data = base64.b64decode(block_data_b64)
                        
                        # Verificar hash si está disponible
                        if "block_hash" in block_info:
                            import hashlib
                            calculated_hash = hashlib.sha256(block_data).hexdigest()
                            if calculated_hash != block_info["block_hash"]:
                                logger.warning(f"Hash mismatch para bloque {block_id}")
                                continue
                        
                        logger.debug(f"Bloque {block_id} descargado desde {datanode_id}")
                        return block_data
                
            except Exception as e:
                logger.warning(f"Error descargando bloque {block_id} desde {datanode_id}: {e}")
                continue
        
        logger.error(f"No se pudo descargar bloque {block_id} desde ningún DataNode")
        return None
    
    def list_files(self, path_prefix: str = "/") -> List[Dict[str, Any]]:
        """Listar archivos del usuario"""
        try:
            response = requests.get(
                f"{self.namenode_url}/files/list",
                headers=self.headers,
                params={"path_prefix": path_prefix}
            )
            response.raise_for_status()
            return response.json()
            
        except requests.RequestException as e:
            logger.error(f"Error listando archivos: {e}")
            raise
    
    def delete_file(self, file_path: str) -> bool:
        """Eliminar archivo"""
        try:
            response = requests.delete(
                f"{self.namenode_url}/files/delete",
                headers=self.headers,
                params={"file_path": file_path}
            )
            response.raise_for_status()
            return True
            
        except requests.RequestException as e:
            logger.error(f"Error eliminando archivo: {e}")
            raise
    
    def create_directory(self, directory_path: str) -> bool:
        """Crear directorio"""
        try:
            response = requests.post(
                f"{self.namenode_url}/files/mkdir",
                headers={"Authorization": f"Bearer {self.auth_token}"},
                data={"directory_path": directory_path}
            )
            response.raise_for_status()
            return True
            
        except requests.RequestException as e:
            logger.error(f"Error creando directorio: {e}")
            raise
    
    def remove_directory(self, directory_path: str) -> Dict[str, Any]:
        """Eliminar directorio"""
        try:
            response = requests.delete(
                f"{self.namenode_url}/files/rmdir",
                headers=self.headers,
                params={"directory_path": directory_path}
            )
            response.raise_for_status()
            return response.json()
            
        except requests.RequestException as e:
            logger.error(f"Error eliminando directorio: {e}")
            raise
    
    def get_system_status(self) -> Dict[str, Any]:
        """Obtener estado del sistema"""
        try:
            response = requests.get(
                f"{self.namenode_url}/system/status",
                headers=self.headers
            )
            response.raise_for_status()
            return response.json()
            
        except requests.RequestException as e:
            logger.error(f"Error obteniendo estado del sistema: {e}")
            raise
