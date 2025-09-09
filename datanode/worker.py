"""
DataNode Worker para GridDFS - RabbitMQ Consumer
"""
import os
import base64
import hashlib
import logging
from typing import Dict, Any
from shared.config import config
from shared.rabbitmq_config import RabbitMQManager, QUEUE_FANOUT_BLOCKS, QUEUE_BLOCK_REQUEST
from shared.models import StoreBlockMessage, RequestBlockMessage, StorageConfirmationMessage, BlockResponseMessage
from .storage_policy import storage_policy
from .heartbeat import heartbeat_service

logger = logging.getLogger(__name__)

class DataNodeWorker:
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.storage_path = config.STORAGE_PATH
        self.rabbitmq = RabbitMQManager()
        self.running = False
        
        # Crear directorio de almacenamiento si no existe
        os.makedirs(self.storage_path, exist_ok=True)
        
        logger.info(f"DataNode {node_id} inicializado en {self.storage_path}")
    
    def start(self):
        """Iniciar el worker DataNode"""
        try:
            if not self.rabbitmq.connect():
                raise Exception("No se pudo conectar a RabbitMQ")
            
            # Configurar colas para este DataNode
            fanout_queue, request_queue = self.rabbitmq.setup_datanode_queues(self.node_id)
            
            # Iniciar servicio de heartbeat
            heartbeat_service.start(self.node_id)
            
            self.running = True
            logger.info(f"DataNode {self.node_id} iniciado y escuchando mensajes")
            
            # Procesar mensajes de ambas colas
            self._consume_messages(fanout_queue, request_queue)
            
        except Exception as e:
            logger.error(f"Error iniciando DataNode {self.node_id}: {e}")
            raise
    
    def stop(self):
        """Detener el worker"""
        self.running = False
        heartbeat_service.stop()
        if self.rabbitmq:
            self.rabbitmq.disconnect()
        logger.info(f"DataNode {self.node_id} detenido")
    
    def _consume_messages(self, fanout_queue: str, request_queue: str):
        """Consumir mensajes de las colas de fanout y request"""
        import threading
        
        # Thread para mensajes de fanout (store_block)
        fanout_thread = threading.Thread(
            target=self._consume_fanout_messages,
            args=(fanout_queue,),
            daemon=True
        )
        fanout_thread.start()
        
        # Thread para mensajes de request (request_block)  
        request_thread = threading.Thread(
            target=self._consume_request_messages,
            args=(request_queue,),
            daemon=True
        )
        request_thread.start()
        
        # Mantener el proceso principal vivo
        try:
            while self.running:
                import time
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Interrupción recibida, cerrando DataNode...")
            self.stop()
    
    def _consume_fanout_messages(self, queue: str):
        """Consumir mensajes de la cola fanout para almacenar bloques"""
        def handle_fanout_message(message: Dict[str, Any]):
            try:
                if message.get("message_type") == "store_block":
                    store_msg = StoreBlockMessage(**message)
                    self._handle_store_block(store_msg)
            except Exception as e:
                logger.error(f"Error procesando mensaje fanout: {e}")
        
        try:
            self.rabbitmq.consume(queue, handle_fanout_message)
        except Exception as e:
            logger.error(f"Error consumiendo cola fanout: {e}")
    
    def _consume_request_messages(self, queue: str):
        """Consumir mensajes de la cola request para enviar bloques"""
        def handle_request_message(message: Dict[str, Any]):
            try:
                if message.get("message_type") == "request_block":
                    request_msg = RequestBlockMessage(**message)
                    self._handle_request_block(request_msg)
            except Exception as e:
                logger.error(f"Error procesando mensaje request: {e}")
        
        try:
            # Usar una nueva conexión para requests
            request_rabbitmq = RabbitMQManager()
            if request_rabbitmq.connect():
                request_rabbitmq.consume(queue, handle_request_message)
        except Exception as e:
            logger.error(f"Error consumiendo cola request: {e}")
    
    def _handle_store_block(self, message: StoreBlockMessage):
        """Manejar solicitud de almacenar bloque"""
        try:
            # Decidir si almacenar basado en política de storage
            should_store = storage_policy.should_store_block(
                message.block_id,
                message.block_size,
                self.node_id
            )
            
            if not should_store:
                logger.debug(f"Bloque {message.block_id} no almacenado por política")
                return
            
            # Decodificar datos del bloque
            block_data = base64.b64decode(message.block_data)
            
            # Verificar hash
            calculated_hash = hashlib.sha256(block_data).hexdigest()
            if calculated_hash != message.block_hash:
                self._send_storage_confirmation(
                    message.block_id, 
                    "error",
                    error_message="Hash del bloque no coincide"
                )
                return
            
            # Crear path de almacenamiento
            block_path = os.path.join(self.storage_path, message.block_id)
            
            # Verificar si ya existe
            if os.path.exists(block_path):
                logger.debug(f"Bloque {message.block_id} ya existe en {self.node_id}")
                self._send_storage_confirmation(
                    message.block_id, 
                    "success",
                    storage_path=block_path
                )
                return
            
            # Verificar espacio disponible
            if not storage_policy.has_sufficient_space(message.block_size):
                self._send_storage_confirmation(
                    message.block_id,
                    "insufficient_space",
                    error_message="Espacio insuficiente"
                )
                return
            
            # Almacenar bloque
            with open(block_path, 'wb') as f:
                f.write(block_data)
            
            # Actualizar estadísticas de storage
            storage_policy.update_storage_used(message.block_size)
            
            # Enviar confirmación exitosa
            self._send_storage_confirmation(
                message.block_id,
                "success", 
                storage_path=block_path
            )
            
            logger.info(f"Bloque {message.block_id} almacenado exitosamente en {self.node_id}")
            
        except Exception as e:
            logger.error(f"Error almacenando bloque {message.block_id}: {e}")
            self._send_storage_confirmation(
                message.block_id,
                "error",
                error_message=str(e)
            )
    
    def _handle_request_block(self, message: RequestBlockMessage):
        """Manejar solicitud de envío de bloque"""
        try:
            block_path = os.path.join(self.storage_path, message.block_id)
            
            # Verificar si el bloque existe
            if not os.path.exists(block_path):
                self._send_block_response(
                    message.block_id,
                    message.response_queue,
                    "not_found",
                    error_message="Bloque no encontrado"
                )
                return
            
            # Leer bloque y codificar en base64
            with open(block_path, 'rb') as f:
                block_data = f.read()
            
            block_data_b64 = base64.b64encode(block_data).decode('utf-8')
            
            # Enviar respuesta
            self._send_block_response(
                message.block_id,
                message.response_queue,
                "success",
                block_data=block_data_b64
            )
            
            logger.info(f"Bloque {message.block_id} enviado desde {self.node_id}")
            
        except Exception as e:
            logger.error(f"Error enviando bloque {message.block_id}: {e}")
            self._send_block_response(
                message.block_id,
                message.response_queue,
                "error",
                error_message=str(e)
            )
    
    def _send_storage_confirmation(self, block_id: str, status: str, 
                                 storage_path: str = None, error_message: str = None):
        """Enviar confirmación de almacenamiento al NameNode"""
        try:
            confirmation = StorageConfirmationMessage(
                block_id=block_id,
                datanode_id=self.node_id,
                storage_path=storage_path or "",
                status=status,
                error_message=error_message
            )
            
            self.rabbitmq.publish_direct("storage.confirm", confirmation.dict())
            
        except Exception as e:
            logger.error(f"Error enviando confirmación de almacenamiento: {e}")
    
    def _send_block_response(self, block_id: str, response_queue: str, status: str,
                           block_data: str = None, error_message: str = None):
        """Enviar respuesta de bloque al cliente"""
        try:
            response = BlockResponseMessage(
                block_id=block_id,
                block_data=block_data,
                status=status,
                error_message=error_message
            )
            
            self.rabbitmq.publish_direct(response_queue, response.dict())
            
        except Exception as e:
            logger.error(f"Error enviando respuesta de bloque: {e}")
