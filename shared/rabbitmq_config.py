"""
Configuración de RabbitMQ para GridDFS
"""
import pika
import json
from typing import Callable, Dict, Any
from shared.config import config
import logging

logger = logging.getLogger(__name__)

# Exchange y Queue Names
EXCHANGE_BLOCK_FANOUT = "griddfs.blocks.fanout"
EXCHANGE_DIRECT = "griddfs.direct"

# Queue patterns
QUEUE_FANOUT_BLOCKS = "fanout.blocks.{datanode_id}"
QUEUE_BLOCK_REQUEST = "block.request.{datanode_id}"
QUEUE_BLOCK_RESPONSE = "block.response.{client_id}"
QUEUE_STORAGE_CONFIRM = "storage.confirm"
QUEUE_HEARTBEAT = "datanode.heartbeat"

class RabbitMQManager:
    def __init__(self, url: str = None):
        self.url = url or config.RABBITMQ_URL
        self.connection = None
        self.channel = None
        
    def connect(self):
        """Establecer conexión con RabbitMQ"""
        try:
            connection_params = pika.URLParameters(self.url)
            self.connection = pika.BlockingConnection(connection_params)
            self.channel = self.connection.channel()
            
            # Declarar exchanges
            self.channel.exchange_declare(
                exchange=EXCHANGE_BLOCK_FANOUT,
                exchange_type='fanout',
                durable=True
            )
            self.channel.exchange_declare(
                exchange=EXCHANGE_DIRECT,
                exchange_type='direct',
                durable=True
            )
            
            logger.info("Conectado a RabbitMQ")
            return True
            
        except Exception as e:
            logger.error(f"Error conectando a RabbitMQ: {e}")
            return False
    
    def disconnect(self):
        """Cerrar conexión con RabbitMQ"""
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            logger.info("Desconectado de RabbitMQ")
    
    def setup_datanode_queues(self, datanode_id: str):
        """Configurar colas específicas para un DataNode"""
        if not self.channel:
            raise Exception("No hay conexión activa con RabbitMQ")
        
        # Queue para recibir bloques via fanout
        fanout_queue = QUEUE_FANOUT_BLOCKS.format(datanode_id=datanode_id)
        self.channel.queue_declare(queue=fanout_queue, durable=True)
        self.channel.queue_bind(
            exchange=EXCHANGE_BLOCK_FANOUT,
            queue=fanout_queue
        )
        
        # Queue para solicitudes directas de bloques
        request_queue = QUEUE_BLOCK_REQUEST.format(datanode_id=datanode_id)
        self.channel.queue_declare(queue=request_queue, durable=True)
        self.channel.queue_bind(
            exchange=EXCHANGE_DIRECT,
            queue=request_queue,
            routing_key=f"block.request.{datanode_id}"
        )
        
        # Queue para confirmaciones de almacenamiento
        self.channel.queue_declare(queue=QUEUE_STORAGE_CONFIRM, durable=True)
        self.channel.queue_bind(
            exchange=EXCHANGE_DIRECT,
            queue=QUEUE_STORAGE_CONFIRM,
            routing_key="storage.confirm"
        )
        
        # Queue para heartbeats
        self.channel.queue_declare(queue=QUEUE_HEARTBEAT, durable=True)
        self.channel.queue_bind(
            exchange=EXCHANGE_DIRECT,
            queue=QUEUE_HEARTBEAT,
            routing_key="heartbeat"
        )
        
        return fanout_queue, request_queue
    
    def setup_client_queue(self, client_id: str):
        """Configurar cola temporal para respuestas a cliente"""
        if not self.channel:
            raise Exception("No hay conexión activa con RabbitMQ")
        
        response_queue = QUEUE_BLOCK_RESPONSE.format(client_id=client_id)
        result = self.channel.queue_declare(queue='', exclusive=True, auto_delete=True)
        response_queue = result.method.queue
        
        return response_queue
    
    def publish_to_fanout(self, message: Dict[str, Any]):
        """Publicar mensaje a fanout exchange (llega a todos los DataNodes)"""
        if not self.channel:
            raise Exception("No hay conexión activa con RabbitMQ")
        
        import time
        self.channel.basic_publish(
            exchange=EXCHANGE_BLOCK_FANOUT,
            routing_key='',
            body=json.dumps(message, default=str),
            properties=pika.BasicProperties(
                delivery_mode=2,  # Hacer mensaje persistente
                timestamp=int(time.time())
            )
        )
    
    def publish_direct(self, routing_key: str, message: Dict[str, Any]):
        """Publicar mensaje directo con routing key específica"""
        if not self.channel:
            raise Exception("No hay conexión activa con RabbitMQ")
        
        import time
        self.channel.basic_publish(
            exchange=EXCHANGE_DIRECT,
            routing_key=routing_key,
            body=json.dumps(message, default=str),
            properties=pika.BasicProperties(
                delivery_mode=2,
                timestamp=int(time.time())
            )
        )
    
    def consume(self, queue: str, callback: Callable):
        """Comenzar a consumir mensajes de una cola"""
        if not self.channel:
            raise Exception("No hay conexión activa con RabbitMQ")
        
        def wrapper(ch, method, properties, body):
            try:
                message = json.loads(body.decode())
                callback(message)
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as e:
                logger.error(f"Error procesando mensaje: {e}")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue=queue, on_message_callback=wrapper)
        
        logger.info(f"Consumiendo cola: {queue}")
        self.channel.start_consuming()
    
    def get_message(self, queue: str, timeout: int = 30):
        """Obtener un mensaje de una cola con timeout"""
        if not self.channel:
            raise Exception("No hay conexión activa con RabbitMQ")
        
        method_frame, header_frame, body = self.channel.basic_get(queue=queue)
        if method_frame:
            self.channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            return json.loads(body.decode())
        return None
