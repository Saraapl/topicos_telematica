"""
Servicio de heartbeat para DataNode
Envía información de estado al NameNode periódicamente
"""
import threading
import time
import logging
from shared.config import config
from shared.rabbitmq_config import RabbitMQManager
from shared.models import HeartbeatMessage
from .storage_policy import storage_policy

logger = logging.getLogger(__name__)

class HeartbeatService:
    def __init__(self):
        self.rabbitmq = RabbitMQManager()
        self.running = False
        self.heartbeat_thread = None
        self.interval = config.HEARTBEAT_INTERVAL
        self.node_id = None
    
    def start(self, node_id: str):
        """Iniciar servicio de heartbeat"""
        if self.running:
            return
        
        self.node_id = node_id
        self.running = True
        
        # Crear thread para heartbeats
        self.heartbeat_thread = threading.Thread(
            target=self._send_heartbeats,
            daemon=True
        )
        self.heartbeat_thread.start()
        
        logger.info(f"Heartbeat service iniciado para {node_id}")
    
    def stop(self):
        """Detener servicio de heartbeat"""
        self.running = False
        if self.rabbitmq:
            self.rabbitmq.disconnect()
        logger.info("Heartbeat service detenido")
    
    def _send_heartbeats(self):
        """Enviar heartbeats periódicamente"""
        logger.info("Iniciando thread de heartbeats")
        try:
            # Conectar a RabbitMQ
            if not self.rabbitmq.connect():
                logger.error("No se pudo conectar a RabbitMQ para heartbeats")
                return
            
            logger.info("Conectado a RabbitMQ para heartbeats")
            
            while self.running:
                try:
                    # Obtener estadísticas de storage
                    stats = storage_policy.get_storage_stats()
                    logger.debug(f"Storage stats: {stats}")
                    
                    # Crear mensaje de heartbeat
                    heartbeat = HeartbeatMessage(
                        datanode_id=self.node_id,
                        status="active",
                        storage_used=stats["storage_used"],
                        storage_capacity=stats["storage_capacity"],
                        storage_available=stats["storage_available"]
                    )
                    
                    logger.info(f"Enviando heartbeat desde {self.node_id}: {heartbeat.dict()}")
                    
                    # Enviar heartbeat
                    self.rabbitmq.publish_direct("heartbeat", heartbeat.dict())
                    
                    logger.info(f"Heartbeat enviado exitosamente desde {self.node_id}")
                    
                except Exception as e:
                    logger.error(f"Error enviando heartbeat: {e}")
                
                # Esperar hasta el próximo heartbeat
                logger.debug(f"Esperando {self.interval} segundos para próximo heartbeat")
                time.sleep(self.interval)
                
        except Exception as e:
            logger.error(f"Error en servicio de heartbeat: {e}")
        finally:
            logger.info("Finalizando servicio de heartbeat")
            self.rabbitmq.disconnect()
    
    def send_immediate_heartbeat(self):
        """Enviar heartbeat inmediato (para eventos especiales)"""
        if not self.running or not self.node_id:
            return
        
        try:
            # Usar conexión temporal para heartbeat inmediato
            temp_rabbitmq = RabbitMQManager()
            if temp_rabbitmq.connect():
                stats = storage_policy.get_storage_stats()
                
                heartbeat = HeartbeatMessage(
                    datanode_id=self.node_id,
                    status="active",
                    storage_used=stats["storage_used"],
                    storage_capacity=stats["storage_capacity"],
                    storage_available=stats["storage_available"]
                )
                
                temp_rabbitmq.publish_direct("heartbeat", heartbeat.dict())
                temp_rabbitmq.disconnect()
                
                logger.info(f"Heartbeat inmediato enviado desde {self.node_id}")
                
        except Exception as e:
            logger.error(f"Error enviando heartbeat inmediato: {e}")

# Instancia global del servicio de heartbeat
heartbeat_service = HeartbeatService()
