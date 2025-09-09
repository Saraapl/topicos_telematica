"""
Servicio para manejar confirmaciones de almacenamiento de los DataNodes
"""
import logging
import threading
import json
from datetime import datetime
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy import create_engine
from shared.config import config
from shared.rabbitmq_config import RabbitMQManager, QUEUE_STORAGE_CONFIRM, QUEUE_HEARTBEAT
from shared.models import StorageConfirmationMessage, HeartbeatMessage
from ..models import BlockLocation as DBBlockLocation, DataNode as DBDataNode, UploadSession as DBUploadSession
from .block_service import block_service

logger = logging.getLogger(__name__)

class ConfirmationHandler:
    def __init__(self):
        self.rabbitmq = RabbitMQManager()
        self.running = False
        self.confirmation_thread = None
        self.heartbeat_thread = None
        
        # Crear engine y session separados para el handler
        self.engine = create_engine(
            config.DATABASE_URL,
            pool_pre_ping=True,
            pool_recycle=300
        )
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
    
    def get_db_session(self):
        """Obtener nueva sesión de base de datos"""
        return self.SessionLocal()
    
    def start(self):
        """Iniciar el handler de confirmaciones"""
        if self.running:
            return
        
        self.running = True
        
        # Thread para confirmaciones de almacenamiento
        self.confirmation_thread = threading.Thread(
            target=self._process_confirmations,
            daemon=True
        )
        self.confirmation_thread.start()
        
        # Thread para heartbeats
        self.heartbeat_thread = threading.Thread(
            target=self._process_heartbeats,
            daemon=True
        )
        self.heartbeat_thread.start()
        
        logger.info("ConfirmationHandler iniciado")
    
    def stop(self):
        """Detener el handler"""
        self.running = False
        if self.rabbitmq:
            self.rabbitmq.disconnect()
        logger.info("ConfirmationHandler detenido")
    
    def _process_confirmations(self):
        """Procesar confirmaciones de almacenamiento de bloques"""
        try:
            if not self.rabbitmq.connect():
                logger.error("No se pudo conectar a RabbitMQ para confirmaciones")
                return
            
            def handle_confirmation(message: dict):
                try:
                    confirmation = StorageConfirmationMessage(**message)
                    self._handle_storage_confirmation(confirmation)
                except Exception as e:
                    logger.error(f"Error procesando confirmación: {e}")
            
            logger.info("Iniciando consumo de confirmaciones de almacenamiento")
            self.rabbitmq.consume(QUEUE_STORAGE_CONFIRM, handle_confirmation)
            
        except Exception as e:
            logger.error(f"Error en proceso de confirmaciones: {e}")
    
    def _process_heartbeats(self):
        """Procesar heartbeats de DataNodes"""
        try:
            # Usar nueva conexión para heartbeats
            heartbeat_rabbitmq = RabbitMQManager()
            if not heartbeat_rabbitmq.connect():
                logger.error("No se pudo conectar a RabbitMQ para heartbeats")
                return
            
            def handle_heartbeat(message: dict):
                try:
                    logger.info(f"Heartbeat recibido: {message}")
                    heartbeat = HeartbeatMessage(**message)
                    self._handle_heartbeat(heartbeat)
                except Exception as e:
                    logger.error(f"Error procesando heartbeat: {e}")
            
            logger.info("Iniciando consumo de heartbeats")
            heartbeat_rabbitmq.consume(QUEUE_HEARTBEAT, handle_heartbeat)
            
        except Exception as e:
            logger.error(f"Error en proceso de heartbeats: {e}")
    
    def _handle_storage_confirmation(self, confirmation: StorageConfirmationMessage):
        """Manejar confirmación individual de almacenamiento"""
        db = self.get_db_session()
        try:
            if confirmation.status == "success":
                # Registrar ubicación del bloque
                existing_location = db.query(DBBlockLocation).filter(
                    DBBlockLocation.block_id == confirmation.block_id,
                    DBBlockLocation.datanode_id == confirmation.datanode_id
                ).first()
                
                if not existing_location:
                    location = DBBlockLocation(
                        block_id=confirmation.block_id,
                        datanode_id=confirmation.datanode_id,
                        status="active",
                        storage_path=confirmation.storage_path,
                        confirmed_at=datetime.utcnow()
                    )
                    db.add(location)
                    db.commit()
                    
                    logger.info(f"Ubicación registrada: bloque {confirmation.block_id} en {confirmation.datanode_id}")
                
                # Actualizar progreso del upload si está disponible
                self._update_upload_progress_from_block(db, confirmation.block_id)
                
            else:
                logger.warning(f"Confirmación de error para bloque {confirmation.block_id}: {confirmation.error_message}")
            
        except Exception as e:
            db.rollback()
            logger.error(f"Error manejando confirmación de almacenamiento: {e}")
        finally:
            db.close()
    
    def _handle_heartbeat(self, heartbeat: HeartbeatMessage):
        """Manejar heartbeat de DataNode"""
        db = self.get_db_session()
        try:
            # Actualizar o crear registro de DataNode
            datanode = db.query(DBDataNode).filter(
                DBDataNode.node_id == heartbeat.datanode_id
            ).first()
            
            if datanode:
                # Actualizar existente
                datanode.status = heartbeat.status
                datanode.storage_used = heartbeat.storage_used
                datanode.storage_capacity = heartbeat.storage_capacity
                datanode.last_heartbeat = datetime.utcnow()
            else:
                # Crear nuevo (asumiendo host y puerto por defecto)
                datanode = DBDataNode(
                    node_id=heartbeat.datanode_id,
                    host="localhost",  # Será actualizado cuando sea necesario
                    port=5672,
                    status=heartbeat.status,
                    storage_used=heartbeat.storage_used,
                    storage_capacity=heartbeat.storage_capacity,
                    last_heartbeat=datetime.utcnow()
                )
                db.add(datanode)
            
            db.commit()
            
            logger.info(f"Heartbeat procesado para {heartbeat.datanode_id}: {heartbeat.status}")
            
        except Exception as e:
            db.rollback()
            logger.error(f"Error manejando heartbeat: {e}")
        finally:
            db.close()
    
    def _update_upload_progress_from_block(self, db: Session, block_id: str):
        """Actualizar progreso de upload basado en confirmación de bloque"""
        try:
            from ..models import Block as DBBlock, File as DBFile
            
            # Encontrar el bloque y su archivo
            block = db.query(DBBlock).filter(DBBlock.block_id == block_id).first()
            if not block:
                return
            
            file = db.query(DBFile).filter(DBFile.id == block.file_id).first()
            if not file:
                return
            
            # Encontrar sesión de upload activa para este archivo
            upload_session = db.query(DBUploadSession).filter(
                DBUploadSession.file_path == file.file_path,
                DBUploadSession.user_id == file.user_id,
                DBUploadSession.status == "pending"
            ).first()
            
            if not upload_session:
                return
            
            # Contar bloques confirmados
            confirmed_blocks = db.query(DBBlockLocation).join(DBBlock).filter(
                DBBlock.file_id == file.id,
                DBBlockLocation.status == "active"
            ).count()
            
            # Actualizar progreso
            block_service.update_upload_progress(db, upload_session.upload_id, confirmed_blocks)
            
        except Exception as e:
            logger.error(f"Error actualizando progreso desde bloque {block_id}: {e}")

# Instancia global del handler
confirmation_handler = ConfirmationHandler()
