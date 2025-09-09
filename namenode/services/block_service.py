"""
Servicio de manejo de bloques en el NameNode
"""
import logging
import base64
from typing import List, Dict, Any
from sqlalchemy.orm import Session
from shared.rabbitmq_config import RabbitMQManager, EXCHANGE_BLOCK_FANOUT
from shared.models import StoreBlockMessage
from ..models import Block as DBBlock, UploadSession as DBUploadSession

logger = logging.getLogger(__name__)

class BlockService:
    def __init__(self):
        self.rabbitmq = RabbitMQManager()
    
    def distribute_blocks(self, upload_id: str, blocks_data: List[Dict[str, Any]]):
        """Distribuir bloques a todos los DataNodes via fanout exchange"""
        try:
            if not self.rabbitmq.connect():
                raise Exception("No se pudo conectar a RabbitMQ")
            
            for block_info in blocks_data:
                # Codificar datos del bloque en base64
                block_data_b64 = base64.b64encode(block_info["block_data"]).decode('utf-8')
                
                # Crear mensaje para envío
                message = StoreBlockMessage(
                    block_id=block_info["block_id"],
                    block_data=block_data_b64,
                    block_hash=block_info["block_hash"],
                    upload_id=upload_id,
                    block_index=block_info["block_index"],
                    block_size=block_info["block_size"]
                ).dict()
                
                # Publicar a fanout exchange (llega a todos los DataNodes)
                self.rabbitmq.publish_to_fanout(message)
                
                logger.info(f"Bloque {block_info['block_id']} enviado via fanout")
            
            logger.info(f"Todos los bloques del upload {upload_id} distribuidos")
            
        except Exception as e:
            logger.error(f"Error distribuyendo bloques: {e}")
            raise
        finally:
            self.rabbitmq.disconnect()
    
    def get_block_locations(self, db: Session, block_id: str) -> List[Dict[str, Any]]:
        """Obtener ubicaciones de un bloque específico"""
        from ..models import BlockLocation as DBBlockLocation, DataNode as DBDataNode
        
        locations_query = db.query(DBBlockLocation, DBDataNode).join(
            DBDataNode, DBBlockLocation.datanode_id == DBDataNode.node_id
        ).filter(
            DBBlockLocation.block_id == block_id,
            DBBlockLocation.status == "active",
            DBDataNode.status == "active"
        )
        
        locations = []
        for location, datanode in locations_query.all():
            locations.append({
                "datanode_id": datanode.node_id,
                "host": datanode.host,
                "port": datanode.port,
                "storage_path": location.storage_path
            })
        
        return locations
    
    def update_upload_progress(self, db: Session, upload_id: str, completed_blocks: int):
        """Actualizar progreso de upload"""
        try:
            upload_session = db.query(DBUploadSession).filter(
                DBUploadSession.upload_id == upload_id
            ).first()
            
            if not upload_session:
                logger.error(f"Upload session no encontrada: {upload_id}")
                return
            
            upload_session.completed_blocks = completed_blocks
            
            # Marcar como completado si todos los bloques están listos
            if completed_blocks >= upload_session.total_blocks:
                upload_session.status = "completed"
                logger.info(f"Upload {upload_id} completado exitosamente")
            
            db.commit()
            
        except Exception as e:
            db.rollback()
            logger.error(f"Error actualizando progreso de upload {upload_id}: {e}")
    
    def mark_upload_failed(self, db: Session, upload_id: str, error_message: str):
        """Marcar upload como fallido"""
        try:
            upload_session = db.query(DBUploadSession).filter(
                DBUploadSession.upload_id == upload_id
            ).first()
            
            if upload_session:
                upload_session.status = "failed"
                db.commit()
                logger.error(f"Upload {upload_id} marcado como fallido: {error_message}")
            
        except Exception as e:
            db.rollback()
            logger.error(f"Error marcando upload como fallido {upload_id}: {e}")

# Instancia global del servicio
block_service = BlockService()
