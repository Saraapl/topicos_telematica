"""
Servicio de manejo de archivos en el NameNode
"""
import hashlib
import logging
from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
from nanoid import generate
from shared.config import config
from shared.models import FileInfo, UploadPlanResponse, DownloadPlanResponse
from shared.rabbitmq_config import RabbitMQManager, EXCHANGE_BLOCK_FANOUT
from ..models import File as DBFile, Block as DBBlock, UploadSession as DBUploadSession, User as DBUser
import math
import os

logger = logging.getLogger(__name__)

class FileService:
    def __init__(self):
        self.block_size = config.BLOCK_SIZE
        self.rabbitmq = RabbitMQManager()
    
    def calculate_file_hash(self, file_path: str) -> str:
        """Calcular hash SHA256 de un archivo"""
        hash_sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_sha256.update(chunk)
        return hash_sha256.hexdigest()
    
    def calculate_blocks_info(self, file_path: str, file_size: int) -> List[Dict[str, Any]]:
        """Calcular información de bloques para un archivo"""
        blocks = []
        total_blocks = math.ceil(file_size / self.block_size)
        
        with open(file_path, "rb") as f:
            for i in range(total_blocks):
                block_data = f.read(self.block_size)
                block_size = len(block_data)
                block_hash = hashlib.sha256(block_data).hexdigest()
                block_id = generate()
                
                blocks.append({
                    "block_id": block_id,
                    "block_index": i,
                    "block_size": block_size,
                    "block_hash": block_hash,
                    "block_data": block_data  # Para envío posterior
                })
        
        return blocks
    
    def create_upload_session(self, db: Session, user_id: int, file_path: str, 
                            local_file_path: str) -> UploadPlanResponse:
        """Crear sesión de upload y generar plan de bloques"""
        try:
            # Verificar que el archivo existe
            if not os.path.exists(local_file_path):
                raise FileNotFoundError(f"Archivo no encontrado: {local_file_path}")
            
            file_size = os.path.getsize(local_file_path)
            file_hash = self.calculate_file_hash(local_file_path)
            
            # Verificar si el archivo ya existe
            existing_file = db.query(DBFile).filter(
                DBFile.user_id == user_id,
                DBFile.file_path == file_path
            ).first()
            
            if existing_file:
                raise ValueError(f"El archivo ya existe en: {file_path}")
            
            # Calcular bloques
            blocks_info = self.calculate_blocks_info(local_file_path, file_size)
            total_blocks = len(blocks_info)
            
            # Crear upload session
            upload_id = generate()
            upload_session = DBUploadSession(
                upload_id=upload_id,
                user_id=user_id,
                file_path=file_path,
                total_blocks=total_blocks,
                status="pending"
            )
            db.add(upload_session)
            db.commit()
            
            # Crear entrada de archivo (pendiente)
            filename = os.path.basename(file_path)
            db_file = DBFile(
                filename=filename,
                file_path=file_path,
                user_id=user_id,
                file_size=file_size,
                file_hash=file_hash
            )
            db.add(db_file)
            db.commit()
            db.refresh(db_file)
            
            # Crear registros de bloques
            for block_info in blocks_info:
                db_block = DBBlock(
                    block_id=block_info["block_id"],
                    file_id=db_file.id,
                    block_index=block_info["block_index"],
                    block_size=block_info["block_size"],
                    block_hash=block_info["block_hash"]
                )
                db.add(db_block)
            
            db.commit()
            
            # Preparar respuesta (sin block_data para respuesta)
            response_blocks = []
            for block in blocks_info:
                response_blocks.append({
                    "block_id": block["block_id"],
                    "block_index": block["block_index"],
                    "block_size": block["block_size"],
                    "block_hash": block["block_hash"]
                })
            
            return UploadPlanResponse(
                upload_id=upload_id,
                blocks=response_blocks,
                total_blocks=total_blocks
            )
            
        except Exception as e:
            db.rollback()
            logger.error(f"Error creando upload session: {e}")
            raise
    
    def get_blocks_for_distribution(self, db: Session, upload_id: str, file_path: str) -> List[Dict[str, Any]]:
        """Obtener bloques para distribución con datos desde archivo"""
        try:
            # Obtener upload session para encontrar el archivo asociado
            upload_session = db.query(DBUploadSession).filter(
                DBUploadSession.upload_id == upload_id
            ).first()
            
            if not upload_session:
                raise ValueError(f"Upload session no encontrada: {upload_id}")
            
            # Obtener archivo asociado
            db_file = db.query(DBFile).filter(
                DBFile.user_id == upload_session.user_id,
                DBFile.file_path == upload_session.file_path
            ).first()
            
            if not db_file:
                raise ValueError(f"Archivo no encontrado para upload: {upload_id}")
            
            # Obtener bloques desde la base de datos
            db_blocks = db.query(DBBlock).filter(
                DBBlock.file_id == db_file.id
            ).order_by(DBBlock.block_index).all()
            
            if not db_blocks:
                raise ValueError(f"Bloques no encontrados para upload: {upload_id}")
            
            # Leer datos de bloques desde archivo
            blocks_info = []
            with open(file_path, "rb") as f:
                for db_block in db_blocks:
                    # Leer datos del bloque
                    f.seek(db_block.block_index * self.block_size)
                    block_data = f.read(db_block.block_size)
                    
                    blocks_info.append({
                        "block_id": db_block.block_id,
                        "block_index": db_block.block_index,
                        "block_size": db_block.block_size,
                        "block_hash": db_block.block_hash,
                        "block_data": block_data
                    })
            
            return blocks_info
            
        except Exception as e:
            logger.error(f"Error obteniendo bloques para distribución: {e}")
            raise
    
    def get_file_info(self, db: Session, user_id: int, file_path: str) -> Optional[FileInfo]:
        """Obtener información de un archivo"""
        db_file = db.query(DBFile).filter(
            DBFile.user_id == user_id,
            DBFile.file_path == file_path
        ).first()
        
        if not db_file:
            return None
        
        return FileInfo(
            id=db_file.id,
            filename=db_file.filename,
            file_path=db_file.file_path,
            user_id=db_file.user_id,
            file_size=db_file.file_size,
            file_hash=db_file.file_hash,
            created_at=db_file.created_at
        )
    
    def list_files(self, db: Session, user_id: int, path_prefix: str = "/") -> List[FileInfo]:
        """Listar archivos del usuario con prefijo de path opcional"""
        query = db.query(DBFile).filter(DBFile.user_id == user_id)
        
        if path_prefix != "/":
            query = query.filter(DBFile.file_path.like(f"{path_prefix}%"))
        
        files = query.order_by(DBFile.file_path).all()
        
        return [
            FileInfo(
                id=file.id,
                filename=file.filename,
                file_path=file.file_path,
                user_id=file.user_id,
                file_size=file.file_size,
                file_hash=file.file_hash,
                created_at=file.created_at
            )
            for file in files
        ]
    
    def delete_file(self, db: Session, user_id: int, file_path: str) -> bool:
        """Eliminar un archivo y todos sus bloques"""
        try:
            db_file = db.query(DBFile).filter(
                DBFile.user_id == user_id,
                DBFile.file_path == file_path
            ).first()
            
            if not db_file:
                return False
            
            # Eliminar archivo (cascade eliminará bloques y ubicaciones)
            db.delete(db_file)
            db.commit()
            
            logger.info(f"Archivo eliminado: {file_path} (user_id: {user_id})")
            return True
            
        except Exception as e:
            db.rollback()
            logger.error(f"Error eliminando archivo {file_path}: {e}")
            raise
    
    def get_download_plan(self, db: Session, user_id: int, file_path: str) -> Optional[DownloadPlanResponse]:
        """Obtener plan de descarga para un archivo"""
        # Obtener información del archivo
        file_info = self.get_file_info(db, user_id, file_path)
        if not file_info:
            return None
        
        # Obtener bloques con sus ubicaciones
        from ..models import BlockLocation as DBBlockLocation
        
        blocks_query = db.query(DBBlock).filter(
            DBBlock.file_id == file_info.id
        ).order_by(DBBlock.block_index)
        
        blocks = []
        for block in blocks_query:
            # Obtener ubicaciones activas para este bloque
            locations_query = db.query(DBBlockLocation).filter(
                DBBlockLocation.block_id == block.block_id,
                DBBlockLocation.status == "active"
            )
            
            locations = [
                {
                    "datanode_id": loc.datanode_id,
                    "storage_path": loc.storage_path
                }
                for loc in locations_query.all()
            ]
            
            blocks.append({
                "block_id": block.block_id,
                "block_index": block.block_index,
                "block_size": block.block_size,
                "block_hash": block.block_hash,
                "locations": locations
            })
        
        return DownloadPlanResponse(
            file_info=file_info,
            blocks=blocks
        )

# Instancia global del servicio
file_service = FileService()
