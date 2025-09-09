"""
Política de almacenamiento para DataNode
Decide qué bloques almacenar basado en capacidad y load balancing
"""
import os
import logging
import random
from shared.config import config

logger = logging.getLogger(__name__)

class StoragePolicy:
    def __init__(self):
        self.storage_path = config.STORAGE_PATH
        self.storage_capacity = config.STORAGE_CAPACITY
        self.min_free_space_ratio = 0.1  # Mantener al menos 10% libre
        self.storage_probability = 0.8   # Probabilidad base de almacenar
        
    def should_store_block(self, block_id: str, block_size: int, node_id: str) -> bool:
        """Decidir si este DataNode debería almacenar el bloque"""
        try:
            # Verificar si ya tenemos el bloque
            block_path = os.path.join(self.storage_path, block_id)
            if os.path.exists(block_path):
                return True  # Ya lo tenemos
            
            # Verificar espacio disponible
            if not self.has_sufficient_space(block_size):
                logger.debug(f"Espacio insuficiente para bloque {block_id} en {node_id}")
                return False
            
            # Load balancing probabilístico
            # Ajustar probabilidad basada en uso de espacio
            used_ratio = self.get_storage_used_ratio()
            adjusted_probability = self.storage_probability * (1 - used_ratio)
            
            # Decisión basada en probabilidad
            should_store = random.random() < adjusted_probability
            
            logger.debug(f"Decisión para bloque {block_id} en {node_id}: {should_store} (prob: {adjusted_probability:.2f})")
            return should_store
            
        except Exception as e:
            logger.error(f"Error en política de almacenamiento: {e}")
            return False
    
    def has_sufficient_space(self, required_size: int) -> bool:
        """Verificar si hay espacio suficiente para almacenar el bloque"""
        try:
            # Obtener estadísticas de espacio del filesystem
            statvfs = os.statvfs(self.storage_path)
            available_bytes = statvfs.f_bavail * statvfs.f_frsize
            
            # Verificar si hay espacio para el bloque más buffer mínimo
            total_capacity = self.storage_capacity
            min_free_space = total_capacity * self.min_free_space_ratio
            
            return available_bytes >= (required_size + min_free_space)
            
        except Exception as e:
            logger.error(f"Error verificando espacio disponible: {e}")
            return False
    
    def get_storage_used_ratio(self) -> float:
        """Obtener ratio de espacio usado (0.0 a 1.0)"""
        try:
            # Calcular espacio usado en el directorio de storage
            total_size = 0
            for dirpath, dirnames, filenames in os.walk(self.storage_path):
                for filename in filenames:
                    filepath = os.path.join(dirpath, filename)
                    if os.path.isfile(filepath):
                        total_size += os.path.getsize(filepath)
            
            return min(total_size / self.storage_capacity, 1.0)
            
        except Exception as e:
            logger.error(f"Error calculando ratio de uso: {e}")
            return 1.0  # Asumir lleno en caso de error
    
    def get_storage_stats(self) -> dict:
        """Obtener estadísticas detalladas de almacenamiento"""
        try:
            # Calcular espacio usado
            total_used = 0
            block_count = 0
            
            for dirpath, dirnames, filenames in os.walk(self.storage_path):
                for filename in filenames:
                    filepath = os.path.join(dirpath, filename)
                    if os.path.isfile(filepath):
                        total_used += os.path.getsize(filepath)
                        block_count += 1
            
            # Espacio disponible del filesystem
            statvfs = os.statvfs(self.storage_path)
            fs_available = statvfs.f_bavail * statvfs.f_frsize
            
            return {
                "storage_capacity": self.storage_capacity,
                "storage_used": total_used,
                "storage_available": min(self.storage_capacity - total_used, fs_available),
                "used_ratio": total_used / self.storage_capacity,
                "block_count": block_count,
                "fs_available": fs_available
            }
            
        except Exception as e:
            logger.error(f"Error obteniendo estadísticas: {e}")
            return {
                "storage_capacity": self.storage_capacity,
                "storage_used": 0,
                "storage_available": 0,
                "used_ratio": 1.0,
                "block_count": 0,
                "fs_available": 0
            }
    
    def update_storage_used(self, block_size: int):
        """Actualizar estadísticas después de almacenar un bloque"""
        # En esta implementación simple, no mantenemos cache de estadísticas
        # Las estadísticas se calculan dinámicamente en get_storage_stats()
        logger.debug(f"Bloque de {block_size} bytes almacenado")
    
    def cleanup_invalid_blocks(self):
        """Limpiar bloques corruptos o inválidos"""
        try:
            cleaned_count = 0
            for filename in os.listdir(self.storage_path):
                filepath = os.path.join(self.storage_path, filename)
                
                if os.path.isfile(filepath):
                    # Verificar si el archivo está corrupto
                    try:
                        with open(filepath, 'rb') as f:
                            f.read(1)  # Intentar leer al menos un byte
                    except Exception:
                        # Archivo corrupto, eliminarlo
                        os.remove(filepath)
                        cleaned_count += 1
                        logger.warning(f"Bloque corrupto eliminado: {filename}")
            
            if cleaned_count > 0:
                logger.info(f"Limpieza completada: {cleaned_count} bloques corruptos eliminados")
                
        except Exception as e:
            logger.error(f"Error en limpieza de bloques: {e}")

# Instancia global de la política de almacenamiento
storage_policy = StoragePolicy()
