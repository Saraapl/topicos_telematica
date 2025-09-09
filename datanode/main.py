"""
DataNode principal para GridDFS
"""
import logging
import os
import signal
import sys
from shared.config import config
from .worker import DataNodeWorker

# Configurar logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL.upper()),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def signal_handler(signum, frame):
    """Manejar señales del sistema para shutdown graceful"""
    logger.info("Señal de shutdown recibida")
    if 'worker' in globals():
        worker.stop()
    sys.exit(0)

def main():
    """Función principal del DataNode"""
    # Obtener NODE_ID de variable de entorno
    node_id = config.NODE_ID
    if not node_id:
        logger.error("NODE_ID no especificado en variables de entorno")
        sys.exit(1)
    
    # Verificar directorio de storage
    storage_path = config.STORAGE_PATH
    if not os.path.exists(storage_path):
        try:
            os.makedirs(storage_path, exist_ok=True)
            logger.info(f"Directorio de storage creado: {storage_path}")
        except Exception as e:
            logger.error(f"No se pudo crear directorio de storage: {e}")
            sys.exit(1)
    
    # Configurar handlers de señales
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Crear y iniciar worker
    global worker
    worker = DataNodeWorker(node_id)
    
    try:
        logger.info(f"Iniciando DataNode {node_id}...")
        logger.info(f"Storage path: {storage_path}")
        logger.info(f"Storage capacity: {config.STORAGE_CAPACITY} bytes")
        
        worker.start()
        
    except KeyboardInterrupt:
        logger.info("Interrupción del usuario")
    except Exception as e:
        logger.error(f"Error fatal en DataNode: {e}")
        sys.exit(1)
    finally:
        worker.stop()
        logger.info(f"DataNode {node_id} cerrado")

if __name__ == "__main__":
    main()
