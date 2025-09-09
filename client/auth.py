"""
Manejo de autenticación para el cliente GridDFS
"""
import os
import json
import logging
from typing import Optional, Dict, Any
from shared.config import config

logger = logging.getLogger(__name__)

class AuthManager:
    def __init__(self):
        self.config_dir = os.path.dirname(config.CLIENT_CONFIG_PATH)
        self.config_file = config.CLIENT_CONFIG_PATH
        self._ensure_config_dir()
    
    def _ensure_config_dir(self):
        """Asegurar que el directorio de configuración existe"""
        os.makedirs(self.config_dir, exist_ok=True)
    
    def save_auth_info(self, username: str, token: str, namenode_url: str = None):
        """Guardar información de autenticación"""
        try:
            auth_info = {
                "username": username,
                "token": token,
                "namenode_url": namenode_url or config.NAMENODE_URL
            }
            
            with open(self.config_file, 'w') as f:
                json.dump(auth_info, f, indent=2)
            
            # Hacer el archivo solo legible por el usuario
            os.chmod(self.config_file, 0o600)
            
            logger.info(f"Información de autenticación guardada para {username}")
            
        except Exception as e:
            logger.error(f"Error guardando información de autenticación: {e}")
            raise
    
    def load_auth_info(self) -> Optional[Dict[str, str]]:
        """Cargar información de autenticación guardada"""
        try:
            if not os.path.exists(self.config_file):
                return None
            
            with open(self.config_file, 'r') as f:
                auth_info = json.load(f)
            
            # Verificar que tiene los campos necesarios
            required_fields = ["username", "token", "namenode_url"]
            if all(field in auth_info for field in required_fields):
                return auth_info
            else:
                logger.warning("Archivo de configuración incompleto")
                return None
                
        except Exception as e:
            logger.error(f"Error cargando información de autenticación: {e}")
            return None
    
    def clear_auth_info(self):
        """Limpiar información de autenticación guardada"""
        try:
            if os.path.exists(self.config_file):
                os.remove(self.config_file)
                logger.info("Información de autenticación eliminada")
        except Exception as e:
            logger.error(f"Error eliminando información de autenticación: {e}")
    
    def is_authenticated(self) -> bool:
        """Verificar si hay información de autenticación válida"""
        auth_info = self.load_auth_info()
        return auth_info is not None and all(
            key in auth_info for key in ["username", "token", "namenode_url"]
        )
    
    def get_current_user(self) -> Optional[str]:
        """Obtener usuario actual"""
        auth_info = self.load_auth_info()
        return auth_info.get("username") if auth_info else None
    
    def get_auth_token(self) -> Optional[str]:
        """Obtener token de autenticación actual"""
        auth_info = self.load_auth_info()
        return auth_info.get("token") if auth_info else None
    
    def get_namenode_url(self) -> Optional[str]:
        """Obtener URL del NameNode"""
        auth_info = self.load_auth_info()
        return auth_info.get("namenode_url") if auth_info else config.NAMENODE_URL

# Instancia global del manejador de autenticación
auth_manager = AuthManager()
