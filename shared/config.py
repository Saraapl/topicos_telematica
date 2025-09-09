"""
Configuración compartida para el sistema GridDFS
"""
import os
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Database
    DATABASE_URL: str = os.getenv("DATABASE_URL", "postgresql://griddfs:griddfs123@localhost:5432/griddfs")
    
    # RabbitMQ
    RABBITMQ_URL: str = os.getenv("RABBITMQ_URL", "amqp://griddfs:griddfs123@localhost:5672/")
    
    # JWT
    JWT_SECRET: str = os.getenv("JWT_SECRET", "your-secret-key-change-in-production")
    JWT_ALGORITHM: str = "HS256"
    JWT_EXPIRE_MINUTES: int = int(os.getenv("JWT_EXPIRE_MINUTES", "60"))
    
    # NameNode
    NAMENODE_URL: str = os.getenv("NAMENODE_URL", "http://localhost:8080")
    
    # Block configuration
    BLOCK_SIZE: int = int(os.getenv("BLOCK_SIZE", "67108864"))  # 64MB
    MAX_UPLOAD_SIZE: int = int(os.getenv("MAX_UPLOAD_SIZE", "10737418240"))  # 10GB
    
    # DataNode
    NODE_ID: Optional[str] = os.getenv("NODE_ID")
    STORAGE_PATH: str = os.getenv("STORAGE_PATH", "./storage")
    STORAGE_CAPACITY: int = int(os.getenv("STORAGE_CAPACITY", "10737418240"))  # 10GB
    
    # System
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    HEARTBEAT_INTERVAL: int = int(os.getenv("HEARTBEAT_INTERVAL", "30"))  # seconds
    
    # Client
    CLIENT_CONFIG_PATH: str = os.path.expanduser("~/.griddfs/config")
    CLIENT_CACHE_PATH: str = os.path.expanduser("~/.griddfs/cache")

config = Config()
