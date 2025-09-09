"""
Configuración de base de datos para el NameNode
"""
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import StaticPool
from shared.config import config
from .models import Base

logger = logging.getLogger(__name__)

# Engine de base de datos
engine = create_engine(
    config.DATABASE_URL,
    pool_pre_ping=True,
    pool_recycle=300,
    echo=(config.LOG_LEVEL == "DEBUG")
)

# SessionLocal para crear sesiones de base de datos
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def create_tables():
    """Crear todas las tablas en la base de datos"""
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("Tablas de base de datos creadas/verificadas")
    except Exception as e:
        logger.error(f"Error creando tablas: {e}")
        raise

def get_db():
    """Dependency para obtener sesión de base de datos"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def init_db():
    """Inicializar base de datos"""
    create_tables()
