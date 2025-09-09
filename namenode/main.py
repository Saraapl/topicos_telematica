"""
NameNode principal de GridDFS - FastAPI Application
"""
import sys
import os
# Agregar el directorio padre al PYTHONPATH
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from shared.config import config
from namenode.database import init_db
from namenode.services.confirmation_handler import confirmation_handler
from namenode.api.auth_routes import router as auth_router
from namenode.api.file_routes import router as file_router
from namenode.api.system_routes import router as system_router

# Configurar logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL.upper()),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Gestión del ciclo de vida de la aplicación"""
    # Startup
    logger.info("Iniciando GridDFS NameNode...")
    
    # Inicializar base de datos
    init_db()
    logger.info("Base de datos inicializada")
    
    # Iniciar handler de confirmaciones
    confirmation_handler.start()
    logger.info("Handler de confirmaciones iniciado")
    
    yield
    
    # Shutdown
    logger.info("Cerrando GridDFS NameNode...")
    confirmation_handler.stop()
    logger.info("NameNode cerrado")

# Crear aplicación FastAPI
app = FastAPI(
    title="GridDFS NameNode",
    description="NameNode para el sistema de archivos distribuido GridDFS",
    version="1.0.0",
    lifespan=lifespan
)

# Configurar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # En producción, especificar orígenes específicos
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Incluir routers
app.include_router(auth_router)
app.include_router(file_router)
app.include_router(system_router)

@app.get("/")
async def root():
    """Endpoint raíz"""
    return {
        "service": "GridDFS NameNode",
        "version": "1.0.0",
        "status": "running"
    }

@app.get("/health")
async def health():
    """Health check"""
    return {
        "status": "healthy",
        "service": "GridDFS NameNode"
    }

def main():
    """Función principal para ejecutar el NameNode"""
    uvicorn.run(
        "namenode.main:app",
        host="0.0.0.0", 
        port=8000,
        reload=False,
        log_level=config.LOG_LEVEL.lower()
    )

if __name__ == "__main__":
    main()
