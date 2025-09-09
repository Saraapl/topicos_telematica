"""
Rutas del sistema para el NameNode API
"""
from typing import List
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from shared.models import SystemStatusResponse, DataNodeInfo
from ..database import get_db
from ..models import DataNode as DBDataNode, User as DBUser
from .auth_routes import get_authenticated_user

router = APIRouter(prefix="/system", tags=["system"])

@router.get("/status", response_model=SystemStatusResponse)
async def get_system_status(
    current_user: DBUser = Depends(get_authenticated_user),
    db: Session = Depends(get_db)
):
    """Obtener estado del sistema GridDFS"""
    try:
        # Obtener información de todos los DataNodes
        datanodes = db.query(DBDataNode).all()
        
        # Calcular métricas del sistema
        total_capacity = sum(node.storage_capacity for node in datanodes)
        total_used = sum(node.storage_used for node in datanodes)
        total_available = total_capacity - total_used
        active_nodes = len([node for node in datanodes if node.status == "active"])
        
        # Convertir a modelo de respuesta
        datanode_list = [
            DataNodeInfo(
                id=node.id,
                node_id=node.node_id,
                host=node.host,
                port=node.port,
                status=node.status,
                last_heartbeat=node.last_heartbeat,
                storage_used=node.storage_used,
                storage_capacity=node.storage_capacity
            )
            for node in datanodes
        ]
        
        return SystemStatusResponse(
            datanodes=datanode_list,
            total_capacity=total_capacity,
            total_used=total_used,
            total_available=total_available,
            active_nodes=active_nodes
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error obteniendo estado del sistema: {str(e)}"
        )

@router.get("/datanodes", response_model=List[DataNodeInfo])
async def list_datanodes(
    current_user: DBUser = Depends(get_authenticated_user),
    db: Session = Depends(get_db)
):
    """Listar todos los DataNodes"""
    try:
        datanodes = db.query(DBDataNode).all()
        
        return [
            DataNodeInfo(
                id=node.id,
                node_id=node.node_id,
                host=node.host,
                port=node.port,
                status=node.status,
                last_heartbeat=node.last_heartbeat,
                storage_used=node.storage_used,
                storage_capacity=node.storage_capacity
            )
            for node in datanodes
        ]
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error listando DataNodes: {str(e)}"
        )

@router.get("/health")
async def health_check():
    """Health check del NameNode"""
    return {
        "status": "healthy",
        "service": "GridDFS NameNode",
        "timestamp": "2024-01-01T00:00:00Z"  # Se actualizará con datetime real
    }
