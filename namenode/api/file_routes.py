"""
Rutas de manejo de archivos para el NameNode API
"""
import os
import tempfile
from typing import List
from fastapi import APIRouter, Depends, HTTPException, status, UploadFile, File, Form
from sqlalchemy.orm import Session
from shared.models import FileInfo, UploadPlanResponse, DownloadPlanResponse
from ..database import get_db
from ..models import User as DBUser
from ..services.file_service import file_service
from ..services.block_service import block_service
from .auth_routes import get_authenticated_user

router = APIRouter(prefix="/files", tags=["files"])

@router.post("/upload/plan", response_model=UploadPlanResponse)
async def create_upload_plan(
    file_path: str = Form(...),
    file: UploadFile = File(...),
    current_user: DBUser = Depends(get_authenticated_user),
    db: Session = Depends(get_db)
):
    """Crear plan de upload para un archivo"""
    try:
        # Crear archivo temporal para análisis
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            content = await file.read()
            temp_file.write(content)
            temp_file.flush()
            
            # Crear plan de upload
            upload_plan = file_service.create_upload_session(
                db, 
                current_user.id, 
                file_path, 
                temp_file.name
            )
            
            # Obtener datos de bloques para distribución (usando los mismos IDs)
            blocks_info = file_service.get_blocks_for_distribution(db, upload_plan.upload_id, temp_file.name)
            
            # Distribuir bloques via fanout
            block_service.distribute_blocks(upload_plan.upload_id, blocks_info)
            
            # Limpiar archivo temporal
            os.unlink(temp_file.name)
            
            return upload_plan
            
    except FileNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error procesando upload: {str(e)}"
        )

@router.get("/download/plan", response_model=DownloadPlanResponse)
async def get_download_plan(
    file_path: str,
    current_user: DBUser = Depends(get_authenticated_user),
    db: Session = Depends(get_db)
):
    """Obtener plan de descarga para un archivo"""
    download_plan = file_service.get_download_plan(db, current_user.id, file_path)
    
    if not download_plan:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Archivo no encontrado"
        )
    
    return download_plan

@router.get("/info", response_model=FileInfo)
async def get_file_info(
    file_path: str,
    current_user: DBUser = Depends(get_authenticated_user),
    db: Session = Depends(get_db)
):
    """Obtener información de un archivo"""
    file_info = file_service.get_file_info(db, current_user.id, file_path)
    
    if not file_info:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Archivo no encontrado"
        )
    
    return file_info

@router.get("/list", response_model=List[FileInfo])
async def list_files(
    path_prefix: str = "/",
    current_user: DBUser = Depends(get_authenticated_user),
    db: Session = Depends(get_db)
):
    """Listar archivos del usuario"""
    try:
        files = file_service.list_files(db, current_user.id, path_prefix)
        return files
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error listando archivos: {str(e)}"
        )

@router.delete("/delete")
async def delete_file(
    file_path: str,
    current_user: DBUser = Depends(get_authenticated_user),
    db: Session = Depends(get_db)
):
    """Eliminar un archivo"""
    try:
        success = file_service.delete_file(db, current_user.id, file_path)
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Archivo no encontrado"
            )
        
        return {"message": f"Archivo {file_path} eliminado exitosamente"}
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error eliminando archivo: {str(e)}"
        )

@router.post("/mkdir")
async def create_directory(
    directory_path: str = Form(...),
    current_user: DBUser = Depends(get_authenticated_user),
    db: Session = Depends(get_db)
):
    """Crear directorio (conceptual - solo para compatibilidad con CLI)"""
    # En GridDFS, los directorios son conceptuales y se crean automáticamente
    # cuando se crean archivos dentro de ellos
    return {"message": f"Directorio {directory_path} marcado para creación"}

@router.delete("/rmdir")
async def remove_directory(
    directory_path: str,
    current_user: DBUser = Depends(get_authenticated_user),
    db: Session = Depends(get_db)
):
    """Eliminar directorio y todos sus archivos"""
    try:
        # Listar todos los archivos en el directorio
        files_in_dir = file_service.list_files(db, current_user.id, directory_path)
        
        if not files_in_dir:
            return {"message": f"Directorio {directory_path} está vacío o no existe"}
        
        # Eliminar todos los archivos en el directorio
        deleted_count = 0
        for file_info in files_in_dir:
            if file_service.delete_file(db, current_user.id, file_info.file_path):
                deleted_count += 1
        
        return {
            "message": f"Directorio {directory_path} eliminado",
            "deleted_files": deleted_count
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error eliminando directorio: {str(e)}"
        )
