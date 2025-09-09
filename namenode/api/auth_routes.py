"""
Rutas de autenticación para el NameNode API
"""
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session
from shared.models import UserCreate, UserLogin, Token, User
from ..database import get_db
from ..auth import auth_service

router = APIRouter(prefix="/auth", tags=["authentication"])
security = HTTPBearer()

@router.post("/register", response_model=User)
async def register(user_data: UserCreate, db: Session = Depends(get_db)):
    """Registrar nuevo usuario"""
    import logging
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger(__name__)
    
    try:
        logger.info(f"Attempting to register user: {user_data.username}")
        
        # Verificar si el usuario ya existe
        from ..models import User as DBUser
        existing_user = db.query(DBUser).filter(DBUser.username == user_data.username).first()
        if existing_user:
            logger.warning(f"User {user_data.username} already exists")
            raise ValueError("El nombre de usuario ya existe")
        
        logger.info("Creating new user...")
        # Crear usuario
        db_user = auth_service.create_user(db, user_data)
        logger.info(f"User created successfully with ID: {db_user.id}")
        
        return User(
            id=db_user.id,
            username=db_user.username,
            created_at=db_user.created_at
        )
        
    except ValueError as e:
        logger.error(f"ValueError in registration: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Unexpected error in registration: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error interno del servidor: {str(e)}"
        )

@router.post("/login", response_model=Token)
async def login(user_data: UserLogin, db: Session = Depends(get_db)):
    """Autenticar usuario y obtener token"""
    # Autenticar usuario
    user = auth_service.authenticate_user(db, user_data.username, user_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Credenciales incorrectas",
            headers={"WWW-Authenticate": "Bearer"}
        )
    
    # Crear token de acceso
    access_token = auth_service.create_access_token(
        data={"sub": user.username, "user_id": user.id}
    )
    
    return Token(access_token=access_token, token_type="bearer")

@router.get("/me", response_model=User)
async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db)
):
    """Obtener información del usuario actual"""
    user = auth_service.get_current_user(db, credentials.credentials)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token inválido",
            headers={"WWW-Authenticate": "Bearer"}
        )
    
    return User(
        id=user.id,
        username=user.username,
        created_at=user.created_at
    )

# Dependency para obtener usuario actual
async def get_authenticated_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db)
):
    """Dependency para obtener usuario autenticado"""
    user = auth_service.get_current_user(db, credentials.credentials)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token inválido",
            headers={"WWW-Authenticate": "Bearer"}
        )
    return user
