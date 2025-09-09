"""
Autenticación y autorización para el NameNode
"""
from jose import JWTError, jwt
from datetime import datetime, timedelta
from typing import Optional
from passlib.context import CryptContext
from passlib.hash import bcrypt
from sqlalchemy.orm import Session
from shared.config import config
from shared.models import User, UserCreate, UserLogin
from .models import User as DBUser

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

class AuthService:
    def __init__(self):
        self.secret_key = config.JWT_SECRET
        self.algorithm = config.JWT_ALGORITHM
        self.expire_minutes = config.JWT_EXPIRE_MINUTES
    
    def verify_password(self, plain_password: str, hashed_password: str) -> bool:
        """Verificar password plano contra hash"""
        return pwd_context.verify(plain_password, hashed_password)
    
    def get_password_hash(self, password: str) -> str:
        """Obtener hash de password"""
        return pwd_context.hash(password)
    
    def authenticate_user(self, db: Session, username: str, password: str) -> Optional[DBUser]:
        """Autenticar usuario con username y password"""
        user = db.query(DBUser).filter(DBUser.username == username).first()
        if not user:
            return None
        if not self.verify_password(password, user.password_hash):
            return None
        return user
    
    def create_user(self, db: Session, user_create: UserCreate) -> DBUser:
        """Crear nuevo usuario"""
        hashed_password = self.get_password_hash(user_create.password)
        db_user = DBUser(
            username=user_create.username,
            password_hash=hashed_password
        )
        db.add(db_user)
        db.commit()
        db.refresh(db_user)
        return db_user
    
    def create_access_token(self, data: dict) -> str:
        """Crear token JWT de acceso"""
        to_encode = data.copy()
        expire = datetime.utcnow() + timedelta(minutes=self.expire_minutes)
        to_encode.update({"exp": expire})
        
        encoded_jwt = jwt.encode(to_encode, self.secret_key, algorithm=self.algorithm)
        return encoded_jwt
    
    def verify_token(self, token: str) -> Optional[dict]:
        """Verificar y decodificar token JWT"""
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
            return payload
        except JWTError:
            return None
    
    def get_current_user(self, db: Session, token: str) -> Optional[DBUser]:
        """Obtener usuario actual desde token"""
        payload = self.verify_token(token)
        if not payload:
            return None
        
        username = payload.get("sub")
        if not username:
            return None
        
        user = db.query(DBUser).filter(DBUser.username == username).first()
        return user

# Instancia global del servicio de autenticación
auth_service = AuthService()
