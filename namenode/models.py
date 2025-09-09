"""
Modelos de base de datos SQLAlchemy para el NameNode
"""
from sqlalchemy import Column, Integer, String, BigInteger, DateTime, ForeignKey, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(50), unique=True, nullable=False, index=True)
    password_hash = Column(String(255), nullable=False)
    created_at = Column(DateTime, server_default=func.now())
    
    # Relationships
    files = relationship("File", back_populates="user", cascade="all, delete-orphan")
    upload_sessions = relationship("UploadSession", back_populates="user", cascade="all, delete-orphan")

class File(Base):
    __tablename__ = "files"
    
    id = Column(Integer, primary_key=True, index=True)
    filename = Column(String(255), nullable=False)
    file_path = Column(String(500), nullable=False)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    file_size = Column(BigInteger, nullable=False)
    file_hash = Column(String(64), nullable=False)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    __table_args__ = (UniqueConstraint('user_id', 'file_path', name='unique_user_file_path'),)
    
    # Relationships
    user = relationship("User", back_populates="files")
    blocks = relationship("Block", back_populates="file", cascade="all, delete-orphan")

class DataNode(Base):
    __tablename__ = "datanodes"
    
    id = Column(Integer, primary_key=True, index=True)
    node_id = Column(String(50), unique=True, nullable=False, index=True)
    host = Column(String(100), nullable=False)
    port = Column(Integer, nullable=False)
    status = Column(String(20), default="active", index=True)
    last_heartbeat = Column(DateTime, server_default=func.now())
    storage_used = Column(BigInteger, default=0)
    storage_capacity = Column(BigInteger, default=0)
    created_at = Column(DateTime, server_default=func.now())
    
    # Relationships
    block_locations = relationship("BlockLocation", back_populates="datanode", cascade="all, delete-orphan")

class Block(Base):
    __tablename__ = "blocks"
    
    id = Column(Integer, primary_key=True, index=True)
    block_id = Column(String(50), unique=True, nullable=False, index=True)
    file_id = Column(Integer, ForeignKey("files.id", ondelete="CASCADE"), nullable=False)
    block_index = Column(Integer, nullable=False)
    block_size = Column(BigInteger, nullable=False)
    block_hash = Column(String(64), nullable=False)
    created_at = Column(DateTime, server_default=func.now())
    
    __table_args__ = (UniqueConstraint('file_id', 'block_index', name='unique_file_block_index'),)
    
    # Relationships
    file = relationship("File", back_populates="blocks")
    locations = relationship("BlockLocation", back_populates="block", cascade="all, delete-orphan")

class BlockLocation(Base):
    __tablename__ = "block_locations"
    
    id = Column(Integer, primary_key=True, index=True)
    block_id = Column(String(50), ForeignKey("blocks.block_id", ondelete="CASCADE"), nullable=False, index=True)
    datanode_id = Column(String(50), ForeignKey("datanodes.node_id", ondelete="CASCADE"), nullable=False, index=True)
    status = Column(String(20), default="active")
    storage_path = Column(String(500))
    created_at = Column(DateTime, server_default=func.now())
    confirmed_at = Column(DateTime, server_default=func.now())
    
    __table_args__ = (UniqueConstraint('block_id', 'datanode_id', name='unique_block_datanode'),)
    
    # Relationships
    block = relationship("Block", back_populates="locations")
    datanode = relationship("DataNode", back_populates="block_locations")

class UploadSession(Base):
    __tablename__ = "upload_sessions"
    
    id = Column(Integer, primary_key=True, index=True)
    upload_id = Column(String(50), unique=True, nullable=False, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    file_path = Column(String(500), nullable=False)
    total_blocks = Column(Integer, nullable=False)
    completed_blocks = Column(Integer, default=0)
    status = Column(String(20), default="pending", index=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    # Relationships
    user = relationship("User", back_populates="upload_sessions")
