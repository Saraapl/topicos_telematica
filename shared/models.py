"""
Modelos de datos compartidos para el sistema GridDFS
"""
from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field

class UserCreate(BaseModel):
    username: str = Field(..., min_length=3, max_length=50)
    password: str = Field(..., min_length=6)

class UserLogin(BaseModel):
    username: str
    password: str

class User(BaseModel):
    id: int
    username: str
    created_at: datetime
    
    class Config:
        from_attributes = True

class Token(BaseModel):
    access_token: str
    token_type: str

class FileInfo(BaseModel):
    id: Optional[int] = None
    filename: str
    file_path: str
    user_id: int
    file_size: int
    file_hash: str
    created_at: Optional[datetime] = None
    
    class Config:
        from_attributes = True

class BlockInfo(BaseModel):
    id: Optional[int] = None
    block_id: str
    file_id: int
    block_index: int
    block_size: int
    block_hash: str
    created_at: Optional[datetime] = None
    
    class Config:
        from_attributes = True

class DataNodeInfo(BaseModel):
    id: Optional[int] = None
    node_id: str
    host: str
    port: int
    status: str = "active"
    last_heartbeat: Optional[datetime] = None
    storage_used: int = 0
    storage_capacity: int = 0
    
    class Config:
        from_attributes = True

class BlockLocation(BaseModel):
    id: Optional[int] = None
    block_id: str
    datanode_id: str
    status: str = "active"
    storage_path: Optional[str] = None
    created_at: Optional[datetime] = None
    confirmed_at: Optional[datetime] = None
    
    class Config:
        from_attributes = True

class UploadSession(BaseModel):
    id: Optional[int] = None
    upload_id: str
    user_id: int
    file_path: str
    total_blocks: int
    completed_blocks: int = 0
    status: str = "pending"
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    class Config:
        from_attributes = True

# RabbitMQ Message Models
class RabbitMQMessage(BaseModel):
    message_type: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)

class StoreBlockMessage(RabbitMQMessage):
    message_type: str = "store_block"
    block_id: str
    block_data: str  # base64 encoded
    block_hash: str
    upload_id: str
    block_index: int
    block_size: int

class StorageConfirmationMessage(RabbitMQMessage):
    message_type: str = "storage_confirmed"
    block_id: str
    datanode_id: str
    storage_path: str
    status: str  # success, error, insufficient_space
    error_message: Optional[str] = None

class RequestBlockMessage(RabbitMQMessage):
    message_type: str = "request_block"
    block_id: str
    client_id: str
    response_queue: str

class BlockResponseMessage(RabbitMQMessage):
    message_type: str = "block_response"
    block_id: str
    block_data: Optional[str] = None  # base64 encoded
    status: str  # success, not_found, error
    error_message: Optional[str] = None

class HeartbeatMessage(RabbitMQMessage):
    message_type: str = "heartbeat"
    datanode_id: str
    status: str
    storage_used: int
    storage_capacity: int
    storage_available: int

# API Response Models
class UploadPlanResponse(BaseModel):
    upload_id: str
    blocks: List[Dict[str, Any]]
    total_blocks: int

class DownloadPlanResponse(BaseModel):
    file_info: FileInfo
    blocks: List[Dict[str, Any]]

class SystemStatusResponse(BaseModel):
    datanodes: List[DataNodeInfo]
    total_capacity: int
    total_used: int
    total_available: int
    active_nodes: int
