-- Initialize PostgreSQL database for GridDFS

-- Usuarios con autenticación
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Archivos del usuario
CREATE TABLE files (
    id SERIAL PRIMARY KEY,
    filename VARCHAR(255) NOT NULL,
    file_path VARCHAR(500) NOT NULL,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    file_size BIGINT NOT NULL,
    file_hash VARCHAR(64) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(user_id, file_path)
);

-- DataNodes disponibles
CREATE TABLE datanodes (
    id SERIAL PRIMARY KEY,
    node_id VARCHAR(50) UNIQUE NOT NULL,
    host VARCHAR(100) NOT NULL,
    port INTEGER NOT NULL,
    status VARCHAR(20) DEFAULT 'active',
    last_heartbeat TIMESTAMP DEFAULT NOW(),
    storage_used BIGINT DEFAULT 0,
    storage_capacity BIGINT DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Bloques de archivos
CREATE TABLE blocks (
    id SERIAL PRIMARY KEY,
    block_id VARCHAR(50) UNIQUE NOT NULL,
    file_id INTEGER REFERENCES files(id) ON DELETE CASCADE,
    block_index INTEGER NOT NULL,
    block_size BIGINT NOT NULL,
    block_hash VARCHAR(64) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(file_id, block_index)
);

-- Ubicaciones de bloques (descubierto via confirmaciones)
CREATE TABLE block_locations (
    id SERIAL PRIMARY KEY,
    block_id VARCHAR(50) REFERENCES blocks(block_id) ON DELETE CASCADE,
    datanode_id VARCHAR(50) REFERENCES datanodes(node_id) ON DELETE CASCADE,
    status VARCHAR(20) DEFAULT 'active',
    storage_path VARCHAR(500),
    created_at TIMESTAMP DEFAULT NOW(),
    confirmed_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(block_id, datanode_id)
);

-- Uploads en progreso para tracking
CREATE TABLE upload_sessions (
    id SERIAL PRIMARY KEY,
    upload_id VARCHAR(50) UNIQUE NOT NULL,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    file_path VARCHAR(500) NOT NULL,
    total_blocks INTEGER NOT NULL,
    completed_blocks INTEGER DEFAULT 0,
    status VARCHAR(20) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Índices para optimizar consultas
CREATE INDEX idx_files_user_path ON files(user_id, file_path);
CREATE INDEX idx_blocks_file_id ON blocks(file_id);
CREATE INDEX idx_block_locations_block_id ON block_locations(block_id);
CREATE INDEX idx_block_locations_datanode ON block_locations(datanode_id);
CREATE INDEX idx_datanodes_status ON datanodes(status);
CREATE INDEX idx_upload_sessions_status ON upload_sessions(status);

-- Crear usuario administrador por defecto
INSERT INTO users (username, password_hash) VALUES 
('admin', '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdBPj3ip6KV7/NG'); -- password: admin123
