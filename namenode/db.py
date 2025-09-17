import sqlite3

# Base de datos temporal sqlite para almacenamiento de metadata

DB_PATH = "nameNode.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    # Tabla usuarios
    c.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        password TEXT NOT NULL
    )
    """)
    # Tabla archivos
    c.execute("""
    CREATE TABLE IF NOT EXISTS files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT NOT NULL,
        filename TEXT NOT NULL,
        metadata TEXT NOT NULL,
        block_location TEXT NOT NULL,
        FOREIGN KEY(username) REFERENCES users(username)
    )
    """)

    # Tabla de DataNodes
    c.execute("""
    CREATE TABLE IF NOT EXISTS datanodes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        host TEXT NOT NULL,
        port INTEGER NOT NULL,
        last_heartbeat TIMESTAMP
    )
    """)

    conn.commit()
    conn.close()

init_db()
