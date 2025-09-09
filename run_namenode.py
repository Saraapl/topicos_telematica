#!/usr/bin/env python3
"""
Script para ejecutar el NameNode con imports absolutos
"""
import sys
import os
import uvicorn

# Agregar el directorio actual al PYTHONPATH para imports absolutos
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Ahora importar y ejecutar el NameNode
if __name__ == "__main__":
    from namenode.main import app
    uvicorn.run(app, host="0.0.0.0", port=8001)
