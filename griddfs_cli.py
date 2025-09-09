#!/usr/bin/env python3
"""
GridDFS CLI - Cliente de línea de comandos para el sistema de archivos distribuido
"""
import os
import sys
import logging
from typing import Optional
import click

# Agregar el directorio actual al path para importar módulos
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from client.client import GridDFSClient
from client.auth import auth_manager
from client.progress import progress_reporter

# Configurar logging
logging.basicConfig(level=logging.WARNING, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# Grupo principal de comandos
@click.group()
@click.option('--verbose', '-v', is_flag=True, help='Habilitar logging verbose')
@click.option('--namenode-url', help='URL del NameNode (override)')
def cli(verbose: bool, namenode_url: Optional[str]):
    """GridDFS - Sistema de Archivos Distribuido"""
    if verbose:
        logging.getLogger().setLevel(logging.INFO)
    
    # Actualizar URL si se proporciona
    if namenode_url and auth_manager.is_authenticated():
        auth_info = auth_manager.load_auth_info()
        if auth_info:
            auth_info['namenode_url'] = namenode_url
            auth_manager.save_auth_info(
                auth_info['username'], 
                auth_info['token'], 
                namenode_url
            )

@cli.command()
@click.argument('username')
@click.option('--password', '-p', prompt=True, hide_input=True, help='Password del usuario')
def login(username: str, password: str):
    """Autenticar con el sistema GridDFS"""
    try:
        client = GridDFSClient()
        token_data = client.login(username, password)
        
        # Guardar información de autenticación
        auth_manager.save_auth_info(
            username, 
            token_data['access_token'],
            client.namenode_url
        )
        
        progress_reporter.print_success(f"Autenticado exitosamente como {username}")
        
    except Exception as e:
        progress_reporter.print_error(f"Error de autenticación: {str(e)}")
        sys.exit(1)

@cli.command()
@click.argument('username')
@click.option('--password', '-p', prompt=True, hide_input=True, help='Password para el nuevo usuario')
@click.option('--confirm-password', '-c', prompt=True, hide_input=True, help='Confirmar password')
def register(username: str, password: str, confirm_password: str):
    """Registrar nuevo usuario en el sistema"""
    if password != confirm_password:
        progress_reporter.print_error("Las contraseñas no coinciden")
        sys.exit(1)
    
    try:
        client = GridDFSClient()
        user_data = client.register(username, password)
        
        progress_reporter.print_success(f"Usuario {username} registrado exitosamente")
        progress_reporter.print_info("Use 'griddfs login' para autenticarse")
        
    except Exception as e:
        progress_reporter.print_error(f"Error en registro: {str(e)}")
        sys.exit(1)

@cli.command()
def logout():
    """Cerrar sesión actual"""
    if not auth_manager.is_authenticated():
        progress_reporter.print_warning("No hay sesión activa")
        return
    
    current_user = auth_manager.get_current_user()
    auth_manager.clear_auth_info()
    progress_reporter.print_success(f"Sesión cerrada para {current_user}")

def _get_authenticated_client() -> GridDFSClient:
    """Obtener cliente autenticado o salir"""
    if not auth_manager.is_authenticated():
        progress_reporter.print_error("No está autenticado. Use 'griddfs login' primero")
        sys.exit(1)
    
    auth_info = auth_manager.load_auth_info()
    return GridDFSClient(
        namenode_url=auth_info['namenode_url'],
        auth_token=auth_info['token']
    )

@cli.command()
@click.argument('local_file', type=click.Path(exists=True, readable=True))
@click.argument('remote_path')
def put(local_file: str, remote_path: str):
    """Subir archivo al sistema distribuido"""
    client = _get_authenticated_client()
    
    try:
        # Obtener información del archivo
        file_size = os.path.getsize(local_file)
        filename = os.path.basename(local_file)
        
        progress_reporter.print_info(f"Subiendo {filename} ({file_size} bytes) a {remote_path}")
        
        # Iniciar progreso
        task_id = progress_reporter.start_upload_progress(filename, file_size)
        
        # Ejecutar upload
        result = client.upload_file(local_file, remote_path)
        
        # Simular progreso (el upload real ya se procesó en el servidor)
        progress_reporter.update_progress(task_id, file_size)
        progress_reporter.finish_progress()
        
        progress_reporter.print_success(f"Archivo subido exitosamente: {remote_path}")
        progress_reporter.print_info(f"Upload ID: {result['upload_id']}")
        
    except Exception as e:
        progress_reporter.finish_progress()
        progress_reporter.print_error(f"Error subiendo archivo: {str(e)}")
        sys.exit(1)

@cli.command()
@click.argument('remote_path')
@click.argument('local_file', type=click.Path())
def get(remote_path: str, local_file: str):
    """Descargar archivo del sistema distribuido"""
    client = _get_authenticated_client()
    
    try:
        # Verificar si el archivo local ya existe
        if os.path.exists(local_file):
            if not progress_reporter.confirm_action(f"El archivo {local_file} ya existe. ¿Sobrescribir?"):
                progress_reporter.print_info("Descarga cancelada")
                return
        
        progress_reporter.print_info(f"Descargando {remote_path} a {local_file}")
        
        # Ejecutar download (con progreso interno)
        success = client.download_file(remote_path, local_file)
        
        if success:
            file_size = os.path.getsize(local_file)
            progress_reporter.print_success(f"Archivo descargado exitosamente: {local_file} ({file_size} bytes)")
        else:
            progress_reporter.print_error("Error descargando archivo")
            sys.exit(1)
            
    except Exception as e:
        progress_reporter.print_error(f"Error descargando archivo: {str(e)}")
        sys.exit(1)

@cli.command()
@click.argument('path', default='/')
def ls(path: str):
    """Listar archivos en el path especificado"""
    client = _get_authenticated_client()
    
    try:
        files = client.list_files(path)
        
        if not files:
            progress_reporter.print_info(f"No hay archivos en {path}")
            return
        
        # Preparar datos para tabla
        headers = ["Nombre", "Ruta", "Tamaño", "Fecha de creación"]
        rows = []
        
        for file_info in files:
            size_mb = file_info['file_size'] / (1024 * 1024)
            created_at = file_info['created_at'][:19] if file_info['created_at'] else 'N/A'
            
            rows.append([
                file_info['filename'],
                file_info['file_path'],
                f"{size_mb:.2f} MB",
                created_at
            ])
        
        progress_reporter.print_table(f"Archivos en {path}", headers, rows)
        progress_reporter.print_info(f"Total: {len(files)} archivos")
        
    except Exception as e:
        progress_reporter.print_error(f"Error listando archivos: {str(e)}")
        sys.exit(1)

@cli.command()
@click.argument('file_path')
@click.option('--force', '-f', is_flag=True, help='Forzar eliminación sin confirmación')
def rm(file_path: str, force: bool):
    """Eliminar archivo del sistema"""
    client = _get_authenticated_client()
    
    try:
        if not force:
            if not progress_reporter.confirm_action(f"¿Eliminar {file_path}?"):
                progress_reporter.print_info("Eliminación cancelada")
                return
        
        success = client.delete_file(file_path)
        
        if success:
            progress_reporter.print_success(f"Archivo eliminado: {file_path}")
        else:
            progress_reporter.print_error("Error eliminando archivo")
            sys.exit(1)
            
    except Exception as e:
        progress_reporter.print_error(f"Error eliminando archivo: {str(e)}")
        sys.exit(1)

@cli.command()
@click.argument('directory_path')
def mkdir(directory_path: str):
    """Crear directorio (conceptual)"""
    client = _get_authenticated_client()
    
    try:
        success = client.create_directory(directory_path)
        
        if success:
            progress_reporter.print_success(f"Directorio marcado para creación: {directory_path}")
            progress_reporter.print_info("Los directorios se crean automáticamente al subir archivos")
        else:
            progress_reporter.print_error("Error creando directorio")
            sys.exit(1)
            
    except Exception as e:
        progress_reporter.print_error(f"Error creando directorio: {str(e)}")
        sys.exit(1)

@cli.command()
@click.argument('directory_path')
@click.option('--force', '-f', is_flag=True, help='Forzar eliminación sin confirmación')
def rmdir(directory_path: str, force: bool):
    """Eliminar directorio y todos sus archivos"""
    client = _get_authenticated_client()
    
    try:
        if not force:
            if not progress_reporter.confirm_action(f"¿Eliminar directorio {directory_path} y todo su contenido?"):
                progress_reporter.print_info("Eliminación cancelada")
                return
        
        result = client.remove_directory(directory_path)
        
        deleted_count = result.get('deleted_files', 0)
        if deleted_count > 0:
            progress_reporter.print_success(f"Directorio eliminado: {directory_path}")
            progress_reporter.print_info(f"Archivos eliminados: {deleted_count}")
        else:
            progress_reporter.print_info(f"Directorio {directory_path} estaba vacío o no existía")
            
    except Exception as e:
        progress_reporter.print_error(f"Error eliminando directorio: {str(e)}")
        sys.exit(1)

@cli.command()
def status():
    """Mostrar estado del sistema GridDFS"""
    client = _get_authenticated_client()
    
    try:
        system_status = client.get_system_status()
        
        # Información general del sistema
        total_capacity_gb = system_status['total_capacity'] / (1024**3)
        total_used_gb = system_status['total_used'] / (1024**3)
        total_available_gb = system_status['total_available'] / (1024**3)
        used_percentage = (system_status['total_used'] / system_status['total_capacity'] * 100) if system_status['total_capacity'] > 0 else 0
        
        system_info = f"""
[bold]Estado del Sistema GridDFS[/bold]

📊 Capacidad Total: {total_capacity_gb:.2f} GB
💾 Espacio Usado: {total_used_gb:.2f} GB ({used_percentage:.1f}%)
🆓 Espacio Disponible: {total_available_gb:.2f} GB
🖥️  DataNodes Activos: {system_status['active_nodes']} / {len(system_status['datanodes'])}
        """
        
        progress_reporter.print_status_panel("Estado del Sistema", system_info.strip())
        
        # Información de DataNodes
        if system_status['datanodes']:
            headers = ["DataNode", "Estado", "Capacidad", "Usado", "Disponible", "Último Heartbeat"]
            rows = []
            
            for node in system_status['datanodes']:
                capacity_gb = node['storage_capacity'] / (1024**3) if node['storage_capacity'] else 0
                used_gb = node['storage_used'] / (1024**3) if node['storage_used'] else 0
                available_gb = capacity_gb - used_gb
                last_hb = node['last_heartbeat'][:19] if node['last_heartbeat'] else 'Nunca'
                
                status_emoji = "🟢" if node['status'] == 'active' else "🔴"
                
                rows.append([
                    f"{status_emoji} {node['node_id']}",
                    node['status'],
                    f"{capacity_gb:.2f} GB",
                    f"{used_gb:.2f} GB",
                    f"{available_gb:.2f} GB",
                    last_hb
                ])
            
            progress_reporter.print_table("DataNodes", headers, rows)
        
        # Usuario actual
        current_user = auth_manager.get_current_user()
        progress_reporter.print_info(f"Usuario: {current_user}")
        
    except Exception as e:
        progress_reporter.print_error(f"Error obteniendo estado del sistema: {str(e)}")
        sys.exit(1)

@cli.command()
def whoami():
    """Mostrar usuario actual"""
    if not auth_manager.is_authenticated():
        progress_reporter.print_warning("No está autenticado")
        return
    
    current_user = auth_manager.get_current_user()
    namenode_url = auth_manager.get_namenode_url()
    
    progress_reporter.print_info(f"Usuario: {current_user}")
    progress_reporter.print_info(f"NameNode: {namenode_url}")

if __name__ == '__main__':
    cli()
