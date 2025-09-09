# GridDFS - Sistema de Archivos Distribuido

GridDFS es un sistema de archivos distribuido implementado con Python que utiliza una arquitectura híbrida con NameNode (FastAPI) y DataNodes (RabbitMQ Workers).

## Características Principales

- **Distribución Automática**: Los archivos se dividen en bloques de 64MB que se distribuyen automáticamente usando RabbitMQ Fanout Exchange
- **Redundancia Adaptativa**: Cada DataNode decide si almacenar bloques basado en políticas de capacidad y load balancing
- **API REST**: NameNode expone APIs RESTful para operaciones de metadatos
- **CLI Intuitivo**: Cliente de línea de comandos con barras de progreso y experiencia de usuario amigable
- **Tolerancia a Fallos**: Sistema robusto con detección automática de nodos caídos y recuperación
- **Autenticación JWT**: Sistema de autenticación seguro con tokens JWT

## Arquitectura

```
Cliente (CLI) ↔ NameNode (FastAPI/REST) ↔ PostgreSQL
                     ↓
                RabbitMQ Broker
                     ↓
            DataNode1, DataNode2, DataNode3
             (RabbitMQ Workers)
```

## Quick Start

### 1. Iniciar el sistema con Docker Compose

```bash
# Clonar el repositorio y navegar al directorio
cd griddfs/

# Construir e iniciar todos los servicios
docker-compose up --build

# O en modo daemon
docker-compose up -d --build
```

### 2. Usar el CLI

```bash
# Registrar nuevo usuario
python3 griddfs_cli.py register usuario123

# Autenticarse
python3 griddfs_cli.py login usuario123

# Subir archivo
python3 griddfs_cli.py put archivo_local.txt /remoto/archivo.txt

# Descargar archivo
python3 griddfs_cli.py get /remoto/archivo.txt archivo_descargado.txt

# Listar archivos
python3 griddfs_cli.py ls /remoto/

# Ver estado del sistema
python3 griddfs_cli.py status
```

## Comandos del CLI

| Comando | Descripción |
|---------|-------------|
| `register <username>` | Registrar nuevo usuario |
| `login <username>` | Autenticar usuario |
| `logout` | Cerrar sesión |
| `put <local> <remote>` | Subir archivo |
| `get <remote> <local>` | Descargar archivo |
| `ls [path]` | Listar archivos |
| `rm <path>` | Eliminar archivo |
| `mkdir <path>` | Crear directorio |
| `rmdir <path>` | Eliminar directorio |
| `status` | Estado del sistema |
| `whoami` | Usuario actual |

## Servicios

### NameNode (Puerto 8080)
- **FastAPI**: APIs REST para metadatos y coordinación
- **PostgreSQL**: Base de datos de metadatos
- **Gestión de Uploads**: Coordinación de distribución de bloques
- **Autenticación**: JWT tokens

### DataNodes
- **Workers RabbitMQ**: Procesamiento de mensajes de bloques
- **Storage Policy**: Decisiones inteligentes de almacenamiento
- **Heartbeats**: Monitoreo de salud en tiempo real
- **Load Balancing**: Distribución probabilística de carga

### RabbitMQ (Puertos 5672, 15672)
- **Fanout Exchange**: Distribución automática a todos los DataNodes
- **Direct Exchange**: Comunicación dirigida para requests
- **Colas Dinámicas**: Configuración automática por DataNode

### PostgreSQL (Puerto 5432)
- **Metadatos**: Información de archivos y usuarios
- **Block Tracking**: Ubicaciones y confirmaciones
- **Session Management**: Tracking de uploads en progreso

## Desarrollo

### Estructura del Proyecto

```
griddfs/
├── docker-compose.yml          # Orquestación de servicios
├── requirements.txt            # Dependencias Python
├── init.sql                   # Schema de base de datos
├── namenode/                  # NameNode (FastAPI)
│   ├── main.py               # Aplicación principal
│   ├── models.py             # Modelos SQLAlchemy
│   ├── database.py           # Configuración DB
│   ├── auth.py              # Autenticación JWT
│   ├── services/            # Servicios de negocio
│   └── api/                 # Rutas REST
├── datanode/                  # DataNode (RabbitMQ Worker)
│   ├── main.py              # Worker principal
│   ├── worker.py            # Lógica de procesamiento
│   ├── storage_policy.py    # Políticas de almacenamiento
│   └── heartbeat.py         # Servicio de heartbeat
├── client/                    # Cliente CLI
│   ├── griddfs_cli.py       # CLI principal
│   ├── client.py            # Cliente HTTP/RabbitMQ
│   ├── auth.py              # Gestión de autenticación
│   └── progress.py          # Barras de progreso
├── shared/                    # Código compartido
│   ├── config.py            # Configuración
│   ├── models.py            # Modelos Pydantic
│   └── rabbitmq_config.py   # Configuración RabbitMQ
└── data/                      # Volúmenes de datos
    ├── postgres/
    ├── rabbitmq/
    ├── datanode1/
    ├── datanode2/
    └── datanode3/
```

### Variables de Entorno

Configurables en `.env`:

```env
DATABASE_URL=postgresql://griddfs:griddfs123@localhost:5432/griddfs
RABBITMQ_URL=amqp://griddfs:griddfs123@localhost:5672/
JWT_SECRET=your-secret-key-change-in-production
NAMENODE_URL=http://localhost:8080
BLOCK_SIZE=67108864  # 64MB
```

## Monitoreo

### RabbitMQ Management
Acceder a http://localhost:15672
- Usuario: `griddfs`
- Password: `griddfs123`

### Logs
```bash
# Ver logs de todos los servicios
docker-compose logs -f

# Ver logs de un servicio específico
docker-compose logs -f namenode
docker-compose logs -f datanode1
```

### Estado del Sistema
```bash
# Desde el CLI
python3 griddfs_cli.py status

# Endpoint directo
curl http://localhost:8080/system/status -H "Authorization: Bearer <token>"
```

## Escalabilidad

Para agregar más DataNodes:

```yaml
# Agregar en docker-compose.yml
datanode4:
  build:
    context: .
    dockerfile: datanode/Dockerfile
  environment:
    - NODE_ID=datanode4
    - RABBITMQ_URL=amqp://griddfs:griddfs123@rabbitmq:5672/
    - STORAGE_CAPACITY=10737418240
  volumes:
    - ./data/datanode4:/app/storage
```

## Troubleshooting

### Problemas Comunes

1. **DataNode no se conecta**
   - Verificar que RabbitMQ esté ejecutándose
   - Verificar configuración de RABBITMQ_URL

2. **Error de autenticación**
   - Verificar que JWT_SECRET coincida entre servicios
   - Hacer login nuevamente si el token expiró

3. **Error de upload**
   - Verificar espacio disponible en DataNodes
   - Verificar que al menos un DataNode esté activo

4. **PostgreSQL connection error**
   - Verificar que PostgreSQL esté ejecutándose
   - Verificar DATABASE_URL

### Comandos Útiles

```bash
# Reiniciar servicios
docker-compose restart

# Ver estado de contenedores
docker-compose ps

# Limpiar volúmenes (¡CUIDADO: elimina todos los datos!)
docker-compose down -v

# Rebuild completo
docker-compose down && docker-compose up --build
```

## Licencia

Este proyecto está licenciado bajo la MIT License.
"# topicos_telematica" 
