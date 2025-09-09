"""
Utilidades para mostrar progreso de operaciones
"""
import time
from typing import Optional, Callable
from rich.progress import Progress, TaskID, BarColumn, TextColumn, TimeRemainingColumn, FileSizeColumn, TransferSpeedColumn
from rich.console import Console

class ProgressReporter:
    def __init__(self):
        self.console = Console()
        self.progress: Optional[Progress] = None
        self.current_task: Optional[TaskID] = None
    
    def start_upload_progress(self, filename: str, total_size: int) -> TaskID:
        """Iniciar barra de progreso para upload"""
        self.progress = Progress(
            TextColumn("[bold blue]{task.description}", justify="right"),
            BarColumn(bar_width=None),
            "[progress.percentage]{task.percentage:>3.1f}%",
            "•",
            FileSizeColumn(),
            "•",
            TransferSpeedColumn(),
            "•",
            TimeRemainingColumn(),
            console=self.console,
            transient=True
        )
        
        self.progress.start()
        self.current_task = self.progress.add_task(
            f"Uploading {filename}",
            total=total_size
        )
        return self.current_task
    
    def start_download_progress(self, filename: str, total_size: int) -> TaskID:
        """Iniciar barra de progreso para download"""
        self.progress = Progress(
            TextColumn("[bold green]{task.description}", justify="right"),
            BarColumn(bar_width=None),
            "[progress.percentage]{task.percentage:>3.1f}%",
            "•",
            FileSizeColumn(),
            "•",
            TransferSpeedColumn(),
            "•",
            TimeRemainingColumn(),
            console=self.console,
            transient=True
        )
        
        self.progress.start()
        self.current_task = self.progress.add_task(
            f"Downloading {filename}",
            total=total_size
        )
        return self.current_task
    
    def update_progress(self, task_id: TaskID, completed: int):
        """Actualizar progreso"""
        if self.progress:
            self.progress.update(task_id, completed=completed)
    
    def advance_progress(self, task_id: TaskID, advance: int):
        """Avanzar progreso por cantidad específica"""
        if self.progress:
            self.progress.advance(task_id, advance)
    
    def finish_progress(self):
        """Finalizar barra de progreso"""
        if self.progress:
            self.progress.stop()
            self.progress = None
            self.current_task = None
    
    def print_success(self, message: str):
        """Imprimir mensaje de éxito"""
        self.console.print(f"✅ {message}", style="bold green")
    
    def print_error(self, message: str):
        """Imprimir mensaje de error"""
        self.console.print(f"❌ {message}", style="bold red")
    
    def print_warning(self, message: str):
        """Imprimir mensaje de advertencia"""
        self.console.print(f"⚠️  {message}", style="bold yellow")
    
    def print_info(self, message: str):
        """Imprimir mensaje informativo"""
        self.console.print(f"ℹ️  {message}", style="bold blue")
    
    def print_table(self, title: str, headers: list, rows: list):
        """Imprimir tabla formateada"""
        from rich.table import Table
        
        table = Table(title=title, show_header=True, header_style="bold magenta")
        
        for header in headers:
            table.add_column(header)
        
        for row in rows:
            table.add_row(*[str(cell) for cell in row])
        
        self.console.print(table)
    
    def print_status_panel(self, title: str, content: str):
        """Imprimir panel de estado"""
        from rich.panel import Panel
        
        panel = Panel(
            content,
            title=title,
            border_style="blue",
            padding=(1, 2)
        )
        
        self.console.print(panel)
    
    def confirm_action(self, message: str) -> bool:
        """Solicitar confirmación del usuario"""
        response = self.console.input(f"[bold yellow]{message} (y/N): [/bold yellow]")
        return response.lower().strip() in ['y', 'yes', 'sí', 's']
    
    def prompt_input(self, prompt: str, password: bool = False) -> str:
        """Solicitar input del usuario"""
        if password:
            import getpass
            return getpass.getpass(prompt)
        else:
            return self.console.input(f"[bold cyan]{prompt}:[/bold cyan] ")

# Instancia global del reporter de progreso
progress_reporter = ProgressReporter()
