import asyncio
import logging
import sys
from typing import Optional

import click

from .client import DFSClient
from core.config import config
from core.logging import setup_logging
from shared.utils import format_bytes
from core.exceptions import DFSClientError, DFSMetadataError

logger = logging.getLogger(__name__)


def setup_cli():
    """Configuración centralizada del CLI"""
    setup_logging()


def progress_bar(progress: float):
    """Muestra una barra de progreso"""
    bar_length = 40
    filled = int(bar_length * progress / 100)
    bar = "=" * filled + "-" * (bar_length - filled)
    print(f"\r[{bar}] {progress:.1f}%", end="", flush=True)


class DFSContext:
    """Contexto compartido para comandos CLI"""

    def __init__(self, metadata_url: str, verbose: bool = False):
        self.client = DFSClient(metadata_url)
        self.verbose = verbose


@click.group()
@click.option(
    "--metadata-url", default=config.metadata_url, help="URL del Metadata Service"
)
@click.option("--verbose", "-v", is_flag=True, help="Logging verbose")
@click.pass_context
def cli(ctx, metadata_url: str, verbose: bool):
    """DFS - Sistema de Archivos Distribuido"""
    setup_cli()

    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    ctx.obj = DFSContext(metadata_url, verbose)


@cli.command()
@click.argument("local_path", type=click.Path(exists=True))
@click.argument("remote_path")
@click.pass_context
def upload(ctx, local_path: str, remote_path: str):
    """Sube un archivo al DFS"""
    client = ctx.obj.client

    click.echo(f"Subiendo {local_path} -> {remote_path}")

    async def do_upload():
        try:
            success = await client.upload(
                local_path, remote_path, progress_callback=progress_bar
            )
            print()  # Nueva línea después de la barra de progreso

            if success:
                click.echo(click.style("✓ Subida completada", fg="green"))
            else:
                click.echo(click.style("✗ Subida fallida", fg="red"))
                sys.exit(1)

        except (DFSClientError, DFSMetadataError) as e:
            click.echo(click.style(f"✗ Error: {e}", fg="red"))
            sys.exit(1)
        except Exception as e:
            click.echo(click.style(f"✗ Error inesperado: {e}", fg="red"))
            sys.exit(1)

    asyncio.run(do_upload())


@cli.command()
@click.argument("remote_path")
@click.argument("local_path", type=click.Path())
@click.pass_context
def download(ctx, remote_path: str, local_path: str):
    """Descarga un archivo del DFS al equipo local del usuario"""
    client = ctx.obj.client

    click.echo(f"Descargando {remote_path} -> {local_path}")

    async def do_download():
        try:
            success = await client.download(
                remote_path, local_path, progress_callback=progress_bar
            )
            print()

            if success:
                click.echo(click.style("✓ Descarga completada", fg="green"))
            else:
                click.echo(click.style("✗ Descarga fallida", fg="red"))
                sys.exit(1)

        except (DFSClientError, DFSMetadataError) as e:
            click.echo(click.style(f"✗ Error: {e}", fg="red"))
            sys.exit(1)

    asyncio.run(do_download())


@cli.command()
@click.option("--prefix", default=None, help="Filtrar por prefijo")
@click.option("--limit", default=100, help="Límite de resultados")
@click.pass_context
def ls(ctx, prefix: Optional[str], limit: int):
    """Lista los archivos en el DFS"""
    client = ctx.obj.client

    async def do_list():
        try:
            files = await client.list_files(prefix=prefix, limit=limit)

            if not files:
                click.echo("No hay archivos")
                return

            # Tabla de archivos
            click.echo(f"\n{'PATH':<40} {'SIZE':<12} {'CHUNKS':<8} {'CREATED':<20}")
            click.echo("-" * 80)

            for file in files:
                size_str = format_bytes(file.size)
                created_str = file.created_at.strftime("%Y-%m-%d %H:%M:%S")
                click.echo(
                    f"{file.path:<40} {size_str:<12} {len(file.chunks):<8} {created_str:<20}"
                )

            click.echo(f"\nTotal: {len(files)} archivos")

        except DFSMetadataError as e:
            click.echo(click.style(f"✗ Error: {e}", fg="red"))
            sys.exit(1)

    asyncio.run(do_list())


@cli.command()
@click.argument("remote_path")
@click.option("--permanent", is_flag=True, help="Eliminar permanentemente")
@click.pass_context
def rm(ctx, remote_path: str, permanent: bool):
    """Elimina permanentemente un archivo del DFS"""
    client = ctx.obj.client

    if permanent:
        if not click.confirm(f"¿Eliminar permanentemente {remote_path}?"):
            return

    async def do_delete():
        try:
            success = await client.delete(remote_path, permanent=permanent)

            if success:
                action = (
                    "eliminado permanentemente"
                    if permanent
                    else "marcado como eliminado"
                )
                click.echo(click.style(f"✓ Archivo {action} exitósamente", fg="green"))
            else:
                click.echo(click.style("✗ Error eliminando archivo", fg="red"))
                sys.exit(1)

        except DFSMetadataError as e:
            click.echo(click.style(f"✗ Error: {e}", fg="red"))
            sys.exit(1)

    asyncio.run(do_delete())


@cli.command()
@click.pass_context
def nodes(ctx):
    """Lista los nodos del cluster"""
    client = ctx.obj.client

    async def do_nodes():
        try:
            nodes = await client.get_nodes()

            if not nodes:
                click.echo("No hay nodos registrados")
                return

            # Tabla de nodos
            click.echo(
                f"\n{'NODE ID':<30} {'HOST':<20} {'PORT':<8} {'FREE':<12} {'CHUNKS':<8} {'STATE':<10}"
            )
            click.echo("-" * 90)

            for node in nodes:
                free_str = format_bytes(node.free_space)
                state_color = "green" if node.state.value == "active" else "red"
                click.echo(
                    f"{node.node_id:<30} {node.host:<20} {node.port:<8} "
                    f"{free_str:<12} {node.chunk_count:<8} "
                    f"{click.style(node.state.value, fg=state_color):<10}"
                )

            click.echo(f"\nTotal: {len(nodes)} nodos")

        except DFSMetadataError as e:
            click.echo(click.style(f"✗ Error: {e}", fg="red"))
            sys.exit(1)

    asyncio.run(do_nodes())


@cli.command()
@click.pass_context
def status(ctx):
    """Muestra el estado del sistema"""
    client = ctx.obj.client

    async def do_status():
        try:
            health = await client.health()
            status_color = "green" if health.get("status") == "healthy" else "yellow"

            click.echo(
                f"\nEstado: {click.style(health.get('status', 'unknown'), fg=status_color)}"
            )

            if "details" in health:
                details = health["details"]
                click.echo(f"\nNodos totales: {details.get('total_nodes', 0)}")
                click.echo(f"Nodos activos: {details.get('active_nodes', 0)}")
                click.echo(
                    f"Factor de replicación: {details.get('replication_factor', 0)}"
                )

        except DFSMetadataError as e:
            click.echo(click.style(f"✗ Error: {e}", fg="red"))
            sys.exit(1)

    asyncio.run(do_status())


@cli.command()
@click.argument("remote_path")
@click.pass_context
def info(ctx, remote_path: str):
    """Muestra información detallada de un archivo"""
    client = ctx.obj.client

    async def do_info():
        try:
            # Obtener metadata
            files = await client.list_files(prefix=remote_path)
            file = None

            for f in files:
                if f.path == remote_path:
                    file = f
                    break

            if not file:
                click.echo(
                    click.style(f"✗ Archivo no encontrado: {remote_path}", fg="red")
                )
                sys.exit(1)

            click.echo(f"\nArchivo: {file.path}")
            click.echo(f"ID: {file.file_id}")
            click.echo(f"Tamaño: {format_bytes(file.size)}")
            click.echo(f"Creado: {file.created_at.strftime('%Y-%m-%d %H:%M:%S')}")
            click.echo(f"Modificado: {file.modified_at.strftime('%Y-%m-%d %H:%M:%S')}")
            click.echo(f"Chunks: {len(file.chunks)}")

            click.echo("\nRéplicas por chunk:")
            for i, chunk in enumerate(file.chunks):
                click.echo(f"\n Chunk {i} ({format_bytes(chunk.size)}):")
                click.echo(f" ID: {chunk.chunk_id}")
                click.echo(f" Checksum: {chunk.checksum or 'N/A'}")
                click.echo(f" Réplicas: {len(chunk.replicas)}")

                for j, replica in enumerate(chunk.replicas):
                    state_color = (
                        "green" if replica.state.value == "committed" else "yellow"
                    )
                    click.echo(
                        f" {j + 1}. {replica.url} - {click.style(replica.state.value, fg=state_color)}"
                    )

        except DFSMetadataError as e:
            click.echo(click.style(f"✗ Error: {e}", fg="red"))
            sys.exit(1)

    asyncio.run(do_info())


if __name__ == "__main__":
    cli(obj={})
