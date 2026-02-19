"""CLI for DBT Lineage Viewer."""

import webbrowser
from pathlib import Path

import click
import uvicorn
from rich.console import Console
from rich.panel import Panel

from . import __version__

console = Console()


@click.group()
@click.version_option(version=__version__)
def main() -> None:
    """DBT Lineage Viewer - Interactive lineage explorer."""
    pass


@main.command()
@click.argument("dbt_project_path", type=click.Path(exists=True, path_type=Path))
@click.option("-p", "--port", default=8142, help="Port to run server on")
@click.option("-h", "--host", default="127.0.0.1", help="Host to bind to")
@click.option(
    "--manifest",
    type=click.Path(path_type=Path),
    default=None,
    help="Path to manifest.json (default: target/manifest.json)",
)
@click.option("--open/--no-open", default=True, help="Auto-open browser")
def serve(
    dbt_project_path: Path,
    port: int,
    host: str,
    manifest: Path | None,
    open: bool,
) -> None:
    """
    Serve the lineage viewer for a DBT project.
    
    DBT_PROJECT_PATH: Path to the DBT project directory (or parent of target/)
    """
    # Resolve manifest path
    if manifest is None:
        manifest = dbt_project_path / "target" / "manifest.json"
    elif not manifest.is_absolute():
        manifest = dbt_project_path / manifest
    
    manifest = manifest.resolve()
    
    if not manifest.exists():
        console.print(
            Panel(
                f"[red]Manifest not found:[/red] {manifest}\n\n"
                "Run [cyan]dbt parse[/cyan] or [cyan]dbt compile[/cyan] in your DBT project first.",
                title="Error",
                border_style="red",
            )
        )
        raise SystemExit(1)
    
    # Set manifest path via env for server startup
    import os
    os.environ["DBT_LINEAGE_MANIFEST"] = str(manifest)
    
    from .server import app
    
    url = f"http://{host}:{port}"
    
    console.print(
        Panel(
            f"[green]✓[/green] Loaded manifest: [cyan]{manifest}[/cyan]\n"
            f"[green]✓[/green] Server running at: [link={url}]{url}[/link]\n\n"
            "[dim]Press Ctrl+C to stop[/dim]",
            title="DBT Lineage Viewer",
            border_style="green",
        )
    )
    
    if open:
        webbrowser.open(url)
    
    uvicorn.run(app, host=host, port=port, log_level="warning")


@main.command()
@click.argument("dbt_project_path", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--manifest",
    type=click.Path(path_type=Path),
    default=None,
    help="Path to manifest.json",
)
@click.option("-o", "--output", type=click.Path(path_type=Path), help="Output JSON file")
def export(
    dbt_project_path: Path,
    manifest: Path | None,
    output: Path | None,
) -> None:
    """
    Export the lineage graph as JSON (for use with other tools).
    """
    import json
    from .parser import parse_manifest
    
    # Resolve manifest path
    if manifest is None:
        manifest = dbt_project_path / "target" / "manifest.json"
    elif not manifest.is_absolute():
        manifest = dbt_project_path / manifest
    
    manifest = manifest.resolve()
    
    if not manifest.exists():
        console.print(f"[red]Manifest not found:[/red] {manifest}")
        raise SystemExit(1)
    
    graph = parse_manifest(manifest)
    
    if output:
        output.write_text(json.dumps(graph, indent=2))
        console.print(f"[green]✓[/green] Exported to {output}")
    else:
        console.print_json(data=graph)


if __name__ == "__main__":
    main()
