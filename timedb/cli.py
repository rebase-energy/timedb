"""
TimeDB CLI - Command line interface for timedb operations.

Provides administrative and operational commands for managing timedb:
- create: Create database schema and series
- delete: Delete database schema
- api: Run the REST API server

Environment:
    - TIMEDB_PG_DSN or DATABASE_URL: PostgreSQL connection string (series_table)
    - TIMEDB_CH_URL: ClickHouse DSN (runs + values tables)
    - Typical usage: timedb --help

Examples:
    # Start the API server
    $ timedb api

    # Create database schema
    $ timedb create tables --pg-dsn postgresql://... --ch-url clickhouse://...

    # Delete all tables
    $ timedb delete tables --pg-dsn postgresql://... --ch-url clickhouse://...
"""
import os
from typing import Optional
from typing import Annotated

import typer
from rich.console import Console
from rich.panel import Panel

# Initialize Typer apps
app = typer.Typer(
    help="timedb CLI - Manage time series database",
    rich_markup_mode="rich",
)
create_app = typer.Typer(help="Create resources (tables, schema)")
delete_app = typer.Typer(help="Delete resources (tables, schema)")

app.add_typer(create_app, name="create")
app.add_typer(delete_app, name="delete")

# Rich console for formatted output
console = Console()


def get_pg_conninfo(dsn: Optional[str]) -> str:
    """Get PostgreSQL connection string from argument or environment."""
    result = dsn or os.environ.get("TIMEDB_PG_DSN") or os.environ.get("DATABASE_URL")
    if not result:
        console.print("[red]ERROR:[/red] No PG DSN provided. Use --pg-dsn or set TIMEDB_PG_DSN / DATABASE_URL")
        raise typer.Exit(2)
    return result


def get_ch_url(ch_url: Optional[str]) -> str:
    """Get ClickHouse DSN from argument or environment."""
    result = ch_url or os.environ.get("TIMEDB_CH_URL")
    if not result:
        console.print("[red]ERROR:[/red] No ClickHouse URL provided. Use --ch-url or set TIMEDB_CH_URL")
        raise typer.Exit(2)
    return result


# =============================================================================
# API Command
# =============================================================================

@app.command()
def api(
    host: Annotated[str, typer.Option("--host", help="Host to bind API server to")] = "127.0.0.1",
    port: Annotated[int, typer.Option("--port", help="Port to bind API server to")] = 8000,
    reload: Annotated[bool, typer.Option("--reload", help="Enable auto-reload for development (watch file changes)")] = False,
):
    """
    Start the FastAPI server.

    The API server provides REST endpoints for reading and writing time series data.

    Examples:
        timedb api
        timedb api --host 0.0.0.0 --port 9000
        timedb api --reload
    """
    try:
        import uvicorn
        console.print(Panel.fit(
            f"[bold green]TimeDB API Server[/bold green]\n\n"
            f"Server: [cyan]http://{host}:{port}[/cyan]\n"
            f"Docs:   [cyan]http://{host}:{port}/docs[/cyan]\n\n"
            f"[dim]Press Ctrl+C to stop[/dim]",
            title="Starting",
            border_style="green",
        ))
        uvicorn.run(
            "timedb.api:app",
            host=host,
            port=port,
            reload=reload,
        )
    except ImportError:
        console.print("[red]ERROR:[/red] FastAPI dependencies not installed. Run: pip install fastapi uvicorn[standard]")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]ERROR:[/red] {e}")
        raise typer.Exit(1)


# =============================================================================
# Create Commands
# =============================================================================

@create_app.command("tables")
def create_tables(
    pg_dsn: Annotated[Optional[str], typer.Option("--pg-dsn", envvar=["TIMEDB_PG_DSN", "DATABASE_URL"], help="PostgreSQL connection string")] = None,
    ch_url: Annotated[Optional[str], typer.Option("--ch-url", envvar="TIMEDB_CH_URL", help="ClickHouse DSN")] = None,
    yes: Annotated[bool, typer.Option("--yes", "-y", help="Skip confirmation prompt")] = False,
):
    """
    Create timedb tables in PostgreSQL (series_table) and ClickHouse (values tables).

    Examples:
        timedb create tables --pg-dsn postgresql://user:pass@localhost/mydb --ch-url clickhouse://localhost/timedb
        TIMEDB_PG_DSN=postgresql://... TIMEDB_CH_URL=clickhouse://... timedb create tables
    """
    conninfo = get_pg_conninfo(pg_dsn)
    ch = get_ch_url(ch_url)

    try:
        from . import db
    except Exception as e:
        console.print(f"[red]ERROR:[/red] Cannot import db module: {e}")
        raise typer.Exit(1)

    if not yes:
        console.print("[bold]About to create timedb schema[/bold]")
        console.print(f"  PostgreSQL: [cyan]{conninfo[:60]}{'...' if len(conninfo) > 60 else ''}[/cyan]")
        console.print(f"  ClickHouse: [cyan]{ch[:60]}{'...' if len(ch) > 60 else ''}[/cyan]")

        if not typer.confirm("\nContinue?"):
            console.print("[yellow]Aborted.[/yellow]")
            return

    try:
        db.create.create_schema(conninfo, ch)
        console.print("\n[bold green]Schema created successfully![/bold green]")
    except Exception as exc:
        console.print(f"[red]ERROR:[/red] {exc}")
        raise typer.Exit(1)


# =============================================================================
# Delete Commands
# =============================================================================

@delete_app.command("tables")
def delete_tables(
    pg_dsn: Annotated[Optional[str], typer.Option("--pg-dsn", envvar=["TIMEDB_PG_DSN", "DATABASE_URL"], help="PostgreSQL connection string")] = None,
    ch_url: Annotated[Optional[str], typer.Option("--ch-url", envvar="TIMEDB_CH_URL", help="ClickHouse DSN")] = None,
    yes: Annotated[bool, typer.Option("--yes", "-y", help="Skip confirmation prompt (use with caution!)")] = False,
):
    """
    Delete all timedb tables from PostgreSQL and ClickHouse.

    [bold red]⚠️  WARNING: This is DESTRUCTIVE[/bold red]

    This will permanently delete all tables and data. This cannot be undone!

    Examples:
        timedb delete tables --pg-dsn postgresql://user:pass@localhost/mydb --ch-url clickhouse://localhost/timedb
        TIMEDB_PG_DSN=postgresql://... TIMEDB_CH_URL=clickhouse://... timedb delete tables --yes
    """
    conninfo = get_pg_conninfo(pg_dsn)
    ch = get_ch_url(ch_url)

    if not yes:
        console.print(Panel(
            "[bold red]WARNING: This will delete ALL timedb tables and data![/bold red]\n\n"
            "PostgreSQL:  series_table\n"
            "ClickHouse:  runs_table, flat, overlapping_short/medium/long",
            title="Destructive Operation",
            border_style="red",
        ))
        console.print(f"\nPostgreSQL: [cyan]{conninfo[:60]}{'...' if len(conninfo) > 60 else ''}[/cyan]")
        console.print(f"ClickHouse: [cyan]{ch[:60]}{'...' if len(ch) > 60 else ''}[/cyan]")

        if not typer.confirm("\n[bold]Are you sure? This cannot be undone.[/bold]", default=False):
            console.print("[yellow]Aborted.[/yellow]")
            return

    try:
        from . import db
    except Exception as e:
        console.print(f"[red]ERROR:[/red] Cannot import db module: {e}")
        raise typer.Exit(1)

    try:
        db.delete.delete_schema(conninfo, ch)
        console.print("[green]✓[/green] All timedb tables deleted")
    except Exception as exc:
        console.print(f"[red]ERROR:[/red] {exc}")
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
