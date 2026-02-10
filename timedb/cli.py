"""
TimeDB CLI - Command line interface for timedb operations.

Provides administrative and operational commands for managing timedb:
- create: Create database schema and series
- delete: Delete database schema
- api: Run the REST API server

Environment:
    - TIMEDB_DSN or DATABASE_URL: Database connection string
    - Typical usage: timedb --help

Examples:
    # Start the API server
    $ timedb api

    # Create database schema
    $ timedb create schema --dsn postgresql://...

    # Create a series
    $ timedb create series --dsn postgresql://... --name wind_power --unit MW

    # Delete schema
    $ timedb delete schema --dsn postgresql://...
"""
import os
from typing import Optional
from typing_extensions import Annotated

import typer
from rich.console import Console
from rich.panel import Panel
from dotenv import load_dotenv

load_dotenv()

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


def get_dsn(dsn: Optional[str]) -> str:
    """Get database connection string from argument or environment."""
    result = dsn or os.environ.get("TIMEDB_DSN") or os.environ.get("DATABASE_URL")
    if not result:
        console.print("[red]ERROR:[/red] No DSN provided. Use --dsn or set TIMEDB_DSN / DATABASE_URL")
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
    dsn: Annotated[Optional[str], typer.Option("--dsn", "-d", envvar=["TIMEDB_DSN", "DATABASE_URL"], help="Postgres connection string (or set TIMEDB_DSN/DATABASE_URL)")] = None,
    schema: Annotated[Optional[str], typer.Option("--schema", "-s", help="Schema name (creates/sets search_path)")] = None,
    retention: Annotated[Optional[str], typer.Option("--retention", "-r", help="Default retention period (overrides --retention-medium), e.g. '5 years'")] = None,
    retention_short: Annotated[str, typer.Option("--retention-short", help="Retention for overlapping_short table")] = "6 months",
    retention_medium: Annotated[str, typer.Option("--retention-medium", help="Retention for overlapping_medium table")] = "3 years",
    retention_long: Annotated[str, typer.Option("--retention-long", help="Retention for overlapping_long table")] = "5 years",
    yes: Annotated[bool, typer.Option("--yes", "-y", help="Skip confirmation prompt")] = False,
    dry_run: Annotated[bool, typer.Option("--dry-run", help="Show DDL without creating tables")] = False,
):
    """
    Create timedb tables and schema in PostgreSQL.

    This creates all necessary tables, views, and continuous aggregates:
      • batches_table - Tracks data batch metadata
      • series_table - Tracks time series definitions
      • flat - Simple table for non-overlapping data
      • overlapping_short/medium/long - For overlapping time ranges
      • Views and continuous aggregates for efficient querying

    Retention policies are applied to overlapping tables (auto-deletes old data).

    Examples:
        timedb create tables --dsn postgresql://user:pass@localhost/mydb
        timedb create tables --schema my_schema --retention '2 years'
        timedb create tables --dry-run
        TIMEDB_DSN=postgresql://... timedb create tables
    """
    conninfo = get_dsn(dsn)

    try:
        from . import db
    except Exception as e:
        console.print(f"[red]ERROR:[/red] Cannot import db module: {e}")
        raise typer.Exit(1)

    # Dry-run: print DDL and exit
    if dry_run:
        ddl = getattr(db.create, "DDL", None)
        if ddl is None:
            console.print("[red]ERROR:[/red] No DDL found in module.")
            raise typer.Exit(1)
        console.print(Panel(ddl, title="DDL", border_style="blue"))
        return

    # Confirmation prompt
    if not yes:
        console.print("[bold]About to create/update timedb schema[/bold]")
        console.print(f"  Connection: [cyan]{conninfo[:50]}...[/cyan]" if len(conninfo) > 50 else f"  Connection: [cyan]{conninfo}[/cyan]")
        if schema:
            console.print(f"  Schema: [cyan]{schema}[/cyan]")

        if not typer.confirm("\nContinue?"):
            console.print("[yellow]Aborted.[/yellow]")
            return

    # Set schema search_path if specified
    old_pgoptions = os.environ.get("PGOPTIONS")
    if schema:
        os.environ["PGOPTIONS"] = f"-c search_path={schema}"

    # Apply shorthand: --retention overrides --retention-medium
    if retention is not None:
        retention_medium = retention

    try:
        # Create base schema
        create_schema = getattr(db.create, "create_schema", None)
        if create_schema is None:
            raise RuntimeError("db.create.create_schema not found")
        create_schema(
            conninfo,
            retention_short=retention_short,
            retention_medium=retention_medium,
            retention_long=retention_long,
        )
        console.print("[green]✓[/green] Base timedb tables created")

        console.print("\n[bold green]Schema created successfully![/bold green]")

    except Exception as exc:
        console.print(f"[red]ERROR:[/red] {exc}")
        raise typer.Exit(1)
    finally:
        # Restore PGOPTIONS
        if schema:
            if old_pgoptions is None:
                os.environ.pop("PGOPTIONS", None)
            else:
                os.environ["PGOPTIONS"] = old_pgoptions


# =============================================================================
# Delete Commands
# =============================================================================

@delete_app.command("tables")
def delete_tables(
    dsn: Annotated[Optional[str], typer.Option("--dsn", "-d", envvar=["TIMEDB_DSN", "DATABASE_URL"], help="Postgres connection string (or set TIMEDB_DSN/DATABASE_URL)")] = None,
    schema: Annotated[Optional[str], typer.Option("--schema", "-s", help="Schema name (sets search_path)")] = None,
    yes: Annotated[bool, typer.Option("--yes", "-y", help="Skip confirmation prompt (use with caution!)")] = False,
):
    """
    Delete all timedb tables from the database.

    [bold red]⚠️  WARNING: This is DESTRUCTIVE[/bold red]

    This will permanently delete:
      • All data tables (batches_table, series_table, flat, overlapping_*)
      • All views and continuous aggregates
      • All schema data

    This cannot be undone!

    Examples:
        timedb delete tables --dsn postgresql://user:pass@localhost/mydb
        timedb delete tables --schema my_schema --yes
        TIMEDB_DSN=postgresql://... timedb delete tables
    """
    conninfo = get_dsn(dsn)

    # Confirmation prompt (always show warning)
    if not yes:
        console.print(Panel(
            "[bold red]WARNING: This will delete ALL timedb tables and data![/bold red]\n\n"
            "This includes:\n"
            "  • batches_table\n"
            "  • series_table\n"
            "  • flat\n"
            "  • overlapping_short / overlapping_medium / overlapping_long\n"
            "  • All views and continuous aggregates",
            title="Destructive Operation",
            border_style="red",
        ))
        console.print(f"\nConnection: [cyan]{conninfo[:50]}...[/cyan]" if len(conninfo) > 50 else f"\nConnection: [cyan]{conninfo}[/cyan]")
        if schema:
            console.print(f"Schema: [cyan]{schema}[/cyan]")

        if not typer.confirm("\n[bold]Are you sure? This cannot be undone.[/bold]", default=False):
            console.print("[yellow]Aborted.[/yellow]")
            return

    try:
        from . import db
    except Exception as e:
        console.print(f"[red]ERROR:[/red] Cannot import db module: {e}")
        raise typer.Exit(1)

    # Set schema search_path if specified
    old_pgoptions = os.environ.get("PGOPTIONS")
    if schema:
        os.environ["PGOPTIONS"] = f"-c search_path={schema}"

    try:
        delete_schema = getattr(db.delete, "delete_schema", None)
        if delete_schema is None:
            raise RuntimeError("db.delete.delete_schema not found")
        delete_schema(conninfo)
        console.print("[green]✓[/green] All timedb tables deleted")

    except Exception as exc:
        console.print(f"[red]ERROR:[/red] {exc}")
        raise typer.Exit(1)
    finally:
        if schema:
            if old_pgoptions is None:
                os.environ.pop("PGOPTIONS", None)
            else:
                os.environ["PGOPTIONS"] = old_pgoptions


if __name__ == "__main__":
    app()
