# timedb/cli.py
"""TimeDB CLI - Command line interface for timedb operations."""
import os
import uuid
from typing import Optional
from typing_extensions import Annotated

import typer
from rich.console import Console
from rich.table import Table
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
users_app = typer.Typer(help="Manage users and API keys")

app.add_typer(create_app, name="create")
app.add_typer(delete_app, name="delete")
app.add_typer(users_app, name="users")

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
    host: Annotated[str, typer.Option("--host", help="Host to bind to")] = "127.0.0.1",
    port: Annotated[int, typer.Option("--port", help="Port to bind to")] = 8000,
    reload: Annotated[bool, typer.Option("--reload", help="Enable auto-reload for development")] = False,
):
    """
    Start the FastAPI server.

    Example: timedb api --host 127.0.0.1 --port 8000
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
    dsn: Annotated[Optional[str], typer.Option("--dsn", "-d", envvar=["TIMEDB_DSN", "DATABASE_URL"], help="Postgres DSN")] = None,
    schema: Annotated[Optional[str], typer.Option("--schema", "-s", help="Schema name (sets search_path)")] = None,
    with_metadata: Annotated[bool, typer.Option("--with-metadata/--no-metadata", help="Create metadata_table addon")] = False,
    with_users: Annotated[bool, typer.Option("--with-users/--no-users", help="Create users_table for authentication")] = False,
    yes: Annotated[bool, typer.Option("--yes", "-y", help="Skip confirmation prompt")] = False,
    dry_run: Annotated[bool, typer.Option("--dry-run", help="Print DDL and exit")] = False,
):
    """
    Create timedb tables.

    Example: timedb create tables --dsn postgresql://... --with-users
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
        if with_metadata:
            console.print("  [dim]+ metadata_table addon[/dim]")
        if with_users:
            console.print("  [dim]+ users_table for authentication[/dim]")

        if not typer.confirm("\nContinue?"):
            console.print("[yellow]Aborted.[/yellow]")
            return

    # Set schema search_path if specified
    old_pgoptions = os.environ.get("PGOPTIONS")
    if schema:
        os.environ["PGOPTIONS"] = f"-c search_path={schema}"

    try:
        # Create base schema
        create_schema = getattr(db.create, "create_schema", None)
        if create_schema is None:
            raise RuntimeError("db.create.create_schema not found")
        create_schema(conninfo)
        console.print("[green]✓[/green] Base timedb tables created")

        # Create metadata schema if requested
        if with_metadata:
            create_schema_metadata = getattr(db.create_with_metadata, "create_schema_metadata", None)
            if create_schema_metadata is None:
                raise RuntimeError("db.create_with_metadata.create_schema_metadata not found")
            create_schema_metadata(conninfo)
            console.print("[green]✓[/green] Metadata table created")

        # Create users schema if requested
        if with_users:
            create_schema_users = getattr(db.create_with_users, "create_schema_users", None)
            if create_schema_users is None:
                raise RuntimeError("db.create_with_users.create_schema_users not found")
            create_schema_users(conninfo)
            console.print("[green]✓[/green] Users table created")

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
    dsn: Annotated[Optional[str], typer.Option("--dsn", "-d", envvar=["TIMEDB_DSN", "DATABASE_URL"], help="Postgres DSN")] = None,
    schema: Annotated[Optional[str], typer.Option("--schema", "-s", help="Schema name (sets search_path)")] = None,
    yes: Annotated[bool, typer.Option("--yes", "-y", help="Skip confirmation prompt")] = False,
):
    """
    Delete all timedb tables.

    [bold red]WARNING:[/bold red] This will delete all data!

    Example: timedb delete tables --dsn postgresql://...
    """
    conninfo = get_dsn(dsn)

    # Confirmation prompt (always show warning)
    if not yes:
        console.print(Panel(
            "[bold red]WARNING: This will delete ALL timedb tables and data![/bold red]\n\n"
            "This includes:\n"
            "  • batches_table\n"
            "  • series_table\n"
            "  • actuals\n"
            "  • projections_short / projections_medium / projections_long\n"
            "  • users_table (if exists)\n"
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


# =============================================================================
# Users Commands
# =============================================================================

@users_app.command("create")
def create_user(
    dsn: Annotated[Optional[str], typer.Option("--dsn", "-d", envvar=["TIMEDB_DSN", "DATABASE_URL"], help="Postgres DSN")] = None,
    tenant_id: Annotated[str, typer.Option("--tenant-id", "-t", help="Tenant UUID")] = ...,
    email: Annotated[str, typer.Option("--email", "-e", help="User email address")] = ...,
):
    """
    Create a new user with an API key.

    Example: timedb users create --tenant-id <uuid> --email user@example.com
    """
    import psycopg

    conninfo = get_dsn(dsn)

    try:
        tenant_uuid = uuid.UUID(tenant_id)
    except ValueError:
        console.print(f"[red]ERROR:[/red] Invalid tenant-id UUID: {tenant_id}")
        raise typer.Exit(1)

    try:
        from . import db
    except Exception as e:
        console.print(f"[red]ERROR:[/red] Cannot import db module: {e}")
        raise typer.Exit(1)

    try:
        with psycopg.connect(conninfo) as conn:
            user = db.users.create_user(conn, tenant_id=tenant_uuid, email=email)

            console.print(Panel(
                f"[bold green]User created successfully![/bold green]\n\n"
                f"  User ID:   [cyan]{user['user_id']}[/cyan]\n"
                f"  Tenant ID: [cyan]{user['tenant_id']}[/cyan]\n"
                f"  Email:     [cyan]{user['email']}[/cyan]\n\n"
                f"  [bold yellow]API Key:[/bold yellow] [bold]{user['api_key']}[/bold]",
                title="New User",
                border_style="green",
            ))
            console.print("\n[yellow]⚠ Save the API key - it will not be shown again.[/yellow]")

    except psycopg.errors.UniqueViolation as e:
        if "api_key" in str(e):
            console.print("[red]ERROR:[/red] API key collision (extremely rare). Please try again.")
        else:
            console.print(f"[red]ERROR:[/red] User with email '{email}' already exists for this tenant.")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]ERROR:[/red] {e}")
        raise typer.Exit(1)


@users_app.command("list")
def list_users(
    dsn: Annotated[Optional[str], typer.Option("--dsn", "-d", envvar=["TIMEDB_DSN", "DATABASE_URL"], help="Postgres DSN")] = None,
    tenant_id: Annotated[Optional[str], typer.Option("--tenant-id", "-t", help="Filter by tenant UUID")] = None,
    include_inactive: Annotated[bool, typer.Option("--include-inactive", help="Include inactive users")] = False,
):
    """
    List all users.

    Example: timedb users list --tenant-id <uuid>
    """
    import psycopg

    conninfo = get_dsn(dsn)

    tenant_uuid = None
    if tenant_id:
        try:
            tenant_uuid = uuid.UUID(tenant_id)
        except ValueError:
            console.print(f"[red]ERROR:[/red] Invalid tenant-id UUID: {tenant_id}")
            raise typer.Exit(1)

    try:
        from . import db
    except Exception as e:
        console.print(f"[red]ERROR:[/red] Cannot import db module: {e}")
        raise typer.Exit(1)

    try:
        with psycopg.connect(conninfo) as conn:
            users_list = db.users.list_users(conn, tenant_id=tenant_uuid, include_inactive=include_inactive)

            if not users_list:
                console.print("[yellow]No users found.[/yellow]")
                return

            # Create Rich table
            table = Table(title=f"Users ({len(users_list)})")
            table.add_column("Email", style="cyan")
            table.add_column("User ID", style="dim")
            table.add_column("Tenant ID", style="dim")
            table.add_column("Status")
            table.add_column("Created", style="dim")

            for user in users_list:
                status = "[green]active[/green]" if user['is_active'] else "[red]inactive[/red]"
                created = user['created_at'].strftime("%Y-%m-%d %H:%M") if hasattr(user['created_at'], 'strftime') else str(user['created_at'])
                table.add_row(
                    user['email'],
                    str(user['user_id'])[:8] + "...",
                    str(user['tenant_id'])[:8] + "...",
                    status,
                    created,
                )

            console.print(table)

    except Exception as e:
        console.print(f"[red]ERROR:[/red] {e}")
        raise typer.Exit(1)


@users_app.command("regenerate-key")
def regenerate_key(
    dsn: Annotated[Optional[str], typer.Option("--dsn", "-d", envvar=["TIMEDB_DSN", "DATABASE_URL"], help="Postgres DSN")] = None,
    email: Annotated[str, typer.Option("--email", "-e", help="User email address")] = ...,
    yes: Annotated[bool, typer.Option("--yes", "-y", help="Skip confirmation prompt")] = False,
):
    """
    Regenerate API key for a user.

    Example: timedb users regenerate-key --email user@example.com
    """
    import psycopg

    conninfo = get_dsn(dsn)

    if not yes:
        console.print(f"[bold]Regenerate API key for:[/bold] [cyan]{email}[/cyan]")
        console.print("[yellow]The old API key will be invalidated immediately.[/yellow]")
        if not typer.confirm("\nContinue?"):
            console.print("[yellow]Aborted.[/yellow]")
            return

    try:
        from . import db
    except Exception as e:
        console.print(f"[red]ERROR:[/red] Cannot import db module: {e}")
        raise typer.Exit(1)

    try:
        with psycopg.connect(conninfo) as conn:
            new_key = db.users.regenerate_api_key(conn, email=email)
            if new_key is None:
                console.print(f"[red]ERROR:[/red] User with email '{email}' not found.")
                raise typer.Exit(1)

            console.print(Panel(
                f"[bold green]API key regenerated![/bold green]\n\n"
                f"  Email:   [cyan]{email}[/cyan]\n\n"
                f"  [bold yellow]New API Key:[/bold yellow] [bold]{new_key}[/bold]",
                title="Key Regenerated",
                border_style="green",
            ))
            console.print("\n[yellow]⚠ Save the API key - it will not be shown again.[/yellow]")

    except Exception as e:
        console.print(f"[red]ERROR:[/red] {e}")
        raise typer.Exit(1)


@users_app.command("deactivate")
def deactivate_user(
    dsn: Annotated[Optional[str], typer.Option("--dsn", "-d", envvar=["TIMEDB_DSN", "DATABASE_URL"], help="Postgres DSN")] = None,
    email: Annotated[str, typer.Option("--email", "-e", help="User email address")] = ...,
    yes: Annotated[bool, typer.Option("--yes", "-y", help="Skip confirmation prompt")] = False,
):
    """
    Deactivate a user (revoke API access).

    Example: timedb users deactivate --email user@example.com
    """
    import psycopg

    conninfo = get_dsn(dsn)

    if not yes:
        console.print(f"[bold]Deactivate user:[/bold] [cyan]{email}[/cyan]")
        console.print("[yellow]The user will no longer be able to authenticate.[/yellow]")
        if not typer.confirm("\nContinue?"):
            console.print("[yellow]Aborted.[/yellow]")
            return

    try:
        from . import db
    except Exception as e:
        console.print(f"[red]ERROR:[/red] Cannot import db module: {e}")
        raise typer.Exit(1)

    try:
        with psycopg.connect(conninfo) as conn:
            success = db.users.deactivate_user(conn, email=email)
            if not success:
                console.print(f"[red]ERROR:[/red] User with email '{email}' not found.")
                raise typer.Exit(1)
            console.print(f"[green]✓[/green] User '{email}' deactivated")

    except Exception as e:
        console.print(f"[red]ERROR:[/red] {e}")
        raise typer.Exit(1)


@users_app.command("activate")
def activate_user(
    dsn: Annotated[Optional[str], typer.Option("--dsn", "-d", envvar=["TIMEDB_DSN", "DATABASE_URL"], help="Postgres DSN")] = None,
    email: Annotated[str, typer.Option("--email", "-e", help="User email address")] = ...,
):
    """
    Activate a user (restore API access).

    Example: timedb users activate --email user@example.com
    """
    import psycopg

    conninfo = get_dsn(dsn)

    try:
        from . import db
    except Exception as e:
        console.print(f"[red]ERROR:[/red] Cannot import db module: {e}")
        raise typer.Exit(1)

    try:
        with psycopg.connect(conninfo) as conn:
            success = db.users.activate_user(conn, email=email)
            if not success:
                console.print(f"[red]ERROR:[/red] User with email '{email}' not found.")
                raise typer.Exit(1)
            console.print(f"[green]✓[/green] User '{email}' activated")

    except Exception as e:
        console.print(f"[red]ERROR:[/red] {e}")
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
