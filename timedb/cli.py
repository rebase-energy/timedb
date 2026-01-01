# timedb/cli.py
import os
import sys
import click
#from importlib import import_module
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

load_dotenv() 

def dsn_option(fn):
    return click.option(
        "--dsn", "-d",
        envvar=["TIMEDB_DSN", "DATABASE_URL"],
        help="Postgres DSN",
    )(fn)

@click.group()
def cli():
    """timedb CLI"""

@cli.group()
def create():
    """Create resources (e.g. tables)"""
    pass

@cli.group()
def delete():
    """Delete resources (e.g. tables)"""
    pass

@cli.command("api")
@click.option("--host", default="127.0.0.1", help="Host to bind to")
@click.option("--port", default=8000, type=int, help="Port to bind to")
@click.option("--reload", is_flag=True, help="Enable auto-reload for development")
def start_api(host, port, reload):
    """
    Start the FastAPI server.
    Example: timedb api --host 127.0.0.1 --port 8000
    """
    try:
        import uvicorn
        click.echo(f"Starting TimeDB API server on http://{host}:{port}")
        click.echo(f"API docs available at http://{host}:{port}/docs")
        click.echo("Press Ctrl+C to stop the server")
        uvicorn.run(
            "timedb.api:app",
            host=host,
            port=port,
            reload=reload,
        )
    except ImportError:
        click.echo("ERROR: FastAPI dependencies not installed. Run: pip install fastapi uvicorn[standard]", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"ERROR: {e}", err=True)
        sys.exit(1)

@create.command("tables")
@dsn_option
@click.option("--schema", "-s", default=None, help="Schema name to use for the tables (sets search_path for the DDL).")
@click.option("--with-metadata/--no-metadata", default=False, help="Also create the optional metadata_table addon.")
@click.option("--yes", "-y", is_flag=True, help="Do not prompt for confirmation")
@click.option("--dry-run", is_flag=True, help="Print the DDL (from db.create.DDL) and exit")
def create_tables(dsn, schema, with_metadata, yes, dry_run):
    """
    Create timedb tables using db.create.create_schema().
    Example: timedb create tables --dsn postgresql://... --schema timedb --with-metadata
    """
    conninfo = dsn
    if not conninfo:
        click.echo("ERROR: no DSN provided. Use --dsn or set TIMEDB_DSN / DATABASE_URL", err=True)
        sys.exit(2)

    # Import the implementation module(s) from repository
    try:
        from . import db
    except Exception as e:
        click.echo(f"ERROR: cannot import db module: {e}", err=True)
        sys.exit(1)

    # Dry-run -> print the DDL string defined in module (db.create.DDL)
    if dry_run:
        ddl = getattr(db.create, "DDL", None)
        if ddl is None:
            click.echo("No DDL found in pg_create_table module.", err=True)
            sys.exit(1)
        click.echo(ddl)
        return

    if not yes:
        click.echo("About to create/update timedb schema/tables using pg_create_table.create_schema().")
        click.echo(f"Connection: {conninfo}")
        if schema:
            click.echo(f"Schema/search_path: {schema}")
        if with_metadata:
            click.echo("Will also create the optional metadata_table addon.")
        if not click.confirm("Continue?"):
            click.echo("Aborted.")
            return

    # If schema specified, set PGOPTIONS to ensure the DDL runs with that search_path.
    old_pgoptions = os.environ.get("PGOPTIONS")
    if schema:
        # Set search_path for server session using libpq/pgoptions mechanism
        # This ensures create_schema's psycopg.connect() inherits the search_path.
        os.environ["PGOPTIONS"] = f"-c search_path={schema}"
    try:
        # call the create_schema function from db.create
        create_schema = getattr(db.create, "create_schema", None)
        if create_schema is None:
            raise RuntimeError("db.create.create_schema not found")
        create_schema(conninfo)
        click.echo("Base timedb tables created/updated successfully.")

        if with_metadata:
            try:
                create_schema_metadata = getattr(db.create_with_metadata, "create_schema_metadata", None)
                if create_schema_metadata is None:
                    raise RuntimeError("db.create_with_metadata.create_schema_metadata not found")
                create_schema_metadata(conninfo)
                click.echo("Optional metadata schema created/updated successfully.")
            except Exception as e:
                click.echo(f"ERROR creating metadata schema: {e}", err=True)
                sys.exit(1)

    except Exception as exc:
        click.echo(f"ERROR creating tables: {exc}", err=True)
        sys.exit(1)
    finally:
        # restore PGOPTIONS
        if schema:
            if old_pgoptions is None:
                os.environ.pop("PGOPTIONS", None)
            else:
                os.environ["PGOPTIONS"] = old_pgoptions

@delete.command("tables")
@dsn_option
@click.option("--schema", "-s", default=None, help="Schema name to use for the tables (sets search_path for the DDL).")
@click.option("--yes", "-y", is_flag=True, help="Do not prompt for confirmation")
def delete_tables(dsn, schema, yes):
    """
    Delete all timedb tables (including metadata tables).
    Example: timedb delete tables --dsn postgresql://... --schema timedb
    """
    conninfo = dsn
    if not conninfo:
        click.echo("ERROR: no DSN provided. Use --dsn or set TIMEDB_DSN / DATABASE_URL", err=True)
        sys.exit(2)

    if not yes:
        click.echo("WARNING: This will delete ALL timedb tables and their data!")
        click.echo("This includes: runs_table, values_table, metadata_table, and all views.")
        click.echo(f"Connection: {conninfo}")
        if schema:
            click.echo(f"Schema/search_path: {schema}")
        if not click.confirm("Are you sure you want to continue? This action cannot be undone."):
            click.echo("Aborted.")
            return

    # Import the implementation module from repository
    try:
        from . import db
    except Exception as e:
        click.echo(f"ERROR: cannot import db module: {e}", err=True)
        sys.exit(1)

    # If schema specified, set PGOPTIONS to ensure the DDL runs with that search_path.
    old_pgoptions = os.environ.get("PGOPTIONS")
    if schema:
        # Set search_path for server session using libpq/pgoptions mechanism
        # This ensures delete_schema's psycopg.connect() inherits the search_path.
        os.environ["PGOPTIONS"] = f"-c search_path={schema}"
    try:
        # call the delete_schema function from db.delete
        delete_schema = getattr(db.delete, "delete_schema", None)
        if delete_schema is None:
            raise RuntimeError("pg_delete_table.delete_schema not found")
        delete_schema(conninfo)
        click.echo("All timedb tables (including metadata) deleted successfully.")
            
    except Exception as exc:
        click.echo(f"ERROR deleting tables: {exc}", err=True)
        sys.exit(1)
    finally:
        # restore PGOPTIONS
        if schema:
            if old_pgoptions is None:
                os.environ.pop("PGOPTIONS", None)
            else:
                os.environ["PGOPTIONS"] = old_pgoptions

if __name__ == "__main__":
    cli()