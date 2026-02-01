import os
from importlib import resources
from dotenv import load_dotenv
import psycopg

load_dotenv()

# -----------------------------------------------------------------------------
# This module creates the TimescaleDB schema for TimeDB:
#   1) batches_table             → one row per batch
#   2) series_table              → series metadata (name, unit, labels)
#   3) values_table              → hypertable for versioned values
#   4) indexes                   → for performance and data integrity
#   5) latest_values             → continuous aggregate for latest values
#   6) current_values_view       → convenience view with series metadata
#
# The script is idempotent (safe to run multiple times).
# Split into two parts because TimescaleDB features require autocommit.
# -----------------------------------------------------------------------------

# Read packaged SQL files
DDL_TABLES = resources.files("timedb").joinpath("sql", "pg_create_table_timescaledb.sql").read_text(encoding="utf-8")
DDL_TIMESCALE = resources.files("timedb").joinpath("sql", "pg_create_timescaledb_features.sql").read_text(encoding="utf-8")


def _is_hypertable(cur, table_name: str) -> bool:
    """Check if a table is already a TimescaleDB hypertable."""
    cur.execute("""
        SELECT EXISTS (
            SELECT 1 FROM timescaledb_information.hypertables
            WHERE hypertable_name = %s
        )
    """, (table_name,))
    result = cur.fetchone()
    return result[0] if result else False


def _table_exists(cur, table_name: str) -> bool:
    """Check if a table exists."""
    cur.execute("""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_name = %s
        )
    """, (table_name,))
    result = cur.fetchone()
    return result[0] if result else False


def create_schema(conninfo: str) -> None:
    """
    Creates (or updates) the database schema (TimescaleDB version).

    - Part 1: Tables and indexes (runs in a transaction)
    - Part 2: Hypertable, compression, continuous aggregates (requires autocommit)
    - Safe to run multiple times
    - Should typically be run at service startup or via a migration step
    """

    print("Creating database schema...")

    # Part 1: Create tables and indexes (can run in a transaction)
    with psycopg.connect(conninfo) as conn:
        with conn.cursor() as cur:
            cur.execute(DDL_TABLES)
        conn.commit()

    # Part 2: TimescaleDB features (must run with autocommit)
    # Each statement is executed separately to handle IF NOT EXISTS gracefully
    with psycopg.connect(conninfo, autocommit=True) as conn:
        with conn.cursor() as cur:
            # Split SQL into individual statements and execute each
            # Don't filter by '--' since statements may have comment blocks before them
            for statement in DDL_TIMESCALE.split(';'):
                statement = statement.strip()
                if not statement:
                    continue
                # Skip if the entire statement is just comments
                non_comment_lines = [l for l in statement.split('\n') if l.strip() and not l.strip().startswith('--')]
                if not non_comment_lines:
                    continue
                try:
                    cur.execute(statement)
                except psycopg.errors.DuplicateObject:
                    # Policy or view already exists - ignore
                    pass
                except psycopg.errors.DuplicateTable:
                    # Table/view already exists - ignore
                    pass
                except Exception as e:
                    err_msg = str(e).lower()
                    # Log but continue for IF NOT EXISTS statements
                    if "already exists" in err_msg:
                        pass
                    else:
                        # Get first non-comment line for error message
                        first_sql = non_comment_lines[0][:60] if non_comment_lines else statement[:60]
                        print(f"  Error: {first_sql}...")
                        raise

    print("✓ Schema created successfully")


if __name__ == "__main__":
    conninfo = os.environ["DATABASE_URL"]
    create_schema(conninfo)
