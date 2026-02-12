import os
from importlib import resources
from dotenv import load_dotenv
import psycopg

load_dotenv()

# -----------------------------------------------------------------------------
# This module creates the TimescaleDB schema for TimeDB:
#   1) batches_table             → one row per batch
#   2) series_table              → series metadata (name, unit, labels, overlapping, retention)
#   3) flat                      → hypertable for immutable fact data
#   4) overlapping_short/medium/long → independent hypertables per retention tier
#   5) all_overlapping_raw       → UNION ALL view across overlapping tables
#
# The script is idempotent (safe to run multiple times).
# Split into two parts because TimescaleDB features require autocommit.
# -----------------------------------------------------------------------------

# Read packaged SQL files
DDL_TABLES = resources.files("timedb").joinpath("sql", "pg_create_table_timescaledb.sql").read_text(encoding="utf-8")
DDL_TIMESCALE = resources.files("timedb").joinpath("sql", "pg_create_timescaledb_features.sql").read_text(encoding="utf-8")


def create_schema(
    conninfo: str,
    retention_short: str = "6 months",
    retention_medium: str = "3 years",
    retention_long: str = "5 years",
) -> None:
    """
    Creates (or updates) the database schema (TimescaleDB version).

    - Part 1: Tables and indexes (runs in a transaction)
    - Part 2: Hypertables, compression, retention policies (requires autocommit)
    - Safe to run multiple times
    - Should typically be run at service startup or via a migration step

    Args:
        conninfo: PostgreSQL connection string
        retention_short: Retention interval for overlapping_short (default: "6 months")
        retention_medium: Retention interval for overlapping_medium (default: "3 years")
        retention_long: Retention interval for overlapping_long (default: "5 years")
    """

    print("Creating database schema...")

    # Part 1: Create tables and indexes (can run in a transaction)
    with psycopg.connect(conninfo) as conn:
        with conn.cursor() as cur:
            cur.execute(DDL_TABLES)
        conn.commit()

    # Part 2: TimescaleDB features (must run with autocommit)
    # Each statement is executed separately to handle IF NOT EXISTS gracefully
    ddl_timescale = DDL_TIMESCALE.format(
        retention_short=retention_short,
        retention_medium=retention_medium,
        retention_long=retention_long,
    )
    with psycopg.connect(conninfo, autocommit=True) as conn:
        with conn.cursor() as cur:
            # Split SQL into individual statements and execute each
            # Don't filter by '--' since statements may have comment blocks before them
            for statement in ddl_timescale.split(';'):
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
                except psycopg.errors.FeatureNotSupported:
                    # create_hypertable on partition children may fail on
                    # some TimescaleDB versions - log and continue
                    first_sql = non_comment_lines[0][:60] if non_comment_lines else statement[:60]
                    print(f"  Skipped (not supported): {first_sql}...")
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
