import os
import sys
import time
import psycopg
import clickhouse_connect

_PG_DROP_STATEMENTS = [
    # Legacy views (safe no-ops if they don't exist)
    "DROP MATERIALIZED VIEW IF EXISTS latest_projections_short CASCADE",
    "DROP MATERIALIZED VIEW IF EXISTS latest_projections_medium CASCADE",
    "DROP MATERIALIZED VIEW IF EXISTS latest_projections_long CASCADE",
    "DROP MATERIALIZED VIEW IF EXISTS latest_values CASCADE",
    "DROP VIEW IF EXISTS latest_projection_curve CASCADE",
    "DROP VIEW IF EXISTS all_overlapping_raw CASCADE",
    "DROP VIEW IF EXISTS all_projections_raw CASCADE",
    "DROP VIEW IF EXISTS current_values_view CASCADE",
    "DROP VIEW IF EXISTS current_metadata_table CASCADE",
    "DROP VIEW IF EXISTS current_values_table CASCADE",
    # Current series table
    "DROP TABLE IF EXISTS series_table CASCADE",
    # Legacy PG tables (clean up if they exist from old schema)
    "DROP TABLE IF EXISTS runs_table CASCADE",
    "DROP TABLE IF EXISTS overlapping_short CASCADE",
    "DROP TABLE IF EXISTS overlapping_medium CASCADE",
    "DROP TABLE IF EXISTS overlapping_long CASCADE",
    "DROP TABLE IF EXISTS flat CASCADE",
    "DROP TABLE IF EXISTS metadata_table CASCADE",
    "DROP TABLE IF EXISTS values_table CASCADE",
    "DROP TABLE IF EXISTS actuals CASCADE",
]

_CH_DROP_TABLES = [
    "overlapping_long",
    "overlapping_medium",
    "overlapping_short",
    "flat",
    "runs_table",
]

_MAX_RETRIES = 3
_RETRY_DELAY = 0.5


def delete_schema(pg_conninfo: str, ch_url: str) -> None:
    """
    Drop all TimeDB tables from both PostgreSQL and ClickHouse.

    Args:
        pg_conninfo: PostgreSQL connection string (drops series_table).
        ch_url: ClickHouse DSN (drops runs_table, flat, overlapping_*).
    """
    # PostgreSQL
    with psycopg.connect(pg_conninfo, autocommit=True) as conn:
        with conn.cursor() as cur:
            for stmt in _PG_DROP_STATEMENTS:
                for attempt in range(_MAX_RETRIES):
                    try:
                        cur.execute(stmt)
                        break
                    except psycopg.errors.DeadlockDetected:
                        if attempt < _MAX_RETRIES - 1:
                            time.sleep(_RETRY_DELAY * (attempt + 1))
                        else:
                            raise

    # ClickHouse
    ch_client = clickhouse_connect.get_client(dsn=ch_url)
    for table in _CH_DROP_TABLES:
        ch_client.command(f"DROP TABLE IF EXISTS {table}")


if __name__ == "__main__":
    pg_conninfo = os.environ.get("TIMEDB_PG_DSN") or os.environ.get("DATABASE_URL")
    ch_url = os.environ.get("TIMEDB_CH_URL")
    if not pg_conninfo or not ch_url:
        print("ERROR: set TIMEDB_PG_DSN and TIMEDB_CH_URL")
        sys.exit(1)
    delete_schema(pg_conninfo, ch_url)
    print("All TimeDB tables deleted successfully.")
