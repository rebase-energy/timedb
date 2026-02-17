import os
import sys
import time
import psycopg

# -----------------------------------------------------------------------------
# This DDL deletes all timedb tables and views (TimescaleDB version):
#   1) Legacy views/aggregates (IF EXISTS, safe to run)
#   2) Overlapping hypertables (short/medium/long)
#   3) Flat hypertable
#   4) series_table, batches_table
#
# Uses autocommit mode with per-statement retry to avoid deadlocks with
# TimescaleDB background workers (compression, retention).
# -----------------------------------------------------------------------------

_DROP_STATEMENTS = [
    # Continuous aggregates first (legacy)
    "DROP MATERIALIZED VIEW IF EXISTS latest_projections_short CASCADE",
    "DROP MATERIALIZED VIEW IF EXISTS latest_projections_medium CASCADE",
    "DROP MATERIALIZED VIEW IF EXISTS latest_projections_long CASCADE",
    "DROP MATERIALIZED VIEW IF EXISTS latest_values CASCADE",
    # Regular views
    "DROP VIEW IF EXISTS latest_projection_curve CASCADE",
    "DROP VIEW IF EXISTS all_overlapping_raw CASCADE",
    "DROP VIEW IF EXISTS all_projections_raw CASCADE",
    "DROP VIEW IF EXISTS current_values_view CASCADE",
    "DROP VIEW IF EXISTS current_metadata_table CASCADE",
    "DROP VIEW IF EXISTS current_values_table CASCADE",
    # Current overlapping hypertables
    "DROP TABLE IF EXISTS overlapping_short CASCADE",
    "DROP TABLE IF EXISTS overlapping_medium CASCADE",
    "DROP TABLE IF EXISTS overlapping_long CASCADE",
    # Legacy projection tables
    "DROP TABLE IF EXISTS projections_short CASCADE",
    "DROP TABLE IF EXISTS projections_medium CASCADE",
    "DROP TABLE IF EXISTS projections_long CASCADE",
    "DROP TABLE IF EXISTS projections CASCADE",
    # Current flat hypertable
    "DROP TABLE IF EXISTS flat CASCADE",
    # Legacy actuals hypertable
    "DROP TABLE IF EXISTS actuals CASCADE",
    # Legacy tables
    "DROP TABLE IF EXISTS metadata_table CASCADE",
    "DROP TABLE IF EXISTS values_table CASCADE",
    # Core tables
    "DROP TABLE IF EXISTS series_table CASCADE",
    "DROP TABLE IF EXISTS batches_table CASCADE",
]

_MAX_RETRIES = 3
_RETRY_DELAY = 0.5  # seconds


def delete_schema(conninfo: str) -> None:
    """
    Deletes all timedb tables and views (TimescaleDB version).

    Uses autocommit mode so each DROP runs independently. Retries individual
    statements on deadlock errors caused by TimescaleDB background workers.
    """
    with psycopg.connect(conninfo, autocommit=True) as conn:
        with conn.cursor() as cur:
            for stmt in _DROP_STATEMENTS:
                for attempt in range(_MAX_RETRIES):
                    try:
                        cur.execute(stmt)
                        break
                    except psycopg.errors.DeadlockDetected:
                        if attempt < _MAX_RETRIES - 1:
                            time.sleep(_RETRY_DELAY * (attempt + 1))
                        else:
                            raise


if __name__ == "__main__":
    conninfo = os.environ.get("TIMEDB_DSN") or os.environ.get("DATABASE_URL")
    if not conninfo:
        print("ERROR: no DSN provided. Set TIMEDB_DSN or DATABASE_URL")
        sys.exit(1)

    delete_schema(conninfo)
    print("All timedb tables (including metadata) deleted successfully.")
