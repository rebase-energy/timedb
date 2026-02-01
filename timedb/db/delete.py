import os
import sys
import psycopg
from dotenv import load_dotenv

load_dotenv()

# -----------------------------------------------------------------------------
# This DDL deletes all timedb tables and views (TimescaleDB version):
#   1) latest_values continuous aggregate (materialized view)
#   2) current_values_view regular view
#   3) values_table (hypertable)
#   4) series_table
#   5) batches_table
#
# Uses CASCADE to handle foreign key dependencies automatically.
# -----------------------------------------------------------------------------

def delete_schema(conninfo: str) -> None:
    """
    Deletes all timedb tables and views (TimescaleDB version).

    - Uses an explicit transaction (BEGIN/COMMIT)
    - autocommit=False so the transaction is respected
    - Drops continuous aggregates first, then views, then tables
    - Uses CASCADE to handle dependencies
    """
    with psycopg.connect(conninfo, autocommit=False) as conn:
        with conn.cursor() as cur:
            # Drop continuous aggregates (materialized views) first
            # These must be dropped before the underlying hypertables
            cur.execute("DROP MATERIALIZED VIEW IF EXISTS latest_values CASCADE;")

            # Drop regular views
            cur.execute("DROP VIEW IF EXISTS current_values_view CASCADE;")
            cur.execute("DROP VIEW IF EXISTS current_metadata_table CASCADE;")
            cur.execute("DROP VIEW IF EXISTS current_values_table CASCADE;")

            # Drop tables in correct order (respecting foreign key constraints)
            # Note: values_table has a foreign key to series_table, so drop values_table first
            cur.execute("DROP TABLE IF EXISTS metadata_table CASCADE;")
            cur.execute("DROP TABLE IF EXISTS values_table CASCADE;")
            cur.execute("DROP TABLE IF EXISTS series_table CASCADE;")
            cur.execute("DROP TABLE IF EXISTS batches_table CASCADE;")

            conn.commit()


if __name__ == "__main__":
    conninfo = os.environ.get("TIMEDB_DSN") or os.environ.get("DATABASE_URL")
    if not conninfo:
        print("ERROR: no DSN provided. Set TIMEDB_DSN or DATABASE_URL")
        sys.exit(1)

    delete_schema(conninfo)
    print("All timedb tables (including metadata) deleted successfully.")
