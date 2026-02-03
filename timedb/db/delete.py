import os
import sys
import psycopg
from dotenv import load_dotenv

load_dotenv()

# -----------------------------------------------------------------------------
# This DDL deletes all timedb tables and views (TimescaleDB version):
#   1) Continuous aggregates (latest_projections_*)
#   2) Regular views (latest_projection_curve, all_projections_raw)
#   3) Projection hypertables (projections_short/medium/long)
#   4) Actuals hypertable
#   5) series_table
#   6) batches_table
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
            cur.execute("DROP MATERIALIZED VIEW IF EXISTS latest_projections_short CASCADE;")
            cur.execute("DROP MATERIALIZED VIEW IF EXISTS latest_projections_medium CASCADE;")
            cur.execute("DROP MATERIALIZED VIEW IF EXISTS latest_projections_long CASCADE;")
            # Legacy continuous aggregate
            cur.execute("DROP MATERIALIZED VIEW IF EXISTS latest_values CASCADE;")

            # Drop regular views
            cur.execute("DROP VIEW IF EXISTS latest_projection_curve CASCADE;")
            cur.execute("DROP VIEW IF EXISTS all_projections_raw CASCADE;")
            # Legacy views
            cur.execute("DROP VIEW IF EXISTS current_values_view CASCADE;")
            cur.execute("DROP VIEW IF EXISTS current_metadata_table CASCADE;")
            cur.execute("DROP VIEW IF EXISTS current_values_table CASCADE;")

            # Drop projection hypertables
            cur.execute("DROP TABLE IF EXISTS projections_short CASCADE;")
            cur.execute("DROP TABLE IF EXISTS projections_medium CASCADE;")
            cur.execute("DROP TABLE IF EXISTS projections_long CASCADE;")

            # Drop actuals hypertable
            cur.execute("DROP TABLE IF EXISTS actuals CASCADE;")

            # Legacy tables
            cur.execute("DROP TABLE IF EXISTS metadata_table CASCADE;")
            cur.execute("DROP TABLE IF EXISTS values_table CASCADE;")

            # Drop core tables
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
