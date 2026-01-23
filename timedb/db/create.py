import os
from importlib import resources
from dotenv import load_dotenv
import psycopg

load_dotenv()

#TODO Possible to make the values_table append only (except is_current) to enforce history

# -----------------------------------------------------------------------------
# This DDL creates:
#   1) batches_table             → one row per batch
#   2) series_table              → series metadata (name, unit, labels)
#   3) values_table              → versioned values (auditable)
#   4) indexes                   → for performance and data integrity
#   5) current_values_table view → safe default for querying "current" data
#
# The script is idempotent (safe to run multiple times).
# -----------------------------------------------------------------------------

# Read packaged SQL
DDL = resources.files("timedb").joinpath("sql", "pg_create_table.sql").read_text(encoding="utf-8")

def create_schema(conninfo: str) -> None:
    """
    Creates (or updates) the database schema.

    - Uses an explicit transaction (BEGIN/COMMIT)
    - autocommit=False so the BEGIN/COMMIT inside the DDL is respected
    - Safe to run multiple times
    - Should typically be run at service startup or via a migration step
    """

    print("Creating database schema...")
    with psycopg.connect(conninfo, autocommit=False) as conn:
        with conn.cursor() as cur:
            cur.execute(DDL)
    
    print("✓ Schema created successfully")


if __name__ == "__main__":
    conninfo = os.environ["DATABASE_URL"]
    create_schema(conninfo)