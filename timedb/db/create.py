import os
from importlib import resources
from dotenv import load_dotenv
import psycopg

load_dotenv()

#TODO Possible to make the values_table append only (except is_current) to enforce history

# -----------------------------------------------------------------------------
# This DDL creates:
#   1) runs_table                → one row per run
#   2) values_table              → versioned values (auditable)
#   3) indexes                   → for performance and data integrity
#   4) current_values_table view → safe default for querying "current" data
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

    with psycopg.connect(conninfo, autocommit=False) as conn:
        with conn.cursor() as cur:
            cur.execute(DDL)


if __name__ == "__main__":
    conninfo = os.environ["NEON_PG_URL2"]

    create_schema(conninfo)
    print("Database schema created or updated successfully.")
