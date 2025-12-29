import os
import sys
import psycopg
from dotenv import load_dotenv

load_dotenv()

# -----------------------------------------------------------------------------
# This DDL deletes all timedb tables and views:
#   1) current_metadata_table view
#   2) current_values_table view
#   3) metadata_table
#   4) values_table
#   5) runs_table
#
# Uses CASCADE to handle foreign key dependencies automatically.
# -----------------------------------------------------------------------------

def delete_schema(conninfo: str) -> None:
    """
    Deletes all timedb tables and views.

    - Uses an explicit transaction (BEGIN/COMMIT)
    - autocommit=False so the transaction is respected
    - Drops views first, then tables
    - Uses CASCADE to handle dependencies
    """
    with psycopg.connect(conninfo, autocommit=False) as conn:
        with conn.cursor() as cur:
            # Drop views first
            cur.execute("DROP VIEW IF EXISTS current_metadata_table CASCADE;")
            cur.execute("DROP VIEW IF EXISTS current_values_table CASCADE;")
            
            # Drop tables in correct order (respecting foreign key constraints)
            cur.execute("DROP TABLE IF EXISTS metadata_table CASCADE;")
            cur.execute("DROP TABLE IF EXISTS values_table CASCADE;")
            cur.execute("DROP TABLE IF EXISTS runs_table CASCADE;")
            
            conn.commit()


if __name__ == "__main__":
    conninfo = os.environ.get("TIMEDB_DSN") or os.environ.get("DATABASE_URL")
    if not conninfo:
        print("ERROR: no DSN provided. Set TIMEDB_DSN or DATABASE_URL")
        sys.exit(1)
    
    delete_schema(conninfo)
    print("All timedb tables (including metadata) deleted successfully.")

