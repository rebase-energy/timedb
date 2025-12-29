import os
import psycopg
from dotenv import load_dotenv
from importlib import resources

from pg_create_table import create_schema

load_dotenv()

# -----------------------------------------------------------------------------
# Optional schema addon: metadata_table
#
# - Anchored to (run_id, valid_time)
# - Long-form metadata (metadata_key + typed value columns)
# - NOT versioned for now (one row per (run_id, valid_time, metadata_key))
# - Idempotent / safe to run multiple times
# -----------------------------------------------------------------------------

# Read packaged SQL
DDL = resources.files(__package__).joinpath("pg_create_table_with_metadata.sql").read_text(encoding="utf-8")


def create_schema_metadata(conninfo: str) -> None:
    """
    Creates (or updates) the optional metadata schema addon.

    Requirements:
      - runs_table must already exist (so run create_schema(conninfo) first)

    Notes:
      - idempotent
      - safe to run multiple times
    """
    with psycopg.connect(conninfo, autocommit=False) as conn:
        with conn.cursor() as cur:
            cur.execute(DDL)


if __name__ == "__main__":
    conninfo = os.environ["NEON_PG_URL3"]

    create_schema(conninfo)
    create_schema_metadata(conninfo)

    print("Base schema + optional metadata schema created/updated successfully.")