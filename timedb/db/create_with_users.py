import os
import psycopg
from dotenv import load_dotenv
from importlib import resources

from .create import create_schema

load_dotenv()

# -----------------------------------------------------------------------------
# Optional schema addon: users_table
#
# - Stores user accounts with API keys for authentication
# - The email field is used to populate the changed_by field in values_table
# - Idempotent / safe to run multiple times
# -----------------------------------------------------------------------------

# Read packaged SQL
DDL = resources.files("timedb").joinpath("sql", "pg_create_users_table.sql").read_text(encoding="utf-8")


def create_schema_users(conninfo: str) -> None:
    """
    Creates (or updates) the optional users schema addon.

    Requirements:
      - Base schema must already exist (so run create_schema(conninfo) first)

    Notes:
      - idempotent
      - safe to run multiple times
    """
    with psycopg.connect(conninfo, autocommit=False) as conn:
        with conn.cursor() as cur:
            cur.execute(DDL)


if __name__ == "__main__":
    conninfo = os.environ.get("TIMEDB_DSN") or os.environ.get("DATABASE_URL")
    
    create_schema(conninfo)
    create_schema_users(conninfo)
    
    print("Base schema + optional users schema created/updated successfully.")

