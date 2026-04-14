import os
from importlib import resources
import psycopg
import clickhouse_connect


DDL_PG = resources.files("timedb").joinpath("sql", "pg_create_tables.sql").read_text(encoding="utf-8")
DDL_CH = resources.files("timedb").joinpath("sql", "ch_create_tables.sql").read_text(encoding="utf-8")


def create_postgres(pg_conninfo: str) -> None:
    """Create the TimeDB PostgreSQL schema (series).

    Idempotent — uses IF NOT EXISTS.
    """
    with psycopg.connect(pg_conninfo) as conn:
        with conn.cursor() as cur:
            cur.execute(DDL_PG)
        conn.commit()
    print("  ✓ PostgreSQL: series")


def create_clickhouse(ch_url: str) -> None:
    """Create the TimeDB ClickHouse tables (runs, flat, overlapping_*).

    Idempotent — uses IF NOT EXISTS.
    """
    ch_client = clickhouse_connect.get_client(dsn=ch_url)
    for statement in DDL_CH.split(";"):
        statement = statement.strip()
        if not statement:
            continue
        non_comment = [l for l in statement.splitlines()
                       if l.strip() and not l.strip().startswith("--")]
        if not non_comment:
            continue
        ch_client.command(statement)
    print("  ✓ ClickHouse: runs, flat, overlapping_short/medium/long")


def create_schema(
    pg_conninfo: str,
    ch_url: str,
) -> None:
    """
    Create the TimeDB schema in both databases.

    - PostgreSQL: series (series metadata, labels, routing)
    - ClickHouse: runs, flat, overlapping_short/medium/long

    Idempotent — safe to run multiple times (uses IF NOT EXISTS).

    Args:
        pg_conninfo: PostgreSQL connection string (series only).
        ch_url: ClickHouse DSN (e.g. ``clickhouse://user:pass@localhost:8123/timedb``).
    """
    print("Creating database schema...")
    create_postgres(pg_conninfo)
    create_clickhouse(ch_url)
    print("✓ Schema created successfully")


if __name__ == "__main__":
    pg_conninfo = os.environ["TIMEDB_PG_DSN"]
    ch_url = os.environ["TIMEDB_CH_URL"]
    create_schema(pg_conninfo, ch_url)
