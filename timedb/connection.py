"""
Connection management for TimeDB.

Provides helpers to resolve PostgreSQL and ClickHouse connection strings from
environment variables and to obtain database connections from a pool or
on-the-fly.
"""
import os
from contextlib import contextmanager
from typing import Optional

import psycopg
from psycopg_pool import ConnectionPool


def _get_pg_conninfo() -> str:
    """Get PostgreSQL connection string from environment variables."""
    conninfo = os.environ.get("TIMEDB_PG_DSN") or os.environ.get("DATABASE_URL")
    if not conninfo:
        raise ValueError(
            "PostgreSQL connection not configured. Set TIMEDB_PG_DSN environment variable."
        )
    return conninfo


def _get_ch_url() -> str:
    """Get ClickHouse DSN from environment variables."""
    ch_url = os.environ.get("TIMEDB_CH_URL")
    if not ch_url:
        raise ValueError(
            "ClickHouse connection not configured. Set TIMEDB_CH_URL environment variable."
        )
    return ch_url


@contextmanager
def _get_connection(_pool: Optional[ConnectionPool] = None, conninfo: Optional[str] = None):
    """
    Context manager that yields a database connection.

    Uses connection pool if available, otherwise creates a new connection.

    Args:
        _pool: Optional connection pool
        conninfo: Optional connection string (fetched via _get_pg_conninfo() if not provided)

    Yields:
        psycopg.Connection
    """
    if _pool is not None:
        with _pool.connection() as conn:
            yield conn
    else:
        if conninfo is None:
            conninfo = _get_pg_conninfo()
        with psycopg.connect(conninfo) as conn:
            yield conn
