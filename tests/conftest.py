"""Pytest configuration and fixtures for timedb tests."""
import os
import pytest
import clickhouse_connect
from datetime import datetime, timezone

from timedb.db import create, delete
from timedb import TimeDataClient


@pytest.fixture(scope="function")
def test_pg_conninfo():
    """Get PostgreSQL test connection string from environment."""
    conninfo = os.environ.get("TEST_TIMEDB_PG_DSN") or os.environ.get("TEST_DATABASE_URL")
    if not conninfo:
        pytest.skip("TEST_TIMEDB_PG_DSN environment variable not set")
    return conninfo


@pytest.fixture(scope="function")
def test_ch_url():
    """Get ClickHouse test URL from environment."""
    ch_url = os.environ.get("TEST_TIMEDB_CH_URL")
    if not ch_url:
        pytest.skip("TEST_TIMEDB_CH_URL environment variable not set")
    return ch_url


@pytest.fixture(scope="function")
def clean_db(test_pg_conninfo, test_ch_url):
    """Create a clean database schema for each test.

    Creates:
    - PostgreSQL: series_table
    - ClickHouse: runs_table, flat, overlapping_short/medium/long
    """
    delete.delete_schema(test_pg_conninfo, test_ch_url)
    create.create_schema(test_pg_conninfo, test_ch_url)
    yield test_pg_conninfo, test_ch_url


@pytest.fixture
def td(clean_db):
    """TimeDataClient with connection pool properly closed after each test."""
    pg_conninfo, ch_url = clean_db
    client = TimeDataClient(pg_conninfo=pg_conninfo, ch_url=ch_url)
    yield client
    client.close()


@pytest.fixture
def ch_client(clean_db):
    """ClickHouse client for direct verification queries."""
    _, ch_url = clean_db
    client = clickhouse_connect.get_client(dsn=ch_url)
    yield client
    client.close()


@pytest.fixture
def sample_workflow_id():
    """Sample workflow ID for testing."""
    return "test-workflow"


@pytest.fixture
def sample_datetime():
    """Sample datetime for testing."""
    return datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
