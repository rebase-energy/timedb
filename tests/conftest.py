"""Pytest configuration and fixtures for timedb tests."""
import os
import pytest
import psycopg
from datetime import datetime, timezone
import uuid

from timedb.db import create, delete


@pytest.fixture(scope="function")
def test_db_conninfo():
    """Get test database connection string from environment."""
    conninfo = os.environ.get("TEST_TIMEDB_DSN") or os.environ.get("TEST_DATABASE_URL")
    if not conninfo:
        pytest.skip("TEST_TIMEDB_DSN or TEST_DATABASE_URL environment variable not set")
    return conninfo


@pytest.fixture(scope="function")
def clean_db(test_db_conninfo):
    """Create a clean database schema for each test.

    Creates the full TimescaleDB schema including:
    - batches_table, series_table
    - flat (hypertable for immutable facts)
    - overlapping_short/medium/long (hypertables for versioned overlapping)
    """
    # Delete existing schema if it exists
    delete.delete_schema(test_db_conninfo)

    # Create fresh schema
    create.create_schema(test_db_conninfo)

    yield test_db_conninfo


@pytest.fixture
def sample_batch_id():
    """Generate a sample batch ID for testing."""
    return uuid.uuid4()


@pytest.fixture
def sample_workflow_id():
    """Sample workflow ID for testing."""
    return "test-workflow"


@pytest.fixture
def sample_datetime():
    """Sample datetime for testing."""
    return datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)



@pytest.fixture
def sample_series_id():
    """Generate a sample series ID for testing."""
    return uuid.uuid4()
