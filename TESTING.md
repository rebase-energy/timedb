# Testing Guide for TimeDB

This document provides information about testing timedb.

## Overview

The test suite is located in the `tests/` directory and uses [pytest](https://pytest.org/) as the testing framework.

## Setup

### 1. Install Test Dependencies

```bash
pip install -e ".[test]"
```

This installs timedb in editable mode along with pytest and pytest-cov.

### 2. Configure Test Databases

Set both environment variables for PostgreSQL and ClickHouse:

```bash
# Bash/Zsh
export TEST_TIMEDB_PG_DSN='postgresql://user:password@host:port/test_database'
export TEST_TIMEDB_CH_URL='http://default:@localhost:8123/default'
```

```fish
# Fish
set -x TEST_TIMEDB_PG_DSN postgresql://user:password@host:port/test_database
set -x TEST_TIMEDB_CH_URL http://default:@localhost:8123/default
```

**Important**: Use a separate test database, not your development database. Tests will create and drop schema objects.

## Running Tests

### Run All Tests

```bash
pytest
```

### Run Specific Test File

```bash
pytest tests/test_insert.py
```

### Run Specific Test Function

```bash
pytest tests/test_insert.py::test_insert_run
```

### Run with Coverage Report

```bash
pytest --cov=timedb --cov-report=html
```

Then open `htmlcov/index.html` in your browser to view the coverage report.

### Run with Verbose Output

```bash
pytest -v
```

### Run Only Fast Tests

```bash
pytest -m "not slow"
```

## Test Structure

### Test Files

- `tests/conftest.py`: Pytest fixtures and configuration
- `tests/test_insert.py`: Tests for inserting runs and values
- `tests/test_read.py`: Tests for reading values
- `tests/test_update.py`: Tests for updating records

### Test Fixtures

Fixtures are defined in `conftest.py`:

- `test_db_conninfo`: Provides the test database connection string
- `clean_db`: Creates a fresh schema in both PostgreSQL and ClickHouse for each test
- `sample_run_id`: Generates a UUID for test runs
- `sample_workflow_id`: Provides a test workflow ID
- `sample_datetime`: Provides a sample datetime for testing

### Schema

The PostgreSQL schema is defined in `pg_create_tables.sql` (series). The ClickHouse schema is defined in `ch_create_tables.sql` (runs, flat, overlapping_short/medium/long). It supports inserts, reads, and updates with full temporal versioning.

Used by: `create`, `insert`, `read`, `update` modules and all test files.

## Writing New Tests

### Basic Test Structure

```python
def test_feature_name(clean_db, sample_run_id, sample_workflow_id, sample_datetime):
    """Test description."""
    # Your test code here
    pass
```

### Best Practices

1. **Use fixtures**: Always use the provided fixtures for database connections and sample data
2. **Clean state**: Each test gets a fresh database schema, so tests are independent
3. **Timezone-aware**: Always use timezone-aware datetimes (UTC)
4. **Descriptive names**: Use clear test function names that describe what is being tested
5. **Docstrings**: Add docstrings explaining what each test verifies

### Example Test

```python
def test_insert_flat_point_in_time(td, clean_db, sample_datetime):
    """Test inserting point-in-time values into the flat table."""
    import psycopg
    import pandas as pd

    td.create_series(name="power", unit="dimensionless", overlapping=False)

    df = pd.DataFrame({
        "valid_time": [sample_datetime],
        "value": [100.5],
    })

    result = td.get_series("power").insert(df=df)
    assert result.run_id is None

    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM flat")
            assert cur.fetchone()[0] == 1
```

## Continuous Integration

For CI/CD pipelines:

```bash
# Install dependencies
pip install -e ".[test]"

# Run tests
pytest --cov=timedb --cov-report=xml

# Or with coverage threshold
pytest --cov=timedb --cov-fail-under=80
```

## Troubleshooting

### Tests Fail with "Database connection not configured"

Make sure you've set `TEST_TIMEDB_PG_DSN` and `TEST_TIMEDB_CH_URL` environment variables.

### Tests Fail with Permission Errors

Ensure your database user has permissions to:
- CREATE/DROP tables
- CREATE/DROP views
- CREATE/DROP indexes

### Tests Leave Data Behind

Tests use the `clean_db` fixture which creates a fresh schema for each test. If you see leftover data, check that:
1. The fixture is being used correctly
2. The test database is separate from your development database

### Import Errors

If you see import errors for `pytest`, make sure you've installed test dependencies:
```bash
pip install -e ".[test]"
```

## Coverage Goals

Aim for:
- **80%+ overall coverage**
- **100% coverage for critical paths** (insert, read, update operations)
- **Coverage for error handling** (validation, edge cases)

View coverage reports:
```bash
pytest --cov=timedb --cov-report=html
open htmlcov/index.html
```

