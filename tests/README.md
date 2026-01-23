# timedb Tests

This directory contains the test suite for timedb.

## Running Tests

### Prerequisites

1. **Test Database**: You need a PostgreSQL database for testing. Set one of these environment variables:
   ```bash
   export TEST_TIMEDB_DSN="postgresql://user:password@host:port/test_database"
   # OR
   export TEST_DATABASE_URL="postgresql://user:password@host:port/test_database"
   ```

2. **Install Test Dependencies**:
   ```bash
   pip install -e ".[test]"
   ```

### Run All Tests

```bash
pytest tests/
```

### Run Specific Test File

```bash
pytest tests/test_insert.py
```

### Run with Coverage

```bash
pytest tests/ --cov=timedb --cov-report=html
```

### Run with Verbose Output

```bash
pytest tests/ -v
```

## Test Structure

- `conftest.py`: Pytest fixtures and configuration
- `test_insert.py`: Tests for inserting runs and values
- `test_read.py`: Tests for reading values
- `test_update.py`: Tests for updating records

## Test Fixtures

The test suite uses fixtures defined in `conftest.py`:

- `test_db_conninfo`: Provides the test database connection string
- `clean_db`: Creates a fresh database schema for each test
- `sample_batch_id`: Generates a UUID for test runs
- `sample_workflow_id`: Provides a test workflow ID
- `sample_datetime`: Provides a sample datetime for testing

## Writing New Tests

When writing new tests:

1. Use the `clean_db` fixture to get a clean database for each test
2. Use the sample fixtures (`sample_batch_id`, etc.) for consistent test data
3. Follow the existing test patterns for consistency
4. Add docstrings explaining what each test verifies

## Notes

- Tests are designed to be independent and can run in any order
- Each test gets a fresh database schema (via the `clean_db` fixture)
- Tests use timezone-aware datetimes (UTC) as required by timedb
- The test database should be separate from your development database

