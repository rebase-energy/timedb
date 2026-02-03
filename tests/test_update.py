"""Tests for updating projection records."""
import os
import pytest
import psycopg
from datetime import datetime, timezone, timedelta
import pandas as pd
from timedb import TimeDataClient


def _setup_projection_series(clean_db, sample_datetime):
    """Helper: create a projection series, insert one value, return (td, result, series_id)."""
    os.environ["TIMEDB_DSN"] = clean_db
    td = TimeDataClient()

    series_id = td.create_series(
        name="forecast", unit="dimensionless",
        data_class="projection", storage_tier="medium",
    )

    df = pd.DataFrame({
        "valid_time": [sample_datetime],
        "forecast": [100.0],
    })
    result = td.series("forecast").insert(df=df, known_time=sample_datetime)

    return td, result, series_id


# =============================================================================
# Update value tests
# =============================================================================

def test_update_projection_value(clean_db, sample_batch_id, sample_tenant_id, sample_series_id, sample_datetime):
    """Test updating a projection's value creates a new version."""
    td, result, series_id = _setup_projection_series(clean_db, sample_datetime)

    record_update = {
        "batch_id": result.batch_id,
        "tenant_id": result.tenant_id,
        "valid_time": sample_datetime,
        "series_id": series_id,
        "value": 150.0,
        "changed_by": "test-user",
    }
    outcome = td.update_records(updates=[record_update])

    assert len(outcome["updated"]) == 1
    assert len(outcome["skipped_no_ops"]) == 0

    # Verify new version exists with updated value
    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT value, changed_by
                FROM projections_medium
                WHERE batch_id = %s AND series_id = %s
                ORDER BY known_time DESC
                LIMIT 1
                """,
                (result.batch_id, series_id)
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] == 150.0
            assert row[1] == "test-user"


def test_update_projection_annotation_only(clean_db, sample_datetime):
    """Test updating only the annotation, leaving value unchanged."""
    td, result, series_id = _setup_projection_series(clean_db, sample_datetime)

    record_update = {
        "batch_id": result.batch_id,
        "tenant_id": result.tenant_id,
        "valid_time": sample_datetime,
        "series_id": series_id,
        "annotation": "Updated annotation",
        "changed_by": "test-user",
    }
    outcome = td.update_records(updates=[record_update])

    assert len(outcome["updated"]) == 1

    # Verify value unchanged, annotation updated
    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT value, annotation
                FROM projections_medium
                WHERE batch_id = %s AND series_id = %s
                ORDER BY known_time DESC
                LIMIT 1
                """,
                (result.batch_id, series_id)
            )
            row = cur.fetchone()
            assert row[0] == 100.0  # Value unchanged
            assert row[1] == "Updated annotation"


def test_update_projection_tags(clean_db, sample_datetime):
    """Test updating tags on a projection."""
    td, result, series_id = _setup_projection_series(clean_db, sample_datetime)

    record_update = {
        "batch_id": result.batch_id,
        "tenant_id": result.tenant_id,
        "valid_time": sample_datetime,
        "series_id": series_id,
        "tags": ["reviewed", "validated"],
        "changed_by": "test-user",
    }
    outcome = td.update_records(updates=[record_update])

    assert len(outcome["updated"]) == 1

    # Verify tags were set
    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT tags
                FROM projections_medium
                WHERE batch_id = %s AND series_id = %s
                ORDER BY known_time DESC
                LIMIT 1
                """,
                (result.batch_id, series_id)
            )
            row = cur.fetchone()
            assert row[0] is not None
            assert set(row[0]) == {"reviewed", "validated"}


def test_update_projection_clear_tags(clean_db, sample_datetime):
    """Test clearing tags by setting to empty list."""
    td, result, series_id = _setup_projection_series(clean_db, sample_datetime)

    # First add tags
    td.update_records(updates=[{
        "batch_id": result.batch_id,
        "tenant_id": result.tenant_id,
        "valid_time": sample_datetime,
        "series_id": series_id,
        "tags": ["tag1", "tag2"],
    }])

    # Then clear tags
    outcome = td.update_records(updates=[{
        "batch_id": result.batch_id,
        "tenant_id": result.tenant_id,
        "valid_time": sample_datetime,
        "series_id": series_id,
        "tags": [],  # Empty list clears tags
    }])

    assert len(outcome["updated"]) == 1

    # Verify tags are cleared (NULL)
    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT tags
                FROM projections_medium
                WHERE batch_id = %s AND series_id = %s
                ORDER BY known_time DESC
                LIMIT 1
                """,
                (result.batch_id, series_id)
            )
            row = cur.fetchone()
            assert row[0] is None  # Tags cleared


def test_update_no_op_skipped(clean_db, sample_datetime):
    """Test that no-op updates are skipped."""
    td, result, series_id = _setup_projection_series(clean_db, sample_datetime)

    record_update = {
        "batch_id": result.batch_id,
        "tenant_id": result.tenant_id,
        "valid_time": sample_datetime,
        "series_id": series_id,
        "value": 100.0,  # Same value as inserted
    }
    outcome = td.update_records(updates=[record_update])

    # Should be skipped
    assert len(outcome["updated"]) == 0
    assert len(outcome["skipped_no_ops"]) == 1


def test_update_nonexistent_record_without_value(clean_db, sample_datetime):
    """Test that updating a non-existent record without value raises error."""
    os.environ["TIMEDB_DSN"] = clean_db
    td = TimeDataClient()

    series_id = td.create_series(
        name="forecast", unit="dimensionless",
        data_class="projection", storage_tier="medium",
    )

    # Insert a batch but no values for this specific valid_time
    df = pd.DataFrame({
        "valid_time": [sample_datetime],
        "forecast": [100.0],
    })
    result = td.series("forecast").insert(df=df, known_time=sample_datetime)

    # Try to update a different valid_time that doesn't exist
    non_existent_time = sample_datetime + timedelta(hours=999)
    record_update = {
        "batch_id": result.batch_id,
        "tenant_id": result.tenant_id,
        "valid_time": non_existent_time,
        "series_id": series_id,
        "annotation": "annotation only",  # No value provided
    }
    with pytest.raises(ValueError, match="No current row exists"):
        td.update_records(updates=[record_update])


def test_update_nonexistent_record_with_value(clean_db, sample_datetime):
    """Test that updating a non-existent record with value creates a new row."""
    os.environ["TIMEDB_DSN"] = clean_db
    td = TimeDataClient()

    series_id = td.create_series(
        name="forecast", unit="dimensionless",
        data_class="projection", storage_tier="medium",
    )

    # Insert initial data
    df = pd.DataFrame({
        "valid_time": [sample_datetime],
        "forecast": [100.0],
    })
    result = td.series("forecast").insert(df=df, known_time=sample_datetime)

    # Update a different valid_time with value provided
    new_time = sample_datetime + timedelta(hours=1)
    record_update = {
        "batch_id": result.batch_id,
        "tenant_id": result.tenant_id,
        "valid_time": new_time,
        "series_id": series_id,
        "value": 200.0,
        "annotation": "new record",
    }
    outcome = td.update_records(updates=[record_update])
    assert len(outcome["updated"]) == 1


# =============================================================================
# Update via collection API
# =============================================================================

def test_update_via_collection(clean_db, sample_datetime):
    """Test updating projections via the SeriesCollection API."""
    td, result, series_id = _setup_projection_series(clean_db, sample_datetime)

    outcome = td.series("forecast").update_records(updates=[{
        "batch_id": result.batch_id,
        "valid_time": sample_datetime,
        "value": 999.0,
        "changed_by": "collection-api",
    }])

    assert len(outcome["updated"]) == 1

    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT value, changed_by
                FROM projections_medium
                WHERE batch_id = %s AND series_id = %s
                ORDER BY known_time DESC
                LIMIT 1
                """,
                (result.batch_id, series_id)
            )
            row = cur.fetchone()
            assert row[0] == 999.0
            assert row[1] == "collection-api"


# =============================================================================
# Versioning: updates create new rows
# =============================================================================

def test_update_creates_new_version(clean_db, sample_datetime):
    """Test that an update creates a new row (version) rather than modifying in place."""
    td, result, series_id = _setup_projection_series(clean_db, sample_datetime)

    # Initial: 1 row
    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM projections_medium WHERE batch_id = %s AND series_id = %s",
                (result.batch_id, series_id)
            )
            assert cur.fetchone()[0] == 1

    # Update creates a new row
    td.update_records(updates=[{
        "batch_id": result.batch_id,
        "tenant_id": result.tenant_id,
        "valid_time": sample_datetime,
        "series_id": series_id,
        "value": 200.0,
    }])

    # Should now have 2 rows (original + updated version)
    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM projections_medium WHERE batch_id = %s AND series_id = %s",
                (result.batch_id, series_id)
            )
            assert cur.fetchone()[0] == 2

            # Latest version should have the new value
            cur.execute(
                """
                SELECT value FROM projections_medium
                WHERE batch_id = %s AND series_id = %s
                ORDER BY known_time DESC LIMIT 1
                """,
                (result.batch_id, series_id)
            )
            assert cur.fetchone()[0] == 200.0
