"""Tests for updating overlapping records."""
import os
import pytest
import psycopg
from datetime import datetime, timezone, timedelta
import pandas as pd
from timedb import TimeDataClient


def _setup_overlapping_series(clean_db, sample_datetime):
    """Helper: create a overlapping series, insert one value, return (td, result, series_id)."""
    os.environ["TIMEDB_DSN"] = clean_db
    td = TimeDataClient()

    series_id = td.create_series(
        name="forecast", unit="dimensionless",
        data_class="overlapping", retention="medium",
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

def test_update_overlapping_value(clean_db, sample_batch_id, sample_series_id, sample_datetime):
    """Test updating a overlapping's value creates a new version."""
    td, result, series_id = _setup_overlapping_series(clean_db, sample_datetime)

    record_update = {
        "batch_id": result.batch_id,
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
                FROM overlapping_medium
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


def test_update_overlapping_annotation_only(clean_db, sample_datetime):
    """Test updating only the annotation, leaving value unchanged."""
    td, result, series_id = _setup_overlapping_series(clean_db, sample_datetime)

    record_update = {
        "batch_id": result.batch_id,
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
                FROM overlapping_medium
                WHERE batch_id = %s AND series_id = %s
                ORDER BY known_time DESC
                LIMIT 1
                """,
                (result.batch_id, series_id)
            )
            row = cur.fetchone()
            assert row[0] == 100.0  # Value unchanged
            assert row[1] == "Updated annotation"


def test_update_overlapping_tags(clean_db, sample_datetime):
    """Test updating tags on a overlapping."""
    td, result, series_id = _setup_overlapping_series(clean_db, sample_datetime)

    record_update = {
        "batch_id": result.batch_id,
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
                FROM overlapping_medium
                WHERE batch_id = %s AND series_id = %s
                ORDER BY known_time DESC
                LIMIT 1
                """,
                (result.batch_id, series_id)
            )
            row = cur.fetchone()
            assert row[0] is not None
            assert set(row[0]) == {"reviewed", "validated"}


def test_update_overlapping_clear_tags(clean_db, sample_datetime):
    """Test clearing tags by setting to empty list."""
    td, result, series_id = _setup_overlapping_series(clean_db, sample_datetime)

    # First add tags
    td.update_records(updates=[{
        "batch_id": result.batch_id,
        "valid_time": sample_datetime,
        "series_id": series_id,
        "tags": ["tag1", "tag2"],
    }])

    # Then clear tags
    outcome = td.update_records(updates=[{
        "batch_id": result.batch_id,
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
                FROM overlapping_medium
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
    td, result, series_id = _setup_overlapping_series(clean_db, sample_datetime)

    record_update = {
        "batch_id": result.batch_id,
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
        data_class="overlapping", retention="medium",
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
        data_class="overlapping", retention="medium",
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
    """Test updating overlappings via the SeriesCollection API."""
    td, result, series_id = _setup_overlapping_series(clean_db, sample_datetime)

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
                FROM overlapping_medium
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
    td, result, series_id = _setup_overlapping_series(clean_db, sample_datetime)

    # Initial: 1 row
    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM overlapping_medium WHERE batch_id = %s AND series_id = %s",
                (result.batch_id, series_id)
            )
            assert cur.fetchone()[0] == 1

    # Update creates a new row
    td.update_records(updates=[{
        "batch_id": result.batch_id,
        "valid_time": sample_datetime,
        "series_id": series_id,
        "value": 200.0,
    }])

    # Should now have 2 rows (original + updated version)
    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM overlapping_medium WHERE batch_id = %s AND series_id = %s",
                (result.batch_id, series_id)
            )
            assert cur.fetchone()[0] == 2

            # Latest version should have the new value
            cur.execute(
                """
                SELECT value FROM overlapping_medium
                WHERE batch_id = %s AND series_id = %s
                ORDER BY known_time DESC LIMIT 1
                """,
                (result.batch_id, series_id)
            )
            assert cur.fetchone()[0] == 200.0


# =============================================================================
# Flat update tests
# =============================================================================

def _setup_flat_series(clean_db, sample_datetime):
    """Helper: create a flat series, insert one value, return (td, series_id)."""
    os.environ["TIMEDB_DSN"] = clean_db
    td = TimeDataClient()

    series_id = td.create_series(
        name="meter_reading", unit="dimensionless", data_class="flat",
    )

    df = pd.DataFrame({
        "valid_time": [sample_datetime],
        "meter_reading": [100.0],
    })
    td.series("meter_reading").insert(df=df)

    return td, series_id


def test_update_flat_value(clean_db, sample_datetime):
    """Test updating a flat value in-place."""
    td, series_id = _setup_flat_series(clean_db, sample_datetime)

    outcome = td.update_records(updates=[{
        "series_id": series_id,
        "valid_time": sample_datetime,
        "value": 150.0,
        "changed_by": "test-user",
    }])

    assert len(outcome["updated"]) == 1
    assert len(outcome["skipped_no_ops"]) == 0

    # Verify value was updated in-place
    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT value, changed_by
                FROM flat
                WHERE series_id = %s AND valid_time = %s
                """,
                (series_id, sample_datetime)
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] == 150.0
            assert row[1] == "test-user"


def test_update_flat_annotation_only(clean_db, sample_datetime):
    """Test updating only the annotation on a flat record."""
    td, series_id = _setup_flat_series(clean_db, sample_datetime)

    outcome = td.update_records(updates=[{
        "series_id": series_id,
        "valid_time": sample_datetime,
        "annotation": "Corrected reading",
    }])

    assert len(outcome["updated"]) == 1

    # Verify value unchanged, annotation updated
    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT value, annotation
                FROM flat
                WHERE series_id = %s AND valid_time = %s
                """,
                (series_id, sample_datetime)
            )
            row = cur.fetchone()
            assert row[0] == 100.0  # Value unchanged
            assert row[1] == "Corrected reading"


def test_update_flat_no_op_skipped(clean_db, sample_datetime):
    """Test that no-op flat updates are skipped."""
    td, series_id = _setup_flat_series(clean_db, sample_datetime)

    outcome = td.update_records(updates=[{
        "series_id": series_id,
        "valid_time": sample_datetime,
        "value": 100.0,  # Same value as inserted
    }])

    assert len(outcome["updated"]) == 0
    assert len(outcome["skipped_no_ops"]) == 1


def test_update_flat_nonexistent_row_errors(clean_db, sample_datetime):
    """Test that updating a non-existent flat row raises an error."""
    td, series_id = _setup_flat_series(clean_db, sample_datetime)

    non_existent_time = sample_datetime + timedelta(hours=999)
    with pytest.raises(ValueError, match="No flat row exists"):
        td.update_records(updates=[{
            "series_id": series_id,
            "valid_time": non_existent_time,
            "value": 200.0,
        }])


def test_update_flat_via_collection(clean_db, sample_datetime):
    """Test updating flat via the SeriesCollection API."""
    td, series_id = _setup_flat_series(clean_db, sample_datetime)

    outcome = td.series("meter_reading").update_records(updates=[{
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
                FROM flat
                WHERE series_id = %s
                """,
                (series_id,)
            )
            row = cur.fetchone()
            assert row[0] == 999.0
            assert row[1] == "collection-api"


def test_update_flat_no_versioning(clean_db, sample_datetime):
    """Test that flat updates are in-place (no new rows created)."""
    td, series_id = _setup_flat_series(clean_db, sample_datetime)

    # Initial: 1 row
    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM flat WHERE series_id = %s", (series_id,))
            assert cur.fetchone()[0] == 1

    # Update should modify in-place, not create new row
    td.update_records(updates=[{
        "series_id": series_id,
        "valid_time": sample_datetime,
        "value": 200.0,
    }])

    # Still only 1 row
    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM flat WHERE series_id = %s", (series_id,))
            assert cur.fetchone()[0] == 1


# =============================================================================
# Overlapping flexible lookup tests
# =============================================================================

def test_update_overlapping_by_known_time(clean_db, sample_datetime):
    """Test updating overlapping by known_time (precise version lookup)."""
    td, result, series_id = _setup_overlapping_series(clean_db, sample_datetime)

    # Update using known_time instead of batch_id
    outcome = td.update_records(updates=[{
        "series_id": series_id,
        "valid_time": sample_datetime,
        "known_time": sample_datetime,  # The known_time from insert
        "value": 200.0,
        "changed_by": "known-time-lookup",
    }])

    assert len(outcome["updated"]) == 1

    # Verify new version exists
    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT value, changed_by
                FROM overlapping_medium
                WHERE series_id = %s
                ORDER BY known_time DESC
                LIMIT 1
                """,
                (series_id,)
            )
            row = cur.fetchone()
            assert row[0] == 200.0
            assert row[1] == "known-time-lookup"


def test_update_overlapping_latest_no_identifiers(clean_db, sample_datetime):
    """Test updating overlapping with just valid_time (latest version)."""
    td, result, series_id = _setup_overlapping_series(clean_db, sample_datetime)

    # Update using only valid_time - should find latest version
    outcome = td.update_records(updates=[{
        "series_id": series_id,
        "valid_time": sample_datetime,
        # No batch_id, no known_time - should find latest
        "value": 300.0,
        "changed_by": "latest-lookup",
    }])

    assert len(outcome["updated"]) == 1

    # Verify new version exists with updated value
    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT value, changed_by
                FROM overlapping_medium
                WHERE series_id = %s
                ORDER BY known_time DESC
                LIMIT 1
                """,
                (series_id,)
            )
            row = cur.fetchone()
            assert row[0] == 300.0
            assert row[1] == "latest-lookup"


def test_update_overlapping_via_collection_no_batch_id(clean_db, sample_datetime):
    """Test updating overlapping via SeriesCollection without batch_id."""
    td, result, series_id = _setup_overlapping_series(clean_db, sample_datetime)

    # Update via collection API without batch_id
    outcome = td.series("forecast").update_records(updates=[{
        "valid_time": sample_datetime,
        "value": 500.0,
    }])

    assert len(outcome["updated"]) == 1

    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT value
                FROM overlapping_medium
                WHERE series_id = %s
                ORDER BY known_time DESC
                LIMIT 1
                """,
                (series_id,)
            )
            assert cur.fetchone()[0] == 500.0
