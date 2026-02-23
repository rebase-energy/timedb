"""Tests for updating overlapping records."""
import pytest
import psycopg
from datetime import datetime, timezone, timedelta
import pandas as pd


def _setup_overlapping_series(td, sample_datetime):
    """Helper: create a overlapping series, insert one value, return (td, result, series_id)."""
    series_id = td.create_series(
        name="forecast", unit="dimensionless",
        overlapping=True, retention="medium",
    )

    df = pd.DataFrame({
        "valid_time": [sample_datetime],
        "value": [100.0],
    })
    result = td.get_series("forecast").insert(df=df, knowledge_time=sample_datetime)

    return td, result, series_id


# =============================================================================
# Update value tests
# =============================================================================

def test_update_overlapping_value(td, clean_db, sample_datetime):
    """Test updating a overlapping's value creates a new version."""
    td, result, series_id = _setup_overlapping_series(td, sample_datetime)

    record_update = {
        "batch_id": result.batch_id,
        "valid_time": sample_datetime,
        "value": 150.0,
        "changed_by": "test-user",
    }
    outcome = td.get_series("forecast").update_records(updates=[record_update])

    assert len(outcome) == 1

    # Verify new version exists with updated value
    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT value, changed_by
                FROM overlapping_medium
                WHERE batch_id = %s AND series_id = %s
                ORDER BY knowledge_time DESC
                LIMIT 1
                """,
                (result.batch_id, series_id)
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] == 150.0
            assert row[1] == "test-user"


def test_update_overlapping_annotation_only(td, clean_db, sample_datetime):
    """Test updating only the annotation, leaving value unchanged."""
    td, result, series_id = _setup_overlapping_series(td, sample_datetime)

    record_update = {
        "batch_id": result.batch_id,
        "valid_time": sample_datetime,
        "annotation": "Updated annotation",
        "changed_by": "test-user",
    }
    outcome = td.get_series("forecast").update_records(updates=[record_update])

    assert len(outcome) == 1

    # Verify value unchanged, annotation updated
    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT value, annotation
                FROM overlapping_medium
                WHERE batch_id = %s AND series_id = %s
                ORDER BY knowledge_time DESC
                LIMIT 1
                """,
                (result.batch_id, series_id)
            )
            row = cur.fetchone()
            assert row[0] == 100.0  # Value unchanged
            assert row[1] == "Updated annotation"


def test_update_overlapping_tags(td, clean_db, sample_datetime):
    """Test updating tags on a overlapping."""
    td, result, series_id = _setup_overlapping_series(td, sample_datetime)

    record_update = {
        "batch_id": result.batch_id,
        "valid_time": sample_datetime,
        "tags": ["reviewed", "validated"],
        "changed_by": "test-user",
    }
    outcome = td.get_series("forecast").update_records(updates=[record_update])

    assert len(outcome) == 1

    # Verify tags were set
    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT tags
                FROM overlapping_medium
                WHERE batch_id = %s AND series_id = %s
                ORDER BY knowledge_time DESC
                LIMIT 1
                """,
                (result.batch_id, series_id)
            )
            row = cur.fetchone()
            assert row[0] is not None
            assert set(row[0]) == {"reviewed", "validated"}


def test_update_overlapping_clear_tags(td, clean_db, sample_datetime):
    """Test clearing tags by setting to empty list."""
    td, result, series_id = _setup_overlapping_series(td, sample_datetime)

    # First add tags
    td.get_series("forecast").update_records(updates=[{
        "batch_id": result.batch_id,
        "valid_time": sample_datetime,
        "tags": ["tag1", "tag2"],
    }])

    # Then clear tags
    outcome = td.get_series("forecast").update_records(updates=[{
        "batch_id": result.batch_id,
        "valid_time": sample_datetime,
        "tags": [],  # Empty list clears tags
    }])

    assert len(outcome) == 1

    # Verify tags are cleared (NULL)
    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT tags
                FROM overlapping_medium
                WHERE batch_id = %s AND series_id = %s
                ORDER BY knowledge_time DESC
                LIMIT 1
                """,
                (result.batch_id, series_id)
            )
            row = cur.fetchone()
            assert row[0] is None  # Tags cleared


def test_update_nonexistent_record_without_value(td, sample_datetime):
    """Test that updating a non-existent record without value raises error."""
    series_id = td.create_series(
        name="forecast", unit="dimensionless",
        overlapping=True, retention="medium",
    )

    # Insert a batch but no values for this specific valid_time
    df = pd.DataFrame({
        "valid_time": [sample_datetime],
        "value": [100.0],
    })
    result = td.get_series("forecast").insert(df=df, knowledge_time=sample_datetime)

    # Try to update a different valid_time that doesn't exist
    non_existent_time = sample_datetime + timedelta(hours=999)
    record_update = {
        "batch_id": result.batch_id,
        "valid_time": non_existent_time,
        "annotation": "annotation only",  # No value provided
    }
    with pytest.raises(ValueError, match="No current row exists"):
        td.get_series("forecast").update_records(updates=[record_update])


def test_update_nonexistent_record_with_value(td, sample_datetime):
    """Test that updating a non-existent record with value creates a new row."""
    series_id = td.create_series(
        name="forecast", unit="dimensionless",
        overlapping=True, retention="medium",
    )

    # Insert initial data
    df = pd.DataFrame({
        "valid_time": [sample_datetime],
        "value": [100.0],
    })
    result = td.get_series("forecast").insert(df=df, knowledge_time=sample_datetime)

    # Update a different valid_time with value provided
    new_time = sample_datetime + timedelta(hours=1)
    record_update = {
        "batch_id": result.batch_id,
        "valid_time": new_time,
        "value": 200.0,
        "annotation": "new record",
    }
    outcome = td.get_series("forecast").update_records(updates=[record_update])
    assert len(outcome) == 1


# =============================================================================
# Update via collection API
# =============================================================================

def test_update_via_collection(td, clean_db, sample_datetime):
    """Test updating overlappings via the SeriesCollection API."""
    td, result, series_id = _setup_overlapping_series(td, sample_datetime)

    outcome = td.get_series("forecast").update_records(updates=[{
        "batch_id": result.batch_id,
        "valid_time": sample_datetime,
        "value": 999.0,
        "changed_by": "collection-api",
    }])

    assert len(outcome) == 1

    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT value, changed_by
                FROM overlapping_medium
                WHERE batch_id = %s AND series_id = %s
                ORDER BY knowledge_time DESC
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

def test_update_creates_new_version(td, clean_db, sample_datetime):
    """Test that an update creates a new row (version) rather than modifying in place."""
    td, result, series_id = _setup_overlapping_series(td, sample_datetime)

    # Initial: 1 row
    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM overlapping_medium WHERE batch_id = %s AND series_id = %s",
                (result.batch_id, series_id)
            )
            assert cur.fetchone()[0] == 1

    # Update creates a new row
    td.get_series("forecast").update_records(updates=[{
        "batch_id": result.batch_id,
        "valid_time": sample_datetime,
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
                ORDER BY knowledge_time DESC LIMIT 1
                """,
                (result.batch_id, series_id)
            )
            assert cur.fetchone()[0] == 200.0


# =============================================================================
# Flat update tests
# =============================================================================

def _setup_flat_series(td, sample_datetime):
    """Helper: create a flat series, insert one value, return (td, series_id)."""
    series_id = td.create_series(
        name="meter_reading", unit="dimensionless", overlapping=False,
    )

    df = pd.DataFrame({
        "valid_time": [sample_datetime],
        "value": [100.0],
    })
    td.get_series("meter_reading").insert(df=df)

    return td, series_id


def test_update_flat_value(td, clean_db, sample_datetime):
    """Test updating a flat value in-place."""
    td, series_id = _setup_flat_series(td, sample_datetime)

    outcome = td.get_series("meter_reading").update_records(updates=[{
        "valid_time": sample_datetime,
        "value": 150.0,
        "changed_by": "test-user",
    }])

    assert len(outcome) == 1

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


def test_update_flat_annotation_only(td, clean_db, sample_datetime):
    """Test updating only the annotation on a flat record."""
    td, series_id = _setup_flat_series(td, sample_datetime)

    outcome = td.get_series("meter_reading").update_records(updates=[{
        "valid_time": sample_datetime,
        "annotation": "Corrected reading",
    }])

    assert len(outcome) == 1

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


def test_update_flat_nonexistent_row_errors(td, sample_datetime):
    """Test that updating a non-existent flat row raises an error."""
    td, series_id = _setup_flat_series(td, sample_datetime)

    non_existent_time = sample_datetime + timedelta(hours=999)
    with pytest.raises(ValueError, match="No flat row exists"):
        td.get_series("meter_reading").update_records(updates=[{
            "valid_time": non_existent_time,
            "value": 200.0,
        }])


def test_update_flat_via_collection(td, clean_db, sample_datetime):
    """Test updating flat via the SeriesCollection API."""
    td, series_id = _setup_flat_series(td, sample_datetime)

    outcome = td.get_series("meter_reading").update_records(updates=[{
        "valid_time": sample_datetime,
        "value": 999.0,
        "changed_by": "collection-api",
    }])

    assert len(outcome) == 1

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


def test_update_flat_no_versioning(td, clean_db, sample_datetime):
    """Test that flat updates are in-place (no new rows created)."""
    td, series_id = _setup_flat_series(td, sample_datetime)

    # Initial: 1 row
    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM flat WHERE series_id = %s", (series_id,))
            assert cur.fetchone()[0] == 1

    # Update should modify in-place, not create new row
    td.get_series("meter_reading").update_records(updates=[{
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

def test_update_overlapping_by_knowledge_time(td, clean_db, sample_datetime):
    """Test updating overlapping by knowledge_time (precise version lookup)."""
    td, result, series_id = _setup_overlapping_series(td, sample_datetime)

    # Update using knowledge_time instead of batch_id
    outcome = td.get_series("forecast").update_records(updates=[{
        "valid_time": sample_datetime,
        "knowledge_time": sample_datetime,  # The knowledge_time from insert
        "value": 200.0,
        "changed_by": "known-time-lookup",
    }])

    assert len(outcome) == 1

    # Verify new version exists
    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT value, changed_by
                FROM overlapping_medium
                WHERE series_id = %s
                ORDER BY knowledge_time DESC
                LIMIT 1
                """,
                (series_id,)
            )
            row = cur.fetchone()
            assert row[0] == 200.0
            assert row[1] == "known-time-lookup"


def test_update_overlapping_latest_no_identifiers(td, clean_db, sample_datetime):
    """Test updating overlapping with just valid_time (latest version)."""
    td, result, series_id = _setup_overlapping_series(td, sample_datetime)

    # Update using only valid_time - should find latest version
    outcome = td.get_series("forecast").update_records(updates=[{
        "valid_time": sample_datetime,
        # No batch_id, no knowledge_time - should find latest
        "value": 300.0,
        "changed_by": "latest-lookup",
    }])

    assert len(outcome) == 1

    # Verify new version exists with updated value
    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT value, changed_by
                FROM overlapping_medium
                WHERE series_id = %s
                ORDER BY knowledge_time DESC
                LIMIT 1
                """,
                (series_id,)
            )
            row = cur.fetchone()
            assert row[0] == 300.0
            assert row[1] == "latest-lookup"


def test_update_overlapping_via_collection_no_batch_id(td, clean_db, sample_datetime):
    """Test updating overlapping via SeriesCollection without batch_id."""
    td, result, series_id = _setup_overlapping_series(td, sample_datetime)

    # Update via collection API without batch_id
    outcome = td.get_series("forecast").update_records(updates=[{
        "valid_time": sample_datetime,
        "value": 500.0,
    }])

    assert len(outcome) == 1

    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT value
                FROM overlapping_medium
                WHERE series_id = %s
                ORDER BY knowledge_time DESC
                LIMIT 1
                """,
                (series_id,)
            )
            assert cur.fetchone()[0] == 500.0
