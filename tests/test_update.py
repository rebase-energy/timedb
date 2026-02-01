"""Tests for updating records."""
import os
import pytest
import psycopg
from datetime import datetime, timezone
from timedb import TimeDataClient


def test_update_record_value(clean_db_for_update, sample_batch_id, sample_tenant_id, sample_series_id, sample_workflow_id, sample_datetime):
    """Test updating a record's value."""
    # Insert initial value with known_time
    initial_known_time = sample_datetime
    with psycopg.connect(clean_db_for_update) as conn:
        with conn.cursor() as cur:
            # Insert batch
            cur.execute(
                "INSERT INTO batches_table (batch_id, tenant_id, workflow_id, batch_start_time, known_time) VALUES (%s, %s, %s, %s, %s)",
                (sample_batch_id, sample_tenant_id, sample_workflow_id, sample_datetime, initial_known_time),
            )
            # Ensure series metadata exists
            cur.execute(
                "INSERT INTO series_table (series_id, name, unit, labels) VALUES (%s, %s, %s, %s::jsonb)",
                (sample_series_id, "mean", "dimensionless", "{}"),
            )
            # Insert value with known_time
            cur.execute(
                """
                INSERT INTO values_table (batch_id, tenant_id, series_id, valid_time, value, known_time)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (sample_batch_id, sample_tenant_id, sample_series_id, sample_datetime, 100.0, initial_known_time),
            )

    # Update the value using SDK (uses TIMEDB_DSN env var)
    os.environ["TIMEDB_DSN"] = clean_db_for_update
    td = TimeDataClient()
    record_update = {
        "batch_id": sample_batch_id,
        "tenant_id": sample_tenant_id,
        "valid_time": sample_datetime,
        "series_id": sample_series_id,
        "value": 150.0,
        "changed_by": "test-user",
    }
    result = td.update_records(updates=[record_update])

    assert len(result["updated"]) == 1
    assert len(result["skipped_no_ops"]) == 0

    # Verify the update - get the latest version by known_time DESC
    with psycopg.connect(clean_db_for_update) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT value, changed_by
                FROM values_table
                WHERE batch_id = %s AND tenant_id = %s AND valid_time = %s AND series_id = %s
                ORDER BY known_time DESC
                LIMIT 1
                """,
                (sample_batch_id, sample_tenant_id, sample_datetime, sample_series_id)
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] == 150.0
            assert row[1] == "test-user"


def test_update_record_annotation_only(clean_db_for_update, sample_batch_id, sample_tenant_id, sample_series_id, sample_workflow_id, sample_datetime):
    """Test updating only the annotation, leaving value unchanged."""
    # Insert initial value with known_time
    initial_known_time = sample_datetime
    with psycopg.connect(clean_db_for_update) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO batches_table (batch_id, tenant_id, workflow_id, batch_start_time, known_time) VALUES (%s, %s, %s, %s, %s)",
                (sample_batch_id, sample_tenant_id, sample_workflow_id, sample_datetime, initial_known_time),
            )
            cur.execute(
                "INSERT INTO series_table (series_id, name, unit, labels) VALUES (%s, %s, %s, %s::jsonb)",
                (sample_series_id, "mean", "dimensionless", "{}"),
            )
            cur.execute(
                """
                INSERT INTO values_table (batch_id, tenant_id, series_id, valid_time, value, known_time)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (sample_batch_id, sample_tenant_id, sample_series_id, sample_datetime, 100.0, initial_known_time),
            )

    # Update only annotation via SDK
    os.environ["TIMEDB_DSN"] = clean_db_for_update
    td = TimeDataClient()
    record_update = {
        "batch_id": sample_batch_id,
        "tenant_id": sample_tenant_id,
        "valid_time": sample_datetime,
        "series_id": sample_series_id,
        "annotation": "Updated annotation",
        "changed_by": "test-user",
    }
    result = td.update_records(updates=[record_update])

    assert len(result["updated"]) == 1

    # Verify value unchanged, annotation updated - get latest by known_time DESC
    with psycopg.connect(clean_db_for_update) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT value, annotation
                FROM values_table
                WHERE batch_id = %s AND tenant_id = %s AND valid_time = %s AND series_id = %s
                ORDER BY known_time DESC
                LIMIT 1
                """,
                (sample_batch_id, sample_tenant_id, sample_datetime, sample_series_id)
            )
            row = cur.fetchone()
            assert row[0] == 100.0  # Value unchanged
            assert row[1] == "Updated annotation"


def test_update_record_tags(clean_db_for_update, sample_batch_id, sample_tenant_id, sample_series_id, sample_workflow_id, sample_datetime):
    """Test updating tags."""
    # Insert initial value with known_time
    initial_known_time = sample_datetime
    with psycopg.connect(clean_db_for_update) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO batches_table (batch_id, tenant_id, workflow_id, batch_start_time, known_time) VALUES (%s, %s, %s, %s, %s)",
                (sample_batch_id, sample_tenant_id, sample_workflow_id, sample_datetime, initial_known_time),
            )
            cur.execute(
                "INSERT INTO series_table (series_id, name, unit, labels) VALUES (%s, %s, %s, %s::jsonb)",
                (sample_series_id, "mean", "dimensionless", "{}"),
            )
            cur.execute(
                """
                INSERT INTO values_table (batch_id, tenant_id, series_id, valid_time, value, known_time)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (sample_batch_id, sample_tenant_id, sample_series_id, sample_datetime, 100.0, initial_known_time),
            )

    # Update with tags via SDK
    os.environ["TIMEDB_DSN"] = clean_db_for_update
    td = TimeDataClient()
    record_update = {
        "batch_id": sample_batch_id,
        "tenant_id": sample_tenant_id,
        "valid_time": sample_datetime,
        "series_id": sample_series_id,
        "tags": ["reviewed", "validated"],
        "changed_by": "test-user",
    }
    result = td.update_records(updates=[record_update])

    assert len(result["updated"]) == 1

    # Verify tags were set - get latest by known_time DESC
    with psycopg.connect(clean_db_for_update) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT tags
                FROM values_table
                WHERE batch_id = %s AND tenant_id = %s AND valid_time = %s AND series_id = %s
                ORDER BY known_time DESC
                LIMIT 1
                """,
                (sample_batch_id, sample_tenant_id, sample_datetime, sample_series_id)
            )
            row = cur.fetchone()
            assert row[0] is not None
            assert set(row[0]) == {"reviewed", "validated"}


def test_update_record_clear_tags(clean_db_for_update, sample_batch_id, sample_tenant_id, sample_series_id, sample_workflow_id, sample_datetime):
    """Test clearing tags by setting to empty list."""
    # Insert value with known_time
    initial_known_time = sample_datetime
    with psycopg.connect(clean_db_for_update) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO batches_table (batch_id, tenant_id, workflow_id, batch_start_time, known_time) VALUES (%s, %s, %s, %s, %s)",
                (sample_batch_id, sample_tenant_id, sample_workflow_id, sample_datetime, initial_known_time),
            )
            cur.execute(
                "INSERT INTO series_table (series_id, name, unit, labels) VALUES (%s, %s, %s, %s::jsonb)",
                (sample_series_id, "mean", "dimensionless", "{}"),
            )
            cur.execute(
                """
                INSERT INTO values_table (batch_id, tenant_id, series_id, valid_time, value, known_time)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (sample_batch_id, sample_tenant_id, sample_series_id, sample_datetime, 100.0, initial_known_time),
            )

    # First add tags via SDK
    os.environ["TIMEDB_DSN"] = clean_db_for_update
    td = TimeDataClient()
    td.update_records(updates=[{
        "batch_id": sample_batch_id,
        "tenant_id": sample_tenant_id,
        "valid_time": sample_datetime,
        "series_id": sample_series_id,
        "tags": ["tag1", "tag2"],
    }])

    # Then clear tags via SDK
    os.environ["TIMEDB_DSN"] = clean_db_for_update
    td = TimeDataClient()
    record_update = {
        "batch_id": sample_batch_id,
        "tenant_id": sample_tenant_id,
        "valid_time": sample_datetime,
        "series_id": sample_series_id,
        "tags": [],  # Empty list clears tags
    }
    result = td.update_records(updates=[record_update])

    assert len(result["updated"]) == 1

    # Verify tags are cleared (NULL) - get latest by known_time DESC
    with psycopg.connect(clean_db_for_update) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT tags
                FROM values_table
                WHERE batch_id = %s AND tenant_id = %s AND valid_time = %s AND series_id = %s
                ORDER BY known_time DESC
                LIMIT 1
                """,
                (sample_batch_id, sample_tenant_id, sample_datetime, sample_series_id)
            )
            row = cur.fetchone()
            assert row[0] is None  # Tags cleared


def test_update_no_op_skipped(clean_db_for_update, sample_batch_id, sample_tenant_id, sample_series_id, sample_workflow_id, sample_datetime):
    """Test that no-op updates are skipped."""
    # Insert initial value with known_time
    initial_known_time = sample_datetime
    with psycopg.connect(clean_db_for_update) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO batches_table (batch_id, tenant_id, workflow_id, batch_start_time, known_time) VALUES (%s, %s, %s, %s, %s)",
                (sample_batch_id, sample_tenant_id, sample_workflow_id, sample_datetime, initial_known_time),
            )
            cur.execute(
                "INSERT INTO series_table (series_id, name, unit, labels) VALUES (%s, %s, %s, %s::jsonb)",
                (sample_series_id, "mean", "dimensionless", "{}"),
            )
            cur.execute(
                """
                INSERT INTO values_table (batch_id, tenant_id, series_id, valid_time, value, known_time)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (sample_batch_id, sample_tenant_id, sample_series_id, sample_datetime, 100.0, initial_known_time),
            )

    # Try to update with same value (no-op)
    os.environ["TIMEDB_DSN"] = clean_db_for_update
    td = TimeDataClient()
    record_update = {
        "batch_id": sample_batch_id,
        "tenant_id": sample_tenant_id,
        "valid_time": sample_datetime,
        "series_id": sample_series_id,
        "value": 100.0,  # Same value
    }
    result = td.update_records(updates=[record_update])

    # Should be skipped
    assert len(result["updated"]) == 0
    assert len(result["skipped_no_ops"]) == 1


def test_update_create_new_record(clean_db_for_update, sample_batch_id, sample_tenant_id, sample_series_id, sample_workflow_id, sample_datetime):
    """Test that updating a non-existent record requires explicit value."""
    initial_known_time = sample_datetime
    with psycopg.connect(clean_db_for_update) as conn:
        # Insert batch but no values
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO batches_table (batch_id, tenant_id, workflow_id, batch_start_time, known_time) VALUES (%s, %s, %s, %s, %s)",
                (sample_batch_id, sample_tenant_id, sample_workflow_id, sample_datetime, initial_known_time),
            )
            cur.execute(
                "INSERT INTO series_table (series_id, name, unit, labels) VALUES (%s, %s, %s, %s::jsonb)",
                (sample_series_id, "mean", "dimensionless", "{}"),
            )

        # Try to update non-existent record without value - should fail
        # Attempt to update non-existent record without value - should fail
        os.environ["TIMEDB_DSN"] = clean_db_for_update
        td = TimeDataClient()
        record_update = {
            "batch_id": sample_batch_id,
            "tenant_id": sample_tenant_id,
            "valid_time": sample_datetime,
            "series_id": sample_series_id,
            "annotation": "annotation only",  # No value provided
        }
        with pytest.raises(ValueError, match="No current row exists"):
            td.update_records(updates=[record_update])

        # But should work if value is provided
        os.environ["TIMEDB_DSN"] = clean_db_for_update
        td = TimeDataClient()
        record_update = {
            "batch_id": sample_batch_id,
            "tenant_id": sample_tenant_id,
            "valid_time": sample_datetime,
            "series_id": sample_series_id,
            "value": 100.0,  # Value provided
            "annotation": "annotation",
        }
        result = td.update_records(updates=[record_update])
        assert len(result["updated"]) == 1
