"""Tests for updating records."""
import pytest
import psycopg
from datetime import datetime, timezone
from timedb.db import update


def test_update_record_value(clean_db_for_update, sample_batch_id, sample_tenant_id, sample_series_id, sample_workflow_id, sample_datetime):
    """Test updating a record's value."""
    # Insert initial value
    with psycopg.connect(clean_db_for_update) as conn:
        with conn.cursor() as cur:
            # Insert run
            cur.execute(
                "INSERT INTO batches_table (batch_id, tenant_id, workflow_id, batch_start_time) VALUES (%s, %s, %s, %s)",
                (sample_batch_id, sample_tenant_id, sample_workflow_id, sample_datetime),
            )
            # Insert value
            cur.execute(
                """
                INSERT INTO values_table (batch_id, tenant_id, series_id, valid_time, value, is_current)
                VALUES (%s, %s, %s, %s, %s, true)
                """,
                (sample_batch_id, sample_tenant_id, sample_series_id, sample_datetime, 100.0),
            )
    
    # Update the value
    with psycopg.connect(clean_db_for_update) as conn:
        record_update = {
            "batch_id": sample_batch_id,
            "tenant_id": sample_tenant_id,
            "valid_time": sample_datetime,
            "series_id": sample_series_id,
            "value": 150.0,
            "changed_by": "test-user",
        }
        result = update.update_records(conn, updates=[record_update])
        
        assert len(result["updated"]) == 1
        assert len(result["skipped_no_ops"]) == 0
    
    # Verify the update
    with psycopg.connect(clean_db_for_update) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT value, is_current, changed_by 
                FROM values_table 
                WHERE batch_id = %s AND tenant_id = %s AND valid_time = %s AND series_id = %s AND is_current = true
                """,
                (sample_batch_id, sample_tenant_id, sample_datetime, sample_series_id)
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] == 150.0
            assert row[1] is True
            assert row[2] == "test-user"


def test_update_record_annotation_only(clean_db_for_update, sample_batch_id, sample_tenant_id, sample_series_id, sample_workflow_id, sample_datetime):
    """Test updating only the annotation, leaving value unchanged."""
    # Insert initial value
    with psycopg.connect(clean_db_for_update) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO batches_table (batch_id, tenant_id, workflow_id, batch_start_time) VALUES (%s, %s, %s, %s)",
                (sample_batch_id, sample_tenant_id, sample_workflow_id, sample_datetime),
            )
            cur.execute(
                """
                INSERT INTO values_table (batch_id, tenant_id, series_id, valid_time, value, is_current)
                VALUES (%s, %s, %s, %s, %s, true)
                """,
                (sample_batch_id, sample_tenant_id, sample_series_id, sample_datetime, 100.0),
            )
    
    # Update only annotation
    with psycopg.connect(clean_db_for_update) as conn:
        record_update = {
            "batch_id": sample_batch_id,
            "tenant_id": sample_tenant_id,
            "valid_time": sample_datetime,
            "series_id": sample_series_id,
            "annotation": "Updated annotation",
            "changed_by": "test-user",
        }
        result = update.update_records(conn, updates=[record_update])
        
        assert len(result["updated"]) == 1
    
    # Verify value unchanged, annotation updated
    with psycopg.connect(clean_db_for_update) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT value, annotation 
                FROM values_table 
                WHERE batch_id = %s AND tenant_id = %s AND valid_time = %s AND series_id = %s AND is_current = true
                """,
                (sample_batch_id, sample_tenant_id, sample_datetime, sample_series_id)
            )
            row = cur.fetchone()
            assert row[0] == 100.0  # Value unchanged
            assert row[1] == "Updated annotation"


def test_update_record_tags(clean_db_for_update, sample_batch_id, sample_tenant_id, sample_series_id, sample_workflow_id, sample_datetime):
    """Test updating tags."""
    # Insert initial value
    with psycopg.connect(clean_db_for_update) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO batches_table (batch_id, tenant_id, workflow_id, batch_start_time) VALUES (%s, %s, %s, %s)",
                (sample_batch_id, sample_tenant_id, sample_workflow_id, sample_datetime),
            )
            cur.execute(
                """
                INSERT INTO values_table (batch_id, tenant_id, series_id, valid_time, value, is_current)
                VALUES (%s, %s, %s, %s, %s, true)
                """,
                (sample_batch_id, sample_tenant_id, sample_series_id, sample_datetime, 100.0),
            )
    
    # Update with tags
    with psycopg.connect(clean_db_for_update) as conn:
        record_update = {
            "batch_id": sample_batch_id,
            "tenant_id": sample_tenant_id,
            "valid_time": sample_datetime,
            "series_id": sample_series_id,
            "tags": ["reviewed", "validated"],
            "changed_by": "test-user",
        }
        result = update.update_records(conn, updates=[record_update])
        
        assert len(result["updated"]) == 1
    
    # Verify tags were set
    with psycopg.connect(clean_db_for_update) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT tags 
                FROM values_table 
                WHERE batch_id = %s AND tenant_id = %s AND valid_time = %s AND series_id = %s AND is_current = true
                """,
                (sample_batch_id, sample_tenant_id, sample_datetime, sample_series_id)
            )
            row = cur.fetchone()
            assert row[0] is not None
            assert set(row[0]) == {"reviewed", "validated"}


def test_update_record_clear_tags(clean_db_for_update, sample_batch_id, sample_tenant_id, sample_series_id, sample_workflow_id, sample_datetime):
    """Test clearing tags by setting to empty list."""
    # Insert value with tags
    with psycopg.connect(clean_db_for_update) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO batches_table (batch_id, tenant_id, workflow_id, batch_start_time) VALUES (%s, %s, %s, %s)",
                (sample_batch_id, sample_tenant_id, sample_workflow_id, sample_datetime),
            )
            cur.execute(
                """
                INSERT INTO values_table (batch_id, tenant_id, series_id, valid_time, value, is_current)
                VALUES (%s, %s, %s, %s, %s, true)
                """,
                (sample_batch_id, sample_tenant_id, sample_series_id, sample_datetime, 100.0),
            )
    
    # First add tags
    with psycopg.connect(clean_db_for_update) as conn:
        update.update_records(conn, updates=[{
            "batch_id": sample_batch_id,
            "tenant_id": sample_tenant_id,
            "valid_time": sample_datetime,
            "series_id": sample_series_id,
            "tags": ["tag1", "tag2"],
        }])
    
    # Then clear tags
    with psycopg.connect(clean_db_for_update) as conn:
        record_update = {
            "batch_id": sample_batch_id,
            "tenant_id": sample_tenant_id,
            "valid_time": sample_datetime,
            "series_id": sample_series_id,
            "tags": [],  # Empty list clears tags
        }
        result = update.update_records(conn, updates=[record_update])
        
        assert len(result["updated"]) == 1
    
    # Verify tags are cleared (NULL)
    with psycopg.connect(clean_db_for_update) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT tags 
                FROM values_table 
                WHERE batch_id = %s AND tenant_id = %s AND valid_time = %s AND series_id = %s AND is_current = true
                """,
                (sample_batch_id, sample_tenant_id, sample_datetime, sample_series_id)
            )
            row = cur.fetchone()
            assert row[0] is None  # Tags cleared


def test_update_no_op_skipped(clean_db_for_update, sample_batch_id, sample_tenant_id, sample_series_id, sample_workflow_id, sample_datetime):
    """Test that no-op updates are skipped."""
    # Insert initial value
    with psycopg.connect(clean_db_for_update) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO batches_table (batch_id, tenant_id, workflow_id, batch_start_time) VALUES (%s, %s, %s, %s)",
                (sample_batch_id, sample_tenant_id, sample_workflow_id, sample_datetime),
            )
            cur.execute(
                """
                INSERT INTO values_table (batch_id, tenant_id, series_id, valid_time, value, is_current)
                VALUES (%s, %s, %s, %s, %s, true)
                """,
                (sample_batch_id, sample_tenant_id, sample_series_id, sample_datetime, 100.0),
            )
    
    # Try to update with same value (no-op)
    with psycopg.connect(clean_db_for_update) as conn:
        record_update = {
            "batch_id": sample_batch_id,
            "tenant_id": sample_tenant_id,
            "valid_time": sample_datetime,
            "series_id": sample_series_id,
            "value": 100.0,  # Same value
        }
        result = update.update_records(conn, updates=[record_update])
        
        # Should be skipped
        assert len(result["updated"]) == 0
        assert len(result["skipped_no_ops"]) == 1


def test_update_create_new_record(clean_db_for_update, sample_batch_id, sample_tenant_id, sample_series_id, sample_workflow_id, sample_datetime):
    """Test that updating a non-existent record requires explicit value."""
    with psycopg.connect(clean_db_for_update) as conn:
        # Insert run but no values
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO batches_table (batch_id, tenant_id, workflow_id, batch_start_time) VALUES (%s, %s, %s, %s)",
                (sample_batch_id, sample_tenant_id, sample_workflow_id, sample_datetime),
            )
        
        # Try to update non-existent record without value - should fail
        with pytest.raises(ValueError, match="No current row exists"):
            record_update = {
                "batch_id": sample_batch_id,
                "tenant_id": sample_tenant_id,
                "valid_time": sample_datetime,
                "series_id": sample_series_id,
                "annotation": "annotation only",  # No value provided
            }
            update.update_records(conn, updates=[record_update])
        
        # But should work if value is provided
        record_update = {
            "batch_id": sample_batch_id,
            "tenant_id": sample_tenant_id,
            "valid_time": sample_datetime,
            "series_id": sample_series_id,
            "value": 100.0,  # Value provided
            "annotation": "annotation",
        }
        result = update.update_records(conn, updates=[record_update])
        assert len(result["updated"]) == 1
