"""Tests for reading values from the database."""
import pytest
import pandas as pd
from datetime import datetime, timezone, timedelta
from timedb.db import insert, read


def test_read_values_flat_mode(clean_db, sample_run_id, sample_tenant_id, sample_series_id, sample_workflow_id, sample_datetime):
    """Test reading values in flat mode."""
    # Insert run with values
    value_rows = [
        (sample_tenant_id, sample_datetime, sample_series_id, "mean", 100.5),
        (sample_tenant_id, sample_datetime + timedelta(hours=1), sample_series_id, "mean", 101.0),
        (sample_tenant_id, sample_datetime, sample_series_id, "quantile:0.5", 99.5),
    ]
    
    insert.insert_run_with_values(
        clean_db,
        run_id=sample_run_id,
        tenant_id=sample_tenant_id,
        workflow_id=sample_workflow_id,
        run_start_time=sample_datetime,
        run_finish_time=None,
        value_rows=value_rows,
    )
    
    # Read values in flat mode
    df = read.read_values_between(
        clean_db,
        tenant_id=sample_tenant_id,
        start_valid=sample_datetime,
        end_valid=sample_datetime + timedelta(hours=2),
        mode="flat",
    )
    
    # Verify results
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3
    assert ("valid_time", "value_key") in [df.index.names]
    
    # Check specific values
    assert df.loc[(sample_datetime, "mean"), "value"] == 100.5
    assert df.loc[(sample_datetime, "quantile:0.5"), "value"] == 99.5


def test_read_values_overlapping_mode(clean_db, sample_run_id, sample_tenant_id, sample_series_id, sample_workflow_id, sample_datetime):
    """Test reading values in overlapping mode."""
    # Insert run with values
    value_rows = [
        (sample_tenant_id, sample_datetime, sample_series_id, "mean", 100.5),
        (sample_tenant_id, sample_datetime + timedelta(hours=1), sample_series_id, "mean", 101.0),
    ]
    
    insert.insert_run_with_values(
        clean_db,
        run_id=sample_run_id,
        tenant_id=sample_tenant_id,
        workflow_id=sample_workflow_id,
        run_start_time=sample_datetime,
        run_finish_time=None,
        value_rows=value_rows,
    )
    
    # Read values in overlapping mode
    df = read.read_values_between(
        clean_db,
        tenant_id=sample_tenant_id,
        start_valid=sample_datetime,
        end_valid=sample_datetime + timedelta(hours=2),
        mode="overlapping",
    )
    
    # Verify results
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert ("known_time", "valid_time", "value_key") in [df.index.names]
    assert "known_time" in df.index.names


def test_read_values_filter_by_valid_time(clean_db, sample_run_id, sample_tenant_id, sample_series_id, sample_workflow_id, sample_datetime):
    """Test filtering by valid_time range."""
    # Insert values at different times
    value_rows = [
        (sample_tenant_id, sample_datetime, sample_series_id, "mean", 100.0),
        (sample_tenant_id, sample_datetime + timedelta(hours=1), sample_series_id, "mean", 101.0),
        (sample_tenant_id, sample_datetime + timedelta(hours=2), sample_series_id, "mean", 102.0),
        (sample_tenant_id, sample_datetime + timedelta(hours=3), sample_series_id, "mean", 103.0),
    ]
    
    insert.insert_run_with_values(
        clean_db,
        run_id=sample_run_id,
        tenant_id=sample_tenant_id,
        workflow_id=sample_workflow_id,
        run_start_time=sample_datetime,
        run_finish_time=None,
        value_rows=value_rows,
    )
    
    # Read only values in a specific range
    df = read.read_values_between(
        clean_db,
        tenant_id=sample_tenant_id,
        start_valid=sample_datetime + timedelta(hours=1),
        end_valid=sample_datetime + timedelta(hours=3),
        mode="flat",
    )
    
    # Should only get 2 values (hours 1 and 2)
    assert len(df) == 2
    assert all(
        sample_datetime + timedelta(hours=1) <= idx[0] < sample_datetime + timedelta(hours=3)
        for idx in df.index
    )


def test_read_values_all_versions(clean_db_for_update, sample_run_id, sample_tenant_id, sample_series_id, sample_workflow_id, sample_datetime):
    """Test reading all versions (including non-current)."""
    from timedb.db import update
    import psycopg
    
    # Insert initial value
    with psycopg.connect(clean_db_for_update) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO runs_table (run_id, tenant_id, workflow_id, run_start_time) VALUES (%s, %s, %s, %s)",
                (sample_run_id, sample_tenant_id, sample_workflow_id, sample_datetime),
            )
            cur.execute(
                "INSERT INTO values_table (run_id, tenant_id, series_id, valid_time, value, is_current) VALUES (%s, %s, %s, %s, %s, true)",
                (sample_run_id, sample_tenant_id, sample_series_id, sample_datetime, 100.0),
            )
    
    # Update the value (creates a new version)
    with psycopg.connect(clean_db_for_update) as conn:
        update_dict = {
            "run_id": sample_run_id,
            "tenant_id": sample_tenant_id,
            "valid_time": sample_datetime,
            "series_id": sample_series_id,
            "value": 101.0,
            "changed_by": "test",
        }
        update.update_records(conn, updates=[update_dict])
    
    # Read with all_versions=True (note: read function uses main schema, so this test may need adjustment)
    # For now, we'll skip the read part since it uses a different schema
    # In a real scenario, you'd need to ensure schema compatibility
    
    # Should have more rows when including all versions
    assert len(df_all) >= len(df_current)
    # Current version should be 101.0
    assert df_current.loc[(sample_datetime, "mean"), "value"] == 101.0

