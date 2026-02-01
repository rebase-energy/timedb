"""Tests for reading values from the database."""
import os
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from timedb import TimeDataClient
from timedb.db import read


def test_read_values_flat_mode(clean_db, sample_batch_id, sample_tenant_id, sample_series_id, sample_workflow_id, sample_datetime):
    """Test reading values in flat mode."""
    # Insert values using TimeDataClient by creating a DataFrame with multiple columns (mean, quantile:0.5)
    os.environ["TIMEDB_DSN"] = clean_db
    td = TimeDataClient()

    # Create series first
    td.create_series(name="mean", unit="dimensionless")
    td.create_series(name="quantile:0.5", unit="dimensionless")

    df_mean = pd.DataFrame({
        "valid_time": [sample_datetime, sample_datetime + timedelta(hours=1)],
        "mean": [100.5, 101.0],
    })
    td.series("mean").insert_batch(df=df_mean)

    df_quantile = pd.DataFrame({
        "valid_time": [sample_datetime],
        "quantile:0.5": [99.5],
    })
    td.series("quantile:0.5").insert_batch(df=df_quantile)

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
    assert list(df.index.names) == ["valid_time", "series_id"]

    # Check specific values by name
    df_reset = df.reset_index()
    row_mean = df_reset[(df_reset["valid_time"] == sample_datetime) & (df_reset["name"] == "mean")]
    assert not row_mean.empty
    assert row_mean.iloc[0]["value"] == 100.5
    row_q = df_reset[(df_reset["valid_time"] == sample_datetime) & (df_reset["name"] == "quantile:0.5")]
    assert not row_q.empty
    assert row_q.iloc[0]["value"] == 99.5


def test_read_values_overlapping_mode(clean_db, sample_batch_id, sample_tenant_id, sample_series_id, sample_workflow_id, sample_datetime):
    """Test reading values in overlapping mode."""
    # Insert values via TimeDataClient
    os.environ["TIMEDB_DSN"] = clean_db
    td = TimeDataClient()

    # Create series first
    td.create_series(name="mean", unit="dimensionless")

    df = pd.DataFrame({
        "valid_time": [sample_datetime, sample_datetime + timedelta(hours=1)],
        "mean": [100.5, 101.0],
    })

    td.series("mean").insert_batch(df=df)

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
    assert list(df.index.names) == ["known_time", "valid_time", "series_id"]
    assert "known_time" in df.index.names


def test_read_values_filter_by_valid_time(clean_db, sample_batch_id, sample_tenant_id, sample_series_id, sample_workflow_id, sample_datetime):
    """Test filtering by valid_time range."""
    # Insert values at different times via TimeDataClient
    os.environ["TIMEDB_DSN"] = clean_db
    td = TimeDataClient()

    # Create series first
    td.create_series(name="mean", unit="dimensionless")

    df = pd.DataFrame({
        "valid_time": [
            sample_datetime,
            sample_datetime + timedelta(hours=1),
            sample_datetime + timedelta(hours=2),
            sample_datetime + timedelta(hours=3),
        ],
        "mean": [100.0, 101.0, 102.0, 103.0],
    })

    td.series("mean").insert_batch(df=df)

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


def test_read_values_multiple_revisions(clean_db_for_update, sample_batch_id, sample_tenant_id, sample_series_id, sample_workflow_id, sample_datetime):
    """Test reading multiple forecast revisions (overlapping batches)."""
    from timedb.db import update
    import psycopg

    # Insert initial batch with known_time
    initial_known_time = sample_datetime
    with psycopg.connect(clean_db_for_update) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO batches_table (batch_id, tenant_id, workflow_id, batch_start_time, known_time) VALUES (%s, %s, %s, %s, %s)",
                (sample_batch_id, sample_tenant_id, sample_workflow_id, sample_datetime, initial_known_time),
            )
            # Ensure the series metadata exists so joins work
            cur.execute(
                "INSERT INTO series_table (series_id, name, unit, labels) VALUES (%s, %s, %s, %s::jsonb)",
                (sample_series_id, "mean", "dimensionless", "{}"),
            )
            cur.execute(
                "INSERT INTO values_table (batch_id, tenant_id, series_id, valid_time, value, known_time) VALUES (%s, %s, %s, %s, %s, %s)",
                (sample_batch_id, sample_tenant_id, sample_series_id, sample_datetime, 100.0, initial_known_time),
            )

    # Update the value (creates a new version with new known_time) via SDK
    os.environ["TIMEDB_DSN"] = clean_db_for_update
    update_dict = {
        "batch_id": sample_batch_id,
        "tenant_id": sample_tenant_id,
        "valid_time": sample_datetime,
        "series_id": sample_series_id,
        "value": 101.0,
        "changed_by": "test",
    }
    import timedb as td
    td.update_records(updates=[update_dict])

    # Read all revisions using overlapping mode
    df_all = read.read_values_overlapping(
        clean_db_for_update,
        tenant_id=sample_tenant_id,
        start_valid=sample_datetime,
        end_valid=sample_datetime + timedelta(hours=1),
    )

    # Read latest values using flat mode
    df_latest = read.read_values_flat(
        clean_db_for_update,
        tenant_id=sample_tenant_id,
        start_valid=sample_datetime,
        end_valid=sample_datetime + timedelta(hours=1),
    )

    # Should have more rows in overlapping mode (all revisions)
    assert len(df_all) >= len(df_latest)

    # Latest value should be 101.0 (find by series name)
    df_latest_reset = df_latest.reset_index()
    row = df_latest_reset[(df_latest_reset["valid_time"] == sample_datetime) & (df_latest_reset["name"] == "mean")]
    assert not row.empty
    assert row.iloc[0]["value"] == 101.0
