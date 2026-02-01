"""Tests for inserting runs and values."""
import os
import pytest
import psycopg
import uuid
from datetime import datetime, timezone, timedelta
import pandas as pd
from timedb import TimeDataClient
from timedb.db import read


def test_insert_batch_creates_batch(clean_db, sample_tenant_id, sample_series_id, sample_workflow_id, sample_datetime):
    """Test inserting a small batch via TimeDataClient (single-series DataFrame)."""
    # Use TimeDataClient to insert a DataFrame-based batch
    os.environ["TIMEDB_DSN"] = clean_db
    td = TimeDataClient()

    # Create series first
    td.create_series(name="mean", unit="dimensionless")

    df = pd.DataFrame({
        "valid_time": [sample_datetime, sample_datetime + timedelta(hours=1)],
        "mean": [100.5, 101.0],
    })

    result = td.series("mean").insert_batch(df=df)

    # Verify batch was created
    assert result.batch_id is not None
    assert "mean" in result.series_ids

    # Verify values exist in values_table for the returned batch
    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM values_table WHERE batch_id = %s",
                (result.batch_id,)
            )
            assert cur.fetchone()[0] == 2


def test_insert_batch_with_known_time(clean_db, sample_tenant_id, sample_workflow_id, sample_datetime):
    """Test inserting a batch with explicit known_time via TimeDataClient."""
    known_time = sample_datetime - timedelta(hours=1)

    os.environ["TIMEDB_DSN"] = clean_db
    td = TimeDataClient()

    # Create series first
    td.create_series(name="mean", unit="dimensionless")

    df = pd.DataFrame({
        "valid_time": [sample_datetime],
        "mean": [100.5],
    })

    result = td.series("mean").insert_batch(df=df, batch_start_time=sample_datetime, batch_finish_time=None, known_time=known_time)

    # Verify batch exists and has known_time stored in batch_params (if stored)
    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT batch_start_time FROM batches_table WHERE batch_id = %s",
                (result.batch_id,)
            )
            row = cur.fetchone()
            assert row is not None
            stored_time = row[0]
            assert abs((stored_time - sample_datetime).total_seconds()) < 1


def test_insert_values_point_in_time(clean_db, sample_tenant_id, sample_series_id, sample_workflow_id, sample_datetime):
    """Test inserting point-in-time values via TimeDataClient (DataFrame -> insert_batch)."""
    os.environ["TIMEDB_DSN"] = clean_db
    td = TimeDataClient()

    # Create series first
    td.create_series(name="mean", unit="dimensionless")

    df = pd.DataFrame({
        "valid_time": [sample_datetime, sample_datetime + timedelta(hours=1), sample_datetime + timedelta(hours=2)],
        "mean": [100.5, 101.0, 102.5],
    })

    result = td.series("mean").insert_batch(df=df)

    # Verify values were inserted in values_table for this batch
    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM values_table WHERE batch_id = %s",
                (result.batch_id,)
            )
            assert cur.fetchone()[0] == 3


def test_insert_values_interval(clean_db, sample_tenant_id, sample_series_id, sample_workflow_id, sample_datetime):
    """Test inserting interval values via TimeDataClient (DataFrame with valid_time_end)."""
    os.environ["TIMEDB_DSN"] = clean_db
    td = TimeDataClient()

    # Create series first
    td.create_series(name="mean", unit="dimensionless")

    df = pd.DataFrame({
        "valid_time": [sample_datetime],
        "valid_time_end": [sample_datetime + timedelta(hours=1)],
        "mean": [100.5],
    })

    result = td.series("mean").insert_batch(df=df, valid_time_end_col='valid_time_end')

    # Verify interval value was inserted
    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT valid_time_end FROM values_table WHERE batch_id = %s",
                (result.batch_id,)
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] is not None


def test_insert_batch_with_values(clean_db, sample_tenant_id, sample_series_id, sample_workflow_id, sample_datetime):
    """Test inserting a batch (run+values equivalent) via TimeDataClient convenience flow."""
    os.environ["TIMEDB_DSN"] = clean_db
    td = TimeDataClient()

    # Create series first
    td.create_series(name="mean", unit="dimensionless")

    df = pd.DataFrame({
        "valid_time": [sample_datetime, sample_datetime + timedelta(hours=1)],
        "mean": [100.5, 101.0],
    })

    result = td.series("mean").insert_batch(df=df)

    # Verify batch and values exist
    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM batches_table WHERE batch_id = %s", (result.batch_id,))
            assert cur.fetchone()[0] == 1

            cur.execute("SELECT COUNT(*) FROM values_table WHERE batch_id = %s", (result.batch_id,))
            assert cur.fetchone()[0] == 2


def test_insert_values_timezone_aware(clean_db, sample_tenant_id, sample_series_id, sample_workflow_id):
    """Test that timezone-aware datetimes are required for insert_batch."""
    os.environ["TIMEDB_DSN"] = clean_db
    td = TimeDataClient()

    # Create series first
    td.create_series(name="mean", unit="dimensionless")

    # Try to insert with timezone-naive datetime - should raise ValueError
    df = pd.DataFrame({
        "valid_time": [datetime(2025, 1, 1, 12, 0)],  # naive datetime
        "mean": [100.5],
    })

    with pytest.raises(ValueError, match="timezone-aware"):
        td.series("mean").insert_batch(df=df)

