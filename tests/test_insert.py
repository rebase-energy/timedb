"""Tests for inserting flat and overlapping data."""
import os
import pytest
import psycopg
from datetime import datetime, timezone, timedelta
import pandas as pd
from timedb import TimeDataClient


# =============================================================================
# Flat insertion tests
# =============================================================================

def test_insert_flat_no_batch(clean_db, sample_datetime):
    """Test inserting flat via SDK creates no batch and rows in the flat table."""
    os.environ["TIMEDB_DSN"] = clean_db
    td = TimeDataClient()

    td.create_series(name="temperature", unit="dimensionless", overlapping=False)

    df = pd.DataFrame({
        "valid_time": [sample_datetime, sample_datetime + timedelta(hours=1)],
        "temperature": [20.5, 21.0],
    })

    result = td.series("temperature").insert(df=df)

    assert result.batch_id is None
    assert "temperature" in result.series_ids

    # Verify rows in flat table
    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM flat")
            assert cur.fetchone()[0] == 2

            # Verify no batch was created
            cur.execute("SELECT COUNT(*) FROM batches_table")
            assert cur.fetchone()[0] == 0

            # Verify no rows in any overlapping table
            cur.execute("SELECT COUNT(*) FROM overlapping_medium")
            assert cur.fetchone()[0] == 0


def test_insert_flat_with_known_time(clean_db, sample_datetime):
    """Test inserting flat with explicit known_time still skips batch."""
    known_time = sample_datetime - timedelta(hours=1)

    os.environ["TIMEDB_DSN"] = clean_db
    td = TimeDataClient()

    td.create_series(name="temperature", unit="dimensionless", overlapping=False)

    df = pd.DataFrame({
        "valid_time": [sample_datetime],
        "temperature": [20.5],
    })

    result = td.series("temperature").insert(
        df=df,
        batch_start_time=sample_datetime,
        known_time=known_time,
    )

    # Flat inserts should not create a batch even with known_time
    assert result.batch_id is None

    # Verify data was inserted
    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM flat")
            assert cur.fetchone()[0] == 1

            cur.execute("SELECT COUNT(*) FROM batches_table")
            assert cur.fetchone()[0] == 0


def test_insert_flat_point_in_time(clean_db, sample_datetime):
    """Test inserting multiple point-in-time flat data."""
    os.environ["TIMEDB_DSN"] = clean_db
    td = TimeDataClient()

    td.create_series(name="power", unit="dimensionless", overlapping=False)

    df = pd.DataFrame({
        "valid_time": [
            sample_datetime,
            sample_datetime + timedelta(hours=1),
            sample_datetime + timedelta(hours=2),
        ],
        "power": [100.5, 101.0, 102.5],
    })

    result = td.series("power").insert(df=df)

    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM flat")
            assert cur.fetchone()[0] == 3


def test_insert_flat_interval(clean_db, sample_datetime):
    """Test inserting interval flat data with valid_time_end."""
    os.environ["TIMEDB_DSN"] = clean_db
    td = TimeDataClient()

    td.create_series(name="energy", unit="dimensionless", overlapping=False)

    df = pd.DataFrame({
        "valid_time": [sample_datetime],
        "valid_time_end": [sample_datetime + timedelta(hours=1)],
        "energy": [500.0],
    })

    result = td.series("energy").insert(df=df, valid_time_end_col="valid_time_end")

    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT valid_time_end FROM flat")
            row = cur.fetchone()
            assert row is not None
            assert row[0] is not None


def test_insert_flat_upsert(clean_db, sample_datetime):
    """Test that inserting the same flat data twice upserts (updates value)."""
    os.environ["TIMEDB_DSN"] = clean_db
    td = TimeDataClient()

    td.create_series(name="meter", unit="dimensionless", overlapping=False)

    # First insert
    df1 = pd.DataFrame({
        "valid_time": [sample_datetime],
        "meter": [100.0],
    })
    td.series("meter").insert(df=df1)

    # Second insert with different value for same valid_time
    df2 = pd.DataFrame({
        "valid_time": [sample_datetime],
        "meter": [150.0],
    })
    td.series("meter").insert(df=df2)

    # Should still have only 1 row (upsert), with updated value
    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM flat")
            assert cur.fetchone()[0] == 1

            cur.execute("SELECT value FROM flat")
            assert cur.fetchone()[0] == 150.0


# =============================================================================
# Overlapping insertion tests
# =============================================================================

def test_insert_overlapping_creates_batch(clean_db, sample_datetime):
    """Test inserting overlapping via SDK creates rows in overlapping_medium table."""
    os.environ["TIMEDB_DSN"] = clean_db
    td = TimeDataClient()

    td.create_series(
        name="wind_forecast", unit="dimensionless",
        overlapping=True, retention="medium",
    )

    df = pd.DataFrame({
        "valid_time": [sample_datetime, sample_datetime + timedelta(hours=1)],
        "wind_forecast": [50.0, 55.0],
    })

    result = td.series("wind_forecast").insert(df=df, known_time=sample_datetime)

    assert result.batch_id is not None
    assert "wind_forecast" in result.series_ids

    # Verify rows in overlapping_medium
    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM overlapping_medium WHERE batch_id = %s",
                (result.batch_id,)
            )
            assert cur.fetchone()[0] == 2

            # Verify no rows in flat
            cur.execute("SELECT COUNT(*) FROM flat")
            assert cur.fetchone()[0] == 0


def test_insert_overlapping_short_tier(clean_db):
    """Test inserting overlapping with retention='short'."""
    # Use a recent datetime to avoid the 6-month retention policy on overlapping_short
    recent_time = datetime.now(timezone.utc).replace(microsecond=0)

    os.environ["TIMEDB_DSN"] = clean_db
    td = TimeDataClient()

    td.create_series(
        name="price_forecast", unit="dimensionless",
        overlapping=True, retention="short",
    )

    df = pd.DataFrame({
        "valid_time": [recent_time],
        "price_forecast": [42.0],
    })

    td.series("price_forecast").insert(df=df, known_time=recent_time)

    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM overlapping_short")
            assert cur.fetchone()[0] == 1

            cur.execute("SELECT COUNT(*) FROM overlapping_medium")
            assert cur.fetchone()[0] == 0

            cur.execute("SELECT COUNT(*) FROM overlapping_long")
            assert cur.fetchone()[0] == 0


def test_insert_overlapping_long_tier(clean_db, sample_datetime):
    """Test inserting overlapping with retention='long'."""
    os.environ["TIMEDB_DSN"] = clean_db
    td = TimeDataClient()

    td.create_series(
        name="climate_forecast", unit="dimensionless",
        overlapping=True, retention="long",
    )

    df = pd.DataFrame({
        "valid_time": [sample_datetime],
        "climate_forecast": [15.0],
    })

    td.series("climate_forecast").insert(df=df, known_time=sample_datetime)

    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM overlapping_long")
            assert cur.fetchone()[0] == 1


def test_insert_overlapping_interval(clean_db, sample_datetime):
    """Test inserting interval overlapping with valid_time_end."""
    os.environ["TIMEDB_DSN"] = clean_db
    td = TimeDataClient()

    td.create_series(
        name="energy_forecast", unit="dimensionless",
        overlapping=True, retention="medium",
    )

    df = pd.DataFrame({
        "valid_time": [sample_datetime],
        "valid_time_end": [sample_datetime + timedelta(hours=1)],
        "energy_forecast": [500.0],
    })

    td.series("energy_forecast").insert(
        df=df, valid_time_end_col="valid_time_end", known_time=sample_datetime,
    )

    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT valid_time_end FROM overlapping_medium")
            row = cur.fetchone()
            assert row is not None
            assert row[0] is not None


# =============================================================================
# Timezone validation
# =============================================================================

def test_insert_timezone_aware_required(clean_db):
    """Test that timezone-aware datetimes are required."""
    os.environ["TIMEDB_DSN"] = clean_db
    td = TimeDataClient()

    td.create_series(name="temp", unit="dimensionless", overlapping=False)

    df = pd.DataFrame({
        "valid_time": [datetime(2025, 1, 1, 12, 0)],  # naive datetime
        "temp": [20.0],
    })

    with pytest.raises(ValueError, match="timezone-aware"):
        td.series("temp").insert(df=df)
