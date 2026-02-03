"""Tests for inserting actuals and projections."""
import os
import pytest
import psycopg
import uuid
from datetime import datetime, timezone, timedelta
import pandas as pd
from timedb import TimeDataClient


# =============================================================================
# Actuals insertion tests
# =============================================================================

def test_insert_actuals_creates_batch(clean_db, sample_datetime):
    """Test inserting actuals via SDK creates a batch and rows in the actuals table."""
    os.environ["TIMEDB_DSN"] = clean_db
    td = TimeDataClient()

    td.create_series(name="temperature", unit="dimensionless", data_class="actual")

    df = pd.DataFrame({
        "valid_time": [sample_datetime, sample_datetime + timedelta(hours=1)],
        "temperature": [20.5, 21.0],
    })

    result = td.series("temperature").insert(df=df)

    assert result.batch_id is not None
    assert "temperature" in result.series_ids

    # Verify rows in actuals table
    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM actuals")
            assert cur.fetchone()[0] == 2

            # Verify no rows in any projections table
            cur.execute("SELECT COUNT(*) FROM projections_medium")
            assert cur.fetchone()[0] == 0


def test_insert_actuals_with_known_time(clean_db, sample_datetime):
    """Test inserting actuals with explicit known_time."""
    known_time = sample_datetime - timedelta(hours=1)

    os.environ["TIMEDB_DSN"] = clean_db
    td = TimeDataClient()

    td.create_series(name="temperature", unit="dimensionless", data_class="actual")

    df = pd.DataFrame({
        "valid_time": [sample_datetime],
        "temperature": [20.5],
    })

    result = td.series("temperature").insert(
        df=df,
        batch_start_time=sample_datetime,
        known_time=known_time,
    )

    # Verify batch exists with correct batch_start_time
    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT batch_start_time FROM batches_table WHERE batch_id = %s",
                (result.batch_id,)
            )
            row = cur.fetchone()
            assert row is not None
            assert abs((row[0] - sample_datetime).total_seconds()) < 1


def test_insert_actuals_point_in_time(clean_db, sample_datetime):
    """Test inserting multiple point-in-time actuals."""
    os.environ["TIMEDB_DSN"] = clean_db
    td = TimeDataClient()

    td.create_series(name="power", unit="dimensionless", data_class="actual")

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
            cur.execute("SELECT COUNT(*) FROM actuals")
            assert cur.fetchone()[0] == 3


def test_insert_actuals_interval(clean_db, sample_datetime):
    """Test inserting interval actuals with valid_time_end."""
    os.environ["TIMEDB_DSN"] = clean_db
    td = TimeDataClient()

    td.create_series(name="energy", unit="dimensionless", data_class="actual")

    df = pd.DataFrame({
        "valid_time": [sample_datetime],
        "valid_time_end": [sample_datetime + timedelta(hours=1)],
        "energy": [500.0],
    })

    result = td.series("energy").insert(df=df, valid_time_end_col="valid_time_end")

    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT valid_time_end FROM actuals")
            row = cur.fetchone()
            assert row is not None
            assert row[0] is not None


def test_insert_actuals_upsert(clean_db, sample_datetime):
    """Test that inserting the same actual twice upserts (updates value)."""
    os.environ["TIMEDB_DSN"] = clean_db
    td = TimeDataClient()

    td.create_series(name="meter", unit="dimensionless", data_class="actual")

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
            cur.execute("SELECT COUNT(*) FROM actuals")
            assert cur.fetchone()[0] == 1

            cur.execute("SELECT value FROM actuals")
            assert cur.fetchone()[0] == 150.0


# =============================================================================
# Projections insertion tests
# =============================================================================

def test_insert_projections_creates_batch(clean_db, sample_datetime):
    """Test inserting projections via SDK creates rows in projections_medium table."""
    os.environ["TIMEDB_DSN"] = clean_db
    td = TimeDataClient()

    td.create_series(
        name="wind_forecast", unit="dimensionless",
        data_class="projection", storage_tier="medium",
    )

    df = pd.DataFrame({
        "valid_time": [sample_datetime, sample_datetime + timedelta(hours=1)],
        "wind_forecast": [50.0, 55.0],
    })

    result = td.series("wind_forecast").insert(df=df, known_time=sample_datetime)

    assert result.batch_id is not None
    assert "wind_forecast" in result.series_ids

    # Verify rows in projections_medium
    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM projections_medium WHERE batch_id = %s",
                (result.batch_id,)
            )
            assert cur.fetchone()[0] == 2

            # Verify no rows in actuals
            cur.execute("SELECT COUNT(*) FROM actuals")
            assert cur.fetchone()[0] == 0


def test_insert_projections_short_tier(clean_db):
    """Test inserting projections with storage_tier='short'."""
    # Use a recent datetime to avoid the 6-month retention policy on projections_short
    recent_time = datetime.now(timezone.utc).replace(microsecond=0)

    os.environ["TIMEDB_DSN"] = clean_db
    td = TimeDataClient()

    td.create_series(
        name="price_forecast", unit="dimensionless",
        data_class="projection", storage_tier="short",
    )

    df = pd.DataFrame({
        "valid_time": [recent_time],
        "price_forecast": [42.0],
    })

    td.series("price_forecast").insert(df=df, known_time=recent_time)

    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM projections_short")
            assert cur.fetchone()[0] == 1

            cur.execute("SELECT COUNT(*) FROM projections_medium")
            assert cur.fetchone()[0] == 0

            cur.execute("SELECT COUNT(*) FROM projections_long")
            assert cur.fetchone()[0] == 0


def test_insert_projections_long_tier(clean_db, sample_datetime):
    """Test inserting projections with storage_tier='long'."""
    os.environ["TIMEDB_DSN"] = clean_db
    td = TimeDataClient()

    td.create_series(
        name="climate_forecast", unit="dimensionless",
        data_class="projection", storage_tier="long",
    )

    df = pd.DataFrame({
        "valid_time": [sample_datetime],
        "climate_forecast": [15.0],
    })

    td.series("climate_forecast").insert(df=df, known_time=sample_datetime)

    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM projections_long")
            assert cur.fetchone()[0] == 1


def test_insert_projections_interval(clean_db, sample_datetime):
    """Test inserting interval projections with valid_time_end."""
    os.environ["TIMEDB_DSN"] = clean_db
    td = TimeDataClient()

    td.create_series(
        name="energy_forecast", unit="dimensionless",
        data_class="projection", storage_tier="medium",
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
            cur.execute("SELECT valid_time_end FROM projections_medium")
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

    td.create_series(name="temp", unit="dimensionless", data_class="actual")

    df = pd.DataFrame({
        "valid_time": [datetime(2025, 1, 1, 12, 0)],  # naive datetime
        "temp": [20.0],
    })

    with pytest.raises(ValueError, match="timezone-aware"):
        td.series("temp").insert(df=df)


# =============================================================================
# Backward compatibility
# =============================================================================

def test_insert_batch_alias(clean_db, sample_datetime):
    """Test that insert_batch still works as alias for insert."""
    os.environ["TIMEDB_DSN"] = clean_db
    td = TimeDataClient()

    td.create_series(name="compat", unit="dimensionless", data_class="actual")

    df = pd.DataFrame({
        "valid_time": [sample_datetime],
        "compat": [99.0],
    })

    result = td.series("compat").insert_batch(df=df)
    assert result.batch_id is not None

    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM actuals")
            assert cur.fetchone()[0] == 1
