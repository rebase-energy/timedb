"""Tests for reading flat and overlapping."""
import os
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from timedb import TimeDataClient
from timedb.db import read


# =============================================================================
# Read flat tests
# =============================================================================

def test_read_flat_via_sdk(clean_db, sample_datetime):
    """Test reading flat via SDK returns a pivoted DataFrame."""
    os.environ["TIMEDB_DSN"] = clean_db
    td = TimeDataClient()

    td.create_series(name="temperature", unit="dimensionless", overlapping=False)
    td.create_series(name="humidity", unit="dimensionless", overlapping=False)

    df_temp = pd.DataFrame({
        "valid_time": [sample_datetime, sample_datetime + timedelta(hours=1)],
        "temperature": [20.5, 21.0],
    })
    td.series("temperature").insert(df=df_temp)

    df_hum = pd.DataFrame({
        "valid_time": [sample_datetime],
        "humidity": [65.0],
    })
    td.series("humidity").insert(df=df_hum)

    # Read all flat via SDK
    df = td.series("temperature").read(
        start_valid=sample_datetime,
        end_valid=sample_datetime + timedelta(hours=2),
    )

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert "valid_time" in df.index.names or df.index.name == "valid_time"


def test_read_flat_db_layer(clean_db, sample_datetime):
    """Test reading flat via the db.read layer."""
    os.environ["TIMEDB_DSN"] = clean_db
    td = TimeDataClient()

    td.create_series(name="power", unit="dimensionless", overlapping=False)

    df = pd.DataFrame({
        "valid_time": [sample_datetime, sample_datetime + timedelta(hours=1)],
        "power": [100.0, 101.0],
    })
    td.series("power").insert(df=df)

    # Read via db layer
    result = read.read_flat(
        clean_db,
        start_valid=sample_datetime,
        end_valid=sample_datetime + timedelta(hours=2),
    )

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert list(result.index.names) == ["valid_time", "series_id"]


def test_read_flat_filter_by_valid_time(clean_db, sample_datetime):
    """Test filtering flat by valid_time range."""
    os.environ["TIMEDB_DSN"] = clean_db
    td = TimeDataClient()

    td.create_series(name="power", unit="dimensionless", overlapping=False)

    df = pd.DataFrame({
        "valid_time": [
            sample_datetime,
            sample_datetime + timedelta(hours=1),
            sample_datetime + timedelta(hours=2),
            sample_datetime + timedelta(hours=3),
        ],
        "power": [100.0, 101.0, 102.0, 103.0],
    })
    td.series("power").insert(df=df)

    # Read only a subset
    result = read.read_flat(
        clean_db,
        start_valid=sample_datetime + timedelta(hours=1),
        end_valid=sample_datetime + timedelta(hours=3),
    )

    # Should only get 2 values (hours 1 and 2)
    assert len(result) == 2
    assert all(
        sample_datetime + timedelta(hours=1) <= idx[0] < sample_datetime + timedelta(hours=3)
        for idx in result.index
    )


# =============================================================================
# Read overlapping tests
# =============================================================================

def test_read_overlapping_latest_via_sdk(clean_db, sample_datetime):
    """Test reading latest overlapping via SDK."""
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
    td.series("wind_forecast").insert(df=df, known_time=sample_datetime)

    # Read latest (default, versions=False)
    result = td.series("wind_forecast").read(
        start_valid=sample_datetime,
        end_valid=sample_datetime + timedelta(hours=2),
    )

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2


def test_read_overlapping_all_versions_via_sdk(clean_db, sample_datetime):
    """Test reading all overlapping versions via SDK (versions=True)."""
    os.environ["TIMEDB_DSN"] = clean_db
    td = TimeDataClient()

    td.create_series(
        name="wind_forecast", unit="dimensionless",
        overlapping=True, retention="medium",
    )

    # Insert first batch
    known_time_1 = sample_datetime
    df1 = pd.DataFrame({
        "valid_time": [sample_datetime, sample_datetime + timedelta(hours=1)],
        "wind_forecast": [50.0, 55.0],
    })
    td.series("wind_forecast").insert(df=df1, known_time=known_time_1)

    # Insert second batch (revision) for the same valid times
    known_time_2 = sample_datetime + timedelta(hours=1)
    df2 = pd.DataFrame({
        "valid_time": [sample_datetime, sample_datetime + timedelta(hours=1)],
        "wind_forecast": [52.0, 57.0],
    })
    td.series("wind_forecast").insert(df=df2, known_time=known_time_2)

    # Read all versions
    result = td.series("wind_forecast").read(
        versions=True,
        start_valid=sample_datetime,
        end_valid=sample_datetime + timedelta(hours=2),
    )

    assert isinstance(result, pd.DataFrame)
    # Should have 4 rows (2 valid_times x 2 known_times)
    assert len(result) == 4
    assert "known_time" in result.index.names


def test_read_overlapping_all_versions_db_layer(clean_db, sample_datetime):
    """Test reading all overlapping versions via the db.read layer."""
    os.environ["TIMEDB_DSN"] = clean_db
    td = TimeDataClient()

    td.create_series(
        name="forecast", unit="dimensionless",
        overlapping=True, retention="medium",
    )

    known_time_1 = sample_datetime
    df1 = pd.DataFrame({
        "valid_time": [sample_datetime],
        "forecast": [100.0],
    })
    td.series("forecast").insert(df=df1, known_time=known_time_1)

    known_time_2 = sample_datetime + timedelta(hours=1)
    df2 = pd.DataFrame({
        "valid_time": [sample_datetime],
        "forecast": [105.0],
    })
    td.series("forecast").insert(df=df2, known_time=known_time_2)

    result = read.read_overlapping_all(
        clean_db,
        start_valid=sample_datetime,
        end_valid=sample_datetime + timedelta(hours=1),
    )

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert list(result.index.names) == ["known_time", "valid_time", "series_id"]


def test_read_overlapping_latest_picks_newest(clean_db, sample_datetime):
    """Test that latest overlapping read picks the most recent known_time."""
    os.environ["TIMEDB_DSN"] = clean_db
    td = TimeDataClient()

    td.create_series(
        name="price", unit="dimensionless",
        overlapping=True, retention="medium",
    )

    # Insert initial forecast
    df1 = pd.DataFrame({
        "valid_time": [sample_datetime],
        "price": [100.0],
    })
    td.series("price").insert(df=df1, known_time=sample_datetime)

    # Insert revised forecast with newer known_time
    df2 = pd.DataFrame({
        "valid_time": [sample_datetime],
        "price": [110.0],
    })
    td.series("price").insert(
        df=df2,
        known_time=sample_datetime + timedelta(hours=1),
    )

    # Read latest via SDK
    result = td.series("price").read(
        start_valid=sample_datetime,
        end_valid=sample_datetime + timedelta(hours=1),
    )

    assert len(result) == 1
    # The latest value should be 110.0
    assert result.iloc[0, 0].m == 110.0  # .m extracts magnitude from pint quantity
