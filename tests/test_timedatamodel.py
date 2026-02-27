"""Tests for timedatamodel integration (TimeSeries, Metadata)."""
import pytest
from datetime import datetime, timezone, timedelta
import pandas as pd

from timedatamodel import TimeSeries, Metadata, Resolution, Frequency, StorageType


# =============================================================================
# insert() with TimeSeries
# =============================================================================

def test_insert_timeseries_flat(td, clean_db, sample_datetime):
    """Insert a TimeSeries object into a flat series."""
    td.create_series(name="temperature", unit="C")

    ts = TimeSeries(
        resolution=Resolution(Frequency.PT1H),
        metadata=Metadata(name="temperature", unit="C"),
        timestamps=[sample_datetime, sample_datetime + timedelta(hours=1)],
        values=[20.5, 21.0],
    )

    result = td.get_series("temperature").insert(ts=ts)
    assert result.series_id > 0

    df = td.get_series("temperature").read()
    assert len(df) == 2
    assert df["value"].tolist() == [20.5, 21.0]


def test_insert_timeseries_overlapping(td, clean_db, sample_datetime):
    """Insert a TimeSeries object into an overlapping series."""
    td.create_series(name="wind_forecast", unit="MW", overlapping=True)

    ts = TimeSeries(
        resolution=Resolution(Frequency.PT1H),
        timestamps=[sample_datetime, sample_datetime + timedelta(hours=1)],
        values=[100.0, 110.0],
    )

    knowledge_time = sample_datetime - timedelta(hours=1)
    result = td.get_series("wind_forecast").insert(
        ts=ts, knowledge_time=knowledge_time,
    )
    assert result.batch_id is not None

    df = td.get_series("wind_forecast").read()
    assert len(df) == 2


def test_insert_both_df_and_ts_raises(td, clean_db, sample_datetime):
    """Providing both df and ts should raise ValueError."""
    td.create_series(name="temperature", unit="C")

    ts = TimeSeries(
        resolution=Resolution(Frequency.PT1H),
        timestamps=[sample_datetime],
        values=[20.0],
    )
    df = pd.DataFrame({"valid_time": [sample_datetime], "value": [20.0]})

    with pytest.raises(ValueError, match="Cannot provide both"):
        td.get_series("temperature").insert(df=df, ts=ts)


def test_insert_neither_df_nor_ts_raises(td, clean_db):
    """Providing neither df nor ts should raise ValueError."""
    td.create_series(name="temperature", unit="C")

    with pytest.raises(ValueError, match="Must provide either"):
        td.get_series("temperature").insert()


# =============================================================================
# read() with as_timeseries
# =============================================================================

def test_read_as_timeseries_flat(td, clean_db, sample_datetime):
    """Read flat series back as TimeSeries."""
    td.create_series(
        name="temperature", unit="C", description="Air temperature",
        labels={"site": "north"},
    )

    df = pd.DataFrame({
        "valid_time": [sample_datetime, sample_datetime + timedelta(hours=1)],
        "value": [20.5, 21.0],
    })
    td.get_series("temperature").where(site="north").insert(df=df)

    ts = td.get_series("temperature").where(site="north").read(as_timeseries=True)

    assert isinstance(ts, TimeSeries)
    assert ts.metadata.name == "temperature"
    assert ts.metadata.unit == "C"
    assert ts.metadata.description == "Air temperature"
    assert ts.metadata.storage_type == StorageType.FLAT
    assert ts.metadata.attributes == {"site": "north"}
    assert len(ts.values) == 2
    assert ts.values[0] == 20.5
    assert ts.values[1] == 21.0


def test_read_as_timeseries_overlapping_latest(td, clean_db, sample_datetime):
    """Read overlapping series (latest view) back as TimeSeries."""
    td.create_series(name="forecast", unit="MW", overlapping=True)

    df = pd.DataFrame({
        "valid_time": [sample_datetime, sample_datetime + timedelta(hours=1)],
        "value": [100.0, 110.0],
    })
    knowledge_time = sample_datetime - timedelta(hours=1)
    td.get_series("forecast").insert(df=df, knowledge_time=knowledge_time)

    ts = td.get_series("forecast").read(as_timeseries=True)

    assert isinstance(ts, TimeSeries)
    assert ts.metadata.storage_type == StorageType.OVERLAPPING
    assert len(ts.values) == 2


def test_read_as_timeseries_with_overlapping_raises(td, clean_db, sample_datetime):
    """as_timeseries with overlapping=True should raise ValueError."""
    td.create_series(name="forecast", unit="MW", overlapping=True)

    df = pd.DataFrame({
        "valid_time": [sample_datetime],
        "value": [100.0],
    })
    td.get_series("forecast").insert(
        df=df, knowledge_time=sample_datetime - timedelta(hours=1),
    )

    with pytest.raises(ValueError, match="as_timeseries=True is not supported"):
        td.get_series("forecast").read(as_timeseries=True, overlapping=True)


def test_read_as_timeseries_with_include_updates_raises(td, clean_db, sample_datetime):
    """as_timeseries with include_updates=True should raise ValueError."""
    td.create_series(name="temperature", unit="C")

    df = pd.DataFrame({
        "valid_time": [sample_datetime],
        "value": [20.0],
    })
    td.get_series("temperature").insert(df=df)

    with pytest.raises(ValueError, match="as_timeseries=True is not supported"):
        td.get_series("temperature").read(as_timeseries=True, include_updates=True)


def test_read_as_pint_and_as_timeseries_raises(td, clean_db, sample_datetime):
    """as_pint and as_timeseries together should raise ValueError."""
    td.create_series(name="temperature", unit="C")

    df = pd.DataFrame({
        "valid_time": [sample_datetime],
        "value": [20.0],
    })
    td.get_series("temperature").insert(df=df)

    with pytest.raises(ValueError, match="Cannot use both"):
        td.get_series("temperature").read(as_pint=True, as_timeseries=True)


# =============================================================================
# create_series() with Metadata
# =============================================================================

def test_create_series_from_metadata(td, clean_db, sample_datetime):
    """Create a series using a Metadata object."""
    meta = Metadata(
        name="solar_power",
        unit="MW",
        description="Solar generation",
        storage_type=StorageType.OVERLAPPING,
        attributes={"site": "south"},
    )

    series_id = td.create_series(metadata=meta)
    assert series_id > 0

    # Verify by inserting and reading
    df = pd.DataFrame({
        "valid_time": [sample_datetime],
        "value": [50.0],
    })
    knowledge_time = sample_datetime - timedelta(hours=1)
    td.get_series("solar_power").where(site="south").insert(
        df=df, knowledge_time=knowledge_time,
    )

    result = td.get_series("solar_power").where(site="south").read()
    assert len(result) == 1


def test_create_series_metadata_with_overrides(td, clean_db):
    """Explicit kwargs should override metadata values."""
    meta = Metadata(
        name="original_name",
        unit="kW",
        description="Original desc",
    )

    series_id = td.create_series(
        name="overridden_name",
        unit="MW",
        description="Overridden desc",
        metadata=meta,
    )
    assert series_id > 0


def test_create_series_no_name_no_metadata_raises(td, clean_db):
    """create_series without name or metadata should raise ValueError."""
    with pytest.raises(ValueError, match="'name' is required"):
        td.create_series()


# =============================================================================
# Round-trip: Metadata -> create, TimeSeries -> insert, read -> TimeSeries
# =============================================================================

def test_roundtrip_timeseries(td, clean_db, sample_datetime):
    """Full round-trip: create from Metadata, insert TimeSeries, read as TimeSeries."""
    meta = Metadata(
        name="wind_power",
        unit="MW",
        description="Wind generation",
        storage_type=StorageType.FLAT,
        attributes={"park": "alpha"},
    )

    td.create_series(metadata=meta)

    ts_in = TimeSeries(
        resolution=Resolution(Frequency.PT1H),
        metadata=meta,
        timestamps=[
            sample_datetime,
            sample_datetime + timedelta(hours=1),
            sample_datetime + timedelta(hours=2),
        ],
        values=[10.0, 20.0, 30.0],
    )

    td.get_series("wind_power").where(park="alpha").insert(ts=ts_in)

    ts_out = td.get_series("wind_power").where(park="alpha").read(as_timeseries=True)

    assert isinstance(ts_out, TimeSeries)
    assert ts_out.metadata.name == "wind_power"
    assert ts_out.metadata.unit == "MW"
    assert ts_out.metadata.attributes == {"park": "alpha"}
    assert len(ts_out.values) == 3
    assert ts_out.values == [10.0, 20.0, 30.0]
