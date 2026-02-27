"""Tests for timedatamodel integration (TimeSeries, MultivariateTimeSeries)."""
import pytest
from datetime import datetime, timezone, timedelta
import numpy as np
import pandas as pd

from timedatamodel import TimeSeries, MultivariateTimeSeries, Resolution, Frequency, TimeSeriesType


# =============================================================================
# insert() with TimeSeries
# =============================================================================

def test_insert_timeseries_flat(td, clean_db, sample_datetime):
    """Insert a TimeSeries object into a flat series."""
    td.create_series(name="temperature", unit="C")

    ts = TimeSeries(
        resolution=Resolution(Frequency.PT1H),
        name="temperature",
        unit="C",
        timestamps=[sample_datetime, sample_datetime + timedelta(hours=1)],
        values=[20.5, 21.0],
    )

    result = td.get_series("temperature").insert(data=ts)
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
        data=ts, knowledge_time=knowledge_time,
    )
    assert result.batch_id is not None

    df = td.get_series("wind_forecast").read()
    assert len(df) == 2


def test_insert_none_raises(td, clean_db):
    """Providing None should raise ValueError."""
    td.create_series(name="temperature", unit="C")

    with pytest.raises(ValueError, match="Must provide"):
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
    td.get_series("temperature").where(site="north").insert(data=df)

    ts = td.get_series("temperature").where(site="north").read(as_timeseries=True)

    assert isinstance(ts, TimeSeries)
    assert ts.name == "temperature"
    assert ts.unit == "C"
    assert ts.description == "Air temperature"
    assert ts.timeseries_type == TimeSeriesType.FLAT
    assert ts.attributes == {"site": "north"}
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
    td.get_series("forecast").insert(data=df, knowledge_time=knowledge_time)

    ts = td.get_series("forecast").read(as_timeseries=True)

    assert isinstance(ts, TimeSeries)
    assert ts.timeseries_type == TimeSeriesType.OVERLAPPING
    assert len(ts.values) == 2


def test_read_as_timeseries_with_overlapping_raises(td, clean_db, sample_datetime):
    """as_timeseries with overlapping=True should raise ValueError."""
    td.create_series(name="forecast", unit="MW", overlapping=True)

    df = pd.DataFrame({
        "valid_time": [sample_datetime],
        "value": [100.0],
    })
    td.get_series("forecast").insert(
        data=df, knowledge_time=sample_datetime - timedelta(hours=1),
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
    td.get_series("temperature").insert(data=df)

    with pytest.raises(ValueError, match="as_timeseries=True is not supported"):
        td.get_series("temperature").read(as_timeseries=True, include_updates=True)


def test_read_as_pint_and_as_timeseries_raises(td, clean_db, sample_datetime):
    """as_pint and as_timeseries together should raise ValueError."""
    td.create_series(name="temperature", unit="C")

    df = pd.DataFrame({
        "valid_time": [sample_datetime],
        "value": [20.0],
    })
    td.get_series("temperature").insert(data=df)

    with pytest.raises(ValueError, match="Cannot use both"):
        td.get_series("temperature").read(as_pint=True, as_timeseries=True)


# =============================================================================
# create_series() without Metadata
# =============================================================================

def test_create_series_no_name_raises(td, clean_db):
    """create_series without name should raise ValueError."""
    with pytest.raises(ValueError, match="'name' is required"):
        td.create_series()


# =============================================================================
# Round-trip: create, insert TimeSeries, read as TimeSeries
# =============================================================================

def test_roundtrip_timeseries(td, clean_db, sample_datetime):
    """Full round-trip: create series, insert TimeSeries, read as TimeSeries."""
    td.create_series(
        name="wind_power", unit="MW", description="Wind generation",
        labels={"park": "alpha"},
    )

    ts_in = TimeSeries(
        resolution=Resolution(Frequency.PT1H),
        name="wind_power",
        unit="MW",
        description="Wind generation",
        attributes={"park": "alpha"},
        timestamps=[
            sample_datetime,
            sample_datetime + timedelta(hours=1),
            sample_datetime + timedelta(hours=2),
        ],
        values=[10.0, 20.0, 30.0],
    )

    td.get_series("wind_power").where(park="alpha").insert(data=ts_in)

    ts_out = td.get_series("wind_power").where(park="alpha").read(as_timeseries=True)

    assert isinstance(ts_out, TimeSeries)
    assert ts_out.name == "wind_power"
    assert ts_out.unit == "MW"
    assert ts_out.attributes == {"park": "alpha"}
    assert len(ts_out.values) == 3
    assert ts_out.values == [10.0, 20.0, 30.0]


# =============================================================================
# MultivariateTimeSeries insert / read
# =============================================================================

def test_insert_multi_timeseries(td, clean_db, sample_datetime):
    """Insert a MultivariateTimeSeries, verify each column stored as separate series."""
    td.create_series(name="power", unit="MW")
    td.create_series(name="temperature", unit="C")

    timestamps = [sample_datetime, sample_datetime + timedelta(hours=1)]
    values = np.array([[10.0, 20.0], [15.0, 25.0]])

    mts = MultivariateTimeSeries(
        Resolution(Frequency.PT1H),
        timestamps=timestamps,
        values=values,
        names=["power", "temperature"],
        units=["MW", "C"],
    )

    results = td.get_series("power").insert(data=mts)
    assert isinstance(results, list)
    assert len(results) == 2

    # Verify power series
    df_power = td.get_series("power").read()
    assert len(df_power) == 2
    assert df_power["value"].tolist() == [10.0, 15.0]

    # Verify temperature series
    df_temp = td.get_series("temperature").read()
    assert len(df_temp) == 2
    assert df_temp["value"].tolist() == [20.0, 25.0]


def test_read_multi_timeseries(td, clean_db, sample_datetime):
    """Read a collection matching N series with as_timeseries=True, verify MTS returned."""
    td.create_series(name="wind", unit="MW", labels={"park": "alpha"})
    td.create_series(name="wind", unit="MW", labels={"park": "beta"})

    timestamps = [sample_datetime, sample_datetime + timedelta(hours=1)]

    df1 = pd.DataFrame({"valid_time": timestamps, "value": [10.0, 20.0]})
    df2 = pd.DataFrame({"valid_time": timestamps, "value": [30.0, 40.0]})

    td.get_series("wind").where(park="alpha").insert(data=df1)
    td.get_series("wind").where(park="beta").insert(data=df2)

    mts = td.get_series("wind").read(as_timeseries=True)

    assert isinstance(mts, MultivariateTimeSeries)
    assert mts.n_columns == 2
    assert len(mts) == 2
    # Values should contain both series' data (order may vary)
    arr = mts.to_numpy()
    col_sums = sorted(arr.sum(axis=0).tolist())
    assert col_sums == [30.0, 70.0]


def test_read_single_returns_timeseries(td, clean_db, sample_datetime):
    """Verify single series still returns univariate TimeSeries."""
    td.create_series(name="solo", unit="MW")

    df = pd.DataFrame({
        "valid_time": [sample_datetime, sample_datetime + timedelta(hours=1)],
        "value": [5.0, 10.0],
    })
    td.get_series("solo").insert(data=df)

    ts = td.get_series("solo").read(as_timeseries=True)

    assert isinstance(ts, TimeSeries)
    assert not isinstance(ts, MultivariateTimeSeries)
    assert ts.name == "solo"
    assert len(ts.values) == 2
