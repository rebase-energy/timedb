"""Tests for timedatamodel integration (TimeSeries, MultivariateTimeSeries)."""
import pytest
from datetime import datetime, timezone, timedelta
import numpy as np
import pandas as pd

from timedatamodel import TimeSeries, MultivariateTimeSeries, Frequency, TimeSeriesType


# =============================================================================
# insert() with TimeSeries
# =============================================================================

def test_insert_timeseries_flat(td, clean_db, sample_datetime):
    """Insert a TimeSeries object into a flat series."""
    td.create_series(name="temperature", unit="C")

    ts = TimeSeries(
        Frequency.PT1H,
        name="temperature",
        unit="C",
        timestamps=[sample_datetime, sample_datetime + timedelta(hours=1)],
        values=[20.5, 21.0],
    )

    result = td.get_series("temperature").insert(data=ts)
    assert result.series_id > 0

    ts_out = td.get_series("temperature").read()
    assert isinstance(ts_out, TimeSeries)
    assert len(ts_out) == 2
    assert ts_out.values == [20.5, 21.0]


def test_insert_timeseries_overlapping(td, clean_db, sample_datetime):
    """Insert a TimeSeries object into an overlapping series."""
    td.create_series(name="wind_forecast", unit="MW", overlapping=True)

    ts = TimeSeries(
        Frequency.PT1H,
        timestamps=[sample_datetime, sample_datetime + timedelta(hours=1)],
        values=[100.0, 110.0],
    )

    knowledge_time = sample_datetime - timedelta(hours=1)
    result = td.get_series("wind_forecast").insert(
        data=ts, knowledge_time=knowledge_time,
    )
    assert result.batch_id is not None

    ts_out = td.get_series("wind_forecast").read()
    assert isinstance(ts_out, TimeSeries)
    assert len(ts_out) == 2


def test_insert_none_raises(td, clean_db):
    """Providing None should raise ValueError."""
    td.create_series(name="temperature", unit="C")

    with pytest.raises(ValueError, match="Must provide"):
        td.get_series("temperature").insert()


# =============================================================================
# read() with output parameter
# =============================================================================

def test_read_timeseries_flat(td, clean_db, sample_datetime):
    """Read flat series back as TimeSeries (default output)."""
    td.create_series(
        name="temperature", unit="C", description="Air temperature",
        labels={"site": "north"},
    )

    df = pd.DataFrame({
        "valid_time": [sample_datetime, sample_datetime + timedelta(hours=1)],
        "value": [20.5, 21.0],
    })
    td.get_series("temperature").where(site="north").insert(data=df)

    ts = td.get_series("temperature").where(site="north").read()

    assert isinstance(ts, TimeSeries)
    assert ts.name == "temperature"
    assert ts.unit == "C"
    assert ts.description == "Air temperature"
    assert ts.timeseries_type == TimeSeriesType.FLAT
    assert ts.attributes == {"site": "north"}
    assert len(ts.values) == 2
    assert ts.values[0] == 20.5
    assert ts.values[1] == 21.0


def test_read_timeseries_overlapping_latest(td, clean_db, sample_datetime):
    """Read overlapping series (latest view) back as TimeSeries."""
    td.create_series(name="forecast", unit="MW", overlapping=True)

    df = pd.DataFrame({
        "valid_time": [sample_datetime, sample_datetime + timedelta(hours=1)],
        "value": [100.0, 110.0],
    })
    knowledge_time = sample_datetime - timedelta(hours=1)
    td.get_series("forecast").insert(data=df, knowledge_time=knowledge_time)

    ts = td.get_series("forecast").read()

    assert isinstance(ts, TimeSeries)
    assert ts.timeseries_type == TimeSeriesType.OVERLAPPING
    assert len(ts.values) == 2


def test_read_overlapping_output_timeseries(td, clean_db, sample_datetime):
    """overlapping=True + output='timeseries' returns multi-index TimeSeries."""
    td.create_series(name="forecast", unit="MW", overlapping=True)

    df = pd.DataFrame({
        "valid_time": [sample_datetime],
        "value": [100.0],
    })
    td.get_series("forecast").insert(
        data=df, knowledge_time=sample_datetime - timedelta(hours=1),
    )

    ts = td.get_series("forecast").read(overlapping=True)

    assert isinstance(ts, TimeSeries)
    assert ts.is_multi_index


def test_read_include_updates_output_timeseries(td, clean_db, sample_datetime):
    """include_updates=True + output='timeseries' returns multi-index TimeSeries."""
    td.create_series(name="temperature", unit="C")

    df = pd.DataFrame({
        "valid_time": [sample_datetime],
        "value": [20.0],
    })
    td.get_series("temperature").insert(data=df)

    ts = td.get_series("temperature").read(include_updates=True)

    assert isinstance(ts, TimeSeries)
    assert ts.is_multi_index


def test_read_as_pint_with_non_pandas_raises(td, clean_db, sample_datetime):
    """as_pint with output != 'pandas' should raise ValueError."""
    td.create_series(name="temperature", unit="C")

    df = pd.DataFrame({
        "valid_time": [sample_datetime],
        "value": [20.0],
    })
    td.get_series("temperature").insert(data=df)

    with pytest.raises(ValueError, match="as_pint=True is only valid"):
        td.get_series("temperature").read(as_pint=True)


def test_read_output_invalid(td, clean_db, sample_datetime):
    """Invalid output value should raise ValueError."""
    td.create_series(name="temperature", unit="C")

    df = pd.DataFrame({
        "valid_time": [sample_datetime],
        "value": [20.0],
    })
    td.get_series("temperature").insert(data=df)

    with pytest.raises(ValueError, match="Invalid output"):
        td.get_series("temperature").read(output="xml")


def test_read_output_pandas(td, clean_db, sample_datetime):
    """output='pandas' returns a DataFrame."""
    td.create_series(name="temperature", unit="C")

    df = pd.DataFrame({
        "valid_time": [sample_datetime, sample_datetime + timedelta(hours=1)],
        "value": [20.5, 21.0],
    })
    td.get_series("temperature").insert(data=df)

    result = td.get_series("temperature").read(output="pandas")

    assert isinstance(result, pd.DataFrame)
    assert "value" in result.columns
    assert len(result) == 2
    assert result["value"].tolist() == [20.5, 21.0]


def test_read_output_numpy(td, clean_db, sample_datetime):
    """output='numpy' returns a numpy ndarray."""
    td.create_series(name="temperature", unit="C")

    df = pd.DataFrame({
        "valid_time": [sample_datetime, sample_datetime + timedelta(hours=1)],
        "value": [20.5, 21.0],
    })
    td.get_series("temperature").insert(data=df)

    result = td.get_series("temperature").read(output="numpy")

    assert isinstance(result, np.ndarray)
    assert len(result) == 2


def test_read_multi_output_pandas(td, clean_db, sample_datetime):
    """Multi-series read with output='pandas' returns a DataFrame."""
    td.create_series(name="wind", unit="MW", labels={"park": "alpha"})
    td.create_series(name="wind", unit="MW", labels={"park": "beta"})

    timestamps = [sample_datetime, sample_datetime + timedelta(hours=1)]
    df1 = pd.DataFrame({"valid_time": timestamps, "value": [10.0, 20.0]})
    df2 = pd.DataFrame({"valid_time": timestamps, "value": [30.0, 40.0]})

    td.get_series("wind").where(park="alpha").insert(data=df1)
    td.get_series("wind").where(park="beta").insert(data=df2)

    result = td.get_series("wind").read(output="pandas")

    assert isinstance(result, pd.DataFrame)
    assert result.shape[1] == 2  # Two columns (one per series)
    assert len(result) == 2


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
        Frequency.PT1H,
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

    ts_out = td.get_series("wind_power").where(park="alpha").read()

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
        Frequency.PT1H,
        timestamps=timestamps,
        values=values,
        names=["power", "temperature"],
        units=["MW", "C"],
    )

    results = td.get_series("power").insert(data=mts)
    assert isinstance(results, list)
    assert len(results) == 2

    # Verify power series
    ts_power = td.get_series("power").read()
    assert len(ts_power) == 2
    assert ts_power.values == [10.0, 15.0]

    # Verify temperature series
    ts_temp = td.get_series("temperature").read()
    assert len(ts_temp) == 2
    assert ts_temp.values == [20.0, 25.0]


def test_read_multi_timeseries(td, clean_db, sample_datetime):
    """Read a collection matching N series, verify MTS returned."""
    td.create_series(name="wind", unit="MW", labels={"park": "alpha"})
    td.create_series(name="wind", unit="MW", labels={"park": "beta"})

    timestamps = [sample_datetime, sample_datetime + timedelta(hours=1)]

    df1 = pd.DataFrame({"valid_time": timestamps, "value": [10.0, 20.0]})
    df2 = pd.DataFrame({"valid_time": timestamps, "value": [30.0, 40.0]})

    td.get_series("wind").where(park="alpha").insert(data=df1)
    td.get_series("wind").where(park="beta").insert(data=df2)

    mts = td.get_series("wind").read()

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

    ts = td.get_series("solo").read()

    assert isinstance(ts, TimeSeries)
    assert not isinstance(ts, MultivariateTimeSeries)
    assert ts.name == "solo"
    assert len(ts.values) == 2


# =============================================================================
# insert() with dict (numpy arrays / Python lists)
# =============================================================================

def test_insert_dict_numpy_flat(td, clean_db, sample_datetime):
    """Insert dict with numpy arrays into a flat series."""
    td.create_series(name="temperature", unit="C")

    timestamps = np.array([
        sample_datetime,
        sample_datetime + timedelta(hours=1),
    ], dtype="datetime64[us]")
    values = np.array([20.5, 21.0])

    result = td.get_series("temperature").insert(data={
        "timestamps": timestamps,
        "values": values,
    })
    assert result.series_id > 0

    ts_out = td.get_series("temperature").read()
    assert isinstance(ts_out, TimeSeries)
    assert len(ts_out) == 2
    assert ts_out.values == [20.5, 21.0]


def test_insert_dict_numpy_overlapping(td, clean_db, sample_datetime):
    """Insert dict with numpy arrays into an overlapping series."""
    td.create_series(name="wind_forecast", unit="MW", overlapping=True)

    timestamps = np.array([
        sample_datetime,
        sample_datetime + timedelta(hours=1),
    ], dtype="datetime64[us]")
    values = np.array([100.0, 110.0])

    knowledge_time = sample_datetime - timedelta(hours=1)
    result = td.get_series("wind_forecast").insert(
        data={"timestamps": timestamps, "values": values},
        knowledge_time=knowledge_time,
    )
    assert result.batch_id is not None

    ts_out = td.get_series("wind_forecast").read()
    assert isinstance(ts_out, TimeSeries)
    assert len(ts_out) == 2


def test_insert_dict_python_lists(td, clean_db, sample_datetime):
    """Insert dict with Python lists (no numpy dependency in data)."""
    td.create_series(name="temperature", unit="C")

    timestamps = [sample_datetime, sample_datetime + timedelta(hours=1)]
    values = [20.5, 21.0]

    result = td.get_series("temperature").insert(data={
        "timestamps": timestamps,
        "values": values,
    })
    assert result.series_id > 0

    ts_out = td.get_series("temperature").read()
    assert len(ts_out) == 2
    assert ts_out.values == [20.5, 21.0]


def test_insert_dict_mixed_types(td, clean_db, sample_datetime):
    """Insert dict with Python list timestamps and numpy array values."""
    td.create_series(name="temperature", unit="C")

    timestamps = [sample_datetime, sample_datetime + timedelta(hours=1)]
    values = np.array([20.5, 21.0])

    result = td.get_series("temperature").insert(data={
        "timestamps": timestamps,
        "values": values,
    })
    assert result.series_id > 0

    ts_out = td.get_series("temperature").read()
    assert len(ts_out) == 2
    assert ts_out.values == [20.5, 21.0]


def test_insert_dict_with_intervals(td, clean_db, sample_datetime):
    """Insert dict with timestamps_end for interval data."""
    td.create_series(name="energy", unit="MWh")

    timestamps = [sample_datetime, sample_datetime + timedelta(hours=1)]
    timestamps_end = [
        sample_datetime + timedelta(hours=1),
        sample_datetime + timedelta(hours=2),
    ]
    values = [100.0, 110.0]

    result = td.get_series("energy").insert(data={
        "timestamps": timestamps,
        "timestamps_end": timestamps_end,
        "values": values,
    })
    assert result.series_id > 0


def test_insert_dict_missing_keys_raises(td, clean_db):
    """Dict without required keys raises ValueError."""
    td.create_series(name="temperature", unit="C")

    with pytest.raises(ValueError, match="Dict must contain 'timestamps' and 'values' keys"):
        td.get_series("temperature").insert(data={"timestamps": [1, 2, 3]})

    with pytest.raises(ValueError, match="Dict must contain 'timestamps' and 'values' keys"):
        td.get_series("temperature").insert(data={"values": [1, 2, 3]})


def test_insert_dict_length_mismatch_raises(td, clean_db, sample_datetime):
    """Mismatched array lengths raises ValueError."""
    td.create_series(name="temperature", unit="C")

    timestamps = [sample_datetime, sample_datetime + timedelta(hours=1)]
    values = [20.5]  # Length mismatch

    with pytest.raises(ValueError, match="same length"):
        td.get_series("temperature").insert(data={
            "timestamps": timestamps,
            "values": values,
        })
