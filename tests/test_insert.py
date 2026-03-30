"""Tests for inserting flat and overlapping data."""
import json
import uuid
import pytest
from datetime import datetime, timezone, timedelta
import pandas as pd
import polars as pl

from timedb import TimeSeries, DataShape, IncompatibleUnitError
from timedb.db import read
from timedatamodel.enums import TimeSeriesType


# =============================================================================
# Flat insertion tests
# =============================================================================

def test_insert_flat_creates_run(td, ch_client, sample_datetime):
    """Test inserting flat via SDK creates one run and rows in the flat table."""
    td.create_series("temperature", unit="dimensionless", overlapping=False)

    df = pd.DataFrame({
        "valid_time": [sample_datetime, sample_datetime + timedelta(hours=1)],
        "value": [20.5, 21.0],
    })

    result = td.get_series("temperature").insert(data=df)

    assert isinstance(result.run_id, uuid.UUID)
    assert result.series_id > 0

    # Verify rows in flat table
    res = ch_client.query("SELECT COUNT(*) FROM flat")
    assert res.result_rows[0][0] == 2

    # Verify one run was created
    res = ch_client.query("SELECT COUNT(*) FROM runs_table")
    assert res.result_rows[0][0] == 1

    # Verify no rows in any overlapping table
    res = ch_client.query("SELECT COUNT(*) FROM overlapping_medium")
    assert res.result_rows[0][0] == 0


def test_insert_flat_with_knowledge_time(td, ch_client, sample_datetime):
    """Test inserting flat with explicit knowledge_time creates one run."""
    knowledge_time = sample_datetime - timedelta(hours=1)

    td.create_series("temperature", unit="dimensionless", overlapping=False)

    df = pd.DataFrame({
        "valid_time": [sample_datetime],
        "value": [20.5],
    })

    result = td.get_series("temperature").insert(
        data=df,
        run_start_time=sample_datetime,
        knowledge_time=knowledge_time,
    )

    assert isinstance(result.run_id, uuid.UUID)

    # Verify data was inserted
    res = ch_client.query("SELECT COUNT(*) FROM flat")
    assert res.result_rows[0][0] == 1

    res = ch_client.query("SELECT COUNT(*) FROM runs_table")
    assert res.result_rows[0][0] == 1


def test_insert_flat_point_in_time(td, ch_client, sample_datetime):
    """Test inserting multiple point-in-time flat data."""
    td.create_series("power", unit="dimensionless", overlapping=False)

    df = pd.DataFrame({
        "valid_time": [
            sample_datetime,
            sample_datetime + timedelta(hours=1),
            sample_datetime + timedelta(hours=2),
        ],
        "value": [100.5, 101.0, 102.5],
    })

    result = td.get_series("power").insert(data=df)

    res = ch_client.query("SELECT COUNT(*) FROM flat")
    assert res.result_rows[0][0] == 3


def test_insert_flat_interval(td, ch_client, sample_datetime):
    """Test inserting interval flat data with valid_time_end."""
    td.create_series("energy", unit="dimensionless", overlapping=False)

    df = pd.DataFrame({
        "valid_time": [sample_datetime],
        "valid_time_end": [sample_datetime + timedelta(hours=1)],
        "value": [500.0],
    })

    result = td.get_series("energy").insert(data=df)

    res = ch_client.query("SELECT valid_time_end FROM flat")
    assert len(res.result_rows) > 0
    assert res.result_rows[0][0] is not None


def test_insert_flat_duplicate_valid_time_raises(td, sample_datetime):
    """Flat insert with duplicate valid_times raises ValueError before hitting the DB."""
    td.create_series("dedup_err", unit="dimensionless")
    vt = sample_datetime
    df = pd.DataFrame([
        {"valid_time": vt, "value": 1.0},
        {"valid_time": vt, "value": 2.0},
    ])

    with pytest.raises(ValueError, match=r"duplicate.*valid_time"):
        td.get_series("dedup_err").insert(data=df)


def test_insert_flat_upsert(td, ch_client, sample_datetime):
    """Test that inserting the same flat valid_time twice — the latest value wins via argMax."""
    series_id = td.create_series("meter", unit="dimensionless", overlapping=False)

    df1 = pd.DataFrame({
        "valid_time": [sample_datetime],
        "value": [100.0],
    })
    td.get_series("meter").insert(data=df1)

    df2 = pd.DataFrame({
        "valid_time": [sample_datetime],
        "value": [150.0],
    })
    td.get_series("meter").insert(data=df2)

    # ClickHouse is append-only; read layer deduplicates via argMax(value, change_time)
    result = read.read_flat(ch_client, series_id=series_id)
    assert result.num_rows == 1
    assert result.column("value").to_pylist()[0] == 150.0


# =============================================================================
# Overlapping insertion tests
# =============================================================================

def test_insert_overlapping_creates_run(td, ch_client, sample_datetime):
    """Test inserting overlapping via SDK creates rows in overlapping_medium table."""
    td.create_series(
        "wind_forecast", unit="dimensionless",
        overlapping=True, retention="medium",
    )

    df = pd.DataFrame({
        "valid_time": [sample_datetime, sample_datetime + timedelta(hours=1)],
        "value": [50.0, 55.0],
    })

    result = td.get_series("wind_forecast").insert(data=df, knowledge_time=sample_datetime)

    assert isinstance(result.run_id, uuid.UUID)
    assert result.series_id > 0

    # Verify rows in overlapping_medium
    res = ch_client.query(
        "SELECT COUNT(*) FROM overlapping_medium WHERE run_id = {bid:String}",
        parameters={"bid": str(result.run_id)},
    )
    assert res.result_rows[0][0] == 2

    # Verify no rows in flat
    res = ch_client.query("SELECT COUNT(*) FROM flat")
    assert res.result_rows[0][0] == 0


def test_insert_overlapping_short_tier(td, ch_client):
    """Test inserting overlapping with retention='short'."""
    # Use a recent datetime to avoid the 6-month retention policy on overlapping_short
    recent_time = datetime.now(timezone.utc).replace(microsecond=0)

    td.create_series(
        "price_forecast", unit="dimensionless",
        overlapping=True, retention="short",
    )

    df = pd.DataFrame({
        "valid_time": [recent_time],
        "value": [42.0],
    })

    td.get_series("price_forecast").insert(data=df, knowledge_time=recent_time)

    res = ch_client.query("SELECT COUNT(*) FROM overlapping_short")
    assert res.result_rows[0][0] == 1

    res = ch_client.query("SELECT COUNT(*) FROM overlapping_medium")
    assert res.result_rows[0][0] == 0

    res = ch_client.query("SELECT COUNT(*) FROM overlapping_long")
    assert res.result_rows[0][0] == 0


def test_insert_overlapping_long_tier(td, ch_client, sample_datetime):
    """Test inserting overlapping with retention='long'."""
    td.create_series(
        "climate_forecast", unit="dimensionless",
        overlapping=True, retention="long",
    )

    df = pd.DataFrame({
        "valid_time": [sample_datetime],
        "value": [15.0],
    })

    td.get_series("climate_forecast").insert(data=df, knowledge_time=sample_datetime)

    res = ch_client.query("SELECT COUNT(*) FROM overlapping_long")
    assert res.result_rows[0][0] == 1


def test_insert_overlapping_interval(td, ch_client, sample_datetime):
    """Test inserting interval overlapping with valid_time_end."""
    td.create_series(
        "energy_forecast", unit="dimensionless",
        overlapping=True, retention="medium",
    )

    df = pd.DataFrame({
        "valid_time": [sample_datetime],
        "valid_time_end": [sample_datetime + timedelta(hours=1)],
        "value": [500.0],
    })

    td.get_series("energy_forecast").insert(
        data=df, knowledge_time=sample_datetime,
    )

    res = ch_client.query("SELECT valid_time_end FROM overlapping_medium")
    assert len(res.result_rows) > 0
    assert res.result_rows[0][0] is not None


# =============================================================================
# Timezone validation
# =============================================================================

def test_insert_timezone_aware_required(td):
    """Test that timezone-aware datetimes are required."""
    td.create_series("temp", unit="dimensionless", overlapping=False)

    df = pd.DataFrame({
        "valid_time": [datetime(2025, 1, 1, 12, 0)],  # naive datetime
        "value": [20.0],
    })

    with pytest.raises(ValueError, match="timezone-aware"):
        td.get_series("temp").insert(data=df)


# =============================================================================
# TimeSeries insert tests
# =============================================================================

def _make_simple_ts(datetimes, values, unit="dimensionless", timeseries_type=TimeSeriesType.FLAT):
    """Helper: build a SIMPLE TimeSeries from lists."""
    df = pl.DataFrame({
        "valid_time": pl.Series(datetimes).cast(pl.Datetime("us", "UTC")),
        "value": pl.Series(values, dtype=pl.Float64),
    })
    return TimeSeries.from_polars(df, unit=unit, timeseries_type=timeseries_type)


def test_insert_timeseries_flat(td, ch_client, sample_datetime):
    """TimeSeries with SIMPLE shape inserts correctly into a flat series."""
    td.create_series("ts_flat", unit="dimensionless", overlapping=False)

    ts = _make_simple_ts(
        [sample_datetime, sample_datetime + timedelta(hours=1)],
        [10.0, 20.0],
    )

    result = td.get_series("ts_flat").insert(data=ts)

    assert isinstance(result.run_id, uuid.UUID)
    assert result.series_id > 0

    res = ch_client.query("SELECT COUNT(*) FROM flat")
    assert res.result_rows[0][0] == 2


def test_insert_timeseries_overlapping(td, ch_client, sample_datetime):
    """TimeSeries with SIMPLE shape inserts correctly into an overlapping series."""
    td.create_series("ts_ovlp", unit="dimensionless", overlapping=True, retention="medium")

    ts = _make_simple_ts(
        [sample_datetime, sample_datetime + timedelta(hours=1)],
        [50.0, 55.0],
        timeseries_type=TimeSeriesType.OVERLAPPING,
    )

    result = td.get_series("ts_ovlp").insert(data=ts, knowledge_time=sample_datetime)

    assert isinstance(result.run_id, uuid.UUID)

    res = ch_client.query(
        "SELECT COUNT(*) FROM overlapping_medium WHERE run_id = {bid:String}",
        parameters={"bid": str(result.run_id)},
    )
    assert res.result_rows[0][0] == 2


def test_insert_timeseries_unit_conversion(td, ch_client, sample_datetime):
    """TimeSeries whose unit differs from the series unit is converted automatically."""
    # Series stores in MW; insert in kW — value should be divided by 1000
    td.create_series("power_mw", unit="MW", overlapping=False)

    ts = _make_simple_ts([sample_datetime], [1000.0], unit="kW")

    td.get_series("power_mw").insert(data=ts)

    res = ch_client.query("SELECT value FROM flat")
    stored = res.result_rows[0][0]
    assert abs(stored - 1.0) < 1e-9, f"Expected 1.0 MW, got {stored}"


def test_insert_df_unit_conversion(td, ch_client, sample_datetime):
    """DataFrame insert with unit kwarg converts values to the series' canonical unit."""
    # Series stores in MW; insert in kW — value should be divided by 1000
    td.create_series("power_mw_df", unit="MW", overlapping=False)

    df = pd.DataFrame({
        "valid_time": [sample_datetime],
        "value": [1000.0],
    })

    td.get_series("power_mw_df").insert(data=df, unit="kW")

    res = ch_client.query("SELECT value FROM flat")
    stored = res.result_rows[0][0]
    assert abs(stored - 1.0) < 1e-9, f"Expected 1.0 MW, got {stored}"


def test_insert_df_unit_incompatible_raises(td, ch_client, sample_datetime):
    """DataFrame insert with an incompatible unit raises IncompatibleUnitError before any DB write."""
    td.create_series("power_mw_df2", unit="MW", overlapping=False)

    df = pd.DataFrame({
        "valid_time": [sample_datetime],
        "value": [1.0],
    })

    with pytest.raises(IncompatibleUnitError):
        td.get_series("power_mw_df2").insert(data=df, unit="meter")

    # Verify nothing was written
    res = ch_client.query("SELECT COUNT(*) FROM flat")
    assert res.result_rows[0][0] == 0


def test_insert_df_unit_none_no_conversion(td, ch_client, sample_datetime):
    """DataFrame insert with unit=None (default) stores values unchanged for dimensionless series."""
    td.create_series("scalar_metric", unit="dimensionless", overlapping=False)

    df = pd.DataFrame({
        "valid_time": [sample_datetime],
        "value": [42.5],
    })

    td.get_series("scalar_metric").insert(data=df, unit=None)

    res = ch_client.query("SELECT value FROM flat")
    stored = res.result_rows[0][0]
    assert abs(stored - 42.5) < 1e-9, f"Expected 42.5, got {stored}"


def test_insert_timeseries_interval(td, ch_client, sample_datetime):
    """TimeSeries with valid_time_end column inserts interval data correctly."""
    td.create_series("energy_ts", unit="dimensionless", overlapping=False)

    df = pl.DataFrame({
        "valid_time":     pl.Series([sample_datetime]).cast(pl.Datetime("us", "UTC")),
        "valid_time_end": pl.Series([sample_datetime + timedelta(hours=1)]).cast(pl.Datetime("us", "UTC")),
        "value":          pl.Series([500.0], dtype=pl.Float64),
    })
    ts = TimeSeries.from_polars(df)

    td.get_series("energy_ts").insert(data=ts)

    res = ch_client.query("SELECT valid_time_end FROM flat")
    assert len(res.result_rows) > 0
    assert res.result_rows[0][0] is not None


def test_insert_timeseries_wrong_shape(td, sample_datetime):
    """TimeSeries with AUDIT or CORRECTED shape raises ValueError."""
    td.create_series("audit_reject", unit="dimensionless", overlapping=True)

    # Build an AUDIT DataFrame (has knowledge_time + change_time columns)
    df = pl.DataFrame({
        "knowledge_time": pl.Series([sample_datetime]).cast(pl.Datetime("us", "UTC")),
        "change_time":    pl.Series([sample_datetime]).cast(pl.Datetime("us", "UTC")),
        "valid_time":     pl.Series([sample_datetime]).cast(pl.Datetime("us", "UTC")),
        "value":          pl.Series([1.0], dtype=pl.Float64),
    })
    ts = TimeSeries.from_polars(df, timeseries_type=TimeSeriesType.OVERLAPPING)
    assert ts.shape == DataShape.AUDIT

    with pytest.raises(ValueError, match="AUDIT"):
        td.get_series("audit_reject").insert(data=ts)


def _make_versioned_ts(knowledge_times, valid_times, values, unit="dimensionless"):
    """Helper: build a VERSIONED TimeSeries from lists."""
    df = pl.DataFrame({
        "knowledge_time": pl.Series(knowledge_times).cast(pl.Datetime("us", "UTC")),
        "valid_time":     pl.Series(valid_times).cast(pl.Datetime("us", "UTC")),
        "value":          pl.Series(values, dtype=pl.Float64),
    })
    ts = TimeSeries.from_polars(df, unit=unit, timeseries_type=TimeSeriesType.OVERLAPPING)
    assert ts.shape == DataShape.VERSIONED
    return ts


# =============================================================================
# VERSIONED insert tests
# =============================================================================

def test_insert_versioned_single_kt(td, ch_client, sample_datetime):
    """VERSIONED TimeSeries with one unique knowledge_time creates exactly one run."""
    td.create_series(
        "v_single", unit="dimensionless",
        overlapping=True, retention="medium",
    )
    kt = sample_datetime
    ts = _make_versioned_ts(
        knowledge_times=[kt, kt],
        valid_times=[sample_datetime, sample_datetime + timedelta(hours=1)],
        values=[10.0, 20.0],
    )
    result = td.get_series("v_single").insert(data=ts)

    assert isinstance(result.run_id, uuid.UUID)

    res = ch_client.query("SELECT COUNT(*) FROM runs_table")
    assert res.result_rows[0][0] == 1
    res = ch_client.query("SELECT COUNT(*) FROM overlapping_medium")
    assert res.result_rows[0][0] == 2


def test_insert_versioned_multi_kt(td, ch_client, sample_datetime):
    """VERSIONED TimeSeries with multiple unique knowledge_times creates exactly one run."""
    td.create_series(
        "v_multi", unit="dimensionless",
        overlapping=True, retention="medium",
    )
    kt1 = sample_datetime
    kt2 = sample_datetime + timedelta(hours=1)
    kt3 = sample_datetime + timedelta(hours=2)

    ts = _make_versioned_ts(
        knowledge_times=[kt1, kt1, kt2, kt2, kt3],
        valid_times=[
            sample_datetime,
            sample_datetime + timedelta(hours=1),
            sample_datetime,
            sample_datetime + timedelta(hours=1),
            sample_datetime,
        ],
        values=[1.0, 2.0, 1.5, 2.5, 1.2],
    )
    result = td.get_series("v_multi").insert(data=ts)

    assert isinstance(result.run_id, uuid.UUID)

    # One run per insert() call regardless of unique knowledge_time count
    res = ch_client.query("SELECT COUNT(*) FROM runs_table")
    assert res.result_rows[0][0] == 1
    res = ch_client.query("SELECT COUNT(*) FROM overlapping_medium")
    assert res.result_rows[0][0] == 5


def test_insert_versioned_into_flat(td, ch_client, sample_datetime):
    """VERSIONED TimeSeries inserted into a flat series stores per-row knowledge_times."""
    td.create_series("v_flat_ok", unit="dimensionless", overlapping=False)

    kt1 = sample_datetime
    kt2 = sample_datetime + timedelta(hours=1)
    ts = _make_versioned_ts(
        knowledge_times=[kt1, kt2],
        valid_times=[sample_datetime, sample_datetime + timedelta(hours=1)],
        values=[1.0, 2.0],
    )

    result = td.get_series("v_flat_ok").insert(data=ts)
    assert isinstance(result.run_id, uuid.UUID)

    res = ch_client.query(
        "SELECT knowledge_time FROM flat WHERE series_id = {sid:Int64} ORDER BY valid_time",
        parameters={"sid": result.series_id},
    )
    rows = res.result_rows
    assert len(rows) == 2


def test_insert_versioned_kt_kwarg_ambiguity_raises(td, sample_datetime):
    """Passing knowledge_time kwarg with a VERSIONED insert raises ValueError (ambiguous)."""
    td.create_series(
        "v_ambiguous", unit="dimensionless",
        overlapping=True, retention="medium",
    )
    ts = _make_versioned_ts(
        knowledge_times=[sample_datetime],
        valid_times=[sample_datetime],
        values=[42.0],
    )

    with pytest.raises(ValueError, match="Ambiguous"):
        td.get_series("v_ambiguous").insert(
            data=ts,
            knowledge_time=sample_datetime,
        )


# =============================================================================
# knowledge_time in DataFrame tests
# =============================================================================

def test_insert_df_with_kt_column_overlapping(td, ch_client, sample_datetime):
    """DataFrame with knowledge_time column is accepted and routes correctly."""
    td.create_series(
        "df_kt_col", unit="dimensionless",
        overlapping=True, retention="medium",
    )

    kt1 = sample_datetime
    kt2 = sample_datetime + timedelta(hours=1)

    df = pd.DataFrame({
        "knowledge_time": [kt1, kt1, kt2],
        "valid_time": [
            sample_datetime,
            sample_datetime + timedelta(hours=1),
            sample_datetime,
        ],
        "value": [10.0, 20.0, 11.0],
    })

    result = td.get_series("df_kt_col").insert(data=df)

    assert isinstance(result.run_id, uuid.UUID)

    # One run per insert() call
    res = ch_client.query("SELECT COUNT(*) FROM runs_table")
    assert res.result_rows[0][0] == 1
    res = ch_client.query("SELECT COUNT(*) FROM overlapping_medium")
    assert res.result_rows[0][0] == 3


def test_insert_df_kt_ambiguity_raises(td, sample_datetime):
    """DataFrame with knowledge_time column + knowledge_time kwarg raises ValueError."""
    td.create_series(
        "df_kt_ambig", unit="dimensionless",
        overlapping=True, retention="medium",
    )

    df = pd.DataFrame({
        "knowledge_time": [sample_datetime],
        "valid_time": [sample_datetime],
        "value": [42.0],
    })

    with pytest.raises(ValueError, match="Ambiguous"):
        td.get_series("df_kt_ambig").insert(
            data=df,
            knowledge_time=sample_datetime,
        )


def test_insert_df_without_kt_defaults_to_now(td, ch_client, sample_datetime):
    """DataFrame without knowledge_time and no kwarg stores rows with a recent knowledge_time."""
    td.create_series("df_no_kt", unit="dimensionless", overlapping=False)

    before = datetime.now(timezone.utc)

    df = pd.DataFrame({
        "valid_time": [sample_datetime],
        "value": [99.0],
    })

    td.get_series("df_no_kt").insert(data=df)

    after = datetime.now(timezone.utc)

    res = ch_client.query("SELECT knowledge_time FROM flat")
    assert len(res.result_rows) > 0
    kt = res.result_rows[0][0]
    # ClickHouse DateTime64('UTC') returns timezone-aware datetime
    if kt.tzinfo is None:
        kt = kt.replace(tzinfo=timezone.utc)
    assert before <= kt <= after


# =============================================================================
# write() tests
# =============================================================================

def test_write_long_format_pandas(td, ch_client, sample_datetime):
    """write() inserts multi-series long-format Pandas data in one run."""
    td.create_series("sensor_a", unit="dimensionless")
    td.create_series("sensor_b", unit="dimensionless")

    df = pd.DataFrame({
        "metric": ["sensor_a", "sensor_a", "sensor_b", "sensor_b"],
        "valid_time": [
            sample_datetime,
            sample_datetime + timedelta(hours=1),
            sample_datetime,
            sample_datetime + timedelta(hours=1),
        ],
        "value": [1.0, 2.0, 3.0, 4.0],
    })

    results = td.write(df, name_col="metric")

    assert len(results) == 2
    assert len({r.run_id for r in results}) == 1  # one run for all

    res = ch_client.query("SELECT COUNT(*) FROM flat")
    assert res.result_rows[0][0] == 4
    res = ch_client.query("SELECT COUNT(*) FROM runs_table")
    assert res.result_rows[0][0] == 1


def test_write_long_format_polars(td, ch_client, sample_datetime):
    """write() inserts multi-series long-format Polars data in one run."""
    td.create_series("sensor_a", unit="dimensionless")
    td.create_series("sensor_b", unit="dimensionless")

    df = pl.DataFrame({
        "metric": ["sensor_a", "sensor_a", "sensor_b", "sensor_b"],
        "valid_time": pl.Series([
            sample_datetime,
            sample_datetime + timedelta(hours=1),
            sample_datetime,
            sample_datetime + timedelta(hours=1),
        ]).cast(pl.Datetime("us", "UTC")),
        "value": pl.Series([1.0, 2.0, 3.0, 4.0], dtype=pl.Float64),
    })

    results = td.write(df, name_col="metric")

    assert len(results) == 2
    assert len({r.run_id for r in results}) == 1

    res = ch_client.query("SELECT COUNT(*) FROM flat")
    assert res.result_rows[0][0] == 4


def test_write_with_label_cols(td, ch_client, sample_datetime):
    """write() with label_cols routes to the correct series."""
    td.create_series("power", unit="dimensionless", labels={"site": "A"})
    td.create_series("power", unit="dimensionless", labels={"site": "B"})

    df = pd.DataFrame({
        "metric": ["power", "power"],
        "site": ["A", "B"],
        "valid_time": [sample_datetime, sample_datetime],
        "value": [10.0, 20.0],
    })

    results = td.write(df, name_col="metric", label_cols=["site"])

    assert len(results) == 2

    res = ch_client.query("SELECT COUNT(*) FROM flat")
    assert res.result_rows[0][0] == 2


def test_write_missing_series_raises(td, sample_datetime):
    """write() raises ValueError before DB write if any series doesn't exist."""
    df = pd.DataFrame({
        "metric": ["nonexistent"],
        "valid_time": [sample_datetime],
        "value": [1.0],
    })

    with pytest.raises(ValueError, match="No series found"):
        td.write(df, name_col="metric")


def test_write_mixed_flat_overlapping(td, ch_client, sample_datetime):
    """write() with flat and overlapping series inserts both in one transaction."""
    td.create_series("flat_metric", unit="dimensionless", overlapping=False)
    td.create_series("ovlp_metric", unit="dimensionless", overlapping=True, retention="medium")

    df = pd.DataFrame({
        "metric": ["flat_metric", "ovlp_metric"],
        "valid_time": [sample_datetime, sample_datetime],
        "value": [1.0, 2.0],
    })

    results = td.write(df, name_col="metric", knowledge_time=sample_datetime)

    assert len(results) == 2
    assert len({r.run_id for r in results}) == 1  # single shared run

    res = ch_client.query("SELECT COUNT(*) FROM flat")
    assert res.result_rows[0][0] == 1
    res = ch_client.query("SELECT COUNT(*) FROM overlapping_medium")
    assert res.result_rows[0][0] == 1
    res = ch_client.query("SELECT COUNT(*) FROM runs_table")
    assert res.result_rows[0][0] == 1


def test_write_unit_incompatible_raises(td, ch_client, sample_datetime):
    """write() raises IncompatibleUnitError before DB write for incompatible units."""
    from timedb import IncompatibleUnitError

    td.create_series("power_mw", unit="MW")

    df = pd.DataFrame({
        "metric": ["power_mw"],
        "valid_time": [sample_datetime],
        "value": [1.0],
    })

    with pytest.raises(IncompatibleUnitError):
        td.write(df, name_col="metric", unit="kg")

    res = ch_client.query("SELECT COUNT(*) FROM flat")
    assert res.result_rows[0][0] == 0




def test_write_passthrough_change_time(td, ch_client, sample_datetime):
    """write() preserves an explicit change_time column rather than using DEFAULT."""
    td.create_series("ct_metric", unit="dimensionless")

    explicit_ct = sample_datetime - timedelta(days=30)

    df = pd.DataFrame({
        "metric": ["ct_metric"],
        "valid_time": [sample_datetime],
        "value": [42.0],
        "change_time": [explicit_ct],
    })

    td.write(df, name_col="metric")

    res = ch_client.query("SELECT change_time FROM flat")
    stored_ct = res.result_rows[0][0]
    if stored_ct.tzinfo is None:
        stored_ct = stored_ct.replace(tzinfo=timezone.utc)
    assert abs((stored_ct - explicit_ct).total_seconds()) < 1


def test_write_upsert_stamps_change_time(td, ch_client, sample_datetime):
    """Two writes to the same valid_time produce distinct change_times; read returns latest."""
    series_id = td.create_series("upsert_ct", unit="dimensionless")

    vt = sample_datetime
    df1 = pd.DataFrame({"metric": ["upsert_ct"], "valid_time": [vt], "value": [1.0]})
    td.write(df1, name_col="metric")

    df2 = pd.DataFrame({"metric": ["upsert_ct"], "valid_time": [vt], "value": [2.0]})
    td.write(df2, name_col="metric")

    # Read latest via argMax — should return value 2.0
    result = read.read_flat(ch_client, series_id=series_id)
    assert result.num_rows == 1
    assert result.column("value").to_pylist()[0] == 2.0


def test_write_passthrough_annotation(td, ch_client, sample_datetime):
    """write() forwards an annotation column to the database unchanged."""
    td.create_series("ann_metric", unit="dimensionless")

    df = pd.DataFrame({
        "metric": ["ann_metric"],
        "valid_time": [sample_datetime],
        "value": [7.0],
        "annotation": ["corrected value"],
    })

    td.write(df, name_col="metric")

    res = ch_client.query("SELECT annotation FROM flat")
    assert len(res.result_rows) > 0
    assert res.result_rows[0][0] == "corrected value"


# =============================================================================
# write() — run_cols: unreserved columns → run_params
# =============================================================================

def test_write_run_cols_unreserved_creates_multiple_runs(td, ch_client, sample_datetime):
    """run_cols with an unreserved column creates one run per unique value."""
    td.create_series("power", unit="dimensionless", labels={"site": "A"})

    # Different valid_times per model: flat series enforce unique (series_id, valid_time)
    df = pd.DataFrame({
        "name":       ["power", "power"],
        "site":       ["A", "A"],
        "model":      ["ECMWF", "GFS"],
        "valid_time": [sample_datetime, sample_datetime + timedelta(hours=1)],
        "value":      [1.0, 2.0],
    })

    results = td.write(df, run_cols=["model"])

    assert len(results) == 2
    assert len({r.run_id for r in results}) == 2  # two distinct runs

    res = ch_client.query("SELECT COUNT(*) FROM runs_table")
    assert res.result_rows[0][0] == 2


def test_write_run_cols_unreserved_run_params_json(td, ch_client, sample_datetime):
    """Unreserved run_col values are packed into run_params JSON on the run record."""
    td.create_series("power", unit="dimensionless", labels={"site": "A"})

    df = pd.DataFrame({
        "name":       ["power", "power"],
        "site":       ["A", "A"],
        "model":      ["ECMWF", "GFS"],
        "valid_time": [sample_datetime, sample_datetime + timedelta(hours=1)],
        "value":      [1.0, 2.0],
    })

    td.write(df, run_cols=["model"])

    res = ch_client.query("SELECT run_params FROM runs_table ORDER BY inserted_at")
    rows = res.result_rows
    stored_models = {json.loads(row[0])["model"] for row in rows}
    assert stored_models == {"ECMWF", "GFS"}


def test_write_run_cols_result_count_n_runs_times_m_series(td, ch_client, sample_datetime):
    """Returns N×M InsertResults: one per (run, series) combination."""
    td.create_series("power", unit="dimensionless", labels={"site": "A"})
    td.create_series("power", unit="dimensionless", labels={"site": "B"})

    models = ["ECMWF", "GFS", "AROME"]
    rows = []
    for i, model in enumerate(models):
        for site in ["A", "B"]:
            # Each model gets a distinct valid_time to avoid (series_id, valid_time) collisions
            rows.append({"name": "power", "site": site, "model": model,
                         "valid_time": sample_datetime + timedelta(hours=i), "value": 1.0})

    df = pd.DataFrame(rows)
    results = td.write(df, run_cols=["model"])

    # 3 runs × 2 series = 6 results
    assert len(results) == 6
    assert len({r.run_id for r in results}) == 3


def test_write_run_cols_shared_workflow_id_kwarg(td, ch_client, sample_datetime):
    """Global workflow_id kwarg is applied to all runs when not in run_cols."""
    td.create_series("power", unit="dimensionless", labels={"site": "A"})

    df = pd.DataFrame({
        "name":       ["power", "power"],
        "site":       ["A", "A"],
        "model":      ["ECMWF", "GFS"],
        "valid_time": [sample_datetime, sample_datetime + timedelta(hours=1)],
        "value":      [1.0, 2.0],
    })

    td.write(df, run_cols=["model"], workflow_id="shared-run")

    res = ch_client.query("SELECT DISTINCT workflow_id FROM runs_table")
    rows = res.result_rows
    assert len(rows) == 1
    assert rows[0][0] == "shared-run"


def test_write_run_cols_global_run_params_merged(td, ch_client, sample_datetime):
    """Global run_params kwarg is merged with per-run unreserved col values."""
    td.create_series("power", unit="dimensionless", labels={"site": "A"})

    df = pd.DataFrame({
        "name":       ["power", "power"],
        "site":       ["A", "A"],
        "model":      ["ECMWF", "GFS"],
        "valid_time": [sample_datetime, sample_datetime + timedelta(hours=1)],
        "value":      [1.0, 2.0],
    })

    td.write(df, run_cols=["model"], run_params={"source": "nwp"})

    res = ch_client.query("SELECT run_params FROM runs_table ORDER BY inserted_at")
    rows = res.result_rows
    for row in rows:
        params = json.loads(row[0])
        assert params["source"] == "nwp"       # global key present
        assert params["model"] in {"ECMWF", "GFS"}  # per-run key present


def test_write_run_cols_compound_key(td, ch_client, sample_datetime):
    """run_cols with multiple columns creates one run per unique combination."""
    td.create_series("power", unit="dimensionless", labels={"site": "A"})

    # Each (model, run) combo gets a distinct valid_time to avoid flat uniqueness violations
    df = pd.DataFrame({
        "name":       ["power"] * 4,
        "site":       ["A"] * 4,
        "model":      ["ECMWF", "ECMWF", "GFS", "GFS"],
        "run":        ["06z", "18z", "06z", "18z"],
        "valid_time": [
            sample_datetime,
            sample_datetime + timedelta(hours=1),
            sample_datetime + timedelta(hours=2),
            sample_datetime + timedelta(hours=3),
        ],
        "value":      [1.0, 2.0, 3.0, 4.0],
    })

    results = td.write(df, run_cols=["model", "run"])

    assert len(results) == 4  # 4 unique (model, run) combos × 1 series
    assert len({r.run_id for r in results}) == 4

    res = ch_client.query("SELECT COUNT(*) FROM runs_table")
    assert res.result_rows[0][0] == 4


# =============================================================================
# write() — run_cols: reserved columns → native runs_table fields
# =============================================================================

def test_write_run_cols_reserved_workflow_id_stored_natively(td, ch_client, sample_datetime):
    """workflow_id in run_cols maps to the native runs_table field, not run_params."""
    td.create_series("power", unit="dimensionless", labels={"site": "A"})

    df = pd.DataFrame({
        "name":        ["power", "power"],
        "site":        ["A", "A"],
        "workflow_id": ["run-06z", "run-18z"],
        "valid_time":  [sample_datetime, sample_datetime + timedelta(hours=12)],
        "value":       [1.0, 2.0],
    })

    td.write(df, run_cols=["workflow_id"])

    res = ch_client.query("SELECT workflow_id, run_params FROM runs_table ORDER BY inserted_at")
    rows = res.result_rows
    assert len(rows) == 2
    stored_wf_ids = {row[0] for row in rows}
    assert stored_wf_ids == {"run-06z", "run-18z"}
    # workflow_id is a reserved field — should NOT appear in run_params
    for row in rows:
        params = json.loads(row[1]) if row[1] else None
        assert params is None or "workflow_id" not in params


def test_write_run_cols_reserved_run_start_finish_time(td, ch_client, sample_datetime):
    """run_start_time and run_finish_time in run_cols are stored as native fields."""
    td.create_series("power", unit="dimensionless", labels={"site": "A"})

    t_start_a = sample_datetime
    t_finish_a = sample_datetime + timedelta(hours=1)
    t_start_b = sample_datetime + timedelta(hours=2)
    t_finish_b = sample_datetime + timedelta(hours=3)

    df = pd.DataFrame({
        "name":             ["power", "power"],
        "site":             ["A", "A"],
        "run_start_time": [t_start_a, t_start_b],
        "run_finish_time":[t_finish_a, t_finish_b],
        "valid_time":       [sample_datetime, sample_datetime + timedelta(hours=2)],
        "value":            [1.0, 2.0],
    })

    td.write(df, run_cols=["run_start_time", "run_finish_time"])

    res = ch_client.query(
        "SELECT run_start_time, run_finish_time, run_params "
        "FROM runs_table ORDER BY run_start_time"
    )
    rows = res.result_rows
    assert len(rows) == 2
    stored_start = rows[0][0]
    if stored_start.tzinfo is None:
        stored_start = stored_start.replace(tzinfo=timezone.utc)
    assert stored_start == t_start_a
    # not packed into run_params (ClickHouse defaults to '{}')
    params = json.loads(rows[0][2]) if rows[0][2] else {}
    assert "run_start_time" not in params
    assert "run_finish_time" not in params


def test_write_run_cols_mix_reserved_and_unreserved(td, ch_client, sample_datetime):
    """Reserved cols go to native fields; unreserved cols go to run_params."""
    td.create_series("power", unit="dimensionless", labels={"site": "A"})

    df = pd.DataFrame({
        "name":        ["power", "power"],
        "site":        ["A", "A"],
        "workflow_id": ["run-A", "run-B"],
        "model":       ["ECMWF", "GFS"],
        "valid_time":  [sample_datetime, sample_datetime + timedelta(hours=1)],
        "value":       [1.0, 2.0],
    })

    td.write(df, run_cols=["workflow_id", "model"])

    res = ch_client.query("SELECT workflow_id, run_params FROM runs_table ORDER BY inserted_at")
    rows = res.result_rows
    assert len(rows) == 2
    for row in rows:
        wf_id, params_str = row
        params = json.loads(params_str)
        assert wf_id in {"run-A", "run-B"}
        assert "model" in params
        assert "workflow_id" not in (params or {})


# =============================================================================
# write() — run_cols: validation errors
# =============================================================================

def test_write_run_cols_missing_column_raises(td, sample_datetime):
    """run_cols referencing a column absent from the DataFrame raises ValueError."""
    td.create_series("power", unit="dimensionless")

    df = pd.DataFrame({
        "name":       ["power"],
        "valid_time": [sample_datetime],
        "value":      [1.0],
    })

    with pytest.raises(ValueError, match="run_cols column"):
        td.write(df, run_cols=["nonexistent_col"])


def test_write_run_cols_overlaps_label_cols_raises(td, sample_datetime):
    """run_cols overlapping with label_cols raises ValueError."""
    td.create_series("power", unit="dimensionless", labels={"site": "A"})

    df = pd.DataFrame({
        "name":       ["power"],
        "site":       ["A"],
        "valid_time": [sample_datetime],
        "value":      [1.0],
    })

    with pytest.raises(ValueError, match="run_cols and label_cols cannot overlap"):
        td.write(df, label_cols=["site"], run_cols=["site"])


# =============================================================================
# write() — run_cols: knowledge_time interactions
# =============================================================================

def test_write_run_cols_with_knowledge_time_kwarg(td, ch_client, sample_datetime):
    """knowledge_time kwarg is broadcast to all rows across all runs."""
    td.create_series("power", unit="dimensionless", overlapping=True, retention="medium")

    kt = sample_datetime + timedelta(hours=6)

    # Different valid_times per model so overlapping uniqueness (series_id, valid_time,
    # knowledge_time, change_time) is not violated when kt is the same for all rows.
    df = pd.DataFrame({
        "name":       ["power", "power"],
        "model":      ["ECMWF", "GFS"],
        "valid_time": [sample_datetime, sample_datetime + timedelta(hours=1)],
        "value":      [1.0, 2.0],
    })

    td.write(df, run_cols=["model"], knowledge_time=kt)

    res = ch_client.query("SELECT DISTINCT knowledge_time FROM overlapping_medium")
    rows = res.result_rows
    assert len(rows) == 1
    stored_kt = rows[0][0]
    if stored_kt.tzinfo is None:
        stored_kt = stored_kt.replace(tzinfo=timezone.utc)
    assert abs((stored_kt - kt).total_seconds()) < 1


def test_write_run_cols_with_per_row_knowledge_time(td, ch_client, sample_datetime):
    """Per-row knowledge_time column works correctly alongside run_cols."""
    td.create_series("power", unit="dimensionless", overlapping=True, retention="medium")

    kt_a = sample_datetime + timedelta(hours=6)
    kt_b = sample_datetime + timedelta(hours=12)

    df = pd.DataFrame({
        "name":           ["power", "power"],
        "model":          ["ECMWF", "GFS"],
        "valid_time":     [sample_datetime, sample_datetime],
        "knowledge_time": [kt_a, kt_b],
        "value":          [1.0, 2.0],
    })

    results = td.write(df, run_cols=["model"])
    assert len(results) == 2

    res = ch_client.query("SELECT knowledge_time FROM overlapping_medium ORDER BY knowledge_time")
    rows = res.result_rows
    assert len(rows) == 2
    stored_kts = {r[0] if r[0].tzinfo is not None else r[0].replace(tzinfo=timezone.utc) for r in rows}
    assert len(stored_kts) == 2  # two distinct knowledge_times stored


def test_write_run_cols_knowledge_time_kwarg_and_column_raises(td, sample_datetime):
    """Providing both knowledge_time kwarg and a knowledge_time column raises ValueError."""
    td.create_series("power", unit="dimensionless")

    df = pd.DataFrame({
        "name":           ["power"],
        "model":          ["ECMWF"],
        "valid_time":     [sample_datetime],
        "knowledge_time": [sample_datetime],
        "value":          [1.0],
    })

    with pytest.raises(ValueError, match="Ambiguous knowledge_time"):
        td.write(df, run_cols=["model"], knowledge_time=sample_datetime)


# =============================================================================
# write() — run_cols: overlapping series
# =============================================================================

def test_write_run_cols_overlapping_series(td, ch_client, sample_datetime):
    """run_cols creates multiple runs in the overlapping table."""
    td.create_series("forecast", unit="dimensionless", overlapping=True, retention="medium")

    kt_ecmwf = sample_datetime + timedelta(hours=6)
    kt_gfs = sample_datetime + timedelta(hours=7)

    # Per-row knowledge_time distinguishes the two model runs at the same valid_times
    df = pd.DataFrame({
        "name":           ["forecast"] * 4,
        "model":          ["ECMWF", "ECMWF", "GFS", "GFS"],
        "valid_time":     [sample_datetime, sample_datetime + timedelta(hours=1)] * 2,
        "knowledge_time": [kt_ecmwf, kt_ecmwf, kt_gfs, kt_gfs],
        "value":          [10.0, 11.0, 12.0, 13.0],
    })

    results = td.write(df, run_cols=["model"])

    assert len(results) == 2
    assert len({r.run_id for r in results}) == 2

    res = ch_client.query("SELECT COUNT(*) FROM overlapping_medium")
    assert res.result_rows[0][0] == 4
    res = ch_client.query("SELECT COUNT(*) FROM runs_table")
    assert res.result_rows[0][0] == 2


# =============================================================================
# write() — unit handling
# =============================================================================

def test_write_unit_kwarg_converts_all_series(td, ch_client, sample_datetime):
    """unit= kwarg triggers pint conversion for all series in the write call."""
    td.create_series("power", unit="MW", labels={"site": "A"})
    td.create_series("power", unit="MW", labels={"site": "B"})

    df = pd.DataFrame({
        "name":       ["power", "power"],
        "site":       ["A", "B"],
        "valid_time": [sample_datetime, sample_datetime],
        "value":      [1000.0, 2000.0],  # in kW — should be stored as 1.0 and 2.0 MW
    })

    td.write(df, unit="kW")

    res = ch_client.query("SELECT value FROM flat ORDER BY series_id")
    rows = res.result_rows
    assert len(rows) == 2
    assert abs(rows[0][0] - 1.0) < 1e-9
    assert abs(rows[1][0] - 2.0) < 1e-9


def test_write_per_row_unit_column_mixed_units(td, ch_client, sample_datetime):
    """unit column applies per-row pint conversion — mixed kW and MW stored as MW."""
    td.create_series("power", unit="MW", labels={"site": "A"})

    df = pd.DataFrame({
        "name":       ["power", "power"],
        "site":       ["A", "A"],
        "valid_time": [sample_datetime, sample_datetime + timedelta(hours=1)],
        "unit":       ["MW", "kW"],
        "value":      [3.0, 5000.0],  # 3 MW and 5000 kW → both should store as 3.0 and 5.0
    })

    td.write(df)

    res = ch_client.query("SELECT value FROM flat ORDER BY valid_time")
    rows = res.result_rows
    assert len(rows) == 2
    assert abs(rows[0][0] - 3.0) < 1e-9
    assert abs(rows[1][0] - 5.0) < 1e-9


def test_write_per_row_unit_column_null_raises(td, ch_client, sample_datetime):
    """unit column with null values raises ValueError before any DB write."""
    td.create_series("power", unit="MW")

    df = pd.DataFrame({
        "name":       ["power", "power"],
        "valid_time": [sample_datetime, sample_datetime + timedelta(hours=1)],
        "unit":       ["MW", None],
        "value":      [1.0, 2.0],
    })

    with pytest.raises(ValueError, match="null"):
        td.write(df)

    res = ch_client.query("SELECT COUNT(*) FROM flat")
    assert res.result_rows[0][0] == 0


def test_write_unit_col_and_unit_kwarg_raises(td, sample_datetime):
    """Providing both unit column and unit= kwarg raises ValueError."""
    td.create_series("power", unit="MW")

    df = pd.DataFrame({
        "name":       ["power"],
        "valid_time": [sample_datetime],
        "unit":       ["kW"],
        "value":      [1000.0],
    })

    with pytest.raises(ValueError, match="unit"):
        td.write(df, unit="kW")


# =============================================================================
# write() — label and routing
# =============================================================================

def test_write_auto_infers_label_cols_excluding_run_cols(td, sample_datetime):
    """label_cols are auto-inferred — run_cols are excluded from the inferred set."""
    td.create_series("power", unit="dimensionless", labels={"site": "A"})

    df = pd.DataFrame({
        "name":       ["power", "power"],
        "site":       ["A", "A"],
        "model":      ["ECMWF", "GFS"],  # run_col — should NOT be inferred as label
        "valid_time": [sample_datetime, sample_datetime + timedelta(hours=1)],
        "value":      [1.0, 2.0],
    })

    # Would raise "No series found" if 'model' were inferred as a label_col,
    # since no series has labels={"site": "A", "model": "ECMWF"}
    results = td.write(df, run_cols=["model"])
    assert len(results) == 2


def test_write_default_name_col(td, ch_client, sample_datetime):
    """name_col defaults to 'name' — no explicit name_col argument needed."""
    td.create_series("temperature", unit="dimensionless")

    df = pd.DataFrame({
        "name":       ["temperature", "temperature"],
        "valid_time": [sample_datetime, sample_datetime + timedelta(hours=1)],
        "value":      [20.0, 21.0],
    })

    results = td.write(df)
    assert len(results) == 1

    res = ch_client.query("SELECT COUNT(*) FROM flat")
    assert res.result_rows[0][0] == 2


def test_write_explicit_empty_label_cols(td, sample_datetime):
    """label_cols=[] works for series with no labels."""
    td.create_series("global_metric", unit="dimensionless")

    df = pd.DataFrame({
        "name":       ["global_metric"],
        "valid_time": [sample_datetime],
        "value":      [42.0],
    })

    results = td.write(df, label_cols=[])
    assert len(results) == 1


def test_write_name_col_not_in_dataframe_raises(td, sample_datetime):
    """Missing name_col raises ValueError before any DB interaction."""
    df = pd.DataFrame({
        "metric":     ["power"],
        "valid_time": [sample_datetime],
        "value":      [1.0],
    })

    with pytest.raises(ValueError, match="name"):
        td.write(df)  # default name_col="name" not present


# =============================================================================
# write() — optional passthrough columns
# =============================================================================

def test_write_passthrough_valid_time_end(td, ch_client, sample_datetime):
    """valid_time_end column is forwarded to the flat table unchanged."""
    td.create_series("interval_metric", unit="dimensionless")

    vte = sample_datetime + timedelta(hours=1)

    df = pd.DataFrame({
        "name":          ["interval_metric"],
        "valid_time":    [sample_datetime],
        "valid_time_end":[vte],
        "value":         [5.0],
    })

    td.write(df)

    res = ch_client.query("SELECT valid_time_end FROM flat")
    assert len(res.result_rows) > 0
    stored_vte = res.result_rows[0][0]
    if stored_vte.tzinfo is None:
        stored_vte = stored_vte.replace(tzinfo=timezone.utc)
    assert abs((stored_vte - vte).total_seconds()) < 1


def test_write_passthrough_changed_by(td, ch_client, sample_datetime):
    """changed_by column is forwarded to the flat table unchanged."""
    td.create_series("audited_metric", unit="dimensionless")

    df = pd.DataFrame({
        "name":       ["audited_metric"],
        "valid_time": [sample_datetime],
        "value":      [9.0],
        "changed_by": ["pipeline-v2"],
    })

    td.write(df)

    res = ch_client.query("SELECT changed_by FROM flat")
    assert len(res.result_rows) > 0
    assert res.result_rows[0][0] == "pipeline-v2"


# =============================================================================
# write() series_col tests
# =============================================================================

def test_write_series_col_basic(td, ch_client, sample_datetime):
    """write(series_col=...) inserts data using series IDs directly."""
    sid_a = td.create_series("sensor_a", unit="dimensionless")
    sid_b = td.create_series("sensor_b", unit="dimensionless")

    df = pd.DataFrame({
        "sid": [sid_a, sid_a, sid_b, sid_b],
        "valid_time": [
            sample_datetime,
            sample_datetime + timedelta(hours=1),
            sample_datetime,
            sample_datetime + timedelta(hours=1),
        ],
        "value": [1.0, 2.0, 3.0, 4.0],
    })

    results = td.write(df, series_col="sid")

    assert len(results) == 2
    assert len({r.run_id for r in results}) == 1

    res = ch_client.query("SELECT COUNT(*) FROM flat")
    assert res.result_rows[0][0] == 4


def test_write_series_col_invalid_id_raises(td, sample_datetime):
    """write(series_col=...) raises ValueError for unknown series IDs."""
    td.create_series("existing", unit="dimensionless")

    df = pd.DataFrame({
        "series_id": [999999],
        "valid_time": [sample_datetime],
        "value": [1.0],
    })

    with pytest.raises(ValueError, match="No series found"):
        td.write(df, series_col="series_id")


def test_write_series_col_with_unit_kwarg(td, ch_client, sample_datetime):
    """write(series_col=..., unit=...) applies unit conversion."""
    sid = td.create_series("power", unit="MW")

    df = pd.DataFrame({
        "sid": [sid],
        "valid_time": [sample_datetime],
        "value": [1000.0],  # 1000 kW = 1.0 MW
    })

    td.write(df, series_col="sid", unit="kW")

    res = ch_client.query("SELECT value FROM flat")
    assert abs(res.result_rows[0][0] - 1.0) < 1e-9


def test_write_series_col_with_run_cols(td, ch_client, sample_datetime):
    """write(series_col=..., run_cols=...) creates multiple runs."""
    sid = td.create_series("metric", unit="dimensionless", overlapping=True)

    df = pd.DataFrame({
        "sid": [sid, sid],
        "model": ["v1", "v2"],
        "valid_time": [sample_datetime, sample_datetime],
        "value": [1.0, 2.0],
    })

    results = td.write(df, series_col="sid", run_cols=["model"])

    assert len(results) == 2
    assert len({r.run_id for r in results}) == 2


def test_write_series_col_mutual_exclusivity_name_col(td, sample_datetime):
    """write() raises ValueError when both series_col and name_col are set."""
    df = pd.DataFrame({
        "series_id": [1],
        "name": ["x"],
        "valid_time": [sample_datetime],
        "value": [1.0],
    })

    with pytest.raises(ValueError, match="mutually exclusive"):
        td.write(df, series_col="series_id", name_col="name")


def test_write_series_col_mutual_exclusivity_label_cols(td, sample_datetime):
    """write() raises ValueError when both series_col and label_cols are set."""
    df = pd.DataFrame({
        "series_id": [1],
        "site": ["x"],
        "valid_time": [sample_datetime],
        "value": [1.0],
    })

    with pytest.raises(ValueError, match="mutually exclusive"):
        td.write(df, series_col="series_id", label_cols=["site"])
