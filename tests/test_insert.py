"""Tests for inserting flat and overlapping data."""
import uuid
import pytest
import psycopg
from datetime import datetime, timezone, timedelta
import pandas as pd
import pyarrow as pa

from timedb import TimeSeries, DataShape
from timedatamodel.enums import TimeSeriesType

_TS_TYPE = pa.timestamp("us", tz="UTC")


# =============================================================================
# Flat insertion tests
# =============================================================================

def test_insert_flat_creates_batch(td, clean_db, sample_datetime):
    """Test inserting flat via SDK creates one batch and rows in the flat table."""
    td.create_series(name="temperature", unit="dimensionless", overlapping=False)

    df = pd.DataFrame({
        "valid_time": [sample_datetime, sample_datetime + timedelta(hours=1)],
        "value": [20.5, 21.0],
    })

    result = td.get_series("temperature").insert(data=df)

    assert isinstance(result.batch_id, uuid.UUID)
    assert result.series_id > 0

    # Verify rows in flat table
    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM flat")
            assert cur.fetchone()[0] == 2

            # Verify one batch was created
            cur.execute("SELECT COUNT(*) FROM batches_table")
            assert cur.fetchone()[0] == 1

            # Verify no rows in any overlapping table
            cur.execute("SELECT COUNT(*) FROM overlapping_medium")
            assert cur.fetchone()[0] == 0


def test_insert_flat_with_knowledge_time(td, clean_db, sample_datetime):
    """Test inserting flat with explicit knowledge_time creates one batch."""
    knowledge_time = sample_datetime - timedelta(hours=1)

    td.create_series(name="temperature", unit="dimensionless", overlapping=False)

    df = pd.DataFrame({
        "valid_time": [sample_datetime],
        "value": [20.5],
    })

    result = td.get_series("temperature").insert(
        data=df,
        batch_start_time=sample_datetime,
        knowledge_time=knowledge_time,
    )

    assert isinstance(result.batch_id, uuid.UUID)

    # Verify data was inserted
    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM flat")
            assert cur.fetchone()[0] == 1

            cur.execute("SELECT COUNT(*) FROM batches_table")
            assert cur.fetchone()[0] == 1


def test_insert_flat_point_in_time(td, clean_db, sample_datetime):
    """Test inserting multiple point-in-time flat data."""
    td.create_series(name="power", unit="dimensionless", overlapping=False)

    df = pd.DataFrame({
        "valid_time": [
            sample_datetime,
            sample_datetime + timedelta(hours=1),
            sample_datetime + timedelta(hours=2),
        ],
        "value": [100.5, 101.0, 102.5],
    })

    result = td.get_series("power").insert(data=df)

    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM flat")
            assert cur.fetchone()[0] == 3


def test_insert_flat_interval(td, clean_db, sample_datetime):
    """Test inserting interval flat data with valid_time_end."""
    td.create_series(name="energy", unit="dimensionless", overlapping=False)

    df = pd.DataFrame({
        "valid_time": [sample_datetime],
        "valid_time_end": [sample_datetime + timedelta(hours=1)],
        "value": [500.0],
    })

    result = td.get_series("energy").insert(data=df)

    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT valid_time_end FROM flat")
            row = cur.fetchone()
            assert row is not None
            assert row[0] is not None


def test_insert_flat_upsert(td, clean_db, sample_datetime):
    """Test that inserting the same flat valid_time twice updates the value."""
    td.create_series(name="meter", unit="dimensionless", overlapping=False)

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

    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM flat")
            assert cur.fetchone()[0] == 1

            cur.execute("SELECT value FROM flat")
            assert cur.fetchone()[0] == 150.0


# =============================================================================
# Overlapping insertion tests
# =============================================================================

def test_insert_overlapping_creates_batch(td, clean_db, sample_datetime):
    """Test inserting overlapping via SDK creates rows in overlapping_medium table."""
    td.create_series(
        name="wind_forecast", unit="dimensionless",
        overlapping=True, retention="medium",
    )

    df = pd.DataFrame({
        "valid_time": [sample_datetime, sample_datetime + timedelta(hours=1)],
        "value": [50.0, 55.0],
    })

    result = td.get_series("wind_forecast").insert(data=df, knowledge_time=sample_datetime)

    assert isinstance(result.batch_id, uuid.UUID)
    assert result.series_id > 0

    # Verify rows in overlapping_medium
    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM overlapping_medium WHERE batch_id = %s",
                (str(result.batch_id),)
            )
            assert cur.fetchone()[0] == 2

            # Verify no rows in flat
            cur.execute("SELECT COUNT(*) FROM flat")
            assert cur.fetchone()[0] == 0


def test_insert_overlapping_short_tier(td, clean_db):
    """Test inserting overlapping with retention='short'."""
    # Use a recent datetime to avoid the 6-month retention policy on overlapping_short
    recent_time = datetime.now(timezone.utc).replace(microsecond=0)

    td.create_series(
        name="price_forecast", unit="dimensionless",
        overlapping=True, retention="short",
    )

    df = pd.DataFrame({
        "valid_time": [recent_time],
        "value": [42.0],
    })

    td.get_series("price_forecast").insert(data=df, knowledge_time=recent_time)

    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM overlapping_short")
            assert cur.fetchone()[0] == 1

            cur.execute("SELECT COUNT(*) FROM overlapping_medium")
            assert cur.fetchone()[0] == 0

            cur.execute("SELECT COUNT(*) FROM overlapping_long")
            assert cur.fetchone()[0] == 0


def test_insert_overlapping_long_tier(td, clean_db, sample_datetime):
    """Test inserting overlapping with retention='long'."""
    td.create_series(
        name="climate_forecast", unit="dimensionless",
        overlapping=True, retention="long",
    )

    df = pd.DataFrame({
        "valid_time": [sample_datetime],
        "value": [15.0],
    })

    td.get_series("climate_forecast").insert(data=df, knowledge_time=sample_datetime)

    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM overlapping_long")
            assert cur.fetchone()[0] == 1


def test_insert_overlapping_interval(td, clean_db, sample_datetime):
    """Test inserting interval overlapping with valid_time_end."""
    td.create_series(
        name="energy_forecast", unit="dimensionless",
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

    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT valid_time_end FROM overlapping_medium")
            row = cur.fetchone()
            assert row is not None
            assert row[0] is not None


# =============================================================================
# Timezone validation
# =============================================================================

def test_insert_timezone_aware_required(td):
    """Test that timezone-aware datetimes are required."""
    td.create_series(name="temp", unit="dimensionless", overlapping=False)

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
    table = pa.table({
        "valid_time": pa.array(datetimes, type=_TS_TYPE),
        "value": pa.array(values, type=pa.float64()),
    })
    return TimeSeries.from_arrow(table, unit=unit, timeseries_type=timeseries_type)


def test_insert_timeseries_flat(td, clean_db, sample_datetime):
    """TimeSeries with SIMPLE shape inserts correctly into a flat series."""
    td.create_series(name="ts_flat", unit="dimensionless", overlapping=False)

    ts = _make_simple_ts(
        [sample_datetime, sample_datetime + timedelta(hours=1)],
        [10.0, 20.0],
    )

    result = td.get_series("ts_flat").insert(data=ts)

    assert isinstance(result.batch_id, uuid.UUID)
    assert result.series_id > 0

    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM flat")
            assert cur.fetchone()[0] == 2


def test_insert_timeseries_overlapping(td, clean_db, sample_datetime):
    """TimeSeries with SIMPLE shape inserts correctly into an overlapping series."""
    td.create_series(name="ts_ovlp", unit="dimensionless", overlapping=True, retention="medium")

    ts = _make_simple_ts(
        [sample_datetime, sample_datetime + timedelta(hours=1)],
        [50.0, 55.0],
        timeseries_type=TimeSeriesType.OVERLAPPING,
    )

    result = td.get_series("ts_ovlp").insert(data=ts, knowledge_time=sample_datetime)

    assert isinstance(result.batch_id, uuid.UUID)

    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM overlapping_medium WHERE batch_id = %s",
                (str(result.batch_id),),
            )
            assert cur.fetchone()[0] == 2


def test_insert_timeseries_unit_conversion(td, clean_db, sample_datetime):
    """TimeSeries whose unit differs from the series unit is converted automatically."""
    # Series stores in MW; insert in kW — value should be divided by 1000
    td.create_series(name="power_mw", unit="MW", overlapping=False)

    ts = _make_simple_ts([sample_datetime], [1000.0], unit="kW")

    td.get_series("power_mw").insert(data=ts)

    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM flat")
            stored = cur.fetchone()[0]
            assert abs(stored - 1.0) < 1e-9, f"Expected 1.0 MW, got {stored}"


def test_insert_timeseries_interval(td, clean_db, sample_datetime):
    """TimeSeries with valid_time_end column inserts interval data correctly."""
    td.create_series(name="energy_ts", unit="dimensionless", overlapping=False)

    table = pa.table({
        "valid_time":     pa.array([sample_datetime], type=_TS_TYPE),
        "valid_time_end": pa.array([sample_datetime + timedelta(hours=1)], type=_TS_TYPE),
        "value":          pa.array([500.0], type=pa.float64()),
    })
    ts = TimeSeries.from_arrow(table)

    td.get_series("energy_ts").insert(data=ts)

    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT valid_time_end FROM flat")
            row = cur.fetchone()
            assert row is not None
            assert row[0] is not None


def test_insert_timeseries_wrong_shape(td, sample_datetime):
    """TimeSeries with AUDIT or CORRECTED shape raises ValueError."""
    td.create_series(name="audit_reject", unit="dimensionless", overlapping=True)

    # Build an AUDIT table (has knowledge_time + change_time columns)
    table = pa.table({
        "knowledge_time": pa.array([sample_datetime], type=_TS_TYPE),
        "change_time":    pa.array([sample_datetime], type=_TS_TYPE),
        "valid_time":     pa.array([sample_datetime], type=_TS_TYPE),
        "value":          pa.array([1.0], type=pa.float64()),
    })
    ts = TimeSeries.from_arrow(table, timeseries_type=TimeSeriesType.OVERLAPPING)
    assert ts.shape == DataShape.AUDIT

    with pytest.raises(ValueError, match="AUDIT"):
        td.get_series("audit_reject").insert(data=ts)


def _make_versioned_ts(knowledge_times, valid_times, values, unit="dimensionless"):
    """Helper: build a VERSIONED TimeSeries from lists."""
    table = pa.table({
        "knowledge_time": pa.array(knowledge_times, type=_TS_TYPE),
        "valid_time":     pa.array(valid_times,     type=_TS_TYPE),
        "value":          pa.array(values,          type=pa.float64()),
    })
    ts = TimeSeries.from_arrow(table, unit=unit, timeseries_type=TimeSeriesType.OVERLAPPING)
    assert ts.shape == DataShape.VERSIONED
    return ts


# =============================================================================
# VERSIONED insert tests
# =============================================================================

def test_insert_versioned_single_kt(td, clean_db, sample_datetime):
    """VERSIONED TimeSeries with one unique knowledge_time creates exactly one batch."""
    td.create_series(
        name="v_single", unit="dimensionless",
        overlapping=True, retention="medium",
    )
    kt = sample_datetime
    ts = _make_versioned_ts(
        knowledge_times=[kt, kt],
        valid_times=[sample_datetime, sample_datetime + timedelta(hours=1)],
        values=[10.0, 20.0],
    )
    result = td.get_series("v_single").insert(data=ts)

    assert isinstance(result.batch_id, uuid.UUID)

    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM batches_table")
            assert cur.fetchone()[0] == 1
            cur.execute("SELECT COUNT(*) FROM overlapping_medium")
            assert cur.fetchone()[0] == 2


def test_insert_versioned_multi_kt(td, clean_db, sample_datetime):
    """VERSIONED TimeSeries with multiple unique knowledge_times creates exactly one batch."""
    td.create_series(
        name="v_multi", unit="dimensionless",
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

    assert isinstance(result.batch_id, uuid.UUID)

    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            # One batch per insert() call regardless of unique knowledge_time count
            cur.execute("SELECT COUNT(*) FROM batches_table")
            assert cur.fetchone()[0] == 1
            cur.execute("SELECT COUNT(*) FROM overlapping_medium")
            assert cur.fetchone()[0] == 5


def test_insert_versioned_into_flat(td, clean_db, sample_datetime):
    """VERSIONED TimeSeries inserted into a flat series stores per-row knowledge_times."""
    td.create_series(name="v_flat_ok", unit="dimensionless", overlapping=False)

    kt1 = sample_datetime
    kt2 = sample_datetime + timedelta(hours=1)
    ts = _make_versioned_ts(
        knowledge_times=[kt1, kt2],
        valid_times=[sample_datetime, sample_datetime + timedelta(hours=1)],
        values=[1.0, 2.0],
    )

    result = td.get_series("v_flat_ok").insert(data=ts)
    assert isinstance(result.batch_id, uuid.UUID)

    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT knowledge_time FROM flat WHERE series_id = %s ORDER BY valid_time",
                (result.series_id,),
            )
            rows = cur.fetchall()
    assert len(rows) == 2


def test_insert_versioned_kt_kwarg_ambiguity_raises(td, clean_db, sample_datetime):
    """Passing knowledge_time kwarg with a VERSIONED insert raises ValueError (ambiguous)."""
    td.create_series(
        name="v_ambiguous", unit="dimensionless",
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

def test_insert_df_with_kt_column_overlapping(td, clean_db, sample_datetime):
    """DataFrame with knowledge_time column is accepted and routes correctly."""
    td.create_series(
        name="df_kt_col", unit="dimensionless",
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

    assert isinstance(result.batch_id, uuid.UUID)

    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            # One batch per insert() call
            cur.execute("SELECT COUNT(*) FROM batches_table")
            assert cur.fetchone()[0] == 1
            cur.execute("SELECT COUNT(*) FROM overlapping_medium")
            assert cur.fetchone()[0] == 3


def test_insert_df_kt_ambiguity_raises(td, sample_datetime):
    """DataFrame with knowledge_time column + knowledge_time kwarg raises ValueError."""
    td.create_series(
        name="df_kt_ambig", unit="dimensionless",
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


def test_insert_df_without_kt_defaults_to_now(td, clean_db, sample_datetime):
    """DataFrame without knowledge_time and no kwarg stores rows with a recent knowledge_time."""
    td.create_series(name="df_no_kt", unit="dimensionless", overlapping=False)

    before = datetime.now(timezone.utc)

    df = pd.DataFrame({
        "valid_time": [sample_datetime],
        "value": [99.0],
    })

    td.get_series("df_no_kt").insert(data=df)

    after = datetime.now(timezone.utc)

    with psycopg.connect(clean_db) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT knowledge_time FROM flat")
            row = cur.fetchone()
            assert row is not None
            kt = row[0]
            # knowledge_time should be between before and after (baked from now())
            assert before <= kt <= after
