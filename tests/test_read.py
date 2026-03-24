"""Tests for reading flat and overlapping."""
import pytest
import pandas as pd
import pyarrow as pa
import numpy as np
from datetime import datetime, timezone, timedelta, time
from timedb.db import read
from timedb import TimeSeries, DataShape


# =============================================================================
# Read flat tests
# =============================================================================

def test_read_flat_via_sdk(td, sample_datetime):
    """Test reading flat via SDK returns a pivoted DataFrame."""
    td.create_series("temperature", unit="dimensionless", overlapping=False)
    td.create_series("humidity", unit="dimensionless", overlapping=False)

    df_temp = pd.DataFrame({
        "valid_time": [sample_datetime, sample_datetime + timedelta(hours=1)],
        "value": [20.5, 21.0],
    })
    td.get_series("temperature").insert(data=df_temp)

    df_hum = pd.DataFrame({
        "valid_time": [sample_datetime],
        "value": [65.0],
    })
    td.get_series("humidity").insert(data=df_hum)

    # Read all flat via SDK
    df = td.get_series("temperature").read(
        start_valid=sample_datetime,
        end_valid=sample_datetime + timedelta(hours=2),
    )

    assert isinstance(df, TimeSeries)
    assert len(df) == 2
    assert df.shape == DataShape.SIMPLE


def test_read_flat_db_layer(td, clean_db, sample_datetime):
    """Test reading flat via the db.read layer."""
    series_id = td.create_series("power", unit="dimensionless", overlapping=False)

    df = pd.DataFrame({
        "valid_time": [sample_datetime, sample_datetime + timedelta(hours=1)],
        "value": [100.0, 101.0],
    })
    td.get_series("power").insert(data=df)

    # Read via db layer
    result = read.read_flat(
        clean_db,
        series_id=series_id,
        start_valid=sample_datetime,
        end_valid=sample_datetime + timedelta(hours=2),
    )

    assert isinstance(result, pa.Table)
    assert result.num_rows == 2
    assert "valid_time" in result.schema.names


def test_read_flat_filter_by_valid_time(td, clean_db, sample_datetime):
    """Test filtering flat by valid_time range."""
    series_id = td.create_series("power", unit="dimensionless", overlapping=False)

    df = pd.DataFrame({
        "valid_time": [
            sample_datetime,
            sample_datetime + timedelta(hours=1),
            sample_datetime + timedelta(hours=2),
            sample_datetime + timedelta(hours=3),
        ],
        "value": [100.0, 101.0, 102.0, 103.0],
    })
    td.get_series("power").insert(data=df)

    # Read only a subset
    result = read.read_flat(
        clean_db,
        series_id=series_id,
        start_valid=sample_datetime + timedelta(hours=1),
        end_valid=sample_datetime + timedelta(hours=3),
    )

    # Should only get 2 values (hours 1 and 2)
    assert result.num_rows == 2
    valid_times = result.column("valid_time").to_pylist()
    assert all(
        sample_datetime + timedelta(hours=1) <= idx < sample_datetime + timedelta(hours=3)
        for idx in valid_times
    )


# =============================================================================
# Read overlapping tests
# =============================================================================

def test_read_overlapping_latest_via_sdk(td, sample_datetime):
    """Test reading latest overlapping via SDK."""
    td.create_series(
        "wind_forecast", unit="dimensionless",
        overlapping=True, retention="medium",
    )

    df = pd.DataFrame({
        "valid_time": [sample_datetime, sample_datetime + timedelta(hours=1)],
        "value": [50.0, 55.0],
    })
    td.get_series("wind_forecast").insert(data=df, knowledge_time=sample_datetime)

    # Read latest (default, versions=False)
    result = td.get_series("wind_forecast").read(
        start_valid=sample_datetime,
        end_valid=sample_datetime + timedelta(hours=2),
    )

    assert isinstance(result, TimeSeries)
    assert len(result) == 2


def test_read_overlapping_all_versions_via_sdk(td, sample_datetime):
    """Test reading forecast history via SDK (overlapping=True)."""
    td.create_series(
        "wind_forecast", unit="dimensionless",
        overlapping=True, retention="medium",
    )

    # Insert first batch
    knowledge_time_1 = sample_datetime
    df1 = pd.DataFrame({
        "valid_time": [sample_datetime, sample_datetime + timedelta(hours=1)],
        "value": [50.0, 55.0],
    })
    td.get_series("wind_forecast").insert(data=df1, knowledge_time=knowledge_time_1)

    # Insert second batch (revision) for the same valid times
    knowledge_time_2 = sample_datetime + timedelta(hours=1)
    df2 = pd.DataFrame({
        "valid_time": [sample_datetime, sample_datetime + timedelta(hours=1)],
        "value": [52.0, 57.0],
    })
    td.get_series("wind_forecast").insert(data=df2, knowledge_time=knowledge_time_2)

    # Read forecast history (one row per knowledge_time × valid_time)
    result = td.get_series("wind_forecast").read(
        overlapping=True,
        start_valid=sample_datetime,
        end_valid=sample_datetime + timedelta(hours=2),
    )

    assert isinstance(result, TimeSeries)
    # Should have 4 rows (2 valid_times x 2 knowledge_times)
    assert len(result) == 4
    assert result.shape == DataShape.VERSIONED


def test_read_overlapping_all_versions_db_layer(td, clean_db, sample_datetime):
    """Test reading overlapping forecast history via the db.read layer."""
    series_id = td.create_series(
        "forecast", unit="dimensionless",
        overlapping=True, retention="medium",
    )

    knowledge_time_1 = sample_datetime
    df1 = pd.DataFrame({
        "valid_time": [sample_datetime],
        "value": [100.0],
    })
    td.get_series("forecast").insert(data=df1, knowledge_time=knowledge_time_1)

    knowledge_time_2 = sample_datetime + timedelta(hours=1)
    df2 = pd.DataFrame({
        "valid_time": [sample_datetime],
        "value": [105.0],
    })
    td.get_series("forecast").insert(data=df2, knowledge_time=knowledge_time_2)

    result = read.read_overlapping(
        clean_db,
        series_id=series_id,
        table="overlapping_medium",
        start_valid=sample_datetime,
        end_valid=sample_datetime + timedelta(hours=1),
    )

    assert isinstance(result, pa.Table)
    assert result.num_rows == 2
    assert set(["knowledge_time", "valid_time", "value"]).issubset(set(result.schema.names))


def test_read_overlapping_latest_picks_newest(td, sample_datetime):
    """Test that latest overlapping read picks the most recent knowledge_time."""
    td.create_series(
        "price", unit="dimensionless",
        overlapping=True, retention="medium",
    )

    # Insert initial forecast
    df1 = pd.DataFrame({
        "valid_time": [sample_datetime],
        "value": [100.0],
    })
    td.get_series("price").insert(data=df1, knowledge_time=sample_datetime)

    # Insert revised forecast with newer knowledge_time
    df2 = pd.DataFrame({
        "valid_time": [sample_datetime],
        "value": [110.0],
    })
    td.get_series("price").insert(
        data=df2,
        knowledge_time=sample_datetime + timedelta(hours=1),
    )

    # Read latest via SDK
    result = td.get_series("price").read(
        start_valid=sample_datetime,
        end_valid=sample_datetime + timedelta(hours=1),
    )

    assert len(result) == 1
    # The latest value should be 110.0
    assert result.to_pandas().iloc[0, 0] == 110.0


# =============================================================================
# read_relative tests
# =============================================================================

def test_read_relative_basic_via_sdk(td, sample_datetime):
    """read_relative returns only forecasts issued before the per-window cutoff."""
    td.create_series(
        "wind_forecast", unit="dimensionless",
        overlapping=True, retention="medium",
    )

    valid_times = [sample_datetime + timedelta(hours=h) for h in range(3)]

    # knowledge_time strictly before the cutoff (sample_datetime - 12h): included
    kt_early = sample_datetime - timedelta(hours=13)
    df_early = pd.DataFrame({"valid_time": valid_times, "value": [10.0, 11.0, 12.0]})
    td.get_series("wind_forecast").insert(data=df_early, knowledge_time=kt_early)

    # knowledge_time after the cutoff: excluded
    kt_late = sample_datetime - timedelta(hours=11)
    df_late = pd.DataFrame({"valid_time": valid_times, "value": [99.0, 99.0, 99.0]})
    td.get_series("wind_forecast").insert(data=df_late, knowledge_time=kt_late)

    result = td.get_series("wind_forecast").read_relative(
        start_valid=sample_datetime,
        end_valid=sample_datetime + timedelta(hours=24),
        window_length=timedelta(hours=24),
        issue_offset=timedelta(hours=-12),
    )

    assert isinstance(result, TimeSeries)
    assert len(result) == 3
    assert result.shape == DataShape.SIMPLE
    # Values should come from the early batch (10, 11, 12), not the late batch (99)
    assert list(result.to_pandas()["value"]) == [10.0, 11.0, 12.0]


def test_read_relative_picks_latest_before_cutoff(td, sample_datetime):
    """read_relative picks the latest forecast that is still before the cutoff."""
    td.create_series(
        "wind_forecast", unit="dimensionless",
        overlapping=True, retention="medium",
    )

    valid_times = [sample_datetime]
    # cutoff = sample_datetime + issue_offset = sample_datetime - 12h
    cutoff = sample_datetime - timedelta(hours=12)

    kt1 = cutoff - timedelta(hours=2)   # early, before cutoff
    kt2 = cutoff - timedelta(hours=1)   # later, still before cutoff
    kt3 = cutoff                        # exactly at cutoff (inclusive)
    kt4 = cutoff + timedelta(hours=1)   # after cutoff, excluded

    for kt, val in [(kt1, 1.0), (kt2, 2.0), (kt3, 3.0), (kt4, 99.0)]:
        td.get_series("wind_forecast").insert(
            data=pd.DataFrame({"valid_time": valid_times, "value": [val]}),
            knowledge_time=kt,
        )

    result = td.get_series("wind_forecast").read_relative(
        start_valid=sample_datetime,
        end_valid=sample_datetime + timedelta(hours=1),
        window_length=timedelta(hours=24),
        issue_offset=timedelta(hours=-12),
    )

    assert len(result) == 1
    # Should return kt3's value (3.0) — latest at or before cutoff
    assert result.to_pandas().iloc[0, 0] == 3.0


def test_read_relative_multi_window(td, sample_datetime):
    """read_relative applies the cutoff independently per window."""
    td.create_series(
        "wind_forecast", unit="dimensionless",
        overlapping=True, retention="medium",
    )

    # Window 1: [sample_datetime, sample_datetime+24h), cutoff = sample_datetime - 12h
    # Window 2: [sample_datetime+24h, sample_datetime+48h), cutoff = sample_datetime + 12h
    w1_valid = sample_datetime + timedelta(hours=6)
    w2_valid = sample_datetime + timedelta(hours=30)

    # For window 1: kt_w1 is before its cutoff; kt_w1_late is after
    kt_w1 = sample_datetime - timedelta(hours=13)
    kt_w1_late = sample_datetime - timedelta(hours=11)

    # For window 2: kt_w2 is before its cutoff; kt_w2_late is after
    kt_w2 = sample_datetime + timedelta(hours=11)
    kt_w2_late = sample_datetime + timedelta(hours=13)

    td.get_series("wind_forecast").insert(
        data=pd.DataFrame({"valid_time": [w1_valid, w2_valid], "value": [1.0, 2.0]}),
        knowledge_time=kt_w1,
    )
    td.get_series("wind_forecast").insert(
        data=pd.DataFrame({"valid_time": [w1_valid, w2_valid], "value": [99.0, 99.0]}),
        knowledge_time=kt_w1_late,
    )
    td.get_series("wind_forecast").insert(
        data=pd.DataFrame({"valid_time": [w2_valid], "value": [2.0]}),
        knowledge_time=kt_w2,
    )
    td.get_series("wind_forecast").insert(
        data=pd.DataFrame({"valid_time": [w2_valid], "value": [99.0]}),
        knowledge_time=kt_w2_late,
    )

    result = td.get_series("wind_forecast").read_relative(
        start_valid=sample_datetime,
        end_valid=sample_datetime + timedelta(hours=48),
        window_length=timedelta(hours=24),
        issue_offset=timedelta(hours=-12),
    )

    assert len(result) == 2
    # w1_valid uses kt_w1 (value 1.0), w2_valid uses kt_w2 (value 2.0)
    result_df = result.to_pandas()
    assert result_df.loc[w1_valid.astimezone(timezone.utc), "value"] == 1.0
    assert result_df.loc[w2_valid.astimezone(timezone.utc), "value"] == 2.0


def test_read_relative_empty_when_no_qualifying_forecasts(td, sample_datetime):
    """read_relative returns empty DataFrame when all forecasts are after the cutoff."""
    td.create_series(
        "wind_forecast", unit="dimensionless",
        overlapping=True, retention="medium",
    )

    # Insert only with knowledge_time after the cutoff
    kt_too_late = sample_datetime - timedelta(hours=11)
    td.get_series("wind_forecast").insert(
        data=pd.DataFrame({"valid_time": [sample_datetime], "value": [99.0]}),
        knowledge_time=kt_too_late,
    )

    result = td.get_series("wind_forecast").read_relative(
        start_valid=sample_datetime,
        end_valid=sample_datetime + timedelta(hours=24),
        window_length=timedelta(hours=24),
        issue_offset=timedelta(hours=-12),
    )

    assert isinstance(result, TimeSeries)
    assert len(result) == 0


def test_read_relative_raises_for_flat_series(td, sample_datetime):
    """read_relative raises ValueError for non-overlapping (flat) series."""
    td.create_series("temperature", unit="dimensionless", overlapping=False)
    td.get_series("temperature").insert(
        data=pd.DataFrame({"valid_time": [sample_datetime], "value": [20.0]})
    )

    with pytest.raises(ValueError, match="flat series"):
        td.get_series("temperature").read_relative(
            start_valid=sample_datetime,
            end_valid=sample_datetime + timedelta(hours=24),
            window_length=timedelta(hours=24),
            issue_offset=timedelta(hours=-12),
        )


def test_read_relative_db_layer(td, clean_db, sample_datetime):
    """read_overlapping_relative can be called directly via the db layer."""
    series_id = td.create_series(
        "wind_forecast", unit="dimensionless",
        overlapping=True, retention="medium",
    )

    kt = sample_datetime - timedelta(hours=13)
    td.get_series("wind_forecast").insert(
        data=pd.DataFrame({"valid_time": [sample_datetime], "value": [42.0]}),
        knowledge_time=kt,
    )

    result = read.read_overlapping_relative(
        clean_db,
        series_id=series_id,
        table="overlapping_medium",
        window_length=timedelta(hours=24),
        issue_offset=timedelta(hours=-12),
        start_window=sample_datetime,
        start_valid=sample_datetime,
        end_valid=sample_datetime + timedelta(hours=1),
    )

    assert isinstance(result, pa.Table)
    assert "valid_time" in result.schema.names
    assert result.num_rows == 1
    assert result.to_pydict()["value"][0] == 42.0


# =============================================================================
# read_relative daily shorthand tests (days_ahead + time_of_day)
# =============================================================================

def test_read_relative_daily_basic(td, sample_datetime):
    """days_ahead + time_of_day shorthand selects the correct forecast batch."""
    td.create_series("wind_forecast", unit="dimensionless",
                     overlapping=True, retention="medium")

    # Window start = midnight of sample_datetime's day
    window_start = sample_datetime.replace(hour=0, minute=0, second=0, microsecond=0)
    # days_ahead=1, time_of_day=06:00 → cutoff = window_start - 1 day + 6h
    cutoff = window_start - timedelta(days=1) + timedelta(hours=6)

    kt_early = cutoff - timedelta(hours=1)   # before cutoff → included
    kt_late  = cutoff + timedelta(hours=1)   # after cutoff  → excluded

    for kt, val in [(kt_early, 10.0), (kt_late, 99.0)]:
        td.get_series("wind_forecast").insert(
            data=pd.DataFrame({"valid_time": [sample_datetime], "value": [val]}),
            knowledge_time=kt,
        )

    result = td.get_series("wind_forecast").read_relative(
        days_ahead=1,
        time_of_day=time(6, 0),
        start_valid=window_start,
        end_valid=window_start + timedelta(days=1),
    )

    assert len(result) == 1
    assert result.to_pandas().iloc[0, 0] == 10.0


def test_read_relative_daily_same_day(td, sample_datetime):
    """days_ahead=0 uses a same-day cutoff at time_of_day."""
    td.create_series("price", unit="dimensionless",
                     overlapping=True, retention="medium")

    window_start = sample_datetime.replace(hour=0, minute=0, second=0, microsecond=0)
    # days_ahead=0, time_of_day=12:00 → cutoff = window_start + 12h (noon same day)
    cutoff = window_start + timedelta(hours=12)

    kt_before = cutoff - timedelta(hours=1)  # included
    kt_after  = cutoff + timedelta(hours=1)  # excluded

    for kt, val in [(kt_before, 5.0), (kt_after, 99.0)]:
        td.get_series("price").insert(
            data=pd.DataFrame({"valid_time": [sample_datetime], "value": [val]}),
            knowledge_time=kt,
        )

    result = td.get_series("price").read_relative(
        days_ahead=0,
        time_of_day=time(12, 0),
        start_valid=window_start,
        end_valid=window_start + timedelta(days=1),
    )

    assert len(result) == 1
    assert result.to_pandas().iloc[0, 0] == 5.0


def test_read_relative_daily_raises_mixed_params(td):
    """Mixing (days_ahead, time_of_day) with (window_length, issue_offset) raises ValueError."""
    td.create_series("wind_forecast", unit="dimensionless",
                     overlapping=True, retention="medium")

    with pytest.raises(ValueError, match="Cannot mix"):
        td.get_series("wind_forecast").read_relative(
            window_length=timedelta(hours=24),
            issue_offset=timedelta(hours=-12),
            days_ahead=1,
        )


def test_read_relative_daily_raises_without_start_valid(td):
    """Using days_ahead/time_of_day without start_valid raises ValueError."""
    td.create_series("wind_forecast", unit="dimensionless",
                     overlapping=True, retention="medium")

    with pytest.raises(ValueError, match="start_valid is required"):
        td.get_series("wind_forecast").read_relative(
            days_ahead=1,
            time_of_day=time(6, 0),
        )


# =============================================================================
# Correction / change_time aware read tests
# =============================================================================

def test_read_mode_history_hides_corrections(td, sample_datetime):
    """overlapping=True returns one row per (knowledge_time, valid_time) — the latest correction."""
    td.create_series("forecast", unit="dimensionless",
                     overlapping=True, retention="medium")

    td.get_series("forecast").insert(
        data=pd.DataFrame({"valid_time": [sample_datetime], "value": [100.0]}),
        knowledge_time=sample_datetime,
    )

    # Apply a correction: re-insert with same knowledge_time, newer change_time wins via argMax
    td.get_series("forecast").insert(
        data=pd.DataFrame({"valid_time": [sample_datetime], "value": [110.0]}),
        knowledge_time=sample_datetime,
    )

    history = td.get_series("forecast").read(overlapping=True)

    # Only one row per (knowledge_time, valid_time) — correction is collapsed
    assert len(history) == 1
    assert history.shape == DataShape.VERSIONED
    assert history.to_pandas()["value"].iloc[0] == 110.0  # latest correction wins


def test_read_mode_audit_shows_correction_chain(td, sample_datetime):
    """overlapping=True, include_updates=True exposes every row including all corrections."""
    td.create_series("forecast", unit="dimensionless",
                     overlapping=True, retention="medium")

    td.get_series("forecast").insert(
        data=pd.DataFrame({"valid_time": [sample_datetime], "value": [100.0]}),
        knowledge_time=sample_datetime,
    )

    # Two corrections: re-insert with same knowledge_time, each with a newer change_time
    for val in [110.0, 120.0]:
        td.get_series("forecast").insert(
            data=pd.DataFrame({"valid_time": [sample_datetime], "value": [val]}),
            knowledge_time=sample_datetime,
        )

    audit = td.get_series("forecast").read(overlapping=True, include_updates=True)

    assert len(audit) == 3  # original + 2 corrections
    assert audit.shape == DataShape.AUDIT
    # Values should be in chronological change_time order: 100, 110, 120
    assert list(audit.to_pandas()["value"]) == [100.0, 110.0, 120.0]


def test_read_include_updates_overlapping(td, sample_datetime):
    """include_updates=True for overlapping shows all corrections for the winning knowledge_time only."""
    td.create_series("forecast", unit="dimensionless",
                     overlapping=True, retention="medium")

    # Insert two batches (two knowledge_times)
    kt_old = sample_datetime
    kt_new = sample_datetime + timedelta(hours=1)

    result_old = td.get_series("forecast").insert(
        data=pd.DataFrame({"valid_time": [sample_datetime], "value": [10.0]}),
        knowledge_time=kt_old,
    )
    result_new = td.get_series("forecast").insert(
        data=pd.DataFrame({"valid_time": [sample_datetime], "value": [20.0]}),
        knowledge_time=kt_new,
    )

    # Apply a correction to the winning (newer) batch via re-insert
    td.get_series("forecast").insert(
        data=pd.DataFrame({
            "valid_time": [sample_datetime],
            "value": [25.0],
            "changed_by": ["tester"],
            "annotation": ["corrected"],
        }),
        knowledge_time=kt_new,
    )

    df = td.get_series("forecast").read(include_updates=True)

    # Index must be [valid_time, change_time]
    assert df.shape == DataShape.CORRECTED
    # Only corrections for the winning knowledge_time (kt_new) are visible
    # kt_new has 2 rows: original (20.0) + correction (25.0)
    assert len(df) == 2
    pdf = df.to_pandas()
    assert list(pdf["value"]) == [20.0, 25.0]
    assert pdf["changed_by"].iloc[1] == "tester"
    assert pdf["annotation"].iloc[1] == "corrected"


def test_read_include_updates_flat(td, sample_datetime):
    """include_updates=True for flat series returns [valid_time, change_time] index with audit columns."""
    td.create_series("temperature", unit="dimensionless", overlapping=False)

    td.get_series("temperature").insert(
        data=pd.DataFrame({
            "valid_time": [sample_datetime],
            "value": [20.0],
        })
    )

    # Correction: re-insert with updated value; argMax(value, change_time) wins at read time
    td.get_series("temperature").insert(
        data=pd.DataFrame({
            "valid_time": [sample_datetime],
            "value": [21.0],
            "changed_by": ["tester"],
            "annotation": ["fixed typo"],
        }),
    )

    df = td.get_series("temperature").read(include_updates=True)

    # Flat corrections are new inserts — both original and correction appear in history
    assert df.shape == DataShape.CORRECTED
    assert len(df) == 2  # original (20.0) + correction (21.0)
    pdf = df.to_pandas()
    assert pdf["value"].iloc[-1] == 21.0   # latest row is the correction
    assert pdf["changed_by"].iloc[-1] == "tester"
    assert pdf["annotation"].iloc[-1] == "fixed typo"
    assert "value" in df.columns
    assert "changed_by" in df.columns
    assert "annotation" in df.columns


def test_read_overlapping_true_raises_for_flat(td, sample_datetime):
    """overlapping=True on a flat series raises ValueError."""
    td.create_series("temperature", unit="dimensionless", overlapping=False)

    td.get_series("temperature").insert(
        data=pd.DataFrame({"valid_time": [sample_datetime], "value": [20.0]})
    )

    with pytest.raises(ValueError, match="overlapping"):
        td.get_series("temperature").read(overlapping=True)


def test_read_relative_picks_latest_correction(td, sample_datetime):
    """read_relative returns the corrected value when the correction shares knowledge_time."""
    td.create_series("forecast", unit="dimensionless",
                     overlapping=True, retention="medium")

    kt = sample_datetime - timedelta(hours=13)

    td.get_series("forecast").insert(
        data=pd.DataFrame({"valid_time": [sample_datetime], "value": [100.0]}),
        knowledge_time=kt,
    )

    # Correct the value — re-insert with same knowledge_time, newer change_time wins via argMax
    td.get_series("forecast").insert(
        data=pd.DataFrame({"valid_time": [sample_datetime], "value": [110.0]}),
        knowledge_time=kt,
    )

    df = td.get_series("forecast").read_relative(
        start_valid=sample_datetime,
        end_valid=sample_datetime + timedelta(hours=1),
        window_length=timedelta(hours=24),
        issue_offset=timedelta(hours=-12),
    )

    assert len(df) == 1
    # Correction (110.0) must win over original (100.0)
    assert df.to_pandas()["value"].iloc[0] == 110.0
