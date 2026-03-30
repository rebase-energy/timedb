"""Tests for read_pipeline — multi-series read orchestration (pure unit tests)."""
from datetime import datetime, timedelta, time as dt_time, timezone

import polars as pl
import pyarrow as pa
import pytest

from timedb.db.read import _COL_ARROW_TYPE, _empty_table
from timedb.read_pipeline import (
    _build_read_result,
    _read,
    _read_multi,
    _read_relative,
)


# ---------------------------------------------------------------------------
# _build_read_result
# ---------------------------------------------------------------------------

def test_build_read_result_joins_metadata():
    """CH data gets name/labels/unit joined via series_id."""
    ch_data = pa.table({
        "series_id": pa.array([1, 1, 2], type=pa.int64()),
        "valid_time": pa.array(
            [datetime(2025, 1, 1, tzinfo=timezone.utc)] * 3,
            type=_COL_ARROW_TYPE["valid_time"],
        ),
        "value": pa.array([10.0, 20.0, 30.0], type=pa.float64()),
    })
    mapping_df = pl.DataFrame({
        "name": ["wind", "solar"],
        "_series_id": [1, 2],
        "_unit": ["MW", "MW"],
        "_table": ["flat", "flat"],
        "_overlapping": [False, False],
    })
    result = _build_read_result(ch_data, mapping_df, name_col="name", label_cols=[], series_col=None)

    assert result.columns == ["name", "unit", "series_id", "valid_time", "value"]
    assert result["name"].to_list() == ["wind", "wind", "solar"]
    assert result["unit"].to_list() == ["MW", "MW", "MW"]


def test_build_read_result_with_labels():
    """Label columns appear between name and unit."""
    ch_data = pa.table({
        "series_id": pa.array([1], type=pa.int64()),
        "valid_time": pa.array(
            [datetime(2025, 1, 1, tzinfo=timezone.utc)],
            type=_COL_ARROW_TYPE["valid_time"],
        ),
        "value": pa.array([42.0], type=pa.float64()),
    })
    mapping_df = pl.DataFrame({
        "metric": ["wind"],
        "site": ["Gotland"],
        "_series_id": [1],
        "_unit": ["MW"],
        "_table": ["flat"],
        "_overlapping": [False],
    })
    result = _build_read_result(ch_data, mapping_df, name_col="metric", label_cols=["site"], series_col=None)

    assert result.columns == ["metric", "site", "unit", "series_id", "valid_time", "value"]
    assert result["site"].to_list() == ["Gotland"]


def test_build_read_result_empty():
    """Empty CH data returns empty DataFrame with correct schema."""
    ch_data = _empty_table(["series_id", "valid_time", "value"])
    mapping_df = pl.DataFrame({
        "name": ["wind"],
        "_series_id": [1],
        "_unit": ["MW"],
        "_table": ["flat"],
        "_overlapping": [False],
    })
    result = _build_read_result(ch_data, mapping_df, name_col="name", label_cols=[], series_col=None)

    assert result.is_empty()
    assert "name" in result.columns
    assert "unit" in result.columns
    assert "series_id" in result.columns


def test_build_read_result_series_col():
    """When using series_col, output uses that column name."""
    ch_data = pa.table({
        "series_id": pa.array([42], type=pa.int64()),
        "valid_time": pa.array(
            [datetime(2025, 1, 1, tzinfo=timezone.utc)],
            type=_COL_ARROW_TYPE["valid_time"],
        ),
        "value": pa.array([1.0], type=pa.float64()),
    })
    mapping_df = pl.DataFrame({
        "sid": [42],
        "_series_id": [42],
        "_unit": ["MW"],
        "_table": ["flat"],
        "_overlapping": [False],
    }).cast({"sid": pl.Int64})
    result = _build_read_result(ch_data, mapping_df, name_col=None, label_cols=[], series_col="sid")

    assert result.columns[0] == "sid"


# ---------------------------------------------------------------------------
# _read_multi — empty table schema for various flag combos
# ---------------------------------------------------------------------------

def test_read_multi_empty_default():
    table = _read_multi(None, {}, overlapping=False, include_updates=False)
    assert table.column_names == ["series_id", "valid_time", "value"]
    assert table.num_rows == 0


def test_read_multi_empty_overlapping():
    table = _read_multi(None, {}, overlapping=True, include_updates=False)
    assert "knowledge_time" in table.column_names
    assert table.num_rows == 0


def test_read_multi_empty_include_updates():
    table = _read_multi(None, {}, overlapping=False, include_updates=True)
    assert "change_time" in table.column_names
    assert "changed_by" in table.column_names
    assert table.num_rows == 0


def test_read_multi_empty_overlapping_with_updates():
    table = _read_multi(None, {}, overlapping=True, include_updates=True)
    assert "knowledge_time" in table.column_names
    assert "change_time" in table.column_names
    assert table.num_rows == 0


# ---------------------------------------------------------------------------
# _read — validation
# ---------------------------------------------------------------------------

def test_read_mutual_exclusivity_series_col_name_col():
    manifest = pl.DataFrame({"sid": [1]})
    with pytest.raises(ValueError, match="mutually exclusive"):
        _read(manifest, name_col="name", label_cols=None, series_col="sid")


def test_read_mutual_exclusivity_series_col_label_cols():
    manifest = pl.DataFrame({"sid": [1]})
    with pytest.raises(ValueError, match="mutually exclusive"):
        _read(manifest, name_col=None, label_cols=["site"], series_col="sid")


def test_read_series_col_not_in_manifest():
    manifest = pl.DataFrame({"name": ["wind"]})
    with pytest.raises(ValueError, match="not found in manifest"):
        _read(manifest, name_col=None, label_cols=None, series_col="sid")


# ---------------------------------------------------------------------------
# _read_relative — validation
# ---------------------------------------------------------------------------

def test_read_relative_mixed_mode_raises():
    manifest = pl.DataFrame({"name": ["wind"]})
    with pytest.raises(ValueError, match="Cannot mix"):
        _read_relative(
            manifest, name_col=None, label_cols=None, series_col=None,
            window_length=timedelta(hours=24),
            issue_offset=timedelta(hours=-12),
            days_ahead=1,
            time_of_day=dt_time(6, 0),
        )


def test_read_relative_daily_missing_time_of_day():
    manifest = pl.DataFrame({"name": ["wind"]})
    with pytest.raises(ValueError, match="Both days_ahead and time_of_day"):
        _read_relative(
            manifest, name_col=None, label_cols=None, series_col=None,
            days_ahead=1,
        )


def test_read_relative_daily_missing_start_valid():
    manifest = pl.DataFrame({"name": ["wind"]})
    with pytest.raises(ValueError, match="start_valid is required"):
        _read_relative(
            manifest, name_col=None, label_cols=None, series_col=None,
            days_ahead=1, time_of_day=dt_time(6, 0),
        )


def test_read_relative_explicit_missing_issue_offset():
    manifest = pl.DataFrame({"name": ["wind"]})
    with pytest.raises(ValueError, match="Both window_length and issue_offset"):
        _read_relative(
            manifest, name_col=None, label_cols=None, series_col=None,
            window_length=timedelta(hours=24),
        )


def test_read_relative_explicit_missing_start_window():
    manifest = pl.DataFrame({"name": ["wind"]})
    with pytest.raises(ValueError, match="start_window is required"):
        _read_relative(
            manifest, name_col=None, label_cols=None, series_col=None,
            window_length=timedelta(hours=24),
            issue_offset=timedelta(hours=-12),
        )


def test_read_relative_mutual_exclusivity():
    manifest = pl.DataFrame({"sid": [1]})
    with pytest.raises(ValueError, match="mutually exclusive"):
        _read_relative(
            manifest, name_col="name", label_cols=None, series_col="sid",
            window_length=timedelta(hours=24),
            issue_offset=timedelta(hours=-12),
            start_window=datetime(2025, 1, 1, tzinfo=timezone.utc),
        )
