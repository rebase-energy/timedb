"""Tests for write_pipeline — normalize_write_input validation (pure unit tests)."""
from datetime import datetime, timezone

import polars as pl
import pytest

from timedb.write_pipeline import normalize_write_input


def _make_mapping(series_col=None, name_col=None, label_cols=None):
    """Build a minimal mapping DataFrame for testing validation."""
    if series_col:
        return pl.DataFrame({
            series_col: [1],
            "_series_id": [1],
            "_unit": ["MW"],
            "_factor": [1.0],
            "_target_table": ["flat"],
            "_overlapping": [False],
            "_retention": ["medium"],
        }).cast({series_col: pl.Int64})
    else:
        cols = {name_col: ["wind"]}
        for lc in (label_cols or []):
            cols[lc] = ["A"]
        cols.update({
            "_series_id": [1],
            "_unit": ["MW"],
            "_factor": [1.0],
            "_target_table": ["flat"],
            "_overlapping": [False],
            "_retention": ["medium"],
        })
        return pl.DataFrame(cols)


def test_normalize_write_input_missing_valid_time():
    df = pl.DataFrame({
        "name": ["wind"],
        "value": [1.0],
    })
    mapping = _make_mapping(name_col="name")
    with pytest.raises(ValueError, match="valid_time"):
        normalize_write_input(df, name_col="name", label_cols=[], mapping_df=mapping, run_id="test")


def test_normalize_write_input_missing_value():
    df = pl.DataFrame({
        "name": ["wind"],
        "valid_time": [datetime(2025, 1, 1, tzinfo=timezone.utc)],
    })
    mapping = _make_mapping(name_col="name")
    with pytest.raises(ValueError, match="value"):
        normalize_write_input(df, name_col="name", label_cols=[], mapping_df=mapping, run_id="test")


def test_normalize_write_input_tz_naive_valid_time_raises():
    df = pl.DataFrame({
        "name": ["wind"],
        "valid_time": [datetime(2025, 1, 1)],  # naive
        "value": [1.0],
    })
    mapping = _make_mapping(name_col="name")
    with pytest.raises(ValueError, match="timezone-aware"):
        normalize_write_input(df, name_col="name", label_cols=[], mapping_df=mapping, run_id="test")


def test_normalize_write_input_knowledge_time_ambiguity():
    df = pl.DataFrame({
        "name": ["wind"],
        "valid_time": [datetime(2025, 1, 1, tzinfo=timezone.utc)],
        "knowledge_time": [datetime(2025, 1, 1, tzinfo=timezone.utc)],
        "value": [1.0],
    })
    mapping = _make_mapping(name_col="name")
    with pytest.raises(ValueError, match="Ambiguous knowledge_time"):
        normalize_write_input(
            df, name_col="name", label_cols=[], mapping_df=mapping,
            run_id="test",
            knowledge_time=datetime(2025, 1, 1, tzinfo=timezone.utc),
        )


def test_normalize_write_input_flat_duplicate_raises():
    df = pl.DataFrame({
        "name": ["wind", "wind"],
        "valid_time": [
            datetime(2025, 1, 1, tzinfo=timezone.utc),
            datetime(2025, 1, 1, tzinfo=timezone.utc),  # duplicate
        ],
        "value": [1.0, 2.0],
    })
    mapping = _make_mapping(name_col="name")
    with pytest.raises(ValueError, match="duplicate"):
        normalize_write_input(df, name_col="name", label_cols=[], mapping_df=mapping, run_id="test")


def test_normalize_write_input_valid_partitions():
    """Valid input produces correct partitioned output."""
    df = pl.DataFrame({
        "name": ["wind", "wind"],
        "valid_time": [
            datetime(2025, 1, 1, 0, tzinfo=timezone.utc),
            datetime(2025, 1, 1, 1, tzinfo=timezone.utc),
        ],
        "value": [1.0, 2.0],
    })
    mapping = _make_mapping(name_col="name")
    result = normalize_write_input(df, name_col="name", label_cols=[], mapping_df=mapping, run_id="test-run")

    assert "flat" in result
    arrow_table, routing = result["flat"]
    assert arrow_table.num_rows == 2
    assert "series_id" in arrow_table.column_names
    assert "run_id" in arrow_table.column_names
    assert routing["overlapping"] is False
