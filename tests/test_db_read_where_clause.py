"""Tests for _build_ch_where_clause_multi handling series IDs."""
from timedb.db.read import _build_ch_where_clause_multi


def test_build_where_clause_single_series():
    where, params = _build_ch_where_clause_multi(series_ids=[1])
    assert "series_id IN {series_ids:Array(Int64)}" in where
    assert params["series_ids"] == [1]


def test_build_where_clause_multiple_series():
    where, params = _build_ch_where_clause_multi(series_ids=[1, 2, 3])
    assert "series_id IN {series_ids:Array(Int64)}" in where
    assert params["series_ids"] == [1, 2, 3]


def test_build_where_clause_with_time_filters():
    from datetime import datetime, timezone

    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    end = datetime(2025, 1, 2, tzinfo=timezone.utc)

    where, params = _build_ch_where_clause_multi(
        series_ids=[1], start_valid=start, end_valid=end
    )

    assert "series_id IN {series_ids:Array(Int64)}" in where
    assert "valid_time >= {start_valid:DateTime64(6, 'UTC')}" in where
    assert "valid_time < {end_valid:DateTime64(6, 'UTC')}" in where
    assert params["series_ids"] == [1]
    # _build_ch_where_clause_multi strips tzinfo via _to_naive_utc
    assert params["start_valid"] == start.replace(tzinfo=None)
    assert params["end_valid"] == end.replace(tzinfo=None)
