"""Tests for _build_where_clause handling single series ID."""
from timedb.db.read import _build_where_clause


def test_build_where_clause_single_series():
    sid = 1
    # Single series ID should produce equality check
    where, params = _build_where_clause(series_id=sid)
    assert "v.series_id = %(series_id)s" in where
    assert params.get("series_id") == sid


def test_build_where_clause_with_time_filters():
    from datetime import datetime, timezone
    
    sid = 1
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    end = datetime(2025, 1, 2, tzinfo=timezone.utc)
    
    where, params = _build_where_clause(
        series_id=sid,
        start_valid=start,
        end_valid=end
    )
    
    assert "v.series_id = %(series_id)s" in where
    assert "v.valid_time >= %(start_valid)s" in where
    assert "v.valid_time < %(end_valid)s" in where
    assert params["series_id"] == sid
    assert params["start_valid"] == start
    assert params["end_valid"] == end
