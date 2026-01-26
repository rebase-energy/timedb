"""Tests for _build_where_clause handling single and multiple series IDs."""
import uuid
from timedb.db.read import _build_where_clause


def test_build_where_clause_single_series():
    sid = uuid.uuid4()
    # Single-item list should produce ANY(...) with a single-element array
    where, params = _build_where_clause(series_ids=[sid])
    assert "v.series_id = ANY(%(series_ids)s)" in where
    assert params.get("series_ids") == [sid]


def test_build_where_clause_multiple_series():
    sids = [uuid.uuid4(), uuid.uuid4()]
    where, params = _build_where_clause(series_ids=sids)
    assert "v.series_id = ANY(%(series_ids)s)" in where
    # Params should include the list of ids
    assert "series_ids" in params
    assert list(params["series_ids"]) == sids
