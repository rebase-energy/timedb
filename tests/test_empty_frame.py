"""Unit tests for the typed empty-frame path in ``timedb.read``.

Pure (no DB): pins the exact column names, order, and dtypes ``read()`` /
``read_relative()`` return when no series resolve, across the four
``(include_updates, include_knowledge_time)`` combinations.
"""

from __future__ import annotations

import polars as pl
import pytest
from timedb.read import RELATIVE_READ_COLUMNS, empty_frame, read, read_columns, read_relative

_TS = pl.Datetime("us", "UTC")


@pytest.mark.parametrize(
    ("include_updates", "include_knowledge_time", "expected"),
    [
        (False, False, [("series_id", pl.UInt64), ("valid_time", _TS), ("value", pl.Float64)]),
        (
            False,
            True,
            [("series_id", pl.UInt64), ("knowledge_time", _TS), ("valid_time", _TS), ("value", pl.Float64)],
        ),
        (
            True,
            False,
            [
                ("series_id", pl.UInt64),
                ("valid_time", _TS),
                ("change_time", _TS),
                ("value", pl.Float64),
                ("changed_by", pl.Utf8),
                ("annotation", pl.Utf8),
            ],
        ),
        (
            True,
            True,
            [
                ("series_id", pl.UInt64),
                ("valid_time", _TS),
                ("knowledge_time", _TS),
                ("change_time", _TS),
                ("value", pl.Float64),
                ("changed_by", pl.Utf8),
                ("annotation", pl.Utf8),
            ],
        ),
    ],
)
def test_read_columns_and_empty_frame_pin_schema(include_updates, include_knowledge_time, expected):
    cols = read_columns(include_updates=include_updates, include_knowledge_time=include_knowledge_time)
    assert cols == [name for name, _ in expected]

    frame = empty_frame(cols)
    assert list(frame.schema.items()) == expected
    assert frame.height == 0


def test_relative_read_columns_constant():
    assert RELATIVE_READ_COLUMNS == ("series_id", "valid_time", "value")
    frame = empty_frame(list(RELATIVE_READ_COLUMNS))
    assert list(frame.schema.items()) == [("series_id", pl.UInt64), ("valid_time", _TS), ("value", pl.Float64)]
    assert frame.height == 0


@pytest.mark.parametrize(
    ("include_updates", "include_knowledge_time"),
    [(False, False), (False, True), (True, False), (True, True)],
)
def test_read_with_no_series_ids_returns_typed_empty_frame(include_updates, include_knowledge_time):
    """No ``ch_client`` call happens: the short-circuit is entirely in-process."""
    result = read(
        None,
        series_ids=[],
        include_updates=include_updates,
        include_knowledge_time=include_knowledge_time,
    )
    expected_cols = read_columns(include_updates=include_updates, include_knowledge_time=include_knowledge_time)
    assert result.columns == expected_cols
    assert result.height == 0


def test_read_relative_with_no_series_ids_returns_typed_empty_frame():
    from datetime import UTC, datetime, timedelta

    result = read_relative(
        None,
        series_ids=[],
        window_length=timedelta(hours=1),
        issue_offset=timedelta(hours=0),
        start_valid=datetime(2026, 1, 1, tzinfo=UTC),
    )
    assert result.columns == ["series_id", "valid_time", "value"]
    assert result.height == 0
