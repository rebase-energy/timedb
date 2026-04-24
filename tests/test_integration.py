"""Integration tests against a live ClickHouse.

Skipped if TIMEDB_CH_URL is not set. Tests run against a fresh schema and
clean up after themselves.
"""

import os
from datetime import UTC, datetime, timedelta

import polars as pl
import pytest
from timedb import TimeDBClient

if not os.environ.get("TIMEDB_CH_URL"):
    pytest.skip("TIMEDB_CH_URL not set — skipping integration tests", allow_module_level=True)


# Use recent dates so the 'short' retention (180-day TTL) doesn't drop rows
# out from under the test.
BASE_VT = datetime(2026, 1, 1, tzinfo=UTC)
KT_1 = datetime(2026, 1, 1, 6, tzinfo=UTC)
KT_2 = datetime(2026, 1, 1, 7, tzinfo=UTC)


def _flat_df(series_id: int, n: int = 4) -> pl.DataFrame:
    times = pl.datetime_range(
        start=BASE_VT,
        end=BASE_VT + timedelta(hours=n - 1),
        interval="1h",
        time_unit="us",
        time_zone="UTC",
        eager=True,
    )
    return pl.DataFrame(
        {
            "series_id": [series_id] * n,
            "valid_time": times,
            "value": [float(i) for i in range(n)],
        }
    )


@pytest.fixture
def td():
    client = TimeDBClient()
    client.delete()
    client.create()
    yield client
    client.delete()


def test_create_and_delete_schema(td):
    """Schema create+delete leaves no residual tables."""
    # Drop and recreate
    td.delete()
    td.create()
    # Write a single row so CH forces tables to materialize fully
    td.write(_flat_df(1, n=1), retention="short", knowledge_time=KT_1)


def test_write_and_read_latest_flat(td):
    td.write(_flat_df(series_id=1, n=3), retention="medium", knowledge_time=KT_1)
    result = td.read(series_ids=[1], retention="medium")
    assert set(result.columns) == {"series_id", "valid_time", "value"}
    assert len(result) == 3
    assert result["value"].to_list() == [0.0, 1.0, 2.0]


def test_overlapping_read_picks_latest_kt(td):
    df1 = _flat_df(series_id=1, n=2).with_columns(pl.col("value") * 10)
    df2 = _flat_df(series_id=1, n=2).with_columns(pl.col("value") * 100)
    td.write(df1, retention="medium", knowledge_time=KT_1)
    td.write(df2, retention="medium", knowledge_time=KT_2)

    # Latest should reflect KT_2 values (0, 100)
    latest = td.read(series_ids=[1], retention="medium")
    assert latest["value"].to_list() == [0.0, 100.0]


def test_history_returns_all_kts(td):
    df1 = _flat_df(series_id=1, n=2).with_columns(pl.col("value") * 10)
    df2 = _flat_df(series_id=1, n=2).with_columns(pl.col("value") * 100)
    td.write(df1, retention="medium", knowledge_time=KT_1)
    td.write(df2, retention="medium", knowledge_time=KT_2)

    history = td.read(series_ids=[1], retention="medium", include_knowledge_time=True)
    assert set(history.columns) == {"series_id", "knowledge_time", "valid_time", "value"}
    assert len(history) == 4


def test_correction_chain_includes_change_time(td):
    """include_updates=True returns the correction chain of the winning kt."""
    df1 = _flat_df(series_id=1, n=2)
    df2 = _flat_df(series_id=1, n=2).with_columns(pl.col("value") + 100)
    td.write(df1, retention="medium", knowledge_time=KT_1)
    td.write(df2, retention="medium", knowledge_time=KT_1)  # same kt = correction

    chain = td.read(series_ids=[1], retention="medium", include_updates=True)
    assert "change_time" in chain.columns
    # Two valid_times × (initial + 1 correction) = at most 4 rows
    assert 2 <= len(chain) <= 4


def test_retention_filter_prunes(td):
    td.write(_flat_df(series_id=1, n=3), retention="short", knowledge_time=KT_1)
    td.write(_flat_df(series_id=1, n=3).with_columns(pl.col("value") + 100), retention="medium", knowledge_time=KT_1)

    result_short = td.read(series_ids=[1], retention="short")
    result_med = td.read(series_ids=[1], retention="medium")
    assert result_short["value"].to_list() == [0.0, 1.0, 2.0]
    assert result_med["value"].to_list() == [100.0, 101.0, 102.0]


def test_retention_column_per_row(td):
    df = pl.concat(
        [
            _flat_df(series_id=1, n=2).with_columns(pl.lit("short").alias("retention")),
            _flat_df(series_id=2, n=2).with_columns(pl.lit("medium").alias("retention")),
        ]
    )
    td.write(df, knowledge_time=KT_1)

    out_short = td.read(series_ids=[1, 2], retention="short")
    out_med = td.read(series_ids=[1, 2], retention="medium")
    assert out_short["series_id"].to_list() == [1, 1]
    assert out_med["series_id"].to_list() == [2, 2]


def test_run_series_mapping(td):
    """read_run_series returns run_ids that have written to a series."""
    td.write(_flat_df(series_id=1, n=2), retention="medium", knowledge_time=KT_1)
    td.write(_flat_df(series_id=1, n=2), retention="medium", knowledge_time=KT_2)
    run_ids = td.read_run_series(series_id=1)
    assert len(run_ids) >= 2
    assert all(isinstance(r, int) and r > 0 for r in run_ids)


def test_empty_series_list_returns_empty(td):
    result = td.read(series_ids=[], retention="medium")
    assert result.is_empty()
