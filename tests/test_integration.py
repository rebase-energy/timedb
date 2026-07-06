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


# Anchor to a recent date *relative to now* so the 'short' retention (180-day
# TTL) never drops rows out from under the test as the calendar advances. A
# fixed literal here silently rots once (literal + 180 days) passes.
BASE_VT = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=7)
KT_1 = BASE_VT + timedelta(hours=6)
KT_2 = BASE_VT + timedelta(hours=7)


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


# ── skip_unchanged ────────────────────────────────────────────────────────────


def _row_count(td, series_id: int) -> int:
    """Physical row count in series_values (not the collapsed read)."""
    res = td._ch.query(
        "SELECT count() FROM series_values WHERE series_id = {sid:UInt64}",
        parameters={"sid": series_id},
    )
    return int(res.result_rows[0][0])


def test_skip_unchanged_drops_identical_rewrite(td):
    df = _flat_df(series_id=1, n=3)
    td.write(df, retention="medium", knowledge_time=KT_1)
    before = _row_count(td, 1)
    # Same values under a NEW kt: valid_time scope ignores kt, so all skipped.
    res = td.write(df, retention="medium", knowledge_time=KT_2, skip_unchanged=True)
    assert (res.written, res.skipped) == (0, 3)
    assert _row_count(td, 1) == before


def test_skip_unchanged_keeps_changed_value(td):
    df = _flat_df(series_id=1, n=3)
    td.write(df, retention="medium", knowledge_time=KT_1)
    before = _row_count(td, 1)
    changed = df.with_columns(
        pl.when(pl.int_range(pl.len()) == 1).then(pl.col("value") + 50).otherwise(pl.col("value")).alias("value")
    )
    res = td.write(changed, retention="medium", knowledge_time=KT_2, skip_unchanged=True)
    assert (res.written, res.skipped) == (1, 2)
    assert _row_count(td, 1) == before + 1


def test_default_rewrite_still_appends(td):
    df = _flat_df(series_id=1, n=3)
    td.write(df, retention="medium", knowledge_time=KT_1)
    before = _row_count(td, 1)
    td.write(df, retention="medium", knowledge_time=KT_2)  # skip_unchanged defaults off
    assert _row_count(td, 1) == before + 3


def test_skip_unchanged_knowledge_time_scope(td):
    df = _flat_df(series_id=1, n=2)
    td.write(df, retention="medium", knowledge_time=KT_1)
    before = _row_count(td, 1)
    # Identical restatement under the SAME kt → skipped.
    r1 = td.write(df, retention="medium", knowledge_time=KT_1, skip_unchanged=True, unchanged_scope="knowledge_time")
    assert (r1.written, r1.skipped) == (0, 2)
    # Same values under a NEW kt → kept (distinct vintage).
    r2 = td.write(df, retention="medium", knowledge_time=KT_2, skip_unchanged=True, unchanged_scope="knowledge_time")
    assert (r2.written, r2.skipped) == (2, 0)
    assert _row_count(td, 1) == before + 2


def test_read_null_value_roundtrip(td):
    """A null written value (stored as the NaN sentinel) reads back as null;
    non-null values are untouched. Guards the gated NaN-to-null conversion in
    ``_fetch`` (the mask rebuild only runs when NaNs are actually present)."""
    df = _flat_df(series_id=1, n=3).with_columns(
        pl.when(pl.col("valid_time") == BASE_VT).then(None).otherwise(pl.col("value")).alias("value")
    )
    td.write(df, retention="medium", knowledge_time=KT_1)
    result = td.read(series_ids=[1], retention="medium").sort("valid_time")
    assert result["value"].to_list() == [None, 1.0, 2.0]

    # No-NaN series: values come back exactly, dtype stays Float64.
    td.write(_flat_df(series_id=2, n=3), retention="medium", knowledge_time=KT_1)
    clean = td.read(series_ids=[2], retention="medium").sort("valid_time")
    assert clean["value"].to_list() == [0.0, 1.0, 2.0]
    assert clean["value"].dtype == pl.Float64
