"""Unit tests for write-time validation — no ClickHouse required."""

from datetime import UTC, datetime

import polars as pl
import pytest
from timedb.write import write


class _RecordingClient:
    """Minimal fake for clickhouse_connect client — captures insert_arrow calls."""

    def __init__(self):
        self.calls: list[tuple[str, int]] = []

    def insert_arrow(self, table, arrow_table, settings=None):  # noqa: ARG002
        self.calls.append((table, arrow_table.num_rows))


def _df(rows=3):
    times = pl.datetime_range(
        start=datetime(2024, 1, 1, tzinfo=UTC),
        end=datetime(2024, 1, 1, tzinfo=UTC),
        interval=f"{rows}h",
        time_unit="us",
        time_zone="UTC",
        eager=True,
    ).head(rows)
    if len(times) < rows:
        times = pl.datetime_range(
            start=datetime(2024, 1, 1, tzinfo=UTC),
            interval="1h",
            time_unit="us",
            time_zone="UTC",
            end=datetime(2024, 1, 1, tzinfo=UTC) + __import__("datetime").timedelta(hours=rows - 1),
            eager=True,
        )
    return pl.DataFrame(
        {
            "series_id": [1] * rows,
            "valid_time": times,
            "value": [1.0 + i for i in range(rows)],
        }
    )


def test_default_retention_is_forever():
    client = _RecordingClient()
    captured: list = []

    def insert_arrow(table, arrow_table, settings=None):  # noqa: ARG001
        if table == "series_values":
            captured.append(arrow_table.to_pydict())
        client.calls.append((table, arrow_table.num_rows))

    client.insert_arrow = insert_arrow  # type: ignore[method-assign]
    write(client, _df(rows=3), knowledge_time=datetime(2024, 6, 1, tzinfo=UTC))
    assert captured, "no series_values insert"
    assert set(captured[0]["retention"]) == {"forever"}


def test_rejects_unknown_retention_kwarg():
    client = _RecordingClient()
    with pytest.raises(ValueError, match="Unknown retention"):
        write(client, _df(), retention="bogus", knowledge_time=datetime(2024, 1, 1, tzinfo=UTC))


def test_rejects_unknown_retention_column():
    client = _RecordingClient()
    df = _df().with_columns(pl.lit("bogus").alias("retention"))
    with pytest.raises(ValueError, match="Unknown retention values"):
        write(client, df, knowledge_time=datetime(2024, 1, 1, tzinfo=UTC))


def test_rejects_retention_column_and_kwarg():
    client = _RecordingClient()
    df = _df().with_columns(pl.lit("short").alias("retention"))
    with pytest.raises(ValueError, match="Ambiguous retention"):
        write(client, df, retention="medium", knowledge_time=datetime(2024, 1, 1, tzinfo=UTC))


def test_rejects_knowledge_time_column_and_kwarg():
    client = _RecordingClient()
    df = _df().with_columns(
        pl.lit(datetime(2024, 1, 1, tzinfo=UTC), dtype=pl.Datetime("us", "UTC")).alias("knowledge_time")
    )
    with pytest.raises(ValueError, match="Ambiguous knowledge_time"):
        write(client, df, retention="medium", knowledge_time=datetime(2024, 1, 1, tzinfo=UTC))


def test_rejects_naive_datetime_column():
    client = _RecordingClient()
    df = pl.DataFrame(
        {
            "series_id": [1],
            "valid_time": [datetime(2024, 1, 1)],
            "value": [1.0],
        }
    )
    with pytest.raises(ValueError, match="must be timezone-aware"):
        write(client, df, retention="medium")


def test_rejects_missing_required_columns():
    client = _RecordingClient()
    df = pl.DataFrame({"series_id": [1], "value": [1.0]})
    with pytest.raises(ValueError, match="missing required columns"):
        write(client, df, retention="medium")


def test_writes_series_values_and_run_series():
    client = _RecordingClient()
    write(
        client,
        _df(rows=3),
        retention="medium",
        knowledge_time=datetime(2024, 6, 1, tzinfo=UTC),
    )
    tables = [name for name, _ in client.calls]
    assert "series_values" in tables
    assert "run_series" in tables
    # run_series should have 1 row (one series_id × one run_id)
    rs_rows = next(n for t, n in client.calls if t == "run_series")
    assert rs_rows == 1


def test_retention_column_accepted():
    client = _RecordingClient()
    df = _df(rows=3).with_columns(pl.lit("short").alias("retention"))
    write(client, df, knowledge_time=datetime(2024, 6, 1, tzinfo=UTC))
    assert any(t == "series_values" for t, _ in client.calls)


def test_multiple_run_ids_produces_multiple_run_series_rows():
    """If the caller puts run_id on the df, run_series should capture all distinct pairs."""
    client = _RecordingClient()
    df = pl.DataFrame(
        {
            "series_id": [1, 1, 2, 2],
            "valid_time": [
                datetime(2024, 1, 1, 0, tzinfo=UTC),
                datetime(2024, 1, 1, 1, tzinfo=UTC),
                datetime(2024, 1, 1, 0, tzinfo=UTC),
                datetime(2024, 1, 1, 1, tzinfo=UTC),
            ],
            "value": [1.0, 2.0, 3.0, 4.0],
            "run_id": [100, 100, 200, 200],
        }
    )
    write(client, df, retention="medium", knowledge_time=datetime(2024, 6, 1, tzinfo=UTC))
    rs_rows = next(n for t, n in client.calls if t == "run_series")
    # 2 distinct (series_id, run_id) pairs: (1, 100), (2, 200)
    assert rs_rows == 2
