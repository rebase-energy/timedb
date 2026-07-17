"""Unit tests for the concurrent insert lanes in ``write`` — no ClickHouse required.

The ``series_values`` and ``run_series`` inserts overlap on a single sessionless
CH client (each insert pays a fixed per-insert commit latency, so serializing
them would pay it twice). A large values batch splits into two halves that run
concurrently on that same client. These tests pin the single-client routing,
the actual overlap, the split, and the failure contract.
"""

import threading
from datetime import UTC, datetime

import polars as pl
import pytest
import timedb.write as write_mod
from timedb.write import write


class _CountingClient:
    """Fake CH client recording ``(table, rows)`` per insert, thread-safe.

    All lanes share one client now, so inserts arrive from several threads at
    once — hence the lock around ``calls``. Optional per-table hooks let a test
    prove the lanes actually overlap (an event set by one lane, awaited by the
    other) or fail a chosen lane.
    """

    def __init__(
        self,
        *,
        block_values_until: threading.Event | None = None,
        signal_on_run_series: threading.Event | None = None,
        fail_on: str | None = None,
    ):
        self._lock = threading.Lock()
        self.calls: list[tuple[str, int]] = []
        self._block_values_until = block_values_until
        self._signal_on_run_series = signal_on_run_series
        self._fail_on = fail_on

    def insert_arrow(self, table, arrow_table, settings=None):  # noqa: ARG002
        if table == "run_series" and self._signal_on_run_series is not None:
            self._signal_on_run_series.set()
        if (
            table == "series_values"
            and self._block_values_until is not None
            and not self._block_values_until.wait(timeout=5)
        ):
            raise TimeoutError("series_values lane never overlapped the run_series lane")
        with self._lock:
            self.calls.append((table, arrow_table.num_rows))
        if self._fail_on is not None and table == self._fail_on:
            raise RuntimeError(f"{table} insert failed")


def _df(rows: int = 4) -> pl.DataFrame:
    vts = [datetime(2024, 1, 1, h, tzinfo=UTC) for h in range(rows)]
    return pl.DataFrame({"series_id": [1] * rows, "valid_time": vts, "value": [float(h) for h in range(rows)]})


def test_small_write_inserts_both_tables_on_one_client():
    ch = _CountingClient()
    write(ch, _df())
    assert sorted(t for t, _ in ch.calls) == ["run_series", "series_values"]
    # No split below the threshold: series_values carries the whole batch.
    assert [n for t, n in ch.calls if t == "series_values"] == [4]


def test_split_write_halves_values_on_one_client(monkeypatch):
    monkeypatch.setattr(write_mod, "_PARALLEL_INSERT_THRESHOLD", 2)
    ch = _CountingClient()
    write(ch, _df(rows=6))
    value_calls = [n for t, n in ch.calls if t == "series_values"]
    assert len(value_calls) == 2  # split into two halves
    assert sum(value_calls) == 6  # the halves cover the whole batch
    assert [t for t, _ in ch.calls].count("run_series") == 1


def test_values_and_run_series_actually_overlap():
    # The values lane blocks until the run_series lane has run, both on the same
    # client. Under a serial order (values first, run_series after) the values
    # lane would wait on an event nobody has set yet; the 5 s guard turns that
    # into a TimeoutError instead of a hang.
    rs_ran = threading.Event()
    ch = _CountingClient(block_values_until=rs_ran, signal_on_run_series=rs_ran)
    write(ch, _df())
    assert sorted(t for t, _ in ch.calls) == ["run_series", "series_values"]


def test_values_failure_awaits_run_series_and_wins():
    ch = _CountingClient(fail_on="series_values")
    with pytest.raises(RuntimeError, match="series_values insert failed"):
        write(ch, _df())
    # The run_series lane ran to completion before the values error propagated.
    assert "run_series" in [t for t, _ in ch.calls]
