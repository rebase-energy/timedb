"""Unit tests for the concurrent insert lanes in ``write`` — no ClickHouse required.

The values and run_series inserts ride separate CH clients so they overlap
(each insert pays a fixed per-insert commit latency). These tests pin the
lane-to-client assignment, the actual overlap, and the failure contract.
"""

import threading
from datetime import UTC, datetime

import polars as pl
import pytest
import timedb.write as write_mod
from timedb.write import write


class _LaneClient:
    """Fake CH client recording (table, rows) per insert, with optional
    blocking/raising behavior to prove overlap and failure semantics."""

    def __init__(self, name: str, *, wait_for: threading.Event | None = None, fail: bool = False):
        self.name = name
        self.calls: list[tuple[str, int]] = []
        self._wait_for = wait_for
        self._fail = fail

    def insert_arrow(self, table, arrow_table, settings=None):  # noqa: ARG002
        if self._wait_for is not None and not self._wait_for.wait(timeout=5):
            raise TimeoutError(f"{self.name}: expected concurrent lane never ran")
        self.calls.append((table, arrow_table.num_rows))
        if self._fail:
            raise RuntimeError(f"{self.name} insert failed")


def _df(rows: int = 4) -> pl.DataFrame:
    vts = [datetime(2024, 1, 1, h, tzinfo=UTC) for h in range(rows)]
    return pl.DataFrame({"series_id": [1] * rows, "valid_time": vts, "value": [float(h) for h in range(rows)]})


def test_small_write_lanes_values_on_main_run_series_on_aux():
    ch, aux0 = _LaneClient("ch"), _LaneClient("aux0")
    asked: list[int] = []

    def aux_clients(n: int) -> list:
        asked.append(n)
        return [aux0]

    write(ch, _df(), aux_clients=aux_clients)
    assert asked == [1]
    assert [t for t, _ in ch.calls] == ["series_values"]
    assert [t for t, _ in aux0.calls] == ["run_series"]


def test_split_write_lanes_use_three_clients(monkeypatch):
    monkeypatch.setattr(write_mod, "_PARALLEL_INSERT_THRESHOLD", 2)
    ch, aux0, aux1 = _LaneClient("ch"), _LaneClient("aux0"), _LaneClient("aux1")
    asked: list[int] = []

    def aux_clients(n: int) -> list:
        asked.append(n)
        return [aux0, aux1]

    write(ch, _df(rows=6), aux_clients=aux_clients)
    assert asked == [2]
    assert [t for t, _ in ch.calls] == ["series_values"]
    assert [t for t, _ in aux0.calls] == ["series_values"]
    assert ch.calls[0][1] + aux0.calls[0][1] == 6  # the two halves cover the batch
    assert [t for t, _ in aux1.calls] == ["run_series"]


def test_values_and_run_series_actually_overlap():
    # The values lane blocks until the run_series lane has run. Under the old
    # serial order (values first, run_series after) this would deadlock; the
    # 5 s guard turns that into a TimeoutError instead of a hang.
    rs_ran = threading.Event()

    class _RsClient(_LaneClient):
        def insert_arrow(self, table, arrow_table, settings=None):
            super().insert_arrow(table, arrow_table, settings)
            rs_ran.set()

    ch = _LaneClient("ch", wait_for=rs_ran)
    aux0 = _RsClient("aux0")
    write(ch, _df(), aux_clients=lambda n: [aux0])
    assert [t for t, _ in ch.calls] == ["series_values"]
    assert [t for t, _ in aux0.calls] == ["run_series"]


def test_values_failure_awaits_run_series_and_wins():
    ch = _LaneClient("ch", fail=True)
    aux0 = _LaneClient("aux0", fail=True)
    with pytest.raises(RuntimeError, match="ch insert failed"):
        write(ch, _df(), aux_clients=lambda n: [aux0])
    # both lanes ran to completion before the error propagated
    assert ch.calls and aux0.calls


def test_no_aux_clients_falls_back_to_serial_on_main():
    ch = _LaneClient("ch")
    write(ch, _df(), aux_clients=None)
    assert [t for t, _ in ch.calls] == ["series_values", "run_series"]
