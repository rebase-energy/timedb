"""Unit tests for write-time validation — no ClickHouse required."""

from datetime import UTC, datetime

import polars as pl
import pyarrow as pa
import pytest
from timedb.write import write

_VT_TYPE = pa.timestamp("us", tz="UTC")


class _RecordingClient:
    """Minimal fake for clickhouse_connect client — captures insert_arrow calls
    and serves a canned ``query_arrow`` result for the skip_unchanged read-back."""

    def __init__(self, stored: pa.Table | None = None):
        self.calls: list[tuple[str, int]] = []
        self._stored = stored
        self.query_calls = 0

    def insert_arrow(self, table, arrow_table, settings=None):  # noqa: ARG002
        self.calls.append((table, arrow_table.num_rows))

    def query_arrow(self, sql, parameters=None):  # noqa: ARG002
        self.query_calls += 1
        if self._stored is not None:
            return self._stored
        return _stored_table([])


def _stored_table(rows: list[tuple]) -> pa.Table:
    """Build a ``(series_id, valid_time, value, annotation, changed_by)`` table
    matching the valid_time-scope read-back shape."""
    return pa.table(
        {
            "series_id": pa.array([r[0] for r in rows], type=pa.uint64()),
            "valid_time": pa.array([r[1] for r in rows], type=_VT_TYPE),
            "value": pa.array([r[2] for r in rows], type=pa.float64()),
            "annotation": pa.array([r[3] for r in rows], type=pa.string()),
            "changed_by": pa.array([r[4] for r in rows], type=pa.string()),
        }
    )


def _incoming(values: list[float]) -> pl.DataFrame:
    vts = [datetime(2024, 1, 1, h, tzinfo=UTC) for h in range(len(values))]
    return pl.DataFrame({"series_id": [1] * len(values), "valid_time": vts, "value": values})


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


# ── skip_unchanged ────────────────────────────────────────────────────────────

_KT = datetime(2024, 6, 1, tzinfo=UTC)


def test_skip_unchanged_off_never_reads_back():
    """Flag off (default) → no read-back query, everything inserted."""
    client = _RecordingClient(stored=_stored_table([(1, datetime(2024, 1, 1, tzinfo=UTC), 0.0, "", "")]))
    res = write(client, _incoming([0.0, 1.0, 2.0]), retention="medium", knowledge_time=_KT)
    assert client.query_calls == 0
    assert (res.written, res.skipped) == (3, 0)
    assert next(n for t, n in client.calls if t == "series_values") == 3


def test_skip_unchanged_drops_value_matches_keeps_new_and_changed():
    stored = _stored_table(
        [
            (1, datetime(2024, 1, 1, 0, tzinfo=UTC), 0.0, "", ""),  # == incoming[0] → drop
            (1, datetime(2024, 1, 1, 1, tzinfo=UTC), 99.0, "", ""),  # != incoming[1] → keep
            # valid_time h=2 absent from store → keep
        ]
    )
    client = _RecordingClient(stored=stored)
    res = write(client, _incoming([0.0, 1.0, 2.0]), retention="medium", knowledge_time=_KT, skip_unchanged=True)
    assert client.query_calls == 1
    assert (res.written, res.skipped) == (2, 1)
    assert next(n for t, n in client.calls if t == "series_values") == 2
    # run_series covers only survivors (still one series_id here)
    assert next(n for t, n in client.calls if t == "run_series") == 1


def test_skip_unchanged_nan_equals_nan_is_dropped():
    stored = _stored_table([(1, datetime(2024, 1, 1, 0, tzinfo=UTC), float("nan"), "", "")])
    client = _RecordingClient(stored=stored)
    res = write(client, _incoming([float("nan")]), retention="medium", knowledge_time=_KT, skip_unchanged=True)
    assert (res.written, res.skipped) == (0, 1)
    assert not any(t == "series_values" for t, _ in client.calls)


def test_skip_unchanged_empty_store_keeps_all():
    """No stored rows for the range → nothing to compare, all kept."""
    client = _RecordingClient(stored=_stored_table([]))
    res = write(client, _incoming([0.0, 1.0]), retention="medium", knowledge_time=_KT, skip_unchanged=True)
    assert (res.written, res.skipped) == (2, 0)


def test_unknown_unchanged_scope_rejected():
    client = _RecordingClient()
    with pytest.raises(ValueError, match="Unknown unchanged_scope"):
        write(
            client,
            _incoming([1.0]),
            retention="medium",
            knowledge_time=_KT,
            skip_unchanged=True,
            unchanged_scope="bogus",  # ty: ignore[invalid-argument-type]
        )


# ── unchanged_scope="auto": per-series comparison keys ───────────────────────

_KT_LATER = datetime(2024, 6, 2, tzinfo=UTC)
_H0 = datetime(2024, 1, 1, 0, tzinfo=UTC)


def _stored_kt_table(rows: list[tuple]) -> pa.Table:
    """``(series_id, valid_time, knowledge_time, value, annotation, changed_by)`` —
    the knowledge_time-scope read-back shape."""
    return pa.table(
        {
            "series_id": pa.array([r[0] for r in rows], type=pa.uint64()),
            "valid_time": pa.array([r[1] for r in rows], type=_VT_TYPE),
            "knowledge_time": pa.array([r[2] for r in rows], type=_VT_TYPE),
            "value": pa.array([r[3] for r in rows], type=pa.float64()),
            "annotation": pa.array([r[4] for r in rows], type=pa.string()),
            "changed_by": pa.array([r[5] for r in rows], type=pa.string()),
        }
    )


class _ScopeAwareClient(_RecordingClient):
    """Serves the read-back shape matching each query's scope.

    ``"auto"`` issues one query per partition, so a single canned table can't
    answer both — this dispatches on the projected columns and records which
    scopes were asked for, which is how the partitioning itself gets asserted.
    """

    def __init__(self, *, vt_rows: list[tuple], kt_rows: list[tuple]):
        super().__init__()
        self._vt = _stored_table(vt_rows)
        self._kt = _stored_kt_table(kt_rows)
        self.scopes: list[str] = []

    def query_arrow(self, sql, parameters=None):  # noqa: ARG002
        self.query_calls += 1
        # The kt-scoped projection carries knowledge_time; the vt-scoped one does not.
        if "valid_time, knowledge_time, value" in sql:
            self.scopes.append("knowledge_time")
            return self._kt
        self.scopes.append("valid_time")
        return self._vt


def _mixed_incoming() -> pl.DataFrame:
    """One row each for series 1 (valid_time-scoped) and series 2, 3 (kt-scoped)."""
    return pl.DataFrame(
        {
            "series_id": [1, 2, 3],
            "valid_time": [_H0, _H0, _H0],
            "value": [5.0, 7.0, 9.0],
        }
    )


def test_auto_with_empty_set_matches_valid_time_scope():
    """No series needs the finer key → 'auto' is exactly today's default, one read-back."""

    def run(**kw):
        client = _RecordingClient(
            stored=_stored_table([(1, _H0, 0.0, "", ""), (1, datetime(2024, 1, 1, 1, tzinfo=UTC), 99.0, "", "")])
        )
        res = write(
            client, _incoming([0.0, 1.0, 2.0]), retention="medium", knowledge_time=_KT, skip_unchanged=True, **kw
        )
        return res, client.query_calls

    baseline, baseline_queries = run(unchanged_scope="valid_time")
    auto, auto_queries = run(unchanged_scope="auto", knowledge_time_scoped_series=[])

    assert (auto.written, auto.skipped) == (baseline.written, baseline.skipped)
    assert auto_queries == baseline_queries == 1


def test_auto_applies_the_right_key_per_series():
    """The core of the fix, in one batch:

    * series 1 (valid_time-scoped) re-sent with an unchanged value at a *new*
      knowledge_time → skipped, as before;
    * series 2 (kt-scoped) re-sent with an unchanged value at a new
      knowledge_time → **written** — a genuine republication, which the
      valid_time key would have silently dropped;
    * series 3 (kt-scoped) re-sent identically at the *same* knowledge_time →
      skipped, so exact re-sends still dedupe.
    """
    client = _ScopeAwareClient(
        vt_rows=[(1, _H0, 5.0, "", "")],  # series 1's winner: same value → drop
        kt_rows=[
            (2, _H0, _KT, 7.0, "", ""),  # series 2 stored at the OLD kt → no match at _KT_LATER → keep
            (3, _H0, _KT_LATER, 9.0, "", ""),  # series 3 stored at THIS kt, same value → drop
        ],
    )
    res = write(
        client,
        _mixed_incoming(),
        retention="medium",
        knowledge_time=_KT_LATER,
        skip_unchanged=True,
        unchanged_scope="auto",
        knowledge_time_scoped_series=[2, 3],
    )
    assert sorted(client.scopes) == ["knowledge_time", "valid_time"]  # one read-back per partition
    assert (res.written, res.skipped) == (1, 2)
    assert res.written + res.skipped == 3
    # Still a single values insert, carrying only the republication.
    assert next(n for t, n in client.calls if t == "series_values") == 1


def test_auto_without_the_series_set_is_rejected():
    client = _RecordingClient()
    with pytest.raises(ValueError, match="requires knowledge_time_scoped_series"):
        write(
            client,
            _incoming([1.0]),
            retention="medium",
            knowledge_time=_KT,
            skip_unchanged=True,
            unchanged_scope="auto",
        )
    assert client.query_calls == 0


@pytest.mark.parametrize("scope", ["valid_time", "knowledge_time"])
def test_series_set_without_auto_scope_is_rejected(scope):
    """Ambiguous: the set would be silently ignored. Same spirit as the
    retention column/kwarg guard."""
    client = _RecordingClient()
    with pytest.raises(ValueError, match="only meaningful with unchanged_scope='auto'"):
        write(
            client,
            _incoming([1.0]),
            retention="medium",
            knowledge_time=_KT,
            skip_unchanged=True,
            unchanged_scope=scope,
            knowledge_time_scoped_series=[1],
        )
    assert client.query_calls == 0


def test_skip_unchanged_off_ignores_both_auto_arguments():
    """With the flag off there is no comparison at all, so neither argument is
    validated or used — including the otherwise-rejected combinations."""
    client = _RecordingClient()
    res = write(
        client,
        _incoming([0.0, 1.0]),
        retention="medium",
        knowledge_time=_KT,
        unchanged_scope="auto",  # would need the set if skip_unchanged were on
        knowledge_time_scoped_series=None,
    )
    assert client.query_calls == 0
    assert (res.written, res.skipped) == (2, 0)

    client2 = _RecordingClient()
    res2 = write(
        client2,
        _incoming([0.0, 1.0]),
        retention="medium",
        knowledge_time=_KT,
        unchanged_scope="valid_time",  # would conflict with the set if the flag were on
        knowledge_time_scoped_series=[1],
    )
    assert client2.query_calls == 0
    assert (res2.written, res2.skipped) == (2, 0)


def test_auto_skips_the_read_back_for_an_untouched_partition():
    """kt_ids naming series absent from this batch costs no extra query."""
    client = _ScopeAwareClient(vt_rows=[], kt_rows=[])
    res = write(
        client,
        _incoming([0.0, 1.0]),  # series 1 only
        retention="medium",
        knowledge_time=_KT,
        skip_unchanged=True,
        unchanged_scope="auto",
        knowledge_time_scoped_series=[999],
    )
    assert client.scopes == ["valid_time"]
    assert (res.written, res.skipped) == (2, 0)
