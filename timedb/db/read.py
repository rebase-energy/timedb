import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import numpy as np
import pyarrow as pa

from .. import profiling


# ---------------------------------------------------------------------------
# Arrow type constants (used for empty-table construction)
# ---------------------------------------------------------------------------

_TS_TYPE = pa.timestamp("us", tz="UTC")

_COL_ARROW_TYPE: Dict[str, pa.DataType] = {
    "series_id":      pa.int64(),
    "valid_time":     _TS_TYPE,
    "valid_time_end": _TS_TYPE,
    "knowledge_time": _TS_TYPE,
    "change_time":    _TS_TYPE,
    "value":          pa.float64(),
    "changed_by":     pa.string(),
    "annotation":     pa.string(),
    "run_id":         pa.string(),
    "workflow_id":    pa.string(),
    "run_start_time":    _TS_TYPE,
    "run_finish_time":   _TS_TYPE,
    "run_params":     pa.string(),
    "inserted_at":    _TS_TYPE,
}


def _empty_table(columns: List[str]) -> pa.Table:
    return pa.table({c: pa.array([], type=_COL_ARROW_TYPE[c]) for c in columns})


def _fetch_ch_arrow(
    ch_client,
    sql: str,
    parameters: dict,
    columns: List[str],
) -> pa.Table:
    """Execute *sql* against ClickHouse and return results as a ``pa.Table``."""
    _prof = profiling._enabled
    _t_total = time.perf_counter() if _prof else 0.0

    _t0 = time.perf_counter() if _prof else 0.0
    result = ch_client.query_arrow(sql, parameters=parameters)
    if _prof:
        profiling._record(profiling.PHASE_READ_SQL_EXEC, time.perf_counter() - _t0)

    _t0 = time.perf_counter() if _prof else 0.0
    if result.num_rows == 0:
        table = _empty_table(columns)
    else:
        table = result.select(columns)
    if "value" in table.schema.names:
        idx = table.schema.get_field_index("value")
        arr = table.column(idx).to_numpy(zero_copy_only=False)
        null_mask = np.isnan(arr)
        if null_mask.any():
            table = table.set_column(idx, "value", pa.array(arr, mask=null_mask))
    if _prof:
        profiling._record(profiling.PHASE_READ_BUILD_ARROW, time.perf_counter() - _t0)

    if _prof:
        profiling._record(profiling.PHASE_READ_TOTAL, time.perf_counter() - _t_total)
    return table


def _to_naive_utc(dt: datetime) -> datetime:
    """Convert to naive UTC so clickhouse-connect doesn't apply server_tz offset.

    See https://github.com/ClickHouse/clickhouse-connect/issues/697
    """
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


# ---------------------------------------------------------------------------
# WHERE clause builder
# ---------------------------------------------------------------------------

def _build_ch_where_clause_multi(
    series_ids: List[int],
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None,
    end_known: Optional[datetime] = None,
) -> tuple[str, dict]:
    """Build a ClickHouse WHERE clause for multiple series_ids."""
    filters = ["series_id IN {series_ids:Array(Int64)}"]
    params: dict = {"series_ids": series_ids}

    if start_valid is not None:
        filters.append("valid_time >= {start_valid:DateTime64(6, 'UTC')}")
        params["start_valid"] = _to_naive_utc(start_valid)
    if end_valid is not None:
        filters.append("valid_time < {end_valid:DateTime64(6, 'UTC')}")
        params["end_valid"] = _to_naive_utc(end_valid)
    if start_known is not None:
        filters.append("knowledge_time >= {start_known:DateTime64(6, 'UTC')}")
        params["start_known"] = _to_naive_utc(start_known)
    if end_known is not None:
        filters.append("knowledge_time < {end_known:DateTime64(6, 'UTC')}")
        params["end_known"] = _to_naive_utc(end_known)

    return "WHERE " + " AND ".join(filters), params


# ---------------------------------------------------------------------------
# Multi-series read primitives: 8 functions (4 flat + 4 overlapping) + relative
#
# Naming: read_{flat|overlapping}[_history][_updates]_multi
#   - no suffix:     overlapping=F, include_updates=F  (latest value)
#   - _history:      overlapping=T, include_updates=F  (all knowledge_time versions)
#   - _updates:      overlapping=F, include_updates=T  (correction chain for latest)
#   - _history_updates: overlapping=T, include_updates=T (full audit)
# ---------------------------------------------------------------------------


# ── Flat reads ────────────────────────────────────────────────────────────────

def read_flat_multi(
    ch_client, *, series_ids: List[int], table: str,
    start_valid: Optional[datetime] = None, end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None, end_known: Optional[datetime] = None,
) -> pa.Table:
    """Flat latest: ``argMax(value, change_time)`` per ``(series_id, valid_time)``.

    Returns ``(series_id, valid_time, value)``.
    """
    where, params = _build_ch_where_clause_multi(
        series_ids=series_ids, start_valid=start_valid, end_valid=end_valid,
        start_known=start_known, end_known=end_known,
    )
    sql = f"""
    SELECT series_id, valid_time, argMax(value, change_time) AS value
    FROM {table}
    {where}
    GROUP BY series_id, valid_time
    ORDER BY series_id, valid_time
    """
    return _fetch_ch_arrow(ch_client, sql, params, ["series_id", "valid_time", "value"])


def read_flat_history_multi(
    ch_client, *, series_ids: List[int], table: str,
    start_valid: Optional[datetime] = None, end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None, end_known: Optional[datetime] = None,
) -> pa.Table:
    """Flat with knowledge_time: ``argMax(value, change_time)`` per ``(series_id, knowledge_time, valid_time)``.

    Returns ``(series_id, knowledge_time, valid_time, value)``.
    """
    where, params = _build_ch_where_clause_multi(
        series_ids=series_ids, start_valid=start_valid, end_valid=end_valid,
        start_known=start_known, end_known=end_known,
    )
    sql = f"""
    SELECT series_id, knowledge_time, valid_time, argMax(value, change_time) AS value
    FROM {table}
    {where}
    GROUP BY series_id, knowledge_time, valid_time
    ORDER BY series_id, knowledge_time, valid_time
    """
    return _fetch_ch_arrow(
        ch_client, sql, params,
        ["series_id", "knowledge_time", "valid_time", "value"],
    )


def _read_flat_corrections_multi(
    ch_client, *, series_ids: List[int], table: str,
    include_knowledge_time: bool,
    start_valid: Optional[datetime] = None, end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None, end_known: Optional[datetime] = None,
) -> pa.Table:
    """Internal: flat correction chain with lagInFrame.

    State tuple includes knowledge_time (a kt change is a meaningful correction
    for flat series). Output includes knowledge_time only when *include_knowledge_time*.
    """
    where, params = _build_ch_where_clause_multi(
        series_ids=series_ids, start_valid=start_valid, end_valid=end_valid,
        start_known=start_known, end_known=end_known,
    )
    sql = f"""
    SELECT series_id, valid_time, knowledge_time, change_time, value, changed_by, annotation
    FROM (
        SELECT *,
            lagInFrame(tuple(value, knowledge_time, annotation, changed_by)) OVER (
                PARTITION BY series_id, valid_time
                ORDER BY change_time ASC
            ) AS prev_state
        FROM {table}
        {where}
    )
    WHERE prev_state IS NULL
       OR tuple(value, knowledge_time, annotation, changed_by) IS DISTINCT FROM prev_state
    ORDER BY series_id, valid_time, change_time
    """
    result = _fetch_ch_arrow(
        ch_client, sql, params,
        ["series_id", "valid_time", "knowledge_time", "change_time", "value", "changed_by", "annotation"],
    )
    if not include_knowledge_time:
        idx = result.schema.get_field_index("knowledge_time")
        if idx >= 0:
            result = result.remove_column(idx)
    return result


def read_flat_updates_multi(
    ch_client, *, series_ids: List[int], table: str,
    start_valid: Optional[datetime] = None, end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None, end_known: Optional[datetime] = None,
) -> pa.Table:
    """Flat corrections without knowledge_time in output.

    Returns ``(series_id, valid_time, change_time, value, changed_by, annotation)``.
    """
    return _read_flat_corrections_multi(
        ch_client, series_ids=series_ids, table=table,
        include_knowledge_time=False,
        start_valid=start_valid, end_valid=end_valid,
        start_known=start_known, end_known=end_known,
    )


def read_flat_history_updates_multi(
    ch_client, *, series_ids: List[int], table: str,
    start_valid: Optional[datetime] = None, end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None, end_known: Optional[datetime] = None,
) -> pa.Table:
    """Flat corrections with knowledge_time in output.

    Returns ``(series_id, valid_time, knowledge_time, change_time, value, changed_by, annotation)``.
    """
    return _read_flat_corrections_multi(
        ch_client, series_ids=series_ids, table=table,
        include_knowledge_time=True,
        start_valid=start_valid, end_valid=end_valid,
        start_known=start_known, end_known=end_known,
    )


# ── Overlapping reads ─────────────────────────────────────────────────────────

def read_overlapping_multi(
    ch_client, *, series_ids: List[int], table: str,
    start_valid: Optional[datetime] = None, end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None, end_known: Optional[datetime] = None,
) -> pa.Table:
    """Overlapping latest: ``argMax(value, (knowledge_time, change_time))`` per ``(series_id, valid_time)``.

    Returns ``(series_id, valid_time, value)``.
    """
    where, params = _build_ch_where_clause_multi(
        series_ids=series_ids, start_valid=start_valid, end_valid=end_valid,
        start_known=start_known, end_known=end_known,
    )
    sql = f"""
    SELECT series_id, valid_time, argMax(value, (knowledge_time, change_time)) AS value
    FROM {table}
    {where}
    GROUP BY series_id, valid_time
    ORDER BY series_id, valid_time
    """
    return _fetch_ch_arrow(ch_client, sql, params, ["series_id", "valid_time", "value"])


def read_overlapping_history_multi(
    ch_client, *, series_ids: List[int], table: str,
    start_valid: Optional[datetime] = None, end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None, end_known: Optional[datetime] = None,
) -> pa.Table:
    """Overlapping history: one row per ``(series_id, knowledge_time, valid_time)``.

    Returns ``(series_id, knowledge_time, valid_time, value)``.
    """
    where, params = _build_ch_where_clause_multi(
        series_ids=series_ids, start_valid=start_valid, end_valid=end_valid,
        start_known=start_known, end_known=end_known,
    )
    sql = f"""
    SELECT series_id, knowledge_time, valid_time, argMax(value, change_time) AS value
    FROM {table}
    {where}
    GROUP BY series_id, knowledge_time, valid_time
    ORDER BY series_id, knowledge_time, valid_time
    """
    return _fetch_ch_arrow(
        ch_client, sql, params,
        ["series_id", "knowledge_time", "valid_time", "value"],
    )


def read_overlapping_updates_multi(
    ch_client, *, series_ids: List[int], table: str,
    start_valid: Optional[datetime] = None, end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None, end_known: Optional[datetime] = None,
) -> pa.Table:
    """Overlapping corrections for winning forecast: ``dense_rank`` by ``knowledge_time DESC``.

    Since ``overlapping=False``, knowledge_time is NOT in the output — the winning
    knowledge_time is used internally but hidden from the result.

    Returns ``(series_id, valid_time, change_time, value, changed_by, annotation)``.
    """
    where, params = _build_ch_where_clause_multi(
        series_ids=series_ids, start_valid=start_valid, end_valid=end_valid,
        start_known=start_known, end_known=end_known,
    )
    sql = f"""
    SELECT series_id, valid_time, change_time, value, changed_by, annotation
    FROM (
        SELECT *,
            dense_rank() OVER (
                PARTITION BY series_id, valid_time
                ORDER BY knowledge_time DESC
            ) AS kt_rank
        FROM {table}
        {where}
    )
    WHERE kt_rank = 1
    ORDER BY series_id, valid_time, change_time
    """
    return _fetch_ch_arrow(
        ch_client, sql, params,
        ["series_id", "valid_time", "change_time", "value", "changed_by", "annotation"],
    )


def read_overlapping_history_updates_multi(
    ch_client, *, series_ids: List[int], table: str,
    start_valid: Optional[datetime] = None, end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None, end_known: Optional[datetime] = None,
) -> pa.Table:
    """Overlapping full audit: ``lagInFrame`` per ``(series_id, knowledge_time, valid_time)``.

    Returns ``(series_id, valid_time, knowledge_time, change_time, value, changed_by, annotation)``.
    """
    where, params = _build_ch_where_clause_multi(
        series_ids=series_ids, start_valid=start_valid, end_valid=end_valid,
        start_known=start_known, end_known=end_known,
    )
    sql = f"""
    SELECT series_id, valid_time, knowledge_time, change_time, value, changed_by, annotation
    FROM (
        SELECT *,
            lagInFrame(tuple(value, annotation, changed_by)) OVER (
                PARTITION BY series_id, knowledge_time, valid_time
                ORDER BY change_time ASC
            ) AS prev_state
        FROM {table}
        {where}
    )
    WHERE prev_state IS NULL
       OR tuple(value, annotation, changed_by) IS DISTINCT FROM prev_state
    ORDER BY series_id, valid_time, knowledge_time, change_time
    """
    return _fetch_ch_arrow(
        ch_client, sql, params,
        ["series_id", "valid_time", "knowledge_time", "change_time", "value", "changed_by", "annotation"],
    )


# ── Relative read ─────────────────────────────────────────────────────────────

def read_relative_multi(
    ch_client, *, series_ids: List[int], table: str,
    window_length: timedelta, issue_offset: timedelta, start_window: datetime,
    start_valid: Optional[datetime] = None, end_valid: Optional[datetime] = None,
) -> pa.Table:
    """Read values with per-window knowledge_time cutoff.

    Returns ``(series_id, valid_time, value)``.
    """
    where, params = _build_ch_where_clause_multi(
        series_ids=series_ids, start_valid=start_valid, end_valid=end_valid,
    )
    params.update({
        "window_secs": int(window_length.total_seconds()),
        "offset_secs": int(issue_offset.total_seconds()),
        "start_window": _to_naive_utc(start_window),
    })
    sql = f"""
    SELECT
        series_id,
        valid_time,
        argMax(value, (knowledge_time, change_time)) AS value
    FROM {table}
    {where}
      AND knowledge_time <= addSeconds(
            toStartOfInterval(valid_time,
                toIntervalSecond({{window_secs:Int64}}),
                {{start_window:DateTime64(6, 'UTC')}}),
            {{offset_secs:Int64}})
    GROUP BY series_id, valid_time
    ORDER BY series_id, valid_time
    """
    return _fetch_ch_arrow(ch_client, sql, params, ["series_id", "valid_time", "value"])


# ---------------------------------------------------------------------------
# Single-series wrappers
# ---------------------------------------------------------------------------

def _drop_series_id(table: pa.Table) -> pa.Table:
    """Remove the series_id column from a multi-series result."""
    idx = table.schema.get_field_index("series_id")
    if idx < 0:
        return table
    return table.remove_column(idx)


def read_flat(ch_client, *, series_id: int, **kw) -> pa.Table:
    """Read latest flat values. Returns ``(valid_time, value)``."""
    return _drop_series_id(read_flat_multi(ch_client, series_ids=[series_id], table="flat", **kw))


def read_flat_with_updates(ch_client, *, series_id: int, **kw) -> pa.Table:
    """Read flat correction history. Returns ``(valid_time, change_time, value, changed_by, annotation)``."""
    return _drop_series_id(read_flat_updates_multi(ch_client, series_ids=[series_id], table="flat", **kw))


def read_overlapping_latest(ch_client, *, series_id: int, table: str, **kw) -> pa.Table:
    """Read latest forecast value per valid_time. Returns ``(valid_time, value)``."""
    return _drop_series_id(read_overlapping_multi(ch_client, series_ids=[series_id], table=table, **kw))


def read_overlapping(ch_client, *, series_id: int, table: str, **kw) -> pa.Table:
    """Read forecast history. Returns ``(knowledge_time, valid_time, value)``."""
    return _drop_series_id(read_overlapping_history_multi(ch_client, series_ids=[series_id], table=table, **kw))


def read_overlapping_relative(ch_client, *, series_id: int, table: str, **kw) -> pa.Table:
    """Read with per-window knowledge_time cutoff. Returns ``(valid_time, value)``."""
    return _drop_series_id(read_relative_multi(ch_client, series_ids=[series_id], table=table, **kw))


def read_overlapping_latest_with_updates(ch_client, *, series_id: int, table: str, **kw) -> pa.Table:
    """Read corrections for winning forecast. Returns ``(valid_time, change_time, value, changed_by, annotation)``."""
    return _drop_series_id(read_overlapping_updates_multi(ch_client, series_ids=[series_id], table=table, **kw))


def read_overlapping_with_updates(ch_client, *, series_id: int, table: str, **kw) -> pa.Table:
    """Read full audit log. Returns ``(valid_time, knowledge_time, change_time, value, changed_by, annotation)``."""
    return _drop_series_id(read_overlapping_history_updates_multi(ch_client, series_ids=[series_id], table=table, **kw))


# ---------------------------------------------------------------------------
# Run listing (single-series only)
# ---------------------------------------------------------------------------

def read_runs_for_series(
    ch_client, *, series_id: int, routing: Dict[str, Any],
) -> List[Dict]:
    """Return all runs that contain data for a given series."""
    data_table = "flat" if not routing["overlapping"] else routing["table"]
    sql = f"""
    SELECT DISTINCT
        b.run_id,
        b.workflow_id,
        b.run_start_time,
        b.run_finish_time,
        b.run_params,
        b.inserted_at
    FROM runs_table b FINAL
    JOIN {data_table} f ON f.run_id = b.run_id
    WHERE f.series_id = {{series_id:Int64}}
    ORDER BY b.inserted_at DESC
    """
    result = ch_client.query(sql, parameters={"series_id": series_id})
    columns = ["run_id", "workflow_id", "run_start_time", "run_finish_time",
               "run_params", "inserted_at"]
    return [dict(zip(columns, row)) for row in result.result_rows]
