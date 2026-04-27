"""Bitemporal read path for timedb.

Output shape by flag combination:

    include_updates=False, include_knowledge_time=False
        (series_id, valid_time, value)
    include_updates=False, include_knowledge_time=True
        (series_id, knowledge_time, valid_time, value)
    include_updates=True,  include_knowledge_time=False
        (series_id, valid_time, change_time, value, changed_by, annotation)
    include_updates=True,  include_knowledge_time=True
        (series_id, valid_time, knowledge_time, change_time, value,
         changed_by, annotation)
"""

from __future__ import annotations

import time as _time
from collections.abc import Sequence
from datetime import datetime, timedelta
from datetime import time as dt_time

import numpy as np
import polars as pl
import pyarrow as pa

from . import profiling

_TS_TYPE = pa.timestamp("us", tz="UTC")
_COL_ARROW_TYPE: dict[str, pa.DataType] = {
    "series_id": pa.uint64(),
    "valid_time": _TS_TYPE,
    "knowledge_time": _TS_TYPE,
    "change_time": _TS_TYPE,
    "value": pa.float64(),
    "changed_by": pa.string(),
    "annotation": pa.string(),
}


def _empty(cols: list[str]) -> pa.Table:
    return pa.table({c: pa.array([], type=_COL_ARROW_TYPE[c]) for c in cols})


def _fetch(ch_client, sql: str, params: dict, cols: list[str]) -> pa.Table:
    _prof = profiling._enabled

    _t = _time.perf_counter() if _prof else 0.0
    result = ch_client.query_arrow(sql, parameters=params)
    if _prof:
        profiling._record(profiling.PHASE_READ_SQL_EXEC, _time.perf_counter() - _t)

    _t = _time.perf_counter() if _prof else 0.0
    table = _empty(cols) if result.num_rows == 0 else result.select(cols)
    if "value" in table.schema.names:
        idx = table.schema.get_field_index("value")
        arr = table.column(idx).to_numpy(zero_copy_only=False)
        mask = np.isnan(arr)
        if mask.any():
            table = table.set_column(idx, "value", pa.array(arr, mask=mask))
    if _prof:
        profiling._record(profiling.PHASE_READ_BUILD_ARROW, _time.perf_counter() - _t)
    return table


def _where(
    *,
    series_ids: Sequence[int],
    retention: str | Sequence[str] | None,
    start_valid: datetime | None = None,
    end_valid: datetime | None = None,
    start_known: datetime | None = None,
    end_known: datetime | None = None,
) -> tuple[str, dict]:
    filters = ["series_id IN {series_ids:Array(UInt64)}"]
    params: dict = {"series_ids": list(series_ids)}

    if retention is not None:
        if isinstance(retention, str):
            filters.append("retention = {retention:String}")
            params["retention"] = retention
        else:
            filters.append("retention IN {retentions:Array(String)}")
            params["retentions"] = list(retention)

    if start_valid is not None:
        filters.append("valid_time >= {start_valid:DateTime64(6, 'UTC')}")
        params["start_valid"] = start_valid
    if end_valid is not None:
        filters.append("valid_time < {end_valid:DateTime64(6, 'UTC')}")
        params["end_valid"] = end_valid
    if start_known is not None:
        filters.append("knowledge_time >= {start_known:DateTime64(6, 'UTC')}")
        params["start_known"] = start_known
    if end_known is not None:
        filters.append("knowledge_time < {end_known:DateTime64(6, 'UTC')}")
        params["end_known"] = end_known
    return "WHERE " + " AND ".join(filters), params


# ---------------------------------------------------------------------------
# Aggregate reads
# ---------------------------------------------------------------------------


def _read_latest(ch_client, where: str, params: dict) -> pa.Table:
    """Latest value per (series_id, valid_time).

    The tuple-argMax picks the row with the largest (knowledge_time,
    change_time) — latest issue, latest correction within it.
    """
    # GROUP BY columns are a primary-key prefix; optimize_aggregation_in_order
    # streams the aggregation rather than building a full hash table.
    sql = f"""
    SELECT series_id, valid_time, argMax(value, (knowledge_time, change_time)) AS value
    FROM events
    {where}
    GROUP BY series_id, valid_time
    ORDER BY series_id, valid_time
    SETTINGS optimize_aggregation_in_order = 1
    """
    return _fetch(ch_client, sql, params, ["series_id", "valid_time", "value"])


def _read_history(ch_client, where: str, params: dict) -> pa.Table:
    """One row per (series_id, knowledge_time, valid_time)."""
    # No final ORDER BY: emits in primary-key order (sid, vt, kt) directly.
    # A re-sort to (sid, kt, vt) would buffer the entire result; callers that
    # need a different order should sort post-aggregation in Polars.
    sql = f"""
    SELECT series_id, knowledge_time, valid_time, argMax(value, change_time) AS value
    FROM events
    {where}
    GROUP BY series_id, valid_time, knowledge_time
    SETTINGS optimize_aggregation_in_order = 1
    """
    return _fetch(
        ch_client,
        sql,
        params,
        ["series_id", "knowledge_time", "valid_time", "value"],
    )


# ---------------------------------------------------------------------------
# Correction-chain reads
# ---------------------------------------------------------------------------


def _read_updates_winning(ch_client, where: str, params: dict) -> pa.Table:
    """Correction chain of the winning forecast per (series_id, valid_time).

    Emits only real state transitions (consecutive duplicates collapsed by
    the lagInFrame distinct-from-previous filter).
    """
    # Semi-join: the inner query picks max(knowledge_time) per (sid, vt) along
    # the sort-key prefix; the outer filter is a tuple PK match (seek, not sort).
    sql = f"""
    SELECT series_id, valid_time, change_time, value, changed_by, annotation
    FROM (
        SELECT
            series_id, valid_time, change_time, value, changed_by, annotation,
            lagInFrame(tuple(value, annotation, changed_by)) OVER (
                PARTITION BY series_id, valid_time
                ORDER BY change_time ASC
            ) AS prev_state
        FROM events
        {where}
          AND (series_id, valid_time, knowledge_time) IN (
              SELECT series_id, valid_time, max(knowledge_time)
              FROM events
              {where}
              GROUP BY series_id, valid_time
          )
    )
    WHERE prev_state IS NULL
       OR tuple(value, annotation, changed_by) IS DISTINCT FROM prev_state
    ORDER BY series_id, valid_time, change_time
    """
    return _fetch(
        ch_client,
        sql,
        params,
        ["series_id", "valid_time", "change_time", "value", "changed_by", "annotation"],
    )


def _read_updates_full(ch_client, where: str, params: dict) -> pa.Table:
    """Full bitemporal audit: every state transition per (series_id, kt, vt)."""
    sql = f"""
    SELECT series_id, valid_time, knowledge_time, change_time, value, changed_by, annotation
    FROM (
        SELECT
            series_id, valid_time, knowledge_time, change_time, value, changed_by, annotation,
            lagInFrame(tuple(value, annotation, changed_by)) OVER (
                PARTITION BY series_id, knowledge_time, valid_time
                ORDER BY change_time ASC
            ) AS prev_state
        FROM events
        {where}
    )
    WHERE prev_state IS NULL
       OR tuple(value, annotation, changed_by) IS DISTINCT FROM prev_state
    ORDER BY series_id, valid_time, knowledge_time, change_time
    """
    return _fetch(
        ch_client,
        sql,
        params,
        ["series_id", "valid_time", "knowledge_time", "change_time", "value", "changed_by", "annotation"],
    )


# ---------------------------------------------------------------------------
# Relative read
# ---------------------------------------------------------------------------


def _read_relative_sql(
    ch_client,
    *,
    series_ids: Sequence[int],
    retention: str | Sequence[str] | None,
    window_length: timedelta,
    issue_offset: timedelta,
    start_window: datetime,
    start_valid: datetime | None,
    end_valid: datetime | None,
) -> pa.Table:
    where, params = _where(
        series_ids=series_ids,
        retention=retention,
        start_valid=start_valid,
        end_valid=end_valid,
    )
    params.update(
        {
            "window_secs": int(window_length.total_seconds()),
            "offset_secs": int(issue_offset.total_seconds()),
            "start_window": start_window,
        }
    )
    sql = f"""
    SELECT series_id, valid_time, argMax(value, (knowledge_time, change_time)) AS value
    FROM events
    {where}
      AND knowledge_time <= addSeconds(
            toStartOfInterval(valid_time,
                toIntervalSecond({{window_secs:Int64}}),
                {{start_window:DateTime64(6, 'UTC')}}),
            {{offset_secs:Int64}})
    GROUP BY series_id, valid_time
    ORDER BY series_id, valid_time
    SETTINGS optimize_aggregation_in_order = 1
    """
    return _fetch(ch_client, sql, params, ["series_id", "valid_time", "value"])


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------


def read(
    ch_client,
    *,
    series_ids: Sequence[int],
    retention: str | Sequence[str] | None = None,
    start_valid: datetime | None = None,
    end_valid: datetime | None = None,
    start_known: datetime | None = None,
    end_known: datetime | None = None,
    include_updates: bool = False,
    include_knowledge_time: bool = False,
) -> pl.DataFrame:
    _prof = profiling._enabled
    _t_total = _time.perf_counter() if _prof else 0.0

    series_ids = list(series_ids)
    if not series_ids:
        return pl.DataFrame()

    where, params = _where(
        series_ids=series_ids,
        retention=retention,
        start_valid=start_valid,
        end_valid=end_valid,
        start_known=start_known,
        end_known=end_known,
    )

    if include_updates:
        arrow = (
            _read_updates_full(ch_client, where, params)
            if include_knowledge_time
            else _read_updates_winning(ch_client, where, params)
        )
    else:
        arrow = (
            _read_history(ch_client, where, params)
            if include_knowledge_time
            else _read_latest(ch_client, where, params)
        )

    _t = _time.perf_counter() if _prof else 0.0
    result = pl.from_arrow(arrow)
    if _prof:
        profiling._record(profiling.PHASE_READ_TO_POLARS, _time.perf_counter() - _t)
        profiling._record(profiling.PHASE_READ_TOTAL, _time.perf_counter() - _t_total)

    assert isinstance(result, pl.DataFrame)
    return result


def read_relative(
    ch_client,
    *,
    series_ids: Sequence[int],
    retention: str | Sequence[str] | None = None,
    window_length: timedelta | None = None,
    issue_offset: timedelta | None = None,
    start_window: datetime | None = None,
    start_valid: datetime | None = None,
    end_valid: datetime | None = None,
    days_ahead: int | None = None,
    time_of_day: dt_time | None = None,
) -> pl.DataFrame:
    using_daily = days_ahead is not None or time_of_day is not None
    using_explicit = window_length is not None or issue_offset is not None
    if using_daily and using_explicit:
        raise ValueError("Cannot mix (days_ahead, time_of_day) with (window_length, issue_offset). Use one set.")

    if using_daily:
        if days_ahead is None or time_of_day is None:
            raise ValueError("Both days_ahead and time_of_day must be provided together.")
        if start_valid is None:
            raise ValueError("start_valid is required when using days_ahead/time_of_day.")
        window_length = timedelta(days=1)
        issue_offset = timedelta(
            hours=time_of_day.hour,
            minutes=time_of_day.minute,
            seconds=time_of_day.second,
            microseconds=time_of_day.microsecond,
        ) - timedelta(days=days_ahead)
        start_window = start_valid.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    else:
        if window_length is None or issue_offset is None:
            raise ValueError("Both window_length and issue_offset are required.")
        start_window = start_window if start_window is not None else start_valid
        if start_window is None:
            raise ValueError("start_window is required when start_valid is not provided.")

    series_ids = list(series_ids)
    if not series_ids:
        return pl.DataFrame()

    _prof = profiling._enabled
    _t_total = _time.perf_counter() if _prof else 0.0

    arrow = _read_relative_sql(
        ch_client,
        series_ids=series_ids,
        retention=retention,
        window_length=window_length,
        issue_offset=issue_offset,
        start_window=start_window,
        start_valid=start_valid,
        end_valid=end_valid,
    )

    _t = _time.perf_counter() if _prof else 0.0
    result = pl.from_arrow(arrow)
    if _prof:
        profiling._record(profiling.PHASE_READ_TO_POLARS, _time.perf_counter() - _t)
        profiling._record(profiling.PHASE_READ_TOTAL, _time.perf_counter() - _t_total)

    assert isinstance(result, pl.DataFrame)
    return result
