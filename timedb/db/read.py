import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import numpy as np
import pyarrow as pa

from .. import profiling


# ---------------------------------------------------------------------------
# Arrow type constants (used for empty-table construction)
# ---------------------------------------------------------------------------

_TS_TYPE = pa.timestamp("us", tz="UTC")

_COL_ARROW_TYPE: Dict[str, pa.DataType] = {
    "valid_time":     _TS_TYPE,
    "valid_time_end": _TS_TYPE,
    "knowledge_time": _TS_TYPE,
    "change_time":    _TS_TYPE,
    "value":          pa.float64(),
    "changed_by":     pa.string(),
    "annotation":     pa.string(),
    "batch_id":       pa.string(),
    "workflow_id":    pa.string(),
    "batch_start_time":  _TS_TYPE,
    "batch_finish_time": _TS_TYPE,
    "batch_params":   pa.string(),
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
    """Execute *sql* against ClickHouse and return results as a ``pa.Table``.

    Parameters
    ----------
    ch_client:
        clickhouse_connect Client instance.
    sql:
        SQL query with ``{name:Type}`` placeholders.
    parameters:
        Query parameter dict.
    columns:
        Column names in the order they appear in the SELECT clause.
    """
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


def _build_ch_where_clause(
    series_id: int,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None,
    end_known: Optional[datetime] = None,
) -> tuple[str, dict]:
    """Build a ClickHouse WHERE clause with ``{name:Type}`` placeholders."""
    filters = ["series_id = {series_id:Int64}"]
    params: dict = {"series_id": series_id}

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
# Flat reads
# ---------------------------------------------------------------------------

def read_flat(
    ch_client,
    *,
    series_id: int,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None,
    end_known: Optional[datetime] = None,
) -> pa.Table:
    """Read latest flat (fact) values.

    Returns the most recent value per valid_time, determined by the highest
    change_time (last write wins).

    Returns:
        ``pa.Table`` with columns ``(valid_time, value)``.
    """
    where, params = _build_ch_where_clause(
        series_id=series_id,
        start_valid=start_valid,
        end_valid=end_valid,
        start_known=start_known,
        end_known=end_known,
    )
    sql = f"""
    SELECT valid_time, argMax(value, change_time) AS value
    FROM flat
    {where}
    GROUP BY valid_time
    ORDER BY valid_time
    """
    return _fetch_ch_arrow(ch_client, sql, params, ["valid_time", "value"])


def read_flat_with_updates(
    ch_client,
    *,
    series_id: int,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None,
    end_known: Optional[datetime] = None,
) -> pa.Table:
    """Read full correction history for flat series.

    Returns every genuine correction row, filtered with lagInFrame on all
    mutable columns so that pure-duplicate inserts (same data written twice)
    are collapsed. Only rows where at least one of value/annotation/tags/
    changed_by actually changed are returned.

    Returns:
        ``pa.Table`` with columns ``(valid_time, change_time, value, changed_by, annotation)``.
    """
    where, params = _build_ch_where_clause(
        series_id=series_id,
        start_valid=start_valid,
        end_valid=end_valid,
        start_known=start_known,
        end_known=end_known,
    )
    sql = f"""
    SELECT valid_time, change_time, value, changed_by, annotation
    FROM (
        SELECT *,
            lagInFrame(tuple(value, annotation, changed_by)) OVER (
                PARTITION BY series_id, valid_time
                ORDER BY change_time ASC
            ) AS prev_state
        FROM flat
        {where}
    )
    WHERE prev_state IS NULL
       OR tuple(value, annotation, changed_by) IS DISTINCT FROM prev_state
    ORDER BY valid_time, change_time
    """
    return _fetch_ch_arrow(
        ch_client, sql, params,
        ["valid_time", "change_time", "value", "changed_by", "annotation"],
    )


# ---------------------------------------------------------------------------
# Overlapping reads
# ---------------------------------------------------------------------------

def read_overlapping_latest(
    ch_client,
    *,
    series_id: int,
    table: str,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None,
    end_known: Optional[datetime] = None,
) -> pa.Table:
    """Read latest forecast value per valid_time.

    Uses argMax with tuple comparison: latest knowledge_time wins,
    then latest change_time as tiebreaker within the same forecast run.

    Returns:
        ``pa.Table`` with columns ``(valid_time, value)``.
    """
    where, params = _build_ch_where_clause(
        series_id=series_id,
        start_valid=start_valid,
        end_valid=end_valid,
        start_known=start_known,
        end_known=end_known,
    )
    sql = f"""
    SELECT valid_time, argMax(value, (knowledge_time, change_time)) AS value
    FROM {table}
    {where}
    GROUP BY valid_time
    ORDER BY valid_time
    """
    return _fetch_ch_arrow(ch_client, sql, params, ["valid_time", "value"])


def read_overlapping(
    ch_client,
    *,
    series_id: int,
    table: str,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None,
    end_known: Optional[datetime] = None,
) -> pa.Table:
    """Read forecast history — one row per (knowledge_time, valid_time).

    Returns the latest correction per forecast run × valid_time, hiding the
    internal correction chain. Use this to see how forecasts evolved across
    runs without seeing individual manual corrections.

    Returns:
        ``pa.Table`` with columns ``(knowledge_time, valid_time, value)``.
    """
    where, params = _build_ch_where_clause(
        series_id=series_id,
        start_valid=start_valid,
        end_valid=end_valid,
        start_known=start_known,
        end_known=end_known,
    )
    sql = f"""
    SELECT knowledge_time, valid_time, argMax(value, change_time) AS value
    FROM {table}
    {where}
    GROUP BY knowledge_time, valid_time
    ORDER BY knowledge_time, valid_time
    """
    return _fetch_ch_arrow(
        ch_client, sql, params,
        ["knowledge_time", "valid_time", "value"],
    )


def read_overlapping_relative(
    ch_client,
    *,
    series_id: int,
    table: str,
    window_length: timedelta,
    issue_offset: timedelta,
    start_window: datetime,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
) -> pa.Table:
    """Read overlapping values using a per-window knowledge_time cutoff.

    For each valid_time, computes the window it belongs to (aligned to
    start_window with period window_length), then returns the latest forecast
    with knowledge_time <= window_start + issue_offset.

    Args:
        window_length: Length of each window (e.g., timedelta(hours=24)).
        issue_offset: Offset from window_start for the knowledge_time cutoff.
                      Negative means before the window starts
                      (e.g., timedelta(hours=-12) = 12h before window start).
        start_window: Origin for window alignment (required).

    Returns:
        ``pa.Table`` with columns ``(valid_time, value)``.
    """
    where, params = _build_ch_where_clause(
        series_id=series_id,
        start_valid=start_valid,
        end_valid=end_valid,
    )
    params.update({
        "window_secs": int(window_length.total_seconds()),
        "offset_secs": int(issue_offset.total_seconds()),
        "start_window": start_window,
    })
    sql = f"""
    SELECT
        valid_time,
        argMax(value, (knowledge_time, change_time)) AS value
    FROM {table}
    {where}
      AND knowledge_time <= addSeconds(
            toStartOfInterval(valid_time,
                toIntervalSecond({{window_secs:Int64}}),
                {{start_window:DateTime64(6, 'UTC')}}),
            {{offset_secs:Int64}})
    GROUP BY valid_time
    ORDER BY valid_time
    """
    return _fetch_ch_arrow(ch_client, sql, params, ["valid_time", "value"])


def read_overlapping_latest_with_updates(
    ch_client,
    *,
    series_id: int,
    table: str,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None,
    end_known: Optional[datetime] = None,
) -> pa.Table:
    """Read all corrections for the currently winning forecast run per valid_time.

    For each valid_time, identifies the winning knowledge_time (the latest one)
    via dense_rank(), then returns every correction row for that
    (valid_time, winning_knowledge_time) pair. This shows who edited the numbers
    you are currently using, and when, without exposing which knowledge_time won.

    Returns:
        ``pa.Table`` with columns ``(valid_time, change_time, value, changed_by, annotation)``.
    """
    where, params = _build_ch_where_clause(
        series_id=series_id,
        start_valid=start_valid,
        end_valid=end_valid,
        start_known=start_known,
        end_known=end_known,
    )
    sql = f"""
    SELECT valid_time, change_time, value, changed_by, annotation
    FROM (
        SELECT *,
            dense_rank() OVER (
                PARTITION BY valid_time
                ORDER BY knowledge_time DESC
            ) AS kt_rank
        FROM {table}
        {where}
    )
    WHERE kt_rank = 1
    ORDER BY valid_time, change_time
    """
    return _fetch_ch_arrow(
        ch_client, sql, params,
        ["valid_time", "change_time", "value", "changed_by", "annotation"],
    )


def read_overlapping_with_updates(
    ch_client,
    *,
    series_id: int,
    table: str,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None,
    end_known: Optional[datetime] = None,
) -> pa.Table:
    """Read the full audit log from the overlapping table.

    Returns every genuine correction row using lagInFrame on all mutable columns.
    Rows where no mutable field changed (pure-duplicate inserts) are filtered out.

    Returns:
        ``pa.Table`` with columns ``(knowledge_time, change_time, valid_time, value, changed_by, annotation)``.
    """
    where, params = _build_ch_where_clause(
        series_id=series_id,
        start_valid=start_valid,
        end_valid=end_valid,
        start_known=start_known,
        end_known=end_known,
    )
    sql = f"""
    SELECT knowledge_time, change_time, valid_time, value, changed_by, annotation
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
    ORDER BY knowledge_time, change_time, valid_time
    """
    return _fetch_ch_arrow(
        ch_client, sql, params,
        ["knowledge_time", "change_time", "valid_time", "value", "changed_by", "annotation"],
    )


def read_batches_for_series(
    ch_client,
    *,
    series_id: int,
    routing: Dict[str, Any],
) -> List[Dict]:
    """Return all batches that contain data for a given series.

    Performs a pure ClickHouse JOIN between batches_table and the values table.

    Args:
        ch_client: clickhouse_connect Client.
        series_id: Series ID.
        routing: Routing dict with keys overlapping (bool) and table (str).

    Returns:
        List of dicts with keys: batch_id, workflow_id, batch_start_time,
        batch_finish_time, batch_params, inserted_at.
    """
    data_table = "flat" if not routing["overlapping"] else routing["table"]
    sql = f"""
    SELECT DISTINCT
        b.batch_id,
        b.workflow_id,
        b.batch_start_time,
        b.batch_finish_time,
        b.batch_params,
        b.inserted_at
    FROM batches_table b FINAL
    JOIN {data_table} f ON f.batch_id = b.batch_id
    WHERE f.series_id = {{series_id:Int64}}
    ORDER BY b.inserted_at DESC
    """
    result = ch_client.query(sql, parameters={"series_id": series_id})
    columns = ["batch_id", "workflow_id", "batch_start_time", "batch_finish_time",
               "batch_params", "inserted_at"]
    return [dict(zip(columns, row)) for row in result.result_rows]
