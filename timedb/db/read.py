import os
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union

import psycopg
import pyarrow as pa

from .. import profiling


# ---------------------------------------------------------------------------
# Arrow type constants
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
}


@contextmanager
def _ensure_conn(conninfo_or_conn):
    """Yield a psycopg Connection, creating one only if given a string."""
    if isinstance(conninfo_or_conn, str):
        with psycopg.connect(conninfo_or_conn) as conn:
            yield conn
    else:
        yield conninfo_or_conn


def _fetch_arrow(
    conn: psycopg.Connection,
    sql: str,
    params: dict,
    columns: List[str],
) -> pa.Table:
    """Execute *sql* with a binary cursor and return results as a ``pa.Table``.

    Using the psycopg3 binary wire protocol avoids the text-to-datetime
    parsing overhead in ``pd.read_sql`` and yields a zero-copy Arrow table
    that can be wrapped in ``TimeSeries.from_arrow()`` without further
    conversion.

    Parameters
    ----------
    conn:
        An open psycopg ``Connection``.
    sql:
        SQL query string with ``%(name)s`` placeholders.
    params:
        Query parameter dict.
    columns:
        Column names in the order they appear in the SELECT clause.
        Used to build the Arrow schema; each name must be a key in
        ``_COL_ARROW_TYPE``.
    """
    _prof = profiling._enabled
    _t_total = time.perf_counter() if _prof else 0.0

    with conn.cursor(binary=True) as cur:
        _t0 = time.perf_counter() if _prof else 0.0
        cur.execute(sql, params)  # type: ignore[arg-type]  # psycopg stubs are stricter than runtime
        if _prof: profiling._record(profiling.PHASE_READ_SQL_EXEC, time.perf_counter() - _t0)

        _t0 = time.perf_counter() if _prof else 0.0
        rows = cur.fetchall()
        if _prof: profiling._record(profiling.PHASE_READ_FETCH_ROWS, time.perf_counter() - _t0)

    _t0 = time.perf_counter() if _prof else 0.0
    if not rows:
        result = pa.table({c: pa.array([], type=_COL_ARROW_TYPE[c]) for c in columns})
    else:
        # Transpose row-major list-of-tuples → column-major tuple-of-lists.
        col_data = list(zip(*rows))
        result = pa.table({
            col: pa.array(data, type=_COL_ARROW_TYPE[col])
            for col, data in zip(columns, col_data)
        })
    if _prof: profiling._record(profiling.PHASE_READ_BUILD_ARROW, time.perf_counter() - _t0)

    if _prof: profiling._record(profiling.PHASE_READ_TOTAL, time.perf_counter() - _t_total)
    return result


def _build_where_clause(
    series_id: int,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None,
    end_known: Optional[datetime] = None,
) -> tuple[str, dict]:
    """
    Build WHERE clause and parameters for time series queries.

    Returns:
        Tuple of (where_clause_string, params_dict)
    """
    filters = []
    params = {}

    filters.append("v.series_id = %(series_id)s")
    params["series_id"] = series_id

    if start_valid is not None:
        filters.append("v.valid_time >= %(start_valid)s")
        params["start_valid"] = start_valid
    if end_valid is not None:
        filters.append("v.valid_time < %(end_valid)s")
        params["end_valid"] = end_valid

    if start_known is not None:
        filters.append("v.knowledge_time >= %(start_known)s")
        params["start_known"] = start_known
    if end_known is not None:
        filters.append("v.knowledge_time < %(end_known)s")
        params["end_known"] = end_known

    where_clause = ""
    if filters:
        where_clause = "WHERE " + " AND ".join(filters)

    return where_clause, params


def read_flat(
    conninfo: Union[psycopg.Connection, str],
    *,
    series_id: int,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None,
    end_known: Optional[datetime] = None,
) -> pa.Table:
    """
    Read flat (fact) values from the flat table.

    Args:
        conninfo: Database connection or connection string
        series_id: Series ID (required)
        start_valid: Start of time range (optional)
        end_valid: End of time range (optional)
        start_known: Start of knowledge_time range (optional)
        end_known: End of knowledge_time range (optional)

    Returns:
        ``pa.Table`` with columns ``(valid_time, value)``.
    """
    where_clause, params = _build_where_clause(
        series_id=series_id,
        start_valid=start_valid,
        end_valid=end_valid,
        start_known=start_known,
        end_known=end_known,
    )

    sql = f"""
    SELECT v.valid_time, v.value
    FROM flat v
    {where_clause}
    ORDER BY v.valid_time;
    """

    with _ensure_conn(conninfo) as conn:
        return _fetch_arrow(conn, sql, params, ["valid_time", "value"])


def read_overlapping_latest(
    conninfo: Union[psycopg.Connection, str],
    *,
    series_id: int,
    table: str,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None,
    end_known: Optional[datetime] = None,
) -> pa.Table:
    """
    Read latest overlapping values from the overlapping table.

    Returns the latest value for each valid_time,
    determined by the most recent knowledge_time via DISTINCT ON.

    Args:
        conninfo: Database connection or connection string
        series_id: Series ID (required)
        start_valid: Start of valid time range (optional)
        end_valid: End of valid time range (optional)
        start_known: Start of knowledge_time range (optional)
        end_known: End of knowledge_time range (optional)

    Returns:
        ``pa.Table`` with columns ``(valid_time, value)``.
    """
    where_clause, params = _build_where_clause(
        series_id=series_id,
        start_valid=start_valid,
        end_valid=end_valid,
        start_known=start_known,
        end_known=end_known,
    )

    sql = f"""
    SELECT DISTINCT ON (v.valid_time)
        v.valid_time, v.value
    FROM {table} v
    {where_clause}
    ORDER BY v.valid_time, v.knowledge_time DESC, v.change_time DESC;
    """

    with _ensure_conn(conninfo) as conn:
        return _fetch_arrow(conn, sql, params, ["valid_time", "value"])


def read_overlapping_relative(
    conninfo: Union[psycopg.Connection, str],
    *,
    series_id: int,
    table: str,
    window_length: timedelta,
    issue_offset: timedelta,
    start_window: datetime,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
) -> pa.Table:
    """
    Read overlapping values using a per-window knowledge_time cutoff.

    For each valid_time, computes the window it belongs to (aligned to
    start_window with period window_length), then returns the latest forecast
    with knowledge_time <= window_start + issue_offset.

    Args:
        conninfo: Database connection or connection string
        series_id: Series ID (required)
        window_length: Length of each window (e.g., timedelta(hours=24))
        issue_offset: Offset from window_start for the knowledge_time cutoff.
                      Negative means before the window starts
                      (e.g., timedelta(hours=-12) = 12h before window start).
        start_window: Origin for window alignment (required).
        start_valid: Start of valid time range (optional)
        end_valid: End of valid time range (optional)

    Returns:
        ``pa.Table`` with columns ``(valid_time, value)``.
    """
    where_clause, params = _build_where_clause(
        series_id=series_id,
        start_valid=start_valid,
        end_valid=end_valid,
    )
    params.update({
        "start_window": start_window,
        "window_secs": window_length.total_seconds(),
        "issue_offset_secs": issue_offset.total_seconds(),
    })

    sql = f"""
    WITH windowed AS (
        SELECT
            v.valid_time,
            v.value,
            v.knowledge_time,
            v.change_time,
            -- Cutoff: latest allowed knowledge_time for the window containing this valid_time
            %(start_window)s
            + make_interval(secs => floor(
                extract(epoch from (v.valid_time - %(start_window)s))
                / %(window_secs)s
              ) * %(window_secs)s
            )
            + make_interval(secs => %(issue_offset_secs)s)
            AS cutoff_time
        FROM {table} v
        {where_clause}
    )
    SELECT DISTINCT ON (valid_time)
        valid_time, value
    FROM windowed
    WHERE knowledge_time <= cutoff_time
    ORDER BY valid_time, knowledge_time DESC, change_time DESC;
    """

    with _ensure_conn(conninfo) as conn:
        return _fetch_arrow(conn, sql, params, ["valid_time", "value"])


def read_overlapping(
    conninfo: Union[psycopg.Connection, str],
    *,
    series_id: int,
    table: str,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None,
    end_known: Optional[datetime] = None,
) -> pa.Table:
    """
    Read overlapping forecast history, one row per (knowledge_time, valid_time).

    Returns the latest correction for each forecast run × valid_time combination,
    hiding the internal correction chain. Use this to see how the forecast evolved
    across runs without seeing individual manual corrections.

    Args:
        conninfo: Database connection or connection string
        series_id: Series ID (required)
        start_valid: Start of valid time range (optional)
        end_valid: End of valid time range (optional)
        start_known: Start of knowledge_time range (optional)
        end_known: End of knowledge_time range (optional)

    Returns:
        ``pa.Table`` with columns ``(knowledge_time, valid_time, value)``.
    """
    where_clause, params = _build_where_clause(
        series_id=series_id,
        start_valid=start_valid,
        end_valid=end_valid,
        start_known=start_known,
        end_known=end_known,
    )

    sql = f"""
    SELECT DISTINCT ON (v.knowledge_time, v.valid_time)
        v.knowledge_time, v.valid_time, v.value
    FROM {table} v
    {where_clause}
    ORDER BY v.knowledge_time, v.valid_time, v.change_time DESC;
    """

    with _ensure_conn(conninfo) as conn:
        return _fetch_arrow(conn, sql, params, ["knowledge_time", "valid_time", "value"])


def read_overlapping_latest_with_updates(
    conninfo: Union[psycopg.Connection, str],
    *,
    series_id: int,
    table: str,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None,
    end_known: Optional[datetime] = None,
) -> pa.Table:
    """
    Read all corrections for the currently winning forecast run per valid_time.

    For each valid_time, identifies the winning knowledge_time (the latest one),
    then returns every correction row for that (valid_time, winning_knowledge_time)
    pair. knowledge_time is intentionally not included in the result — use this to
    see who edited the numbers you are currently using, and when.

    Args:
        conninfo: Database connection or connection string
        series_id: Series ID (required)
        start_valid: Start of valid time range (optional)
        end_valid: End of valid time range (optional)
        start_known: Start of knowledge_time range (optional)
        end_known: End of knowledge_time range (optional)

    Returns:
        ``pa.Table`` with columns ``(valid_time, change_time, value, changed_by, annotation)``.
    """
    where_clause, params = _build_where_clause(
        series_id=series_id,
        start_valid=start_valid,
        end_valid=end_valid,
        start_known=start_known,
        end_known=end_known,
    )

    sql = f"""
    WITH winning AS (
        SELECT DISTINCT ON (v.valid_time)
            v.valid_time, v.knowledge_time
        FROM {table} v
        {where_clause}
        ORDER BY v.valid_time, v.knowledge_time DESC
    )
    SELECT v.valid_time, v.change_time, v.value, v.changed_by, v.annotation
    FROM {table} v
    JOIN winning w ON w.valid_time = v.valid_time AND w.knowledge_time = v.knowledge_time
    WHERE v.series_id = %(series_id)s
    ORDER BY v.valid_time, v.change_time;
    """

    with _ensure_conn(conninfo) as conn:
        return _fetch_arrow(
            conn, sql, params,
            ["valid_time", "change_time", "value", "changed_by", "annotation"],
        )


def read_flat_with_updates(
    conninfo: Union[psycopg.Connection, str],
    *,
    series_id: int,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None,
    end_known: Optional[datetime] = None,
) -> pa.Table:
    """
    Read flat values with edit metadata (change_time, changed_by, annotation).

    Since flat series use in-place updates, this returns exactly one row
    per valid_time reflecting the latest state, with edit metadata exposed.

    Args:
        conninfo: Database connection or connection string
        series_id: Series ID (required)
        start_valid: Start of valid time range (optional)
        end_valid: End of valid time range (optional)
        start_known: Start of knowledge_time range (optional)
        end_known: End of knowledge_time range (optional)

    Returns:
        ``pa.Table`` with columns ``(valid_time, change_time, value, changed_by, annotation)``.
    """
    where_clause, params = _build_where_clause(
        series_id=series_id,
        start_valid=start_valid,
        end_valid=end_valid,
        start_known=start_known,
        end_known=end_known,
    )

    sql = f"""
    SELECT v.valid_time, v.change_time, v.value, v.changed_by, v.annotation
    FROM flat v
    {where_clause}
    ORDER BY v.valid_time, v.change_time;
    """

    with _ensure_conn(conninfo) as conn:
        return _fetch_arrow(
            conn, sql, params,
            ["valid_time", "change_time", "value", "changed_by", "annotation"],
        )


def read_overlapping_with_updates(
    conninfo: Union[psycopg.Connection, str],
    *,
    series_id: int,
    table: str,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None,
    end_known: Optional[datetime] = None,
) -> pa.Table:
    """
    Read the full audit log from the overlapping table.

    Returns every row ever written, including all manual corrections.
    Each (knowledge_time, valid_time) pair may appear multiple times — once per
    correction — ordered by change_time within each group.

    Args:
        conninfo: Database connection or connection string
        series_id: Series ID (required)
        start_valid: Start of valid time range (optional)
        end_valid: End of valid time range (optional)
        start_known: Start of knowledge_time range (optional)
        end_known: End of knowledge_time range (optional)

    Returns:
        ``pa.Table`` with columns ``(knowledge_time, change_time, valid_time, value, changed_by, annotation)``.
    """
    where_clause, params = _build_where_clause(
        series_id=series_id,
        start_valid=start_valid,
        end_valid=end_valid,
        start_known=start_known,
        end_known=end_known,
    )

    sql = f"""
    SELECT v.knowledge_time, v.change_time, v.valid_time, v.value, v.changed_by, v.annotation
    FROM {table} v
    {where_clause}
    ORDER BY v.knowledge_time, v.change_time, v.valid_time;
    """

    with _ensure_conn(conninfo) as conn:
        return _fetch_arrow(
            conn, sql, params,
            ["knowledge_time", "change_time", "valid_time", "value", "changed_by", "annotation"],
        )


if __name__ == "__main__":
    conninfo = os.environ["DATABASE_URL"]
    # Example: df = read_overlapping_latest(conninfo, series_id=1)
    print("Run with series_id parameter")
