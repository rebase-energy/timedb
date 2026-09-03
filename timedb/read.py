"""3-dimensional read path for timedb.

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
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from datetime import time as dt_time

import numpy as np
import polars as pl
import pyarrow as pa
import pyarrow.compute as pc

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


RELATIVE_READ_COLUMNS: tuple[str, ...] = ("series_id", "valid_time", "value")


def valid_time_range(ch_client, series_ids: Sequence[int]) -> tuple[datetime, datetime] | None:
    """Overall ``[min, max]`` valid_time across ``series_ids`` — one cheap
    aggregate, for callers that need the series extent without reading rows
    (e.g. to default a read window to "the last week of data"). ``None``
    when the series hold no rows."""
    if not series_ids:
        return None
    rows = ch_client.query(
        "SELECT minOrNull(valid_time), maxOrNull(valid_time) FROM series_values "
        "WHERE series_id IN {series_ids:Array(UInt64)}",
        parameters={"series_ids": list(series_ids)},
    ).result_rows
    lo, hi = rows[0]
    if lo is None or hi is None:
        return None
    # clickhouse-connect returns naive datetimes for DateTime64(…,'UTC') —
    # stamp the timezone so callers can mix these with tz-aware inputs.
    if lo.tzinfo is None:
        lo = lo.replace(tzinfo=UTC)
    if hi.tzinfo is None:
        hi = hi.replace(tzinfo=UTC)
    return lo, hi


def resolved_stats(
    ch_client,
    series_ids: Sequence[int],
    start_valid: datetime | None = None,
    end_valid: datetime | None = None,
) -> tuple[int, bool]:
    """``(max_points, has_versions)`` for the window: the largest per-series
    count of distinct valid_times (approximate, ``uniq``), and whether any
    series carries multiple versions per valid_time.

    Version detection compares row count to distinct-valid_time count with a
    2% margin (well above uniq()'s error): forecasts overlap by design and
    trip it; append-only sensor data does not. A series with only a handful
    of corrections stays below the margin — for bucketed charting averages
    that inaccuracy is negligible and buys the single-pass aggregate.
    """
    if not series_ids:
        return 0, False
    conds = ["series_id IN {series_ids:Array(UInt64)}"]
    params: dict = {"series_ids": list(series_ids)}
    if start_valid is not None:
        conds.append("valid_time >= {start_valid:DateTime64(6, 'UTC')}")
        params["start_valid"] = start_valid
    if end_valid is not None:
        conds.append("valid_time < {end_valid:DateTime64(6, 'UTC')}")
        params["end_valid"] = end_valid
    sql = (
        "SELECT max(u), max(if(u = 0, 0, c / u)) FROM ("
        "SELECT series_id, uniq(valid_time) AS u, count() AS c FROM series_values "
        "WHERE " + " AND ".join(conds) + " GROUP BY series_id)"
    )
    rows = ch_client.query(sql, parameters=params).result_rows
    if not rows or rows[0][0] is None:
        return 0, False
    max_points, ratio = rows[0]
    return int(max_points), float(ratio or 0) > 1.02


def distinct_knowledge_times(
    ch_client,
    series_ids: Sequence[int],
    start_valid: datetime | None = None,
    end_valid: datetime | None = None,
    limit: int = 20,
) -> list[datetime]:
    """Newest-first distinct knowledge_times ("runs") across ``series_ids``
    in the valid_time window. Returns up to ``limit + 1`` entries so callers
    can detect truncation — run-structured data (forecast issues) has few;
    streamed data has thousands and callers should treat it as unstructured.
    """
    if not series_ids:
        return []
    conds = ["series_id IN {series_ids:Array(UInt64)}"]
    params: dict = {"series_ids": list(series_ids), "lim": int(limit) + 1}
    if start_valid is not None:
        conds.append("valid_time >= {start_valid:DateTime64(6, 'UTC')}")
        params["start_valid"] = start_valid
    if end_valid is not None:
        conds.append("valid_time < {end_valid:DateTime64(6, 'UTC')}")
        params["end_valid"] = end_valid
    sql = (
        "SELECT DISTINCT knowledge_time FROM series_values WHERE "
        + " AND ".join(conds)
        + " ORDER BY knowledge_time DESC LIMIT {lim:UInt32}"
    )
    rows = ch_client.query(sql, parameters=params).result_rows
    return [r[0].replace(tzinfo=UTC) if r[0].tzinfo is None else r[0] for r in rows]


def read_columns(*, include_updates: bool = False, include_knowledge_time: bool = False) -> list[str]:
    """Column names, in order, that :func:`read` returns for these flags.

    Mirrors the module docstring's output-shape table; used both by the real
    reads and to build the correctly-shaped zero-row frame when no series
    resolved.
    """
    if include_updates:
        if include_knowledge_time:
            return ["series_id", "valid_time", "knowledge_time", "change_time", "value", "changed_by", "annotation"]
        return ["series_id", "valid_time", "change_time", "value", "changed_by", "annotation"]
    if include_knowledge_time:
        return ["series_id", "knowledge_time", "valid_time", "value"]
    return ["series_id", "valid_time", "value"]


def empty_frame(cols: list[str]) -> pl.DataFrame:
    """A zero-row Polars frame with the correct dtypes for ``cols``."""
    result = pl.from_arrow(_empty(cols))
    assert isinstance(result, pl.DataFrame)
    return result


def _fetch(ch_client, sql: str, params: dict, cols: list[str]) -> pa.Table:
    _prof = profiling._enabled

    _t = _time.perf_counter() if _prof else 0.0
    result = ch_client.query_arrow(sql, parameters=params)
    if _prof:
        profiling._record(profiling.PHASE_READ_SQL_EXEC, _time.perf_counter() - _t)

    _t = _time.perf_counter() if _prof else 0.0
    table = _empty(cols) if result.num_rows == 0 else result.select(cols)
    if "value" in table.schema.names:
        # NaN is the storage sentinel for null. Only rebuild the column with a
        # null mask when NaNs are actually present, to skip the copy otherwise.
        idx = table.schema.get_field_index("value")
        col = table.column(idx)
        # ty: pyarrow.compute kernels are generated at runtime; the stubs lack them.
        if len(col) and pc.any(pc.is_nan(col)).as_py():  # ty: ignore[unresolved-attribute]
            arr = col.to_numpy(zero_copy_only=False)
            table = table.set_column(idx, "value", pa.array(arr, mask=np.isnan(arr)))
    if _prof:
        profiling._record(profiling.PHASE_READ_BUILD_ARROW, _time.perf_counter() - _t)
    return table


@dataclass(frozen=True)
class PgEngineMeta:
    """Resolve the series_id set inside ClickHouse via a PostgreSQL engine table over the
    ``series_meta`` view, instead of the caller passing an explicit id array. The read then
    filters ``series_id`` and ``retention`` by subqueries over this ``meta`` CTE rather than
    array parameters.

    Exactly one addressing field must be set:

    * ``root_path``: a node subtree (the root itself + descendants, path-prefix match);
    * ``paths``: an exact set of node paths (path-routed manifests);
    * ``node_uuids`` / ``edge_uuids``: owner-uuid sets (uuid-routed manifests, edge scopes);
    * ``edge_triple``: one ``(from_path, to_path, edge_type)`` edge identity (edge scopes);
    * ``edge_triples``: a set of ``(from_path, to_path, edge_type)`` identities
      (triple-routed manifests), pushed down as three single-column ``IN`` filters. Like
      the set-valued ``data_type`` / ``name`` below, this resolves a *cartesian superset*
      of the requested triples; the caller trims against its exactly-resolved meta.

    ``data_type`` / ``name`` narrow the series set; each accepts a scalar (scope reads) or a
    set of values (manifests). Set-valued filters make the engine-resolved ids a *superset*
    (the cartesian of the sets). Every predicate here pushes down to PG as a
    single-column comparison, and the caller is expected to trim against its
    exactly-resolved meta.
    """

    table: str
    root_path: str | None = None
    paths: tuple[str, ...] | None = None
    node_uuids: tuple[str, ...] | None = None
    edge_uuids: tuple[str, ...] | None = None
    edge_triple: tuple[str, str, str] | None = None
    edge_triples: tuple[tuple[str, str, str], ...] | None = None
    data_type: str | tuple[str, ...] | None = None
    name: str | tuple[str, ...] | None = None


def _scalar_or_set(conds: list[str], params: dict, column: str, value, key: str) -> None:
    """Append an ``=`` (scalar) or ``IN`` (set) condition for a series filter."""
    if isinstance(value, str):
        conds.append(f"{column} = {{{key}:String}}")
        params[key] = value
    else:
        conds.append(f"{column} IN {{{key}:Array(String)}}")
        params[key] = list(value)


def _meta_cte(ms: PgEngineMeta) -> tuple[str, dict]:
    """Build the scalar ``WITH (...) AS _meta`` prefix + params for a :class:`PgEngineMeta` source.

    A **scalar** tuple subquery, not a named CTE: ClickHouse substitutes named CTEs
    textually, so every ``IN (SELECT .. FROM meta)`` reference would re-query
    Postgres through the engine (measured: 2 external queries per read, 4 for the
    latest-with-changes body, which embeds the WHERE twice). A scalar subquery is
    evaluated exactly once and referenced as a constant: one engine round-trip
    per read however many times the filters mention it, and ``IN <constant
    array>`` keeps index analysis and partition pruning.

    Size note: the scalar result must fit ClickHouse's constant-value limits; fine for
    metadata-sized catalogs (thousands to tens of thousands of matching series).
    """
    params: dict = {}
    if ms.root_path is not None:
        conds = ["(path = {ms_root:String} OR path LIKE {ms_prefix:String})"]
        params |= {"ms_root": ms.root_path, "ms_prefix": ms.root_path.rstrip("/") + "/%"}
    elif ms.paths is not None:
        conds = ["path IN {ms_paths:Array(String)}"]
        params["ms_paths"] = list(ms.paths)
    elif ms.node_uuids is not None:
        conds = ["node_uuid IN {ms_node_uuids:Array(String)}"]
        params["ms_node_uuids"] = list(ms.node_uuids)
    elif ms.edge_uuids is not None:
        conds = ["edge_uuid IN {ms_edge_uuids:Array(String)}"]
        params["ms_edge_uuids"] = list(ms.edge_uuids)
    elif ms.edge_triple is not None:
        conds = [
            "from_path = {ms_from:String}",
            "to_path = {ms_to:String}",
            "edge_type = {ms_etype:String}",
        ]
        params |= {"ms_from": ms.edge_triple[0], "ms_to": ms.edge_triple[1], "ms_etype": ms.edge_triple[2]}
    elif ms.edge_triples is not None:
        # Three single-column INs resolve the cartesian superset of the requested
        # triples in one round-trip; the caller trims to the exact PG resolve.
        conds = [
            "from_path IN {ms_from:Array(String)}",
            "to_path IN {ms_to:Array(String)}",
            "edge_type IN {ms_etype:Array(String)}",
        ]
        params |= {
            "ms_from": list({t[0] for t in ms.edge_triples}),
            "ms_to": list({t[1] for t in ms.edge_triples}),
            "ms_etype": list({t[2] for t in ms.edge_triples}),
        }
    else:
        raise ValueError(
            "PgEngineMeta needs one of root_path / paths / node_uuids / edge_uuids / edge_triple / edge_triples."
        )

    if ms.data_type is not None:
        _scalar_or_set(conds, params, "data_type", ms.data_type, "ms_dt")
    if ms.name is not None:
        _scalar_or_set(conds, params, "name", ms.name, "ms_name")

    cte = (
        "WITH (SELECT (groupArray(toUInt64(series_id)), groupUniqArray(retention)) "
        f"FROM {ms.table} WHERE " + " AND ".join(conds) + ") AS _meta "
    )
    return cte, params


def _where(
    *,
    series_ids: Sequence[int],
    retention: str | Sequence[str] | None,
    start_valid: datetime | None = None,
    end_valid: datetime | None = None,
    start_known: datetime | None = None,
    end_known: datetime | None = None,
    meta_source: PgEngineMeta | None = None,
) -> tuple[str, dict]:
    params: dict = {}
    if meta_source is None:
        filters = ["series_id IN {series_ids:Array(UInt64)}"]
        params["series_ids"] = list(series_ids)
        if retention is not None:
            if isinstance(retention, str):
                filters.append("retention = {retention:String}")
                params["retention"] = retention
            else:
                filters.append("retention IN {retentions:Array(String)}")
                params["retentions"] = list(retention)
    else:
        # _meta.1 is Array(UInt64) series_ids, _meta.2 Array(String) retentions,
        # both from the engine-resolved scalar prefixed onto the query.
        filters = [
            "series_id IN _meta.1",
            "retention IN _meta.2",
        ]

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


def _read_latest(ch_client, where: str, params: dict, cte: str = "") -> pa.Table:
    """Latest value per (series_id, valid_time).

    The tuple-argMax picks the row with the largest (knowledge_time,
    change_time): the latest issue, and the latest correction within it.
    """
    sql = f"""
    {cte}
    SELECT series_id, valid_time, argMax(value, (knowledge_time, change_time)) AS value
    FROM series_values
    {where}
    GROUP BY series_id, valid_time
    ORDER BY series_id, valid_time
    SETTINGS optimize_aggregation_in_order = 1
    """
    return _fetch(ch_client, sql, params, ["series_id", "valid_time", "value"])


def _read_latest_bucketed(
    ch_client, where: str, params: dict, bucket_us: int, dedup_versions: bool, cte: str = ""
) -> pa.Table:
    """The resolved view, time-bucket averaged in ClickHouse.

    For dense series (sensor data at Hz rates) this is the difference between
    shipping millions of resolved rows to the client and shipping one row per
    bucket — and for version-free data (``dedup_versions=False``) it is a
    single-pass aggregate: no per-valid_time argMax state for millions of
    groups, which measures ~10x faster on tens of millions of rows.
    ``bucket_us`` is code-supplied (never user text), inlined as an integer.
    """
    bucket_us = int(bucket_us)
    if bucket_us <= 0:
        raise ValueError("bucket_us must be a positive number of microseconds")
    bucket = f"toStartOfInterval(valid_time, INTERVAL {bucket_us} MICROSECOND)"
    if dedup_versions:
        # argMax by knowledge_time alone (no change_time tie-break): the
        # change_time column never has to be read, which measures ~2x faster
        # on tens of millions of rows. Corrections within one knowledge_time
        # are therefore resolved arbitrarily here — irrelevant at bucket-mean
        # granularity; exact reads keep the full tuple semantics.
        sql = f"""
        {cte}
        SELECT series_id, {bucket} AS valid_time, avg(value) AS value
        FROM (
            SELECT series_id, valid_time, argMax(value, knowledge_time) AS value
            FROM series_values
            {where}
            GROUP BY series_id, valid_time
        )
        GROUP BY series_id, valid_time
        ORDER BY series_id, valid_time
        """
    else:
        sql = f"""
        {cte}
        SELECT series_id, {bucket} AS valid_time, avg(value) AS value
        FROM series_values
        {where}
        GROUP BY series_id, valid_time
        ORDER BY series_id, valid_time
        """
    return _fetch(ch_client, sql, params, ["series_id", "valid_time", "value"])


def _read_latest_with_changes(ch_client, where: str, params: dict, cte: str = "") -> pa.Table:
    """Correction chain of the winning forecast per (series_id, valid_time).

    Emits only real state transitions (consecutive duplicates collapsed by
    the lagInFrame distinct-from-previous filter).
    """
    # The inner query picks max(knowledge_time) per (sid, vt) along the sort-key
    # prefix, so the outer filter is a PK seek rather than a sort.
    sql = f"""
    {cte}
    SELECT series_id, valid_time, change_time, value, changed_by, annotation
    FROM (
        SELECT
            series_id, valid_time, change_time, value, changed_by, annotation,
            lagInFrame(tuple(value, annotation, changed_by)) OVER (
                PARTITION BY series_id, valid_time
                ORDER BY change_time ASC
            ) AS prev_state
        FROM series_values
        {where}
          AND (series_id, valid_time, knowledge_time) IN (
              SELECT series_id, valid_time, max(knowledge_time)
              FROM series_values
              {where}
              GROUP BY series_id, valid_time
              SETTINGS optimize_aggregation_in_order = 1
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


def _read_overlapping(ch_client, where: str, params: dict, cte: str = "") -> pa.Table:
    """One row per (series_id, knowledge_time, valid_time).

    LIMIT 1 BY streams a "first row per group" pass: the input arrives in
    (sid, vt, kt, ct) order, the ORDER BY reverses ct so the row with the
    largest change_time is the first one seen per (sid, vt, kt) group, and
    LIMIT 1 BY emits that and skips the rest. Equivalent to argMax(value,
    change_time) GROUP BY (sid, vt, kt), but with no aggregation state.
    """
    sql = f"""
    {cte}
    SELECT series_id, knowledge_time, valid_time, value
    FROM series_values
    {where}
    ORDER BY series_id, valid_time, knowledge_time, change_time DESC
    LIMIT 1 BY series_id, valid_time, knowledge_time
    """
    return _fetch(
        ch_client,
        sql,
        params,
        ["series_id", "knowledge_time", "valid_time", "value"],
    )


def _read_overlapping_with_changes(ch_client, where: str, params: dict, cte: str = "") -> pa.Table:
    """Full 3D audit: every state transition per (series_id, kt, vt)."""
    sql = f"""
    {cte}
    SELECT series_id, valid_time, knowledge_time, change_time, value, changed_by, annotation
    FROM (
        SELECT
            series_id, valid_time, knowledge_time, change_time, value, changed_by, annotation,
            lagInFrame(tuple(value, annotation, changed_by)) OVER (
                PARTITION BY series_id, knowledge_time, valid_time
                ORDER BY change_time ASC
            ) AS prev_state
        FROM series_values
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
    meta_source: PgEngineMeta | None = None,
) -> pa.Table:
    where, params = _where(
        series_ids=series_ids,
        retention=retention,
        start_valid=start_valid,
        end_valid=end_valid,
        meta_source=meta_source,
    )
    cte = ""
    if meta_source is not None:
        cte, cte_params = _meta_cte(meta_source)
        params.update(cte_params)
    params.update(
        {
            "window_secs": int(window_length.total_seconds()),
            "offset_secs": int(issue_offset.total_seconds()),
            "start_window": start_window,
        }
    )
    sql = f"""
    {cte}
    SELECT series_id, valid_time, argMax(value, (knowledge_time, change_time)) AS value
    FROM series_values
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
    bucket_us: int | None = None,
    bucket_dedup: bool = True,
    meta_source: PgEngineMeta | None = None,
) -> pl.DataFrame:
    if bucket_us is not None and (include_updates or include_knowledge_time):
        raise ValueError("bucket_us requires the plain resolved read (no updates / knowledge_time).")
    _prof = profiling._enabled
    _t_total = _time.perf_counter() if _prof else 0.0

    series_ids = list(series_ids)
    if meta_source is None and not series_ids:
        return empty_frame(read_columns(include_updates=include_updates, include_knowledge_time=include_knowledge_time))

    where, params = _where(
        series_ids=series_ids,
        retention=retention,
        start_valid=start_valid,
        end_valid=end_valid,
        start_known=start_known,
        end_known=end_known,
        meta_source=meta_source,
    )
    cte = ""
    if meta_source is not None:
        cte, cte_params = _meta_cte(meta_source)
        params.update(cte_params)

    if include_updates:
        arrow = (
            _read_overlapping_with_changes(ch_client, where, params, cte)
            if include_knowledge_time
            else _read_latest_with_changes(ch_client, where, params, cte)
        )
    elif bucket_us is not None:
        arrow = _read_latest_bucketed(ch_client, where, params, bucket_us, bucket_dedup, cte)
    else:
        arrow = (
            _read_overlapping(ch_client, where, params, cte)
            if include_knowledge_time
            else _read_latest(ch_client, where, params, cte)
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
    meta_source: PgEngineMeta | None = None,
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
    if meta_source is None and not series_ids:
        return empty_frame(list(RELATIVE_READ_COLUMNS))

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
        meta_source=meta_source,
    )

    _t = _time.perf_counter() if _prof else 0.0
    result = pl.from_arrow(arrow)
    if _prof:
        profiling._record(profiling.PHASE_READ_TO_POLARS, _time.perf_counter() - _t)
        profiling._record(profiling.PHASE_READ_TOTAL, _time.perf_counter() - _t_total)

    assert isinstance(result, pl.DataFrame)
    return result
