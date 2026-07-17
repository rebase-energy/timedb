"""Bulk write path for timedb.

Stamps defaults for any missing optional columns and issues one Arrow bulk
insert each into ``series_values`` and ``run_series``. No run metadata, no
identity resolution — both are the caller's responsibility (energydb).
"""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from typing import Literal, NamedTuple

import pandas as pd
import polars as pl
import pyarrow as pa
from uuid6 import uuid7

from . import profiling

_SERIES_VALUES_COLUMNS = [
    "series_id",
    "valid_time",
    "knowledge_time",
    "change_time",
    "value",
    "valid_time_end",
    "run_id",
    "changed_by",
    "annotation",
    "retention",
]

RETENTION_TIERS: frozenset[str] = frozenset({"short", "medium", "long", "forever"})
"""Public set of valid retention tier names. timedb owns this vocabulary;
downstream libraries (energydb) consume it via ``from timedb import
RETENTION_TIERS``."""

_VALID_RETENTIONS = RETENTION_TIERS  # backwards-compat alias for in-module use
_DEFAULT_RETENTION = "forever"

# Allow inserts that span many monthly partitions (e.g. multi-year backfills).
# ClickHouse's default of 100 is too tight for a time-series store intentionally
# partitioned by month × retention.
_CH_INSERT_SETTINGS = {"max_partitions_per_insert_block": 1000}

# When ``values_arrow`` exceeds this row count we split the batch in half and
# fire both halves in parallel. The two inserts share the one sessionless CH
# client — a sessionless client places no concurrency limit on its queries, so
# the parallelism lives in the client's HTTP connection pool, not in separate
# client instances. 100K rows is the break-even point we measured — below that,
# the thread pool / Arrow-slice overhead exceeds the wire-time savings; above it
# the parallel insert is consistently 1.4–1.6× faster (556 → 349 ms on 1.7 M).
_PARALLEL_INSERT_THRESHOLD = 100_000


def _generate_run_id() -> int:
    """Client-side run id: top 63 bits of a uuid7.

    Time-sortable (uuid7 has the ms timestamp in the high bits) and fits a
    signed Int64 so the same value passes cleanly through the energydb PG
    ``BIGINT`` and the timedb CH ``UInt64`` columns.
    """
    return uuid7().int >> 65


def _validate_columns(df: pl.DataFrame) -> None:
    required = {"series_id", "valid_time", "value"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"df missing required columns: {sorted(missing)}")

    for col in ("valid_time", "valid_time_end", "knowledge_time", "change_time"):
        if col in df.columns:
            dtype = df.schema[col]
            if isinstance(dtype, pl.Datetime) and dtype.time_zone is None:
                raise ValueError(f"{col!r} must be timezone-aware.")


def _insert_arrow_parallel(
    ch_client,
    table: str,
    arrow: pa.Table,
    *,
    settings: dict,
) -> None:
    """Insert a large Arrow batch as two concurrent halves on one CH client.

    Splits ``arrow`` roughly in half by row range (``Table.slice`` is
    zero-copy) and fires both halves through a 2-worker thread pool. Both
    inserts run on ``ch_client``; it is sessionless, so its two queries
    overlap over the client's HTTP connection pool. Falls back to a single
    serial insert below the parallel-insert threshold.

    Two parallel inserts produce two parts instead of one — CH merges them
    in the background, and at the row counts we hit (one batch per write)
    this stays well within the part budget. ``max_partitions_per_insert_block``
    is applied to each half independently.
    """
    if arrow.num_rows < _PARALLEL_INSERT_THRESHOLD:
        ch_client.insert_arrow(table, arrow, settings=settings)
        return

    half = arrow.num_rows // 2
    a = arrow.slice(0, half)
    b = arrow.slice(half, arrow.num_rows - half)
    with ThreadPoolExecutor(max_workers=2) as ex:
        fa = ex.submit(ch_client.insert_arrow, table, a, settings=settings)
        fb = ex.submit(ch_client.insert_arrow, table, b, settings=settings)
        fa.result()
        fb.result()


def _run_inserts(ch_client, values_arrow: pa.Table, rs_arrow: pa.Table) -> None:
    """Insert ``series_values`` and ``run_series`` concurrently on one CH client.

    Every insert pays a fixed per-insert commit latency (~135 ms on ClickHouse
    Cloud, where each part is committed to object storage before the ack), so
    running the tiny ``run_series`` insert after the values insert doubles the
    CH cost of a small write. Instead the two lanes overlap: ``ch_client`` is
    sessionless, so its concurrent queries run in parallel over the client's
    HTTP connection pool. When the values batch is large it splits further (see
    :func:`_insert_arrow_parallel`), for up to three inserts in flight at once —
    still well within the pool. Lanes with no rows are skipped.

    Every lane is awaited even when another fails — leaking an in-flight
    insert past the write call would leave its outcome unknown to the caller.
    The first error is re-raised, values lane first.
    """
    n_values, n_rs = values_arrow.num_rows, rs_arrow.num_rows

    def _values() -> None:
        with profiling._phase(profiling.PHASE_WRITE_SERIES_VALUES_INSERT):
            _insert_arrow_parallel(ch_client, "series_values", values_arrow, settings=_CH_INSERT_SETTINGS)

    def _run_series() -> None:
        with profiling._phase(profiling.PHASE_WRITE_RUN_SERIES_INSERT):
            ch_client.insert_arrow("run_series", rs_arrow, settings=_CH_INSERT_SETTINGS)

    if not n_rs:
        if n_values:
            _values()
        return
    if not n_values:
        _run_series()
        return

    with ThreadPoolExecutor(max_workers=2) as ex:
        futures = [ex.submit(_values), ex.submit(_run_series)]
        errors: list[BaseException] = []
        for f in futures:
            try:
                f.result()
            except BaseException as e:  # noqa: BLE001 — collected and re-raised below
                errors.append(e)
        if errors:
            raise errors[0]


class WriteResult(NamedTuple):
    """Counts returned by :func:`write`. ``skipped`` is always 0 unless
    ``skip_unchanged`` was set."""

    written: int
    skipped: int


UnchangedScope = Literal["valid_time", "knowledge_time"]


def _filter_unchanged(ch_client, pl_df: pl.DataFrame, *, scope: UnchangedScope) -> pl.DataFrame:
    """Drop incoming rows whose ``(value, annotation, changed_by)`` already
    match the latest stored state for their comparison key.

    ``scope="valid_time"`` compares against the single winning value per
    ``(series_id, valid_time)`` (largest ``(knowledge_time, change_time)`` —
    identical to ``read._read_latest``'s argMax). ``scope="knowledge_time"``
    compares per ``(series_id, valid_time, knowledge_time)``.

    Operates on the stamped frame (retention present, value NaN-filled). The
    comparison tuple matches what ``read._read_latest_with_changes`` collapses
    on, so a dropped row is one no reader would ever surface.
    """
    if pl_df.is_empty():
        return pl_df

    keys = ["series_id", "valid_time"]
    if scope == "knowledge_time":
        keys.append("knowledge_time")
        cols = "series_id, valid_time, knowledge_time, value, annotation, changed_by"
        order = "series_id, valid_time, knowledge_time, change_time DESC"
    else:
        cols = "series_id, valid_time, value, annotation, changed_by"
        order = "series_id, valid_time, knowledge_time DESC, change_time DESC"

    params = {
        "sids": pl_df.get_column("series_id").unique().to_list(),
        "rets": pl_df.get_column("retention").unique().to_list(),
        "min_vt": pl_df.get_column("valid_time").min(),
        "max_vt": pl_df.get_column("valid_time").max(),
    }
    # ponytail: reads the whole [min_vt, max_vt] valid_time slab per series.
    # Fine for contiguous write windows; revisit if sparse batches dominate.
    sql = f"""
    SELECT {cols}
    FROM series_values
    WHERE series_id IN {{sids:Array(UInt64)}}
      AND retention IN {{rets:Array(String)}}
      AND valid_time >= {{min_vt:DateTime64(6, 'UTC')}}
      AND valid_time <= {{max_vt:DateTime64(6, 'UTC')}}
    ORDER BY {order}
    LIMIT 1 BY {", ".join(keys)}
    """
    stored = pl.from_arrow(ch_client.query_arrow(sql, parameters=params))
    assert isinstance(stored, pl.DataFrame)
    if stored.is_empty():
        return pl_df

    # changed_by is LowCardinality (arrives as an Arrow dictionary → Categorical);
    # annotation is plain String. Cast both so the == comparison stays Utf8.
    stored = stored.with_columns(
        pl.col("annotation").cast(pl.Utf8),
        pl.col("changed_by").cast(pl.Utf8),
        pl.lit(True).alias("_in_store"),
    )
    j = pl_df.join(stored, on=keys, how="left", suffix="_st")
    val_same = (pl.col("value") == pl.col("value_st")) | (pl.col("value").is_nan() & pl.col("value_st").is_nan())
    same = (
        val_same & (pl.col("annotation") == pl.col("annotation_st")) & (pl.col("changed_by") == pl.col("changed_by_st"))
    )
    keep = pl.col("_in_store").is_null() | (~same).fill_null(False)
    return j.filter(keep).select(pl_df.columns)


def write(
    ch_client,
    df: pd.DataFrame | pl.DataFrame,
    *,
    retention: str | None = None,
    knowledge_time: datetime | None = None,
    skip_unchanged: bool = False,
    unchanged_scope: UnchangedScope = "valid_time",
) -> WriteResult:
    """Write time-series rows into ``series_values`` plus their ``run_series`` mapping.

    Required columns on ``df``: ``series_id``, ``valid_time``, ``value``.

    Optional columns, stamped with defaults when absent:
        knowledge_time  — kwarg, else ``datetime.now(UTC)`` for the whole batch.
        change_time     — ``datetime.now(UTC)`` for the whole batch.
        run_id          — one ``uuid7 & UINT64_MAX`` generated for the batch.
        changed_by      — empty string.
        annotation      — empty string.
        valid_time_end  — CH sentinel default ``2200-01-01``.
        retention       — kwarg or column; if neither is present, defaults to
                          ``"forever"`` (no TTL — appropriate for actuals).

    ``retention`` and ``knowledge_time`` cannot be supplied as both column and
    kwarg at once — that's almost always a producer bug; we raise rather than
    guess which takes precedence.

    ``(series_id, run_id)`` pairs are also written to ``run_series`` so that
    "which runs touched this series" lookups don't need to scan ``series_values``.
    That insert runs concurrently with the ``series_values`` insert (each CH
    insert pays a fixed commit latency; see :func:`_run_inserts`), so a failed
    values insert can leave ``run_series`` rows for a run whose data never
    landed — the mirror image of the pre-existing values-without-``run_series``
    failure mode, detectable by ``run_id`` either way.

    When ``skip_unchanged`` is set, rows whose latest stored
    ``(value, annotation, changed_by)`` already matches are dropped before the
    insert (one bounded read-back). ``unchanged_scope="valid_time"`` (default)
    compares the winning value per ``(series_id, valid_time)``;
    ``"knowledge_time"`` compares per ``(series_id, valid_time, knowledge_time)``
    — a near-noop unless a stable ``knowledge_time`` is supplied, since the
    default stamps ``now()`` per batch. Returns counts of rows written/skipped.
    """
    _prof = profiling._enabled
    _t_total = time.perf_counter() if _prof else 0.0
    _t_norm = time.perf_counter() if _prof else 0.0

    pl_df: pl.DataFrame = pl.from_pandas(df) if isinstance(df, pd.DataFrame) else df
    _validate_columns(pl_df)

    source_has_retention = "retention" in pl_df.columns
    if source_has_retention and retention is not None:
        raise ValueError(
            "Ambiguous retention: df has a 'retention' column and retention "
            "was also passed as a kwarg. Use one or the other."
        )
    if retention is not None and retention not in _VALID_RETENTIONS:
        raise ValueError(f"Unknown retention {retention!r}. Valid values: {sorted(_VALID_RETENTIONS)}")
    if source_has_retention:
        present = set(pl_df.get_column("retention").unique().to_list())
        unknown = present - _VALID_RETENTIONS
        if unknown:
            raise ValueError(
                f"Unknown retention values in 'retention' column: {sorted(unknown)}. "
                f"Valid values: {sorted(_VALID_RETENTIONS)}"
            )
    if not source_has_retention and retention is None:
        retention = _DEFAULT_RETENTION

    source_has_kt = "knowledge_time" in pl_df.columns
    if source_has_kt and knowledge_time is not None:
        raise ValueError(
            "Ambiguous knowledge_time: df has a 'knowledge_time' column and knowledge_time was also passed as a kwarg."
        )

    stamps: list[pl.Expr] = [
        pl.col("series_id").cast(pl.UInt64),
        pl.col("value").cast(pl.Float64).fill_null(float("nan")),
    ]

    if not source_has_kt:
        kt = knowledge_time if knowledge_time is not None else datetime.now(UTC)
        stamps.append(pl.lit(kt, dtype=pl.Datetime("us", "UTC")).alias("knowledge_time"))

    # change_time: one per batch unless passed as column
    if "change_time" not in pl_df.columns:
        ct = datetime.now(UTC)
        stamps.append(pl.lit(ct, dtype=pl.Datetime("us", "UTC")).alias("change_time"))

    if "run_id" in pl_df.columns:
        stamps.append(pl.col("run_id").cast(pl.UInt64))
    else:
        stamps.append(pl.lit(_generate_run_id(), dtype=pl.UInt64).alias("run_id"))

    if not source_has_retention:
        stamps.append(pl.lit(retention, dtype=pl.Utf8).alias("retention"))

    for optional_str in ("changed_by", "annotation"):
        if optional_str not in pl_df.columns:
            stamps.append(pl.lit("", dtype=pl.Utf8).alias(optional_str))

    pl_df = pl_df.with_columns(stamps)

    skipped = 0
    if skip_unchanged:
        if unchanged_scope not in ("valid_time", "knowledge_time"):
            raise ValueError(
                f"Unknown unchanged_scope {unchanged_scope!r}. Valid values: 'valid_time', 'knowledge_time'."
            )
        with profiling._phase(profiling.PHASE_WRITE_SKIP_UNCHANGED):
            before = pl_df.height
            pl_df = _filter_unchanged(ch_client, pl_df, scope=unchanged_scope)
            skipped = before - pl_df.height

    values_cols = [c for c in _SERIES_VALUES_COLUMNS if c in pl_df.columns]
    # ``rechunk()`` on the polars side beats pyarrow's ``combine_chunks()``
    # by ~1.4× on the 1.7M-row insert (36 ms → 26 ms measured), and still
    # produces the single-chunk Arrow table that clickhouse-connect's
    # insert path needs — without it, per-chunk HTTP framing + compression
    # context resets give back ~120 ms on the forecast_write @ 200 path.
    values_arrow = pl_df.select(values_cols).rechunk().to_arrow()
    rs_arrow = pl_df.select(["series_id", "run_id"]).unique().rechunk().to_arrow()

    if _prof:
        profiling._record(profiling.PHASE_WRITE_NORMALIZE, time.perf_counter() - _t_norm)

    if values_arrow.num_rows > 0 or rs_arrow.num_rows > 0:
        _run_inserts(ch_client, values_arrow, rs_arrow)

    if _prof:
        profiling._record(profiling.PHASE_WRITE_TOTAL, time.perf_counter() - _t_total)

    return WriteResult(written=pl_df.height, skipped=skipped)
