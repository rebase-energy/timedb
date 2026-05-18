"""Bulk write path for timedb.

Stamps defaults for any missing optional columns and issues one Arrow bulk
insert each into ``series_values`` and ``run_series``. No run metadata, no
identity resolution — both are the caller's responsibility (energydb).
"""

from __future__ import annotations

import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime

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
# fire both halves in parallel on two CH clients. clickhouse-connect rejects
# concurrent calls on a single client (per-session lock), so the
# ``TimeDBClient`` keeps a sidecar pool exposed via the ``aux_clients`` arg.
# 100K rows is the break-even point we measured — below that, the thread
# pool / Arrow-slice overhead exceeds the wire-time savings; above it the
# parallel insert is consistently 1.4–1.6× faster (556 → 349 ms on 1.7 M).
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
    aux_clients: list,
    table: str,
    arrow: pa.Table,
    *,
    settings: dict,
) -> None:
    """Insert a large Arrow batch via two CH clients in parallel.

    Splits ``arrow`` roughly in half by row range (``Table.slice`` is
    zero-copy) and fires both halves through a 2-worker thread pool.
    Falls back to a single serial insert if the row count is below the
    parallel-insert threshold or no aux clients are available.

    Two parallel inserts produce two parts instead of one — CH merges them
    in the background, and at the row counts we hit (one batch per write)
    this stays well within the part budget. ``max_partitions_per_insert_block``
    is applied to each half independently.
    """
    if arrow.num_rows < _PARALLEL_INSERT_THRESHOLD or not aux_clients:
        ch_client.insert_arrow(table, arrow, settings=settings)
        return

    half = arrow.num_rows // 2
    a = arrow.slice(0, half)
    b = arrow.slice(half, arrow.num_rows - half)
    with ThreadPoolExecutor(max_workers=2) as ex:
        fa = ex.submit(ch_client.insert_arrow, table, a, settings=settings)
        fb = ex.submit(aux_clients[0].insert_arrow, table, b, settings=settings)
        fa.result()
        fb.result()


def write(
    ch_client,
    df: pd.DataFrame | pl.DataFrame,
    *,
    retention: str | None = None,
    knowledge_time: datetime | None = None,
    aux_clients: Callable[[], list] | None = None,
) -> None:
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

    values_cols = [c for c in _SERIES_VALUES_COLUMNS if c in pl_df.columns]
    # ``combine_chunks()`` is kept: clickhouse-connect's insert path streams
    # chunks one at a time, and on chunked input the wire-side compression
    # ends up resetting context per chunk (and per-chunk HTTP framing
    # overhead on large arrays). Measured ~120 ms regression on the
    # forecast_write @ 200 path when ``combine_chunks()`` was removed.
    values_arrow = pl_df.select(values_cols).to_arrow().combine_chunks()
    rs_arrow = pl_df.select(["series_id", "run_id"]).unique().to_arrow().combine_chunks()

    if _prof:
        profiling._record(profiling.PHASE_WRITE_NORMALIZE, time.perf_counter() - _t_norm)

    if values_arrow.num_rows > 0:
        _t = time.perf_counter() if _prof else 0.0
        aux = aux_clients() if aux_clients is not None else []
        _insert_arrow_parallel(ch_client, aux, "series_values", values_arrow, settings=_CH_INSERT_SETTINGS)
        if _prof:
            profiling._record(profiling.PHASE_WRITE_SERIES_VALUES_INSERT, time.perf_counter() - _t)

    if rs_arrow.num_rows > 0:
        _t = time.perf_counter() if _prof else 0.0
        # ``run_series`` is tiny (one row per series_id) — always serial.
        ch_client.insert_arrow("run_series", rs_arrow, settings=_CH_INSERT_SETTINGS)
        if _prof:
            profiling._record(profiling.PHASE_WRITE_RUN_SERIES_INSERT, time.perf_counter() - _t)

    if _prof:
        profiling._record(profiling.PHASE_WRITE_TOTAL, time.perf_counter() - _t_total)
