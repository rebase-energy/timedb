import json
import time
from typing import Dict, Tuple, Any
import pyarrow as pa
import warnings

from .. import profiling
from ..types import BatchContext

# Allow inserts that span many monthly partitions (e.g. multi-year benchmarks).
# ClickHouse's default of 100 is a safety net for accidental misuse; for a
# time series DB that intentionally partitions by month, 1000 covers ~83 years.
_CH_INSERT_SETTINGS = {"max_partitions_per_insert_block": 1000}



# ── Public API ───────────────────────────────────────────────────────────────

def insert_tables(
    pg_conn,
    ch_client,
    *,
    partitioned: Dict[str, Tuple[pa.Table, Dict[str, Any]]],
    batch_contexts: Dict[str, BatchContext],
) -> None:
    """
    Insert multiple pre-partitioned Arrow tables into ClickHouse.

    Batch metadata is written to ClickHouse first, then each data partition.
    Both operations are idempotent: batch records use ReplacingMergeTree and
    values tables are pure-append with argMax reads for deduplication.

    Args:
        pg_conn: Active psycopg connection (used by the series layer, not here).
        ch_client: clickhouse_connect client for all data writes.
        partitioned: ``{target_table: (arrow_table, routing_dict)}``
        batch_contexts: ``{batch_id_str: BatchContext}`` — all batches present
            in the data, inserted before any data rows.
    """
    _prof = profiling._enabled
    _t_total = time.perf_counter() if _prof else 0.0

    _t_batch = time.perf_counter() if _prof else 0.0
    _insert_batch_metadata(ch_client, batch_contexts)
    if _prof:
        profiling._record(profiling.PHASE_INSERT_BATCH_METADATA, time.perf_counter() - _t_batch)

    for target_table, (table, routing) in partitioned.items():
        if len(table) == 0:
            continue
        if not routing["overlapping"]:
            _insert_flat(ch_client, table=table)
        else:
            _insert_overlapping(ch_client, table=table, target_table=target_table)

    if _prof:
        profiling._record(profiling.PHASE_INSERT_TOTAL, time.perf_counter() - _t_total)


def insert_table(
    ch_client,
    pg_conn,
    *,
    table: pa.Table,
    routing: Dict[str, Any],
    batch_ctx: BatchContext,
) -> None:
    """
    Insert a fully-decorated ``pa.Table`` into the correct ClickHouse table.

    Single-series convenience wrapper around :func:`insert_tables`.

    Args:
        ch_client: clickhouse_connect client.
        pg_conn: psycopg connection (passed through to insert_tables, not used for data).
        table: Fully-decorated Arrow table (must include batch_id, series_id, knowledge_time).
        routing: Routing info dict with keys: overlapping (bool), retention (str), table (str).
        batch_ctx: Batch metadata container.
    """
    if len(table) == 0:
        warnings.warn(
            "insert_table called with an empty table — no batch was created and no data was inserted.",
            stacklevel=2,
        )
        return

    target_table = routing["table"]
    insert_tables(
        pg_conn,
        ch_client,
        partitioned={target_table: (table, routing)},
        batch_contexts={batch_ctx.batch_id: batch_ctx},
    )


# ── Private helpers ──────────────────────────────────────────────────────────

def _insert_batch_metadata(
    ch_client,
    batch_contexts: Dict[str, BatchContext],
) -> None:
    """Insert batch records into ClickHouse batches_table.

    Uses ReplacingMergeTree semantics — re-inserting an existing batch_id is safe.
    """
    if not batch_contexts:
        return

    rows = [
        {
            "batch_id": ctx.batch_id,
            "workflow_id": ctx.workflow_id,
            "batch_start_time": ctx.batch_start_time,
            "batch_finish_time": ctx.batch_finish_time,
            "batch_params": json.dumps(ctx.batch_params, default=lambda o: o.isoformat() if hasattr(o, "isoformat") else str(o)) if ctx.batch_params is not None else "{}",
        }
        for ctx in batch_contexts.values()
    ]

    # Build Arrow table for insert_arrow
    batch_table = pa.table({
        "batch_id":          pa.array([r["batch_id"] for r in rows], type=pa.string()),
        "workflow_id":       pa.array([r["workflow_id"] for r in rows], type=pa.string()),
        "batch_start_time":  pa.array([r["batch_start_time"] for r in rows], type=pa.timestamp("us", tz="UTC")),
        "batch_finish_time": pa.array([r["batch_finish_time"] for r in rows], type=pa.timestamp("us", tz="UTC")),
        "batch_params":      pa.array([r["batch_params"] for r in rows], type=pa.string()),
    })
    ch_client.insert_arrow("batches_table", batch_table)


def _insert_flat(
    ch_client,
    *,
    table: pa.Table,
) -> None:
    """Insert flat values into ClickHouse flat table."""
    _prof = profiling._enabled

    _t0 = time.perf_counter() if _prof else 0.0
    ch_client.insert_arrow("flat", table, settings=_CH_INSERT_SETTINGS)
    if _prof:
        profiling._record(profiling.PHASE_INSERT_ARROW, time.perf_counter() - _t0)


def _insert_overlapping(
    ch_client,
    *,
    table: pa.Table,
    target_table: str,
) -> None:
    """Insert overlapping rows into the target ClickHouse table (pure-append)."""
    _prof = profiling._enabled

    _t0 = time.perf_counter() if _prof else 0.0
    ch_client.insert_arrow(target_table, table, settings=_CH_INSERT_SETTINGS)
    if _prof:
        profiling._record(profiling.PHASE_INSERT_ARROW, time.perf_counter() - _t0)
