import json
import time
from typing import Dict, Tuple, Any
import pyarrow as pa
import warnings

from .. import profiling
from ..types import RunContext

# Allow inserts that span many monthly partitions (e.g. multi-year benchmarks).
# ClickHouse's default of 100 is a safety net for accidental misuse; for a
# time series DB that intentionally partitions by month, 1000 covers ~83 years.
_CH_INSERT_SETTINGS = {"max_partitions_per_insert_block": 1000}



# ── Public API ───────────────────────────────────────────────────────────────

def insert_tables(
    ch_client,
    *,
    partitioned: Dict[str, Tuple[pa.Table, Dict[str, Any]]],
    run_contexts: Dict[str, RunContext],
) -> None:
    """
    Insert multiple pre-partitioned Arrow tables into ClickHouse.

    Run metadata is written to ClickHouse first, then each data partition.
    Both operations are idempotent: run records use ReplacingMergeTree and
    values tables are pure-append with argMax reads for deduplication.

    Args:
        ch_client: clickhouse_connect client for all data writes.
        partitioned: ``{target_table: (arrow_table, routing_dict)}``
        run_contexts: ``{run_id_str: RunContext}`` — all runs present
            in the data, inserted before any data rows.
    """
    _prof = profiling._enabled
    _t_total = time.perf_counter() if _prof else 0.0

    _t_run = time.perf_counter() if _prof else 0.0
    _insert_run_metadata(ch_client, run_contexts)
    if _prof:
        profiling._record(profiling.PHASE_INSERT_RUN_METADATA, time.perf_counter() - _t_run)

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
    *,
    table: pa.Table,
    routing: Dict[str, Any],
    run_ctx: RunContext,
) -> None:
    """
    Insert a fully-decorated ``pa.Table`` into the correct ClickHouse table.

    Single-series convenience wrapper around :func:`insert_tables`.

    Args:
        ch_client: clickhouse_connect client.
        table: Fully-decorated Arrow table (must include run_id, series_id, knowledge_time).
        routing: Routing info dict with keys: overlapping (bool), retention (str), table (str).
        run_ctx: Run metadata container.
    """
    if len(table) == 0:
        warnings.warn(
            "insert_table called with an empty table — no run was created and no data was inserted.",
            stacklevel=2,
        )
        return

    target_table = routing["table"]
    insert_tables(
        ch_client,
        partitioned={target_table: (table, routing)},
        run_contexts={run_ctx.run_id: run_ctx},
    )


# ── Private helpers ──────────────────────────────────────────────────────────

def _insert_run_metadata(
    ch_client,
    run_contexts: Dict[str, RunContext],
) -> None:
    """Insert run records into ClickHouse runs_table.

    Uses ReplacingMergeTree semantics — re-inserting an existing run_id is safe.
    """
    if not run_contexts:
        return

    rows = [
        {
            "run_id": ctx.run_id,
            "workflow_id": ctx.workflow_id,
            "run_start_time": ctx.run_start_time,
            "run_finish_time": ctx.run_finish_time,
            "run_params": json.dumps(ctx.run_params, default=lambda o: o.isoformat() if hasattr(o, "isoformat") else str(o)) if ctx.run_params is not None else "{}",
        }
        for ctx in run_contexts.values()
    ]

    # Build Arrow table for insert_arrow
    run_table = pa.table({
        "run_id":           pa.array([r["run_id"] for r in rows], type=pa.string()),
        "workflow_id":      pa.array([r["workflow_id"] for r in rows], type=pa.string()),
        "run_start_time":   pa.array([r["run_start_time"] for r in rows], type=pa.timestamp("us", tz="UTC")),
        "run_finish_time":  pa.array([r["run_finish_time"] for r in rows], type=pa.timestamp("us", tz="UTC")),
        "run_params":       pa.array([r["run_params"] for r in rows], type=pa.string()),
    })
    ch_client.insert_arrow("runs_table", run_table)


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
