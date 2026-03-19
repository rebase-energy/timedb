import io
import json
import secrets
import time
from contextlib import contextmanager
from typing import Dict, Tuple, Union, Any
import psycopg
from psycopg import sql as pgsql
import pyarrow as pa
import pyarrow.csv as pa_csv
import warnings

from .. import profiling
from ..types import BatchContext


@contextmanager
def _ensure_conn(conninfo_or_conn):
    """Yield a psycopg Connection, creating one only if given a string."""
    if isinstance(conninfo_or_conn, str):
        with psycopg.connect(conninfo_or_conn) as conn:
            yield conn
    else:
        yield conninfo_or_conn

def _to_csv_bytes(table: pa.Table) -> memoryview:
    buf = io.BytesIO()
    pa_csv.write_csv(table, buf)
    return buf.getbuffer()


# ── Public API ───────────────────────────────────────────────────────────────

def insert_tables(
    conn: psycopg.Connection,
    *,
    partitioned: Dict[str, Tuple[pa.Table, Dict[str, Any]]],
    batch_contexts: Dict[str, BatchContext],
) -> None:
    """
    Insert multiple pre-partitioned Arrow tables in one transaction.

    Each entry in *partitioned* maps a target table name to ``(table, routing)``.
    All batch metadata is inserted once at the start of the transaction via
    :func:`_insert_batch_metadata`, then each data partition is inserted.

    Args:
        conn: Active psycopg connection (caller owns lifecycle and transaction).
        partitioned: ``{target_table: (arrow_table, routing_dict)}``
        batch_contexts: ``{batch_id_str: BatchContext}`` — all batches present
            in the data, inserted atomically before any data.
    """
    _prof = profiling._enabled
    _t_total = time.perf_counter() if _prof else 0.0

    with conn.transaction():
        _insert_batch_metadata(conn, batch_contexts)
        for target_table, (table, routing) in partitioned.items():
            if len(table) == 0:
                continue
            if not routing["overlapping"]:
                _insert_flat(conn, table=table)
            else:
                _insert_overlapping(conn, table=table, target_table=target_table)

    if _prof:
        profiling._record(profiling.PHASE_INSERT_TOTAL, time.perf_counter() - _t_total)


def insert_table(
    conninfo: Union[psycopg.Connection, str],
    *,
    table: pa.Table,
    routing: Dict[str, Any],
    batch_ctx: BatchContext,
) -> None:
    """
    Insert a fully-decorated ``pa.Table`` into the correct table based on series routing.

    Single-series convenience wrapper around :func:`insert_tables`.

    *table* must already contain all columns the database expects:
    ``[batch_id, series_id, valid_time, (valid_time_end,) value, knowledge_time]``.
    All decoration is performed by the SDK layer before this call.

    Args:
        conninfo: Database connection or connection string
        table: Fully-decorated Arrow table (must include batch_id, series_id, knowledge_time)
        routing: Routing info dict with keys: overlapping (bool), retention (str), table (str)
        batch_ctx: Batch metadata container
    """
    if len(table) == 0:
        warnings.warn(
            "insert_table called with an empty table — no batch was created and no data was inserted.",
            stacklevel=2,
        )
        return

    target_table = routing["table"]
    with _ensure_conn(conninfo) as conn:
        insert_tables(
            conn,
            partitioned={target_table: (table, routing)},
            batch_contexts={batch_ctx.batch_id: batch_ctx},
        )


# ── Private helpers ──────────────────────────────────────────────────────────

def _insert_batch_metadata(
    conn: psycopg.Connection,
    batch_contexts: Dict[str, BatchContext],
) -> None:
    """Insert all batch records into batches_table in one executemany call.

    Uses ON CONFLICT (batch_id) DO NOTHING so re-inserting an existing batch
    is a no-op (safe for retry scenarios).
    """
    rows = [
        (
            ctx.batch_id,
            ctx.workflow_id,
            ctx.batch_start_time,
            ctx.batch_finish_time,
            json.dumps(ctx.batch_params) if ctx.batch_params is not None else None,
        )
        for ctx in batch_contexts.values()
    ]
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO batches_table "
            "(batch_id, workflow_id, batch_start_time, batch_finish_time, batch_params) "
            "VALUES (%s, %s, %s, %s, %s::jsonb) "
            "ON CONFLICT (batch_id) DO NOTHING",
            rows,
        )


def _insert_flat(
    conn: psycopg.Connection,
    *,
    table: pa.Table,
) -> None:
    """
    Upsert flat values via COPY to a temp staging table, then merge.

    *table* must already contain ``batch_id`` and ``series_id`` columns (stamped
    by the SDK layer).  The staging table includes all columns so IDs are read
    directly from the staged data — no SQL parameter injection for IDs.

    Batch metadata is inserted by the caller (_insert_batch_metadata) before
    this function is called.

    Round trips:
      1. CREATE TEMP TABLE _flat_stage_{hex} ON COMMIT DROP
      2. COPY _flat_stage_{hex} FROM STDIN (CSV)
      3. INSERT INTO flat ... SELECT ... FROM _flat_stage_{hex} ON CONFLICT DO UPDATE
    """
    _prof = profiling._enabled

    # Flat series require unique (series_id, valid_time) pairs — one value per series per timestamp.
    # This holds even across multiple batches: the last write wins via ON CONFLICT DO UPDATE,
    # so having two batches write to the same (series_id, valid_time) in one call is ambiguous.
    import polars as pl
    pl_table = pl.from_arrow(table.select(["series_id", "valid_time"]))
    n_unique = pl_table.n_unique()
    if n_unique < len(table):
        dupes = len(table) - n_unique
        raise ValueError(
            f"Cannot insert {dupes} duplicate (series_id, valid_time) pair(s) into a flat series. "
            f"A flat series stores exactly one value per (series_id, valid_time). "
            f"Deduplicate your data before calling insert()."
        )

    # Known PostgreSQL types for each optional column — exhaustive whitelist.
    # Any column in the Arrow table must appear here; unknown cols raise KeyError.
    _STAGE_COL_TYPES = {
        "batch_id":       "uuid        NOT NULL",
        "series_id":      "bigint      NOT NULL",
        "valid_time":     "timestamptz NOT NULL",
        "value":          "float8",
        "knowledge_time": "timestamptz NOT NULL",
        "valid_time_end": "timestamptz",
        "change_time":    "timestamptz",
        "changed_by":     "text",
        "annotation":     "text",
    }

    schema_names = table.schema.names
    copy_cols = ", ".join(schema_names)

    _t0 = time.perf_counter() if _prof else 0.0
    csv_bytes = _to_csv_bytes(table)
    if _prof:
        profiling._record(profiling.PHASE_INSERT_CSV_SERIALIZE, time.perf_counter() - _t0)

    # Build dynamic staging DDL from the Arrow schema
    cols_ddl = ", ".join(f"{c} {_STAGE_COL_TYPES[c]}" for c in schema_names)

    # Build INSERT and ON CONFLICT UPDATE SET dynamically
    insert_cols = copy_cols
    conflict_keys = {"series_id", "valid_time"}
    update_cols = [c for c in schema_names if c not in conflict_keys]
    update_clauses = [f"{c} = EXCLUDED.{c}" for c in update_cols]
    # PostgreSQL DEFAULT doesn't fire during ON CONFLICT DO UPDATE (only on INSERT).
    # If the caller didn't supply change_time, stamp it now so corrections are dated.
    if "change_time" not in update_cols:
        update_clauses.append("change_time = now()")
    update_set = ", ".join(update_clauses)

    # Use a unique stage table name per call to avoid collision when multiple flat
    # partitions exist within the same transaction (ON COMMIT DROP fires at
    # transaction END, not after each _insert_flat call).
    stage_name = f"_flat_stage_{secrets.token_hex(4)}"

    with conn.cursor() as cur:
        # Round trip 1: create temp staging table
        _t1 = time.perf_counter() if _prof else 0.0
        cur.execute(f"CREATE TEMP TABLE {stage_name} ({cols_ddl}) ON COMMIT DROP")
        if _prof:
            profiling._record(profiling.PHASE_INSERT_STAGE_DDL, time.perf_counter() - _t1)

        # Round trip 2: COPY into staging
        _t2 = time.perf_counter() if _prof else 0.0
        with cur.copy(
            f"COPY {stage_name} ({copy_cols}) FROM STDIN (FORMAT csv, HEADER true)"
        ) as copy:
            copy.write(csv_bytes)
        if _prof:
            profiling._record(profiling.PHASE_INSERT_COPY, time.perf_counter() - _t2)

        # Round trip 3: upsert merge from staging (batch metadata already inserted)
        _t3 = time.perf_counter() if _prof else 0.0
        cur.execute(
            f"""
            INSERT INTO flat ({insert_cols})
            SELECT {insert_cols} FROM {stage_name}
            ON CONFLICT (series_id, valid_time) DO UPDATE SET {update_set}
            """
        )
        if _prof:
            profiling._record(profiling.PHASE_INSERT_UPSERT, time.perf_counter() - _t3)


def _insert_overlapping(
    conn: psycopg.Connection,
    *,
    table: pa.Table,
    target_table: str,
) -> None:
    """
    Insert overlapping rows via direct COPY FROM STDIN to the target hypertable.

    Overlapping tables are pure-append (no ON CONFLICT), so no staging table is
    needed.  *table* already contains ``batch_id`` and ``series_id`` columns
    (stamped by the SDK layer), so no Arrow column appending is required here.

    Batch metadata is inserted by the caller (_insert_batch_metadata) before
    this function is called.

    Round trips:
      1. COPY {target_table} FROM STDIN (CSV with all columns)
    """
    _prof = profiling._enabled

    copy_cols = ", ".join(table.schema.names)

    _t0 = time.perf_counter() if _prof else 0.0
    csv_bytes = _to_csv_bytes(table)
    if _prof:
        profiling._record(profiling.PHASE_INSERT_CSV_SERIALIZE, time.perf_counter() - _t0)

    with conn.cursor() as cur:
        _t1 = time.perf_counter() if _prof else 0.0
        copy_sql = pgsql.SQL(
            "COPY {table} ({cols}) FROM STDIN (FORMAT csv, HEADER true)"
        ).format(table=pgsql.Identifier(target_table), cols=pgsql.SQL(copy_cols))
        with cur.copy(copy_sql) as copy:
            copy.write(csv_bytes)
        if _prof:
            profiling._record(profiling.PHASE_INSERT_COPY, time.perf_counter() - _t1)
