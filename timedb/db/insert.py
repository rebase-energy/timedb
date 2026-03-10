import io
import json
import time
from contextlib import contextmanager
from typing import Optional, Dict, Union, Any
from datetime import datetime
import psycopg
from psycopg import sql as pgsql
import pyarrow as pa
import pyarrow.csv as pa_csv
import warnings

try:
    from uuid import uuid7
except ImportError:
    from uuid6 import uuid7

from .series import SeriesRegistry, OVERLAPPING_TABLES
from .. import profiling


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

def insert_table(
    conninfo: Union[psycopg.Connection, str],
    *,
    table: pa.Table,
    workflow_id: Optional[str] = None,
    batch_start_time: Optional[datetime] = None,
    batch_finish_time: Optional[datetime] = None,
    batch_params: Optional[Dict] = None,
    series_id: int,
    routing: Dict[str, Any],
):
    """
    Insert a ``pa.Table`` into the correct table based on series routing.

    Single-series insert.  *table* must have columns
    ``[knowledge_time, valid_time, (valid_time_end,) value]`` with
    timezone-aware timestamps.  The ``knowledge_time`` column must always be
    present and non-null — it is baked in by the SDK layer before this call.

    Generates a UUIDv7 batch_id client-side, then serializes the Arrow table
    to CSV bytes via Arrow's C++ writer and bulk-loads via COPY FROM STDIN.

    Args:
        conninfo: Database connection or connection string
        table: Arrow table with time-series data (must include knowledge_time column)
        workflow_id: Workflow identifier
        batch_start_time: Batch start time
        batch_finish_time: Batch finish time
        batch_params: Batch parameters
        series_id: Series identifier
        routing: Routing info dict with keys: overlapping (bool), retention (str), table (str)

    Returns:
        ``uuid.UUID`` batch_id for every insert (flat and overlapping).
    """
    if len(table) == 0:
        warnings.warn(
            "insert_table called with an empty table — no batch was created and no data was inserted.",
            stacklevel=2,
        )
        return uuid7()

    _prof = profiling._enabled
    _t_total = time.perf_counter() if _prof else 0.0

    batch_id = uuid7()
    batch_params_json = json.dumps(batch_params) if batch_params is not None else None

    with _ensure_conn(conninfo) as conn:
        with conn.transaction():
            if not routing["overlapping"]:
                _insert_flat(
                    conn,
                    table=table,
                    series_id=series_id,
                    batch_id=batch_id,
                    workflow_id=workflow_id,
                    batch_start_time=batch_start_time,
                    batch_finish_time=batch_finish_time,
                    batch_params_json=batch_params_json,
                )
            else:
                target_table = OVERLAPPING_TABLES[routing["retention"]]
                _insert_overlapping(
                    conn,
                    table=table,
                    series_id=series_id,
                    target_table=target_table,
                    batch_id=batch_id,
                    workflow_id=workflow_id,
                    batch_start_time=batch_start_time,
                    batch_finish_time=batch_finish_time,
                    batch_params_json=batch_params_json,
                )

    if _prof:
        profiling._record(profiling.PHASE_INSERT_TOTAL, time.perf_counter() - _t_total)

    return batch_id


# ── Private helpers ──────────────────────────────────────────────────────────

def _insert_flat(
    conn: psycopg.Connection,
    *,
    table: pa.Table,
    series_id: int,
    batch_id,
    workflow_id: Optional[str],
    batch_start_time: Optional[datetime],
    batch_finish_time: Optional[datetime],
    batch_params_json: Optional[str],
) -> None:
    """
    Upsert flat values via COPY to a temp staging table, then merge.

    Round trips:
      1. CREATE TEMP TABLE _flat_stage ON COMMIT DROP
      2. COPY _flat_stage FROM STDIN (CSV)
      3. WITH batch AS (INSERT INTO batches_table)
         INSERT INTO flat ... SELECT ... FROM _flat_stage ON CONFLICT DO UPDATE
    """
    has_vte = "valid_time_end" in table.schema.names
    _prof = profiling._enabled

    # Sort for TimescaleDB chunk routing; omit valid_time_end column if absent
    table = table.sort_by([("valid_time", "ascending")])
    cols = ["valid_time", "valid_time_end", "value", "knowledge_time"] if has_vte \
           else ["valid_time", "value", "knowledge_time"]
    table = table.select(cols)

    _t0 = time.perf_counter() if _prof else 0.0
    csv_bytes = _to_csv_bytes(table)
    if _prof:
        profiling._record(profiling.PHASE_INSERT_CSV_SERIALIZE, time.perf_counter() - _t0)

    batch_id_str = str(batch_id)
    copy_cols = "valid_time, valid_time_end, value, knowledge_time" if has_vte \
                else "valid_time, value, knowledge_time"

    with conn.cursor() as cur:
        # Round trip 1: create temp staging table
        _t1 = time.perf_counter() if _prof else 0.0
        cur.execute("""
            CREATE TEMP TABLE _flat_stage (
                valid_time     timestamptz NOT NULL,
                valid_time_end timestamptz,
                value          float8,
                knowledge_time timestamptz NOT NULL
            ) ON COMMIT DROP
        """)
        if _prof:
            profiling._record(profiling.PHASE_INSERT_STAGE_DDL, time.perf_counter() - _t1)

        # Round trip 2: COPY into staging
        _t2 = time.perf_counter() if _prof else 0.0
        with cur.copy(
            f"COPY _flat_stage ({copy_cols}) FROM STDIN (FORMAT csv, HEADER true)"
        ) as copy:
            copy.write(csv_bytes)
        if _prof:
            profiling._record(profiling.PHASE_INSERT_COPY, time.perf_counter() - _t2)

        # Round trip 3: batch INSERT + upsert merge from staging
        _t3 = time.perf_counter() if _prof else 0.0
        cur.execute(
            """
            WITH batch AS (
                INSERT INTO batches_table
                    (batch_id, workflow_id, batch_start_time, batch_finish_time, batch_params)
                VALUES (%s, %s, %s, %s, %s::jsonb)
            )
            INSERT INTO flat (batch_id, series_id, valid_time, valid_time_end, value, knowledge_time)
            SELECT %s, %s, valid_time, valid_time_end, value, knowledge_time
            FROM _flat_stage
            ON CONFLICT (series_id, valid_time) DO UPDATE SET
                batch_id       = EXCLUDED.batch_id,
                valid_time_end = EXCLUDED.valid_time_end,
                value          = EXCLUDED.value,
                knowledge_time = EXCLUDED.knowledge_time
            """,
            (batch_id_str, workflow_id, batch_start_time, batch_finish_time, batch_params_json,
             batch_id_str, series_id),
        )
        if _prof:
            profiling._record(profiling.PHASE_INSERT_UPSERT, time.perf_counter() - _t3)


def _insert_overlapping(
    conn: psycopg.Connection,
    *,
    table: pa.Table,
    series_id: int,
    target_table: str,
    batch_id,
    workflow_id: Optional[str],
    batch_start_time: Optional[datetime],
    batch_finish_time: Optional[datetime],
    batch_params_json: Optional[str],
) -> None:
    """
    Insert overlapping rows via direct COPY FROM STDIN to the target hypertable.

    Overlapping tables are pure-append (no ON CONFLICT), so no staging table is
    needed. batch_id and series_id are appended as constant Arrow columns in
    Python (C++ array build, microseconds) and included in the CSV payload,
    eliminating the DB-side INSERT SELECT double-write.

    Round trips:
      1. INSERT INTO batches_table (batch metadata)
      2. COPY {target_table} FROM STDIN (CSV with all columns)
    """
    has_vte = "valid_time_end" in table.schema.names
    n = len(table)
    _prof = profiling._enabled

    # Sort by valid_time first for TimescaleDB chunk locality — groups rows destined for
    # the same chunk contiguously regardless of how many knowledge_times are present.
    table = table.sort_by([("valid_time", "ascending"), ("knowledge_time", "ascending")])

    # Append constant columns in Arrow C++ — no staging table needed (pure append, no conflicts)
    batch_id_str = str(batch_id)
    table = table.append_column("batch_id",  pa.array([batch_id_str] * n, type=pa.string()))
    table = table.append_column("series_id", pa.array([series_id]    * n, type=pa.int64()))

    cols = ["batch_id", "series_id", "valid_time", "valid_time_end", "value", "knowledge_time"] if has_vte \
           else ["batch_id", "series_id", "valid_time", "value", "knowledge_time"]
    table = table.select(cols)

    _t0 = time.perf_counter() if _prof else 0.0
    csv_bytes = _to_csv_bytes(table)
    if _prof:
        profiling._record(profiling.PHASE_INSERT_CSV_SERIALIZE, time.perf_counter() - _t0)

    copy_cols = "batch_id, series_id, valid_time, valid_time_end, value, knowledge_time" if has_vte \
                else "batch_id, series_id, valid_time, value, knowledge_time"

    with conn.cursor() as cur:
        # Round trip 1: batch metadata
        cur.execute(
            "INSERT INTO batches_table "
            "(batch_id, workflow_id, batch_start_time, batch_finish_time, batch_params) "
            "VALUES (%s, %s, %s, %s, %s::jsonb)",
            (batch_id_str, workflow_id, batch_start_time, batch_finish_time, batch_params_json),
        )

        # Round trip 2: COPY directly to target hypertable
        _t1 = time.perf_counter() if _prof else 0.0
        copy_sql = pgsql.SQL(
            "COPY {table} ({cols}) FROM STDIN (FORMAT csv, HEADER true)"
        ).format(table=pgsql.Identifier(target_table), cols=pgsql.SQL(copy_cols))
        with cur.copy(copy_sql) as copy:
            copy.write(csv_bytes)
        if _prof:
            profiling._record(profiling.PHASE_INSERT_COPY, time.perf_counter() - _t1)
