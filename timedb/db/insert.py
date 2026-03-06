import io
import json
import time
from contextlib import contextmanager
from typing import Optional, Iterable, Tuple, Dict, List, Union, Any
from datetime import datetime
import psycopg
from psycopg import sql as pgsql
import pyarrow as pa
import pyarrow.csv as pa_csv

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

_TS_TYPE = pa.timestamp("us", tz="UTC")


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
) -> Optional[List[int]]:
    """
    Insert a ``pa.Table`` into the correct table based on series routing.

    Single-series insert.  *table* must have columns
    ``[knowledge_time, valid_time, (valid_time_end,) value]`` with
    timezone-aware timestamps.  The ``knowledge_time`` column must always be
    present and non-null — it is baked in by the SDK layer before this call.

    The data is serialised to CSV in-memory by Arrow's C++ engine and fed to
    Postgres via a single ``COPY FROM STDIN`` call — no Python row iteration.

    Args:
        conninfo: Database connection or connection string
        table: Arrow table with time-series data (must include knowledge_time column)
        workflow_id: Workflow identifier (overlapping only)
        batch_start_time: Batch start time
        batch_finish_time: Batch finish time
        batch_params: Batch parameters (overlapping only)
        series_id: Series identifier
        routing: Routing info dict with keys: overlapping (bool), retention (str), table (str)

    Returns:
        ``None`` for flat inserts.  ``List[int]`` (sorted distinct batch_ids)
        for overlapping inserts — single element for a uniform knowledge_time,
        multiple elements for VERSIONED inputs with distinct knowledge_times.
    """
    if len(table) == 0:
        return None if not routing["overlapping"] else []

    _prof = profiling._enabled
    _t_total = time.perf_counter() if _prof else 0.0
    with _ensure_conn(conninfo) as conn:
        with conn.transaction():
            if not routing["overlapping"]:
                _insert_flat_from_table(
                    conn,
                    table=table,
                    series_id=series_id,
                )
                if _prof: profiling._record(profiling.PHASE_INSERT_TOTAL, time.perf_counter() - _t_total)
                return None
            else:
                result = _insert_overlapping_from_table(
                    conn,
                    table=table,
                    series_id=series_id,
                    routing=routing,
                    workflow_id=workflow_id,
                    batch_start_time=batch_start_time,
                    batch_finish_time=batch_finish_time,
                    batch_params=batch_params,
                )
                if _prof: profiling._record(profiling.PHASE_INSERT_TOTAL, time.perf_counter() - _t_total)
                return result


# ── Private helpers ──────────────────────────────────────────────────────────

def _insert_flat_from_table(
    conn: psycopg.Connection,
    *,
    table: pa.Table,
    series_id: int,
) -> None:
    """
    Upsert flat values via a staging table + INSERT ON CONFLICT DO UPDATE.

    Serialises the Arrow table to CSV, COPYs into a temp staging table, then
    upserts into ``flat`` — updating ``valid_time_end``, ``value``, and
    ``knowledge_time`` when a ``(series_id, valid_time)`` row already exists.

    *table* must have columns ``[knowledge_time, valid_time, (valid_time_end,) value]``.
    The ``knowledge_time`` column must be present and non-null.
    """
    if len(table) == 0:
        return

    n = len(table)
    _prof = profiling._enabled

    # ------------------------------------------------------------------
    # Build Arrow table with series_id injected
    # ------------------------------------------------------------------
    _t0 = time.perf_counter() if _prof else 0.0
    valid_time_end_arr = (
        table.column("valid_time_end")
        if "valid_time_end" in table.schema.names
        else pa.array([None] * n, type=_TS_TYPE)
    )
    arrow_table = pa.table({
        "series_id":      pa.array([series_id] * n, type=pa.int64()),
        "valid_time":     table.column("valid_time"),
        "valid_time_end": valid_time_end_arr,
        "value":          table.column("value"),
        "knowledge_time": table.column("knowledge_time"),
    })

    # ------------------------------------------------------------------
    # Serialise to in-memory CSV (C++ level)
    # ------------------------------------------------------------------
    buf = io.BytesIO()
    pa_csv.write_csv(arrow_table, buf)
    csv_bytes = buf.getvalue()
    if _prof: profiling._record(profiling.PHASE_INSERT_CSV_SERIALIZE, time.perf_counter() - _t0)

    with conn.cursor() as cur:
        # ------------------------------------------------------------------
        # Create temp staging table
        # ------------------------------------------------------------------
        _t0 = time.perf_counter() if _prof else 0.0
        cur.execute("""
            CREATE TEMP TABLE _flat_stage (
                series_id       bigint NOT NULL,
                valid_time      timestamptz NOT NULL,
                valid_time_end  timestamptz,
                value           double precision,
                knowledge_time  timestamptz NOT NULL
            ) ON COMMIT DROP
        """)
        if _prof: profiling._record(profiling.PHASE_INSERT_STAGE_DDL, time.perf_counter() - _t0)

        # ------------------------------------------------------------------
        # COPY into staging, then upsert into flat
        # ------------------------------------------------------------------
        _t0 = time.perf_counter() if _prof else 0.0
        with cur.copy(
            "COPY _flat_stage (series_id, valid_time, valid_time_end, value, knowledge_time)"
            " FROM STDIN (FORMAT CSV, HEADER TRUE)"
        ) as copy:
            copy.write(csv_bytes)
        if _prof: profiling._record(profiling.PHASE_INSERT_COPY, time.perf_counter() - _t0)

        _t0 = time.perf_counter() if _prof else 0.0
        cur.execute("""
            INSERT INTO flat (series_id, valid_time, valid_time_end, value, knowledge_time)
            SELECT series_id, valid_time, valid_time_end, value, knowledge_time
            FROM _flat_stage
            ON CONFLICT (series_id, valid_time) DO UPDATE SET
                valid_time_end = EXCLUDED.valid_time_end,
                value          = EXCLUDED.value,
                knowledge_time = EXCLUDED.knowledge_time
        """)
        if _prof: profiling._record(profiling.PHASE_INSERT_UPSERT, time.perf_counter() - _t0)


def _insert_overlapping_from_table(
    conn: psycopg.Connection,
    *,
    table: pa.Table,
    series_id: int,
    routing: Dict[str, Any],
    workflow_id: Optional[str] = None,
    batch_start_time: Optional[datetime] = None,
    batch_finish_time: Optional[datetime] = None,
    batch_params: Optional[Dict] = None,
) -> List[int]:
    """
    Insert an Arrow table into overlapping storage using decoupled metadata + direct COPY.

    Each unique ``knowledge_time`` value in *table* becomes one row in
    ``batches_table``.  The relational join (knowledge_time → batch_id) is
    performed in Python/Arrow memory rather than inside the database:

    1. Python: ``pc.unique()`` extracts distinct knowledge_times in memory.
    2. SQL:    tiny ``INSERT INTO batches_table ... RETURNING batch_id`` (1–N rows).
    3. Python: builds ``batch_id`` Arrow column via dict lookup (O(n), in-memory).
    4. SQL:    single ``COPY`` of the fully-shaped Arrow table directly into the
               target hypertable — no staging table, no in-DB join.

    *table* must have columns ``[knowledge_time, valid_time, (valid_time_end,) value]``.

    Returns:
        Sorted list of distinct ``batch_id`` values created.
    """
    if len(table) == 0:
        return []

    n = len(table)
    retention = routing["retention"]
    target_table = OVERLAPPING_TABLES[retention]
    batch_params_json = json.dumps(batch_params) if batch_params is not None else None
    _prof = profiling._enabled

    # ------------------------------------------------------------------
    # Step 1 & 2: extract unique knowledge_times in Arrow, register batches
    # ------------------------------------------------------------------
    _t0 = time.perf_counter() if _prof else 0.0
    valid_time_end_arr = (
        table.column("valid_time_end")
        if "valid_time_end" in table.schema.names
        else pa.array([None] * n, type=_TS_TYPE)
    )
    kt_col = table.column("knowledge_time")
    unique_kts_py = sorted(kt_col.unique().to_pylist())

    with conn.cursor() as cur:
        batch_placeholders = pgsql.SQL(", ").join(
            [pgsql.SQL("(%s, %s, %s, %s, %s::jsonb)")] * len(unique_kts_py)
        )
        cur.execute(
            pgsql.SQL(
                "INSERT INTO batches_table "
                "(workflow_id, batch_start_time, batch_finish_time, knowledge_time, batch_params) "
                "VALUES {} RETURNING batch_id, knowledge_time"
            ).format(batch_placeholders),
            [x for kt in unique_kts_py
               for x in (workflow_id, batch_start_time, batch_finish_time, kt, batch_params_json)],
        )
        kt_to_batch = {row[1]: row[0] for row in cur.fetchall()}
    if _prof: profiling._record(profiling.PHASE_INSERT_UPSERT, time.perf_counter() - _t0)

    # ------------------------------------------------------------------
    # Step 3 & 4: build batch_id column in Python, serialize, COPY to hypertable
    # ------------------------------------------------------------------
    _t0 = time.perf_counter() if _prof else 0.0
    if len(unique_kts_py) == 1:
        # Fast path: single knowledge_time — O(1) constant fill
        batch_id_arr = pa.array([kt_to_batch[unique_kts_py[0]]] * n, type=pa.int64())
    else:
        # Multi-kt: O(n) dict lookup per row
        batch_id_arr = pa.array(
            [kt_to_batch[kt] for kt in kt_col.to_pylist()], type=pa.int64()
        )

    arrow_table = pa.table({
        "batch_id":       batch_id_arr,
        "series_id":      pa.array([series_id] * n, type=pa.int64()),
        "valid_time":     table.column("valid_time"),
        "valid_time_end": valid_time_end_arr,
        "value":          table.column("value"),
        "knowledge_time": kt_col,
    })

    buf = io.BytesIO()
    pa_csv.write_csv(arrow_table, buf)
    csv_bytes = buf.getvalue()
    if _prof: profiling._record(profiling.PHASE_INSERT_CSV_SERIALIZE, time.perf_counter() - _t0)

    with conn.cursor() as cur:
        _t0 = time.perf_counter() if _prof else 0.0
        with cur.copy(
            pgsql.SQL(
                "COPY {} (batch_id, series_id, valid_time, valid_time_end, value, knowledge_time)"
                " FROM STDIN (FORMAT CSV, HEADER TRUE)"
            ).format(pgsql.Identifier(target_table))
        ) as copy:
            copy.write(csv_bytes)
        if _prof: profiling._record(profiling.PHASE_INSERT_COPY, time.perf_counter() - _t0)

    return sorted(kt_to_batch.values())


