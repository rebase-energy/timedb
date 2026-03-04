import io
import json
import time
from contextlib import contextmanager
from typing import Optional, Iterable, Tuple, Dict, List, Union, Any
from datetime import datetime
import psycopg
from psycopg import sql
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
    Insert flat values using Arrow CSV serialisation → COPY → INSERT ON CONFLICT.

    Serialises the entire Arrow table to CSV in-memory with Arrow's C++ engine
    (no Python row iteration), then feeds the bytes to Postgres in a single
    ``COPY FROM STDIN`` call.  A temporary staging table is used so that the
    ``ON CONFLICT`` upsert logic can be applied after the bulk load.

    *table* must have columns ``[knowledge_time, valid_time, (valid_time_end,) value]``.
    The ``knowledge_time`` column must be present and non-null.
    """
    if len(table) == 0:
        return

    n = len(table)
    _prof = profiling._enabled

    # ------------------------------------------------------------------
    # Build staging table: series_id | valid_time | valid_time_end | value | knowledge_time
    # ------------------------------------------------------------------
    _t0 = time.perf_counter() if _prof else 0.0
    valid_time_end_arr = (
        table.column("valid_time_end")
        if "valid_time_end" in table.schema.names
        else pa.array([None] * n, type=_TS_TYPE)
    )
    staging = pa.table({
        "series_id":      pa.array([series_id] * n, type=pa.int64()),
        "valid_time":     table.column("valid_time"),
        "valid_time_end": valid_time_end_arr,
        "value":          table.column("value"),
        "knowledge_time": table.column("knowledge_time"),
    })

    # ------------------------------------------------------------------
    # Serialise to in-memory CSV (C++ level) and COPY to Postgres
    # ------------------------------------------------------------------
    buf = io.BytesIO()
    pa_csv.write_csv(staging, buf)
    csv_bytes = buf.getvalue()
    if _prof: profiling._record(profiling.PHASE_INSERT_CSV_SERIALIZE, time.perf_counter() - _t0)

    with conn.cursor() as cur:
        _t0 = time.perf_counter() if _prof else 0.0
        cur.execute("""
            CREATE TEMP TABLE _flat_staging (
                series_id bigint,
                valid_time timestamptz,
                valid_time_end timestamptz,
                value double precision,
                knowledge_time timestamptz
            ) ON COMMIT DROP
        """)
        if _prof: profiling._record(profiling.PHASE_INSERT_STAGE_DDL, time.perf_counter() - _t0)

        _t0 = time.perf_counter() if _prof else 0.0
        with cur.copy(
            "COPY _flat_staging (series_id, valid_time, valid_time_end, value, knowledge_time)"
            " FROM STDIN (FORMAT CSV, HEADER TRUE)"
        ) as copy:
            copy.write(csv_bytes)
        if _prof: profiling._record(profiling.PHASE_INSERT_COPY, time.perf_counter() - _t0)

        _t0 = time.perf_counter() if _prof else 0.0
        cur.execute("""
            INSERT INTO flat (series_id, valid_time, valid_time_end, value, knowledge_time)
            SELECT series_id, valid_time, valid_time_end, value, knowledge_time
            FROM _flat_staging
            ON CONFLICT (series_id, valid_time)
            DO UPDATE SET
                value = EXCLUDED.value,
                valid_time_end = EXCLUDED.valid_time_end,
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
    Insert an Arrow table into overlapping storage using a staging-table + single CTE pattern.

    Each unique ``knowledge_time`` value in *table* becomes one row in
    ``batches_table`` and one batch of rows in the target overlapping table.
    The entire operation is a single round-trip after the COPY:

    1. COPY staging rows (knowledge_time, valid_time, valid_time_end, value).
    2. One CTE that INSERT INTO batches_table ... SELECT DISTINCT knowledge_time
       ... RETURNING batch_id, then INSERT INTO {tier} ... JOIN new_batches ON
       knowledge_time.

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
    # Build staging table: knowledge_time | valid_time | valid_time_end | value
    # series_id is supplied as a SQL parameter, not in the staging rows.
    # ------------------------------------------------------------------
    _t0 = time.perf_counter() if _prof else 0.0
    valid_time_end_arr = (
        table.column("valid_time_end")
        if "valid_time_end" in table.schema.names
        else pa.array([None] * n, type=_TS_TYPE)
    )
    staging = pa.table({
        "knowledge_time":  table.column("knowledge_time"),
        "valid_time":      table.column("valid_time"),
        "valid_time_end":  valid_time_end_arr,
        "value":           table.column("value"),
    })

    # ------------------------------------------------------------------
    # Serialise and COPY to temp staging table
    # ------------------------------------------------------------------
    buf = io.BytesIO()
    pa_csv.write_csv(staging, buf)
    csv_bytes = buf.getvalue()
    if _prof: profiling._record(profiling.PHASE_INSERT_CSV_SERIALIZE, time.perf_counter() - _t0)

    with conn.cursor() as cur:
        _t0 = time.perf_counter() if _prof else 0.0
        cur.execute("""
            CREATE TEMP TABLE _overlap_staging (
                knowledge_time timestamptz NOT NULL,
                valid_time     timestamptz NOT NULL,
                valid_time_end timestamptz,
                value          double precision NOT NULL
            ) ON COMMIT DROP
        """)
        if _prof: profiling._record(profiling.PHASE_INSERT_STAGE_DDL, time.perf_counter() - _t0)

        _t0 = time.perf_counter() if _prof else 0.0
        with cur.copy(
            "COPY _overlap_staging (knowledge_time, valid_time, valid_time_end, value)"
            " FROM STDIN (FORMAT CSV, HEADER TRUE)"
        ) as copy:
            copy.write(csv_bytes)
        if _prof: profiling._record(profiling.PHASE_INSERT_COPY, time.perf_counter() - _t0)

        # ------------------------------------------------------------------
        # Single-round-trip CTE:
        #   1. INSERT DISTINCT knowledge_times into batches_table.
        #   2. INSERT staging rows into overlapping tier, joined to new batches.
        # (batch_meta is merged into the CTE, so PHASE_INSERT_BATCH_META = 0)
        # ------------------------------------------------------------------
        cte_sql = sql.SQL("""
            WITH new_batches AS (
                INSERT INTO batches_table (
                    workflow_id, batch_start_time, batch_finish_time,
                    knowledge_time, batch_params
                )
                SELECT DISTINCT
                    %(workflow_id)s::text,
                    %(batch_start_time)s::timestamptz,
                    %(batch_finish_time)s::timestamptz,
                    knowledge_time,
                    %(batch_params)s::jsonb
                FROM _overlap_staging
                RETURNING batch_id, knowledge_time
            )
            INSERT INTO {target} (
                batch_id, series_id, valid_time, valid_time_end, value, knowledge_time
            )
            SELECT
                nb.batch_id,
                %(series_id)s::bigint,
                s.valid_time,
                s.valid_time_end,
                s.value,
                s.knowledge_time
            FROM _overlap_staging s
            JOIN new_batches nb ON nb.knowledge_time = s.knowledge_time
            RETURNING batch_id
        """).format(target=sql.Identifier(target_table))

        _t0 = time.perf_counter() if _prof else 0.0
        cur.execute(
            cte_sql,
            {
                "workflow_id":       workflow_id,
                "batch_start_time":  batch_start_time,
                "batch_finish_time": batch_finish_time,
                "batch_params":      batch_params_json,
                "series_id":         series_id,
            },
        )
        if _prof: profiling._record(profiling.PHASE_INSERT_UPSERT, time.perf_counter() - _t0)
        rows = cur.fetchall()

    return sorted({row[0] for row in rows})


