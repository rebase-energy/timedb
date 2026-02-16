import json
from contextlib import contextmanager
from typing import Optional, Iterable, Tuple, Dict, List, Union, Any
from datetime import datetime
import psycopg
from psycopg import sql

from .series import SeriesRegistry, OVERLAPPING_TABLES


@contextmanager
def _ensure_conn(conninfo_or_conn):
    """Yield a psycopg Connection, creating one only if given a string."""
    if isinstance(conninfo_or_conn, str):
        with psycopg.connect(conninfo_or_conn) as conn:
            yield conn
    else:
        yield conninfo_or_conn

_COPY_THRESHOLD = 50


# ── Public API ───────────────────────────────────────────────────────────────

def insert_values(
    conninfo: Union[psycopg.Connection, str],
    *,
    workflow_id: Optional[str] = None,
    batch_start_time: Optional[datetime] = None,
    batch_finish_time: Optional[datetime] = None,
    value_rows: Iterable[Tuple],
    known_time: Optional[datetime] = None,
    batch_params: Optional[Dict] = None,
    series_id: int,
    routing: Dict[str, Any],
) -> Optional[int]:
    """
    Insert values into the correct table based on series overlapping flag.
    
    Single-series inserts only (all rows have the same series_id).
    Routes to flat table (upsert) or overlapping table (versioned with batch).
    
    value_rows is expected to be an iterable where each item is either:
      - (valid_time, series_id, value)                      # point-in-time
      - (valid_time, valid_time_end, series_id, value)      # interval
    
    Args:
        conninfo: Database connection or connection string
        workflow_id: Workflow identifier
        batch_start_time: Batch start time
        batch_finish_time: Batch finish time
        value_rows: Iterable of value tuples
        known_time: Time of knowledge
        batch_params: Batch parameters
        series_id: Series identifier
        routing: Routing info dict with keys: overlapping (bool), retention (str), table (str)
    
    Returns:
        The batch_id if overlapping, None for flat.
    """
    rows_list = list(value_rows)
    if not rows_list:
        return None
    
    with _ensure_conn(conninfo) as conn:
        with conn.transaction():
            is_overlapping = routing["overlapping"]
            
            # Route to appropriate insert function
            if not is_overlapping:
                _insert_flat(conn, value_rows=rows_list, known_time=known_time)
                return None
            else:
                batch_id, batch_known_time = _create_batch(
                    conn,
                    workflow_id=workflow_id,
                    batch_start_time=batch_start_time,
                    batch_finish_time=batch_finish_time,
                    known_time=known_time,
                    batch_params=batch_params,
                )
                _insert_overlapping(
                    conn,
                    batch_id=batch_id,
                    known_time=batch_known_time,
                    value_rows=rows_list,
                    series_id=series_id,
                    routing=routing,
                )
                return batch_id


# ── Private helpers ──────────────────────────────────────────────────────────

def _create_batch(
    conn: psycopg.Connection,
    *,
    workflow_id: Optional[str] = None,
    batch_start_time: Optional[datetime] = None,
    batch_finish_time: Optional[datetime] = None,
    known_time: Optional[datetime] = None,
    batch_params: Optional[Dict] = None,
) -> Tuple[int, datetime]:
    """
    Insert one batch into batches_table.

    Returns:
        Tuple of (batch_id, known_time)
    """
    batch_params_json = json.dumps(batch_params) if batch_params is not None else None

    # Validate timezone-aware datetimes
    if batch_start_time is not None and batch_start_time.tzinfo is None:
        raise ValueError("batch_start_time must be timezone-aware")
    if batch_finish_time is not None and batch_finish_time.tzinfo is None:
        raise ValueError("batch_finish_time must be timezone-aware")
    if known_time is not None and known_time.tzinfo is None:
        raise ValueError("known_time must be timezone-aware")

    with conn.cursor() as cur:
        if known_time is not None:
            cur.execute(
                """
                INSERT INTO batches_table (
                    workflow_id, batch_start_time, batch_finish_time,
                    known_time, batch_params
                )
                VALUES (%s, %s, %s, %s, %s::jsonb)
                RETURNING batch_id, known_time;
                """,
                (workflow_id, batch_start_time, batch_finish_time, known_time, batch_params_json),
            )
        else:
            cur.execute(
                """
                INSERT INTO batches_table (
                    workflow_id, batch_start_time, batch_finish_time,
                    batch_params
                )
                VALUES (%s, %s, %s, %s::jsonb)
                RETURNING batch_id, known_time;
                """,
                (workflow_id, batch_start_time, batch_finish_time, batch_params_json),
            )
        result = cur.fetchone()
        return result[0], result[1]



def _insert_flat(
    conn: psycopg.Connection,
    *,
    value_rows: List[Tuple],
    known_time: Optional[datetime] = None,
) -> None:
    """
    Insert flat (fact) values into the flat table.

    Each row is: (valid_time, series_id, value)
    or with valid_time_end: (valid_time, valid_time_end, series_id, value)

    Uses ON CONFLICT to upsert (update value on duplicate).
    For large batches (>=50 rows), uses COPY into a staging table for performance.
    
    Note: Validation is already done in SDK layer (_dataframe_to_value_rows).
    """
    if not value_rows:
        return
    
    # Normalize to (series_id, valid_time, valid_time_end, value, known_time) format
    first_row = value_rows[0]
    
    if len(first_row) == 3:
        # Point-in-time: (valid_time, series_id, value)
        rows_with_kt = [(row[1], row[0], None, row[2], known_time) for row in value_rows]
    elif len(first_row) == 4:
        # Interval: (valid_time, valid_time_end, series_id, value)
        rows_with_kt = [(row[2], row[0], row[1], row[3], known_time) for row in value_rows]
    else:
        raise ValueError(
            "Flat rows must be (valid_time, series_id, value) "
            "or (valid_time, valid_time_end, series_id, value)"
        )

    with conn.cursor() as cur:
        if len(rows_with_kt) >= _COPY_THRESHOLD:
            # COPY path: staging table → INSERT...ON CONFLICT
            cur.execute("""
                CREATE TEMP TABLE _flat_staging (
                    series_id bigint,
                    valid_time timestamptz,
                    valid_time_end timestamptz,
                    value double precision,
                    known_time timestamptz
                ) ON COMMIT DROP
            """)
            with cur.copy("COPY _flat_staging (series_id, valid_time, valid_time_end, value, known_time) FROM STDIN") as copy:
                for row in rows_with_kt:
                    copy.write_row(row)
            cur.execute("""
                INSERT INTO flat (series_id, valid_time, valid_time_end, value, known_time)
                SELECT series_id, valid_time, valid_time_end, value, COALESCE(known_time, now()) FROM _flat_staging
                ON CONFLICT (series_id, valid_time)
                DO UPDATE SET value = EXCLUDED.value, valid_time_end = EXCLUDED.valid_time_end, known_time = EXCLUDED.known_time
            """)
        else:
            cur.executemany(
                """
                INSERT INTO flat (series_id, valid_time, valid_time_end, value, known_time)
                VALUES (%s, %s, %s, %s, COALESCE(%s, now()))
                ON CONFLICT (series_id, valid_time)
                DO UPDATE SET value = EXCLUDED.value, valid_time_end = EXCLUDED.valid_time_end, known_time = EXCLUDED.known_time
                """,
                rows_with_kt,
            )


def _insert_overlapping(
    conn: psycopg.Connection,
    *,
    batch_id: int,
    known_time: datetime,
    value_rows: List[Tuple],
    series_id: int,
    routing: Dict[str, Any],
) -> None:
    """
    Insert overlapping values for a single series.
    
    Single-series inserts: all rows have same format, series_id, and retention tier.
    Routes to overlapping_short/medium/long based on retention from routing.

    Each row: (valid_time, series_id, value) or (valid_time, valid_time_end, series_id, value)

    For large batches (>=50 rows), uses COPY protocol for performance.
    
    Note: SDK validates all data (timezone, intervals, types) in _dataframe_to_value_rows.
    """
    if not value_rows:
        return
    
    # Detect format from first row (all rows have same format in single-series inserts)
    first_row = value_rows[0]
    
    if len(first_row) == 3:
        # Point-in-time: (valid_time, series_id, value)
        rows_prepared = [
            (batch_id, series_id, row[0], None, row[2], known_time)
            for row in value_rows
        ]
    elif len(first_row) == 4:
        # Interval: (valid_time, valid_time_end, series_id, value)
        rows_prepared = [
            (batch_id, series_id, row[0], row[1], row[3], known_time)
            for row in value_rows
        ]
    else:
        raise ValueError(
            "Overlapping rows must be (valid_time, series_id, value) "
            "or (valid_time, valid_time_end, series_id, value)"
        )
    
    # Route to retention tier
    retention = routing["retention"]
    table = OVERLAPPING_TABLES[retention]
    
    with conn.cursor() as cur:
        if len(rows_prepared) >= _COPY_THRESHOLD:
            # COPY path for large batches
            copy_sql = sql.SQL(
                "COPY {} (batch_id, series_id, valid_time, valid_time_end, value, known_time) FROM STDIN"
            ).format(sql.Identifier(table))
            with cur.copy(copy_sql) as copy:
                for row in rows_prepared:
                    copy.write_row(row)
        else:
            cur.executemany(
                sql.SQL("""
                INSERT INTO {} (batch_id, series_id, valid_time, valid_time_end, value, known_time)
                VALUES (%s, %s, %s, %s, %s, %s)
                """).format(sql.Identifier(table)),
                rows_prepared,
            )


