import json
from contextlib import contextmanager
from typing import Optional, Iterable, Tuple, Dict, List, Union
from datetime import datetime
from collections import defaultdict
import psycopg
from psycopg import sql


@contextmanager
def _ensure_conn(conninfo_or_conn):
    """Yield a psycopg Connection, creating one only if given a string."""
    if isinstance(conninfo_or_conn, str):
        with psycopg.connect(conninfo_or_conn) as conn:
            yield conn
    else:
        yield conninfo_or_conn

_OVERLAPPING_TABLES = {
    "short":  "overlapping_short",
    "medium": "overlapping_medium",
    "long":   "overlapping_long",
}

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
    series_routing: Optional[Dict[int, Dict[str, str]]] = None,
) -> Optional[int]:
    """
    Insert values into the correct tables based on series overlapping flag.

    Flat series: inserted directly (upsert), no batch created.
    Overlapping series: a batch is created and values are appended with known_time.

    value_rows is expected to be an iterable where each item is either:
      - (valid_time, series_id, value),                      # point-in-time
      - (valid_time, valid_time_end, series_id, value),      # interval

    Returns:
        The batch_id if overlapping values were inserted, None for flat-only.
    """
    rows_list = list(value_rows)
    if not rows_list:
        return None

    with _ensure_conn(conninfo) as conn:
        with conn.transaction():
            # Look up routing if not provided
            if series_routing is None:
                series_ids_in_rows = set()
                for item in rows_list:
                    if len(item) == 3:
                        series_ids_in_rows.add(item[1])
                    elif len(item) == 4:
                        series_ids_in_rows.add(item[2])
                series_routing = _lookup_series_routing(conn, list(series_ids_in_rows))

            # Separate rows by overlapping flag
            flat_rows: List[Tuple] = []
            overlapping_rows: List[Tuple] = []

            for item in rows_list:
                if len(item) == 3:
                    valid_time, series_id, value = item
                    valid_time_end = None
                elif len(item) == 4:
                    valid_time, valid_time_end, series_id, value = item
                else:
                    raise ValueError(
                        "Each value row must be either "
                        "(valid_time, series_id, value) or "
                        "(valid_time, valid_time_end, series_id, value)"
                    )

                routing = series_routing.get(series_id)
                if routing is None:
                    raise ValueError(f"No routing info for series_id {series_id}. Ensure it exists in series_table.")

                if not routing["overlapping"]:
                    if valid_time_end is not None:
                        flat_rows.append((valid_time, valid_time_end, series_id, value))
                    else:
                        flat_rows.append((valid_time, series_id, value))
                else:
                    if valid_time_end is not None:
                        overlapping_rows.append((valid_time, valid_time_end, series_id, value))
                    else:
                        overlapping_rows.append((valid_time, series_id, value))

            # Insert flat rows (no batch needed)
            if flat_rows:
                _insert_flat(conn, value_rows=flat_rows)

            # Insert overlapping rows (batch required)
            if overlapping_rows:
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
                    value_rows=overlapping_rows,
                    series_routing=series_routing,
                )
                return batch_id

    return None


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


def _lookup_series_routing(
    conn: psycopg.Connection,
    series_ids: List[int],
) -> Dict[int, Dict[str, str]]:
    """
    Look up overlapping flag and retention for a list of series IDs.

    Returns:
        Dict mapping series_id -> {'overlapping': bool, 'retention': str}
    """
    if not series_ids:
        return {}
    with conn.cursor() as cur:
        cur.execute(
            "SELECT series_id, overlapping, retention FROM series_table WHERE series_id = ANY(%s)",
            (list(series_ids),),
        )
        rows = cur.fetchall()
    return {row[0]: {"overlapping": row[1], "retention": row[2]} for row in rows}


def _validate_value_rows(value_rows: Iterable[Tuple]) -> List[Tuple]:
    """Validate and normalize value rows, returning (series_id, valid_time, valid_time_end, value) tuples."""
    rows_list = []
    for item in value_rows:
        if len(item) == 3:
            valid_time, series_id, value = item
            valid_time_end = None
        elif len(item) == 4:
            valid_time, valid_time_end, series_id, value = item
        else:
            raise ValueError(
                "Each row must be "
                "(valid_time, series_id, value) or "
                "(valid_time, valid_time_end, series_id, value)"
            )

        if not isinstance(valid_time, datetime):
            raise ValueError("valid_time must be a datetime")
        if valid_time.tzinfo is None:
            raise ValueError("valid_time must be timezone-aware (timestamptz).")

        if valid_time_end is not None:
            if not isinstance(valid_time_end, datetime):
                raise ValueError("valid_time_end must be a datetime or None")
            if valid_time_end.tzinfo is None:
                raise ValueError("valid_time_end must be timezone-aware (timestamptz).")
            if not (valid_time_end > valid_time):
                raise ValueError("valid_time_end must be strictly after valid_time")

        rows_list.append((series_id, valid_time, valid_time_end, value))
    return rows_list


def _insert_flat(
    conn: psycopg.Connection,
    *,
    value_rows: Iterable[Tuple],
) -> None:
    """
    Insert flat (fact) values into the flat table.

    Each row is: (valid_time, series_id, value)
    or with valid_time_end: (valid_time, valid_time_end, series_id, value)

    Uses ON CONFLICT to upsert (update value on duplicate).
    For large batches (>=50 rows), uses COPY into a staging table for performance.
    """
    rows_list = _validate_value_rows(value_rows)

    if not rows_list:
        return

    with conn.cursor() as cur:
        if len(rows_list) >= _COPY_THRESHOLD:
            # COPY path: staging table → INSERT...ON CONFLICT
            cur.execute("""
                CREATE TEMP TABLE _flat_staging (
                    series_id bigint,
                    valid_time timestamptz,
                    valid_time_end timestamptz,
                    value double precision
                ) ON COMMIT DROP
            """)
            with cur.copy("COPY _flat_staging (series_id, valid_time, valid_time_end, value) FROM STDIN") as copy:
                for row in rows_list:
                    copy.write_row(row)
            cur.execute("""
                INSERT INTO flat (series_id, valid_time, valid_time_end, value)
                SELECT series_id, valid_time, valid_time_end, value FROM _flat_staging
                ON CONFLICT (series_id, valid_time)
                DO UPDATE SET value = EXCLUDED.value, valid_time_end = EXCLUDED.valid_time_end
            """)
        else:
            cur.executemany(
                """
                INSERT INTO flat (series_id, valid_time, valid_time_end, value)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (series_id, valid_time)
                DO UPDATE SET value = EXCLUDED.value, valid_time_end = EXCLUDED.valid_time_end
                """,
                rows_list,
            )


def _insert_overlapping(
    conn: psycopg.Connection,
    *,
    batch_id: int,
    known_time: datetime,
    value_rows: Iterable[Tuple],
    series_routing: Dict[int, Dict[str, str]],
) -> None:
    """
    Insert overlapping values into the correct tier table.

    Routes rows to overlapping_short/medium/long based on the series'
    retention from series_routing.

    Each row is: (valid_time, series_id, value)
    or with valid_time_end: (valid_time, valid_time_end, series_id, value)

    For large batches (>=50 rows per tier), uses COPY protocol for performance.
    """
    # Bucket rows by storage tier
    by_tier: Dict[str, List[Tuple]] = defaultdict(list)

    for item in value_rows:
        if len(item) == 3:
            valid_time, series_id, value = item
            valid_time_end = None
        elif len(item) == 4:
            valid_time, valid_time_end, series_id, value = item
        else:
            raise ValueError(
                "Each overlapping row must be "
                "(valid_time, series_id, value) or "
                "(valid_time, valid_time_end, series_id, value)"
            )

        if not isinstance(valid_time, datetime):
            raise ValueError("valid_time must be a datetime")
        if valid_time.tzinfo is None:
            raise ValueError("valid_time must be timezone-aware (timestamptz).")

        if valid_time_end is not None:
            if not isinstance(valid_time_end, datetime):
                raise ValueError("valid_time_end must be a datetime or None")
            if valid_time_end.tzinfo is None:
                raise ValueError("valid_time_end must be timezone-aware (timestamptz).")
            if not (valid_time_end > valid_time):
                raise ValueError("valid_time_end must be strictly after valid_time")

        retention = series_routing[series_id]["retention"]
        by_tier[retention].append(
            (batch_id, series_id, valid_time, valid_time_end, value, known_time)
        )

    with conn.cursor() as cur:
        for tier, rows in by_tier.items():
            table = _OVERLAPPING_TABLES[tier]
            if len(rows) >= _COPY_THRESHOLD:
                # COPY path for large batches
                copy_sql = sql.SQL(
                    "COPY {} (batch_id, series_id, valid_time, valid_time_end, value, known_time) FROM STDIN"
                ).format(sql.Identifier(table))
                with cur.copy(copy_sql) as copy:
                    for row in rows:
                        copy.write_row(row)
            else:
                cur.executemany(
                    sql.SQL("""
                    INSERT INTO {} (batch_id, series_id, valid_time, valid_time_end, value, known_time)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """).format(sql.Identifier(table)),
                    rows,
                )


