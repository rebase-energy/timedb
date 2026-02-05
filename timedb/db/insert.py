import uuid
import json
from typing import Optional, Iterable, Tuple, Dict, List
from datetime import datetime
from collections import defaultdict
import psycopg
from psycopg import sql

_OVERLAPPING_TABLES = {
    "short":  "overlapping_short",
    "medium": "overlapping_medium",
    "long":   "overlapping_long",
}



def insert_batch(
    conn: psycopg.Connection,
    *,
    batch_id: uuid.UUID,
    tenant_id: uuid.UUID,
    workflow_id: Optional[str] = None,
    batch_start_time: Optional[datetime] = None,
    batch_finish_time: Optional[datetime] = None,
    known_time: Optional[datetime] = None,
    batch_params: Optional[Dict] = None,
) -> datetime:
    """
    Insert one batch into batches_table.

    Uses ON CONFLICT so retries are safe.

    Args:
        batch_id: Unique identifier for the batch
        tenant_id: Tenant identifier
        workflow_id: Optional workflow/pipeline identifier (NULL for manual insertions)
        batch_start_time: Optional start time of the batch
        batch_finish_time: Optional finish time of the batch
        known_time: Time of knowledge - when the data was known/available.
                   If not provided, defaults to now() in the database.
        batch_params: Optional parameters/config used for this batch

    Returns:
        The known_time that was used (either provided or database now())
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
                    batch_id, tenant_id, workflow_id,
                    batch_start_time, batch_finish_time, known_time, batch_params
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (batch_id) DO NOTHING
                RETURNING known_time;
                """,
                (batch_id, tenant_id, workflow_id, batch_start_time, batch_finish_time, known_time, batch_params_json),
            )
            result = cur.fetchone()
            if result:
                return result[0]
            cur.execute("SELECT known_time FROM batches_table WHERE batch_id = %s", (batch_id,))
            existing = cur.fetchone()
            if existing is None:
                raise ValueError(f"Batch {batch_id} not found after conflict - unexpected state")
            return existing[0]
        else:
            cur.execute(
                """
                INSERT INTO batches_table (
                    batch_id, tenant_id, workflow_id,
                    batch_start_time, batch_finish_time, batch_params
                )
                VALUES (%s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (batch_id) DO NOTHING
                RETURNING known_time;
                """,
                (batch_id, tenant_id, workflow_id, batch_start_time, batch_finish_time, batch_params_json),
            )
            result = cur.fetchone()
            if result:
                return result[0]
            cur.execute("SELECT known_time FROM batches_table WHERE batch_id = %s", (batch_id,))
            existing = cur.fetchone()
            if existing is None:
                raise ValueError(f"Batch {batch_id} not found after conflict - unexpected state")
            return existing[0]


def _lookup_series_routing(
    conn: psycopg.Connection,
    series_ids: List[uuid.UUID],
) -> Dict[uuid.UUID, Dict[str, str]]:
    """
    Look up data_class and retention for a list of series IDs.

    Returns:
        Dict mapping series_id -> {'data_class': ..., 'retention': ...}
    """
    if not series_ids:
        return {}
    with conn.cursor() as cur:
        cur.execute(
            "SELECT series_id, data_class, retention FROM series_table WHERE series_id = ANY(%s)",
            (list(series_ids),),
        )
        rows = cur.fetchall()
    return {row[0]: {"data_class": row[1], "retention": row[2]} for row in rows}


def insert_flat(
    conn: psycopg.Connection,
    *,
    value_rows: Iterable[Tuple],
) -> None:
    """
    Insert flat (fact) values into the flat table.

    Each row is: (tenant_id, valid_time, series_id, value)
    or with valid_time_end: (tenant_id, valid_time, valid_time_end, series_id, value)

    Uses ON CONFLICT to upsert (update value on duplicate).

    Args:
        conn: Database connection
        value_rows: Iterable of value tuples
    """
    rows_list = []
    for item in value_rows:
        if len(item) == 4:
            tenant_id, valid_time, series_id, value = item
            valid_time_end = None
        elif len(item) == 5:
            tenant_id, valid_time, valid_time_end, series_id, value = item
        else:
            raise ValueError(
                "Each flat row must be "
                "(tenant_id, valid_time, series_id, value) or "
                "(tenant_id, valid_time, valid_time_end, series_id, value)"
            )

        if not isinstance(tenant_id, uuid.UUID):
            raise ValueError("tenant_id must be a UUID")
        if not isinstance(series_id, uuid.UUID):
            raise ValueError("series_id must be a UUID")
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

        rows_list.append((tenant_id, series_id, valid_time, valid_time_end, value))

    if not rows_list:
        return

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO flat (tenant_id, series_id, valid_time, valid_time_end, value)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (tenant_id, series_id, valid_time)
            DO UPDATE SET value = EXCLUDED.value, valid_time_end = EXCLUDED.valid_time_end
            """,
            rows_list,
        )


def insert_overlapping(
    conn: psycopg.Connection,
    *,
    batch_id: uuid.UUID,
    known_time: datetime,
    value_rows: Iterable[Tuple],
    series_routing: Dict[uuid.UUID, Dict[str, str]],
) -> None:
    """
    Insert overlapping values into the correct tier table.

    Routes rows to overlapping_short/medium/long based on the series'
    retention from series_routing.

    Each row is: (tenant_id, valid_time, series_id, value)
    or with valid_time_end: (tenant_id, valid_time, valid_time_end, series_id, value)

    Args:
        conn: Database connection
        batch_id: UUID of the batch these values belong to
        known_time: The known_time for these overlapping entries
        value_rows: Iterable of value tuples
        series_routing: Dict mapping series_id -> {'data_class': ..., 'retention': ...}
    """
    # Bucket rows by storage tier
    by_tier: Dict[str, List[Tuple]] = defaultdict(list)

    for item in value_rows:
        if len(item) == 4:
            tenant_id, valid_time, series_id, value = item
            valid_time_end = None
        elif len(item) == 5:
            tenant_id, valid_time, valid_time_end, series_id, value = item
        else:
            raise ValueError(
                "Each overlapping row must be "
                "(tenant_id, valid_time, series_id, value) or "
                "(tenant_id, valid_time, valid_time_end, series_id, value)"
            )

        if not isinstance(tenant_id, uuid.UUID):
            raise ValueError("tenant_id must be a UUID")
        if not isinstance(series_id, uuid.UUID):
            raise ValueError("series_id must be a UUID")
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
            (batch_id, tenant_id, series_id, valid_time, valid_time_end, value, known_time)
        )

    with conn.cursor() as cur:
        for tier, rows in by_tier.items():
            table = _OVERLAPPING_TABLES[tier]
            cur.executemany(
                sql.SQL("""
                INSERT INTO {} (batch_id, tenant_id, series_id, valid_time, valid_time_end, value, known_time)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """).format(sql.Identifier(table)),
                rows,
            )


def insert_values(
    conn: psycopg.Connection,
    *,
    batch_id: uuid.UUID,
    known_time: datetime,
    value_rows: Iterable[Tuple],
    series_routing: Dict[uuid.UUID, Dict[str, str]],
    changed_by: Optional[str] = None,
) -> None:
    """
    Route and insert values to the correct table based on series data_class and retention.

    Accepts rows in either of two shapes:
      - (tenant_id, valid_time, series_id, value)                       # point-in-time
      - (tenant_id, valid_time, valid_time_end, series_id, value)       # interval

    Args:
        conn: Database connection
        batch_id: UUID of the batch these values belong to
        known_time: The known_time from the batch
        value_rows: Iterable of value tuples
        series_routing: Dict mapping series_id -> {'data_class': ..., 'retention': ...}
        changed_by: Optional user identifier
    """
    # Separate rows by data_class
    flat_rows: List[Tuple] = []
    overlapping_rows: List[Tuple] = []

    for item in value_rows:
        if len(item) == 4:
            tenant_id, valid_time, series_id, value = item
            valid_time_end = None
        elif len(item) == 5:
            tenant_id, valid_time, valid_time_end, series_id, value = item
        else:
            raise ValueError(
                "Each value row must be either "
                "(tenant_id, valid_time, series_id, value) or "
                "(tenant_id, valid_time, valid_time_end, series_id, value)"
            )

        routing = series_routing.get(series_id)
        if routing is None:
            raise ValueError(f"No routing info for series_id {series_id}. Ensure it exists in series_table.")

        if routing["data_class"] == "flat":
            if valid_time_end is not None:
                flat_rows.append((tenant_id, valid_time, valid_time_end, series_id, value))
            else:
                flat_rows.append((tenant_id, valid_time, series_id, value))
        else:
            if valid_time_end is not None:
                overlapping_rows.append((tenant_id, valid_time, valid_time_end, series_id, value))
            else:
                overlapping_rows.append((tenant_id, valid_time, series_id, value))

    # Insert flat
    if flat_rows:
        insert_flat(conn, value_rows=flat_rows)

    # Insert overlapping (PostgreSQL partitioning routes by retention)
    if overlapping_rows:
        insert_overlapping(
            conn,
            batch_id=batch_id,
            known_time=known_time,
            value_rows=overlapping_rows,
            series_routing=series_routing,
        )


def insert_batch_with_values(
    conninfo: str,
    *,
    batch_id: uuid.UUID,
    tenant_id: uuid.UUID,
    workflow_id: Optional[str] = None,
    batch_start_time: Optional[datetime] = None,
    batch_finish_time: Optional[datetime] = None,
    value_rows: Iterable[Tuple],
    known_time: Optional[datetime] = None,
    batch_params: Optional[Dict] = None,
    changed_by: Optional[str] = None,
    series_routing: Optional[Dict[uuid.UUID, Dict[str, str]]] = None,
) -> None:
    """
    One-shot helper: inserts the batch + all values atomically.

    Routes values to flat or overlapping tables based on series_routing.

    value_rows is expected to be an iterable where each item is either:
      - (tenant_id, valid_time, series_id, value),                      # point-in-time
      - (tenant_id, valid_time, valid_time_end, series_id, value),      # interval

    Args:
        batch_id: Unique identifier for the batch
        tenant_id: Tenant ID for the batch entry
        workflow_id: Optional workflow/pipeline identifier
        batch_start_time: Optional start time of the batch
        batch_finish_time: Optional finish time of the batch
        value_rows: Iterable of value tuples
        known_time: Time of knowledge
        batch_params: Optional parameters/config
        changed_by: Optional user identifier
        series_routing: Dict mapping series_id -> {'data_class': ..., 'retention': ...}.
                       If None, will be looked up from the database.
    """
    # Materialize rows so we can iterate multiple times and extract series_ids
    rows_list = list(value_rows)

    with psycopg.connect(conninfo) as conn:
        with conn.transaction():
            # Look up routing if not provided
            if series_routing is None:
                series_ids_in_rows = set()
                for item in rows_list:
                    if len(item) == 4:
                        series_ids_in_rows.add(item[2])  # series_id at index 2
                    elif len(item) == 5:
                        series_ids_in_rows.add(item[3])  # series_id at index 3
                series_routing = _lookup_series_routing(conn, list(series_ids_in_rows))

            # Check if any overlapping entries exist (need a batch for them)
            has_overlapping = any(
                series_routing.get(
                    item[2] if len(item) == 4 else item[3], {}
                ).get("data_class") == "overlapping"
                for item in rows_list
            )

            # Insert batch (always, for tracking purposes)
            batch_known_time = insert_batch(
                conn,
                batch_id=batch_id,
                tenant_id=tenant_id,
                workflow_id=workflow_id,
                batch_start_time=batch_start_time,
                batch_finish_time=batch_finish_time,
                known_time=known_time,
                batch_params=batch_params,
            )

            # Insert values with routing
            insert_values(
                conn,
                batch_id=batch_id,
                known_time=batch_known_time,
                value_rows=rows_list,
                series_routing=series_routing,
                changed_by=changed_by,
            )
