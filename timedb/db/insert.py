import uuid
import json
from typing import Optional, Sequence, Iterable, Tuple, Dict
from datetime import datetime
import psycopg


def insert_run(
    conn: psycopg.Connection,
    *,
    run_id: uuid.UUID,
    tenant_id: uuid.UUID,
    workflow_id: str,
    run_start_time: datetime,
    run_finish_time: Optional[datetime] = None,
    known_time: Optional[datetime] = None,
    run_params: Optional[Dict] = None,
) -> None:
    """
    Insert one forecast run into runs_table.

    Uses ON CONFLICT so retries are safe.
    
    Args:
        known_time: Time of knowledge - when the data was known/available.
                   If not provided, defaults to inserted_at (now()) in the database.
                   Useful for backfill operations where data is inserted later.
    """

    run_params = json.dumps(run_params) if run_params is not None else None

    if run_start_time.tzinfo is None:
        raise ValueError("run_start_time must be timezone-aware")
    if run_finish_time is not None and run_finish_time.tzinfo is None:
        raise ValueError("run_finish_time must be timezone-aware")
    if known_time is not None and known_time.tzinfo is None:
        raise ValueError("known_time must be timezone-aware")

    with conn.cursor() as cur:
        if known_time is not None:
            # If known_time is provided, include it in the INSERT
            cur.execute(
                """
                INSERT INTO runs_table (run_id, tenant_id, workflow_id, run_start_time, run_finish_time, known_time, run_params)
                VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (run_id) DO NOTHING;
                """,
                (run_id, tenant_id, workflow_id, run_start_time, run_finish_time, known_time, run_params),
            )
        else:
            # If known_time is not provided, let the database default to inserted_at (now())
            cur.execute(
                """
                INSERT INTO runs_table (run_id, tenant_id, workflow_id, run_start_time, run_finish_time, run_params)
                VALUES (%s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (run_id) DO NOTHING;
                """,
                (run_id, tenant_id, workflow_id, run_start_time, run_finish_time, run_params),
            )


def insert_values(
    conn: psycopg.Connection,
    *,
    run_id: uuid.UUID,
    value_rows: Iterable[Tuple],
    changed_by: Optional[str] = None,
) -> None:
    """
    Bulk insert forecast values for a run.

    Accepts rows in either of two shapes:
      - (tenant_id, valid_time, series_id, value)                       # point-in-time
      - (tenant_id, valid_time, valid_time_end, series_id, value)       # interval

    The function will prepend the provided run_id to each row
    and insert tuples of shape (run_id, tenant_id, series_id, valid_time, valid_time_end, value).
    
    Note: tenant_id is included in each row to support both multi-tenant scenarios
    (where different rows may have different tenant_ids) and single-tenant scenarios
    (where all rows use the same default tenant_id).
    
    Note: Values must already be in the canonical unit for the series (unit conversion
    should happen before calling this function).
    """

    rows_list = []
    for item in value_rows:
        # either 4-tuple (point-in-time) or 5-tuple (interval)
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

        # Basic checks
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

        rows_list.append((run_id, tenant_id, valid_time, valid_time_end, series_id, value))

    if not rows_list:
        return

    with conn.cursor() as cur:
        # Insert with conflict check matching the unique index
        # The unique index is on (run_id, tenant_id, valid_time, COALESCE(valid_time_end, valid_time), series_id) WHERE is_current
        # We use WHERE NOT EXISTS to prevent duplicates, matching the index logic 
        # It mirrors the index's uniqueness rule to avoid constraint violations.
        cur.executemany(
            """
            INSERT INTO values_table (
                run_id, tenant_id, series_id, valid_time, valid_time_end, value,
                changed_by, change_time, is_current
            )
            SELECT %s, %s, %s, %s, %s, %s, %s, now(), true
            WHERE NOT EXISTS (
                SELECT 1 FROM values_table
                WHERE run_id = %s
                  AND tenant_id = %s
                  AND valid_time = %s
                  AND COALESCE(valid_time_end, valid_time) = COALESCE(%s, %s)
                  AND series_id = %s
                  AND is_current = true
            );
            """,
            [(r[0], r[1], r[4], r[2], r[3], r[5], changed_by, r[0], r[1], r[2], r[3], r[2], r[4]) for r in rows_list],
        )


def insert_run_with_values(
    conninfo: str,
    *,
    run_id: uuid.UUID,
    tenant_id: uuid.UUID,
    workflow_id: str,
    run_start_time: datetime,
    run_finish_time: Optional[datetime],
    value_rows: Iterable[Tuple],  # accepts (tenant_id, valid_time, series_id, value) or (tenant_id, valid_time, valid_time_end, series_id, value)
    known_time: Optional[datetime] = None,
    run_params: Optional[Dict] = None,
    changed_by: Optional[str] = None,
) -> None:
    """
    One-shot helper: inserts the run + all values atomically.

    This function ensures atomicity: either all operations succeed and are committed,
    or all operations are rolled back if any error occurs. No partial writes are possible.

    value_rows is expected to be an iterable where each item is either:
      - (tenant_id, valid_time, series_id, value),                      # point-in-time
      - (tenant_id, valid_time, valid_time_end, series_id, value),      # interval

    Note: The run's tenant_id parameter is used for the runs_table entry, but each value row
    can specify its own tenant_id. For single-tenant installations, all rows will typically
    use the same default tenant_id value.
    
    Note: Values must already be in the canonical unit for the series (unit conversion
    should happen before calling this function).
    
    Args:
        tenant_id: Tenant ID for the run entry in runs_table (may differ from tenant_ids in value_rows)
        known_time: Time of knowledge - when the data was known/available.
                   If not provided, defaults to inserted_at (now()) in the database.
                   Useful for backfill operations where data is inserted later.
    
    Raises:
        Exception: Any exception raised during insertion will cause a complete rollback
                  of the transaction, ensuring no partial writes.
    """

    with psycopg.connect(conninfo) as conn:
        # Use transaction context manager to ensure atomicity
        # If any exception occurs, the transaction will automatically rollback
        # This ensures either all operations succeed or none do (no partial writes)
        with conn.transaction():
            insert_run(
                conn,
                run_id=run_id,
                tenant_id=tenant_id,
                workflow_id=workflow_id,
                run_start_time=run_start_time,
                run_finish_time=run_finish_time,
                known_time=known_time,
                run_params=run_params,
            )

            insert_values(conn, run_id=run_id, value_rows=value_rows, changed_by=changed_by)
    
    print("Data values inserted successfully.")
