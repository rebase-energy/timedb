import uuid
import json
from typing import Optional, Iterable, Tuple, Dict
from datetime import datetime
import psycopg


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
) -> None:
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
                   Useful for backfill operations where data is inserted later.
        batch_params: Optional parameters/config used for this batch (e.g., model version, API args)
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
                ON CONFLICT (batch_id) DO NOTHING;
                """,
                (batch_id, tenant_id, workflow_id, batch_start_time, batch_finish_time, known_time, batch_params_json),
            )
        else:
            cur.execute(
                """
                INSERT INTO batches_table (
                    batch_id, tenant_id, workflow_id, 
                    batch_start_time, batch_finish_time, batch_params
                )
                VALUES (%s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (batch_id) DO NOTHING;
                """,
                (batch_id, tenant_id, workflow_id, batch_start_time, batch_finish_time, batch_params_json),
            )


def insert_values(
    conn: psycopg.Connection,
    *,
    batch_id: uuid.UUID,
    value_rows: Iterable[Tuple],
    changed_by: Optional[str] = None,
) -> None:
    """
    Bulk insert values for a batch.

    Accepts rows in either of two shapes:
      - (tenant_id, valid_time, series_id, value)                       # point-in-time
      - (tenant_id, valid_time, valid_time_end, series_id, value)       # interval

    The function will prepend the provided batch_id to each row
    and insert tuples of shape (batch_id, tenant_id, series_id, valid_time, valid_time_end, value).
    
    Note: tenant_id is included in each row to support both multi-tenant scenarios
    (where different rows may have different tenant_ids) and single-tenant scenarios
    (where all rows use the same default tenant_id).
    
    Note: Values must already be in the canonical unit for the series (unit conversion
    should happen before calling this function).
    
    Note: series_id is REQUIRED - it must exist in series_table before calling this function.
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

        rows_list.append((batch_id, tenant_id, valid_time, valid_time_end, series_id, value))

    if not rows_list:
        return

    with conn.cursor() as cur:
        # Insert with conflict check matching the unique index
        # The unique index is on (batch_id, tenant_id, valid_time, COALESCE(valid_time_end, valid_time), series_id) WHERE is_current
        cur.executemany(
            """
            INSERT INTO values_table (
                batch_id, tenant_id, series_id, valid_time, valid_time_end, value,
                changed_by, change_time, is_current
            )
            SELECT %s, %s, %s, %s, %s, %s, %s, now(), true
            WHERE NOT EXISTS (
                SELECT 1 FROM values_table
                WHERE batch_id = %s
                  AND tenant_id = %s
                  AND valid_time = %s
                  AND COALESCE(valid_time_end, valid_time) = COALESCE(%s, %s)
                  AND series_id = %s
                  AND is_current = true
            );
            """,
            [(r[0], r[1], r[4], r[2], r[3], r[5], changed_by, r[0], r[1], r[2], r[3], r[2], r[4]) for r in rows_list],
        )


def insert_batch_with_values(
    conninfo: str,
    *,
    batch_id: uuid.UUID,
    tenant_id: uuid.UUID,
    workflow_id: Optional[str] = None,
    batch_start_time: Optional[datetime] = None,
    batch_finish_time: Optional[datetime] = None,
    value_rows: Iterable[Tuple],  # accepts (tenant_id, valid_time, series_id, value) or (tenant_id, valid_time, valid_time_end, series_id, value)
    known_time: Optional[datetime] = None,
    batch_params: Optional[Dict] = None,
    changed_by: Optional[str] = None,
) -> None:
    """
    One-shot helper: inserts the batch + all values atomically.

    This function ensures atomicity: either all operations succeed and are committed,
    or all operations are rolled back if any error occurs. No partial writes are possible.

    value_rows is expected to be an iterable where each item is either:
      - (tenant_id, valid_time, series_id, value),                      # point-in-time
      - (tenant_id, valid_time, valid_time_end, series_id, value),      # interval

    Note: The batch's tenant_id parameter is used for the batches_table entry, but each value row
    can specify its own tenant_id. For single-tenant installations, all rows will typically
    use the same default tenant_id value.
    
    Note: Values must already be in the canonical unit for the series (unit conversion
    should happen before calling this function).
    
    Note: series_id is REQUIRED in each value row - it must exist in series_table.
    
    Args:
        batch_id: Unique identifier for the batch
        tenant_id: Tenant ID for the batch entry in batches_table (may differ from tenant_ids in value_rows)
        workflow_id: Optional workflow/pipeline identifier (NULL for manual insertions)
        batch_start_time: Optional start time of the batch
        batch_finish_time: Optional finish time of the batch
        value_rows: Iterable of value tuples (see above)
        known_time: Time of knowledge - when the data was known/available.
                   If not provided, defaults to now() in the database.
                   Useful for backfill operations where data is inserted later.
        batch_params: Optional parameters/config (e.g., model version, API args)
        changed_by: Optional user identifier who made the change
    
    Raises:
        Exception: Any exception raised during insertion will cause a complete rollback
                  of the transaction, ensuring no partial writes.
    """
    with psycopg.connect(conninfo) as conn:
        # Use transaction context manager to ensure atomicity
        with conn.transaction():
            insert_batch(
                conn,
                batch_id=batch_id,
                tenant_id=tenant_id,
                workflow_id=workflow_id,
                batch_start_time=batch_start_time,
                batch_finish_time=batch_finish_time,
                known_time=known_time,
                batch_params=batch_params,
            )

            insert_values(conn, batch_id=batch_id, value_rows=value_rows, changed_by=changed_by)
    
    print("Data values inserted successfully.")


# Backward compatibility aliases
insert_run = insert_batch
insert_run_with_values = insert_batch_with_values
