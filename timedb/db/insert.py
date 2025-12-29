import uuid
import json
from typing import Optional, Sequence, Iterable, Tuple, Dict
from datetime import datetime
import psycopg


def insert_run(
    conn: psycopg.Connection,
    *,
    run_id: uuid.UUID,
    workflow_id: str,
    run_start_time: datetime,
    run_finish_time: Optional[datetime] = None,
    run_params: Optional[Dict] = None,
) -> None:
    """
    Insert one forecast run into runs_table.

    Uses ON CONFLICT so retries are safe.
    """

    run_params = json.dumps(run_params) if run_params is not None else None


    if run_start_time.tzinfo is None:
        raise ValueError("run_start_time must be timezone-aware")
    if run_finish_time is not None and run_finish_time.tzinfo is None:
        raise ValueError("run_finish_time must be timezone-aware")

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO runs_table (run_id, workflow_id, run_start_time, run_finish_time, run_params)
            VALUES (%s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (run_id) DO NOTHING;
            """,
            (run_id, workflow_id, run_start_time, run_finish_time, run_params),
        )


def insert_values(
    conn: psycopg.Connection,
    *,
    run_id: uuid.UUID,
    value_rows: Iterable[Tuple[datetime, str, Optional[float]]],
) -> None:
    """
    Bulk insert forecast values for a run.

    Accepts rows in either of two shapes:
      - (valid_time, value_key, value)                       # point-in-time
      - (valid_time, valid_time_end, value_key, value)       # interval

    The function will prepend the provided run_id to each row and insert tuples of
    shape (run_id, valid_time, valid_time_end, value_key, value).
    """

    rows_list = []
    for item in value_rows:
        # back-compat: either 3-tuple or 4-tuple
        if len(item) == 3:
            valid_time, value_key, value = item
            valid_time_end = None
        elif len(item) == 4:
            valid_time, valid_time_end, value_key, value = item
        else:
            raise ValueError(
                "Each value row must be either "
                "(valid_time, value_key, value) or "
                "(valid_time, valid_time_end, value_key, value)"
            )

        # Basic checks
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

        rows_list.append((run_id, valid_time, valid_time_end, value_key, value))

    if not rows_list:
        return

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO values_table (
                run_id, valid_time, valid_time_end, value_key, value,
                change_time, is_current
            )
            VALUES (%s, %s, %s, %s, %s, now(), true)
            ON CONFLICT DO NOTHING;
            """,
            rows_list,
        )


def insert_run_with_values(
    conninfo: str,
    *,
    run_id: uuid.UUID,
    workflow_id: str,
    run_start_time: datetime,
    run_finish_time: Optional[datetime],
    value_rows: Iterable[Tuple],  # accepts (valid_time, value_key, value) or (valid_time, valid_time_end, value_key, value)
    run_params: Optional[Dict] = None,
) -> None:
    """
    One-shot helper: inserts the run + all values atomically.

    value_rows is expected to be an iterable where each item is either:
      - (valid_time, value_key, value),                      # point-in-time
      - (valid_time, valid_time_end, value_key, value),      # interval

    This function will prepend the provided run_id to each row and call insert_values,
    inserting tuples of shape (run_id, valid_time, valid_time_end, value_key, value).
    """

    with psycopg.connect(conninfo) as conn:
        with conn.transaction():
            insert_run(
                conn,
                run_id=run_id,
                workflow_id=workflow_id,
                run_start_time=run_start_time,
                run_finish_time=run_finish_time,
                run_params=run_params,
            )

            insert_values(conn, run_id, value_rows)
    
    print("Data values inserted successfully.")
