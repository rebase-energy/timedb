# pg_insert_table_with_metadata.py
import psycopg
import json
from typing import Optional, Iterable, Tuple, Any, Dict
from datetime import datetime

from pg_insert_table import insert_run, insert_values


def _choose_typed_slots(val: Any):
    """
    Return a 5-tuple (value_number, value_string, value_bool, value_time, value_json)
    with exactly one non-None element chosen by Python type heuristics.
    Minimal rules:
      - bool -> value_bool
      - int/float (but not bool) -> value_number (float)
      - datetime -> value_time
      - dict/list -> value_json
      - str -> try ISO datetime parse (supports trailing 'Z'), else value_string
      - fallback -> value_string(str(val))
    """
    v_num = v_str = v_bool = v_time = v_json = None

    if val is None:
        # skip / treat as string 'null' would violate CHECK, so caller should not pass None
        v_str = None
        return v_num, v_str, v_bool, v_time, v_json

    if isinstance(val, bool):
        v_bool = val
        return v_num, v_str, v_bool, v_time, v_json

    if isinstance(val, (int, float)):
        v_num = float(val)
        return v_num, v_str, v_bool, v_time, v_json

    if isinstance(val, datetime):
        if val.tzinfo is None:
            raise ValueError("datetime must be timezone-aware (timestamptz)")
        v_time = val
        return v_num, v_str, v_bool, v_time, v_json

    if isinstance(val, (dict, list)):
        v_json = val # Or json.dumps(val) to do the serialisation before psycopg
        return v_num, v_str, v_bool, v_time, v_json

    if isinstance(val, str):
        s = val
        # accept trailing 'Z' as UTC for fromisoformat
        if s.endswith("Z"):
            s2 = s[:-1] + "+00:00"
        else:
            s2 = s
        try:
            parsed = datetime.fromisoformat(s2)
            if parsed.tzinfo is None:
                raise ValueError("datetime must be timezone-aware (timestamptz)")
            v_time = parsed
            return v_num, v_str, v_bool, v_time, v_json
        except Exception:
            v_str = s
            return v_num, v_str, v_bool, v_time, v_json

    # fallback
    v_str = str(val)
    return v_num, v_str, v_bool, v_time, v_json


def insert_metadata(
    conn: psycopg.Connection,
    run_id,
    metadata_rows: Optional[Iterable[Tuple[datetime, dict]]] = None,
) -> None:

    if metadata_rows is None:
        metadata_rows = []

    # prepare metadata insert tuples:
    # (run_id, valid_time, metadata_key, value_number, value_string, value_bool, value_time, value_json)
    meta_inserts = []
    for valid_time, meta_dict in metadata_rows:
        if isinstance(valid_time, datetime) and valid_time.tzinfo is None:
            raise ValueError("valid_time (metadata context) must be timezone-aware (timestamptz)")
        if not meta_dict:
            continue
        for metadata_key, meta_val in meta_dict.items():
            v_num, v_str, v_bool, v_time, v_json = _choose_typed_slots(meta_val)
            # Only insert if we chose a typed slot (this also prevents violating the CHECK)
            # (If all None, we still insert a NULL-string row? Here we skip)
            if v_num is None and v_str is None and v_bool is None and v_time is None and v_json is None:
                continue
            meta_inserts.append(
                (run_id, valid_time, metadata_key, v_num, v_str, v_bool, v_time, v_json)
            )

    # upsert metadata into metadata_table
    if meta_inserts:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO metadata_table (
                    run_id, valid_time, metadata_key,
                    value_number, value_string, value_bool, value_time, value_json,
                    inserted_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (run_id, valid_time, metadata_key) DO UPDATE
                    SET value_number = EXCLUDED.value_number,
                        value_string = EXCLUDED.value_string,
                        value_bool   = EXCLUDED.value_bool,
                        value_time   = EXCLUDED.value_time,
                        value_json   = EXCLUDED.value_json,
                        inserted_at  = now();
                """,
                meta_inserts,
            )

def insert_run_with_values_and_metadata(
    conninfo: str,
    *,
    run_id,
    workflow_id: str,
    run_start_time: datetime,
    run_finish_time: Optional[datetime],
    value_rows: Iterable[Tuple[datetime, str, Optional[float]]],
    run_params: Optional[Dict] = None,
    metadata_rows: Optional[Iterable[Tuple[datetime, dict]]] = None,
) -> None:
    """
    Atomically insert run, values and metadata.

    - value_rows: iterable of (valid_time, value_key, value)
    - metadata_rows: iterable of (valid_time, {metadata_key: value, ...})
    """

    with psycopg.connect(conninfo) as conn:
        with conn.transaction():
            # insert run (uses pg_insert_table.insert_run)
            insert_run(
                conn,
                run_id=run_id,
                workflow_id=workflow_id,
                run_start_time=run_start_time,
                run_finish_time=run_finish_time,
                run_params=run_params,
            )

            # insert values (uses pg_insert_table.insert_values)
            insert_values(conn, run_id=run_id, value_rows=value_rows)

            insert_metadata(conn, run_id=run_id, metadata_rows=metadata_rows)

    print("Inserted run + values + metadata (atomic).")
