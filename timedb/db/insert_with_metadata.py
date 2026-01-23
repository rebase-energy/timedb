# pg_insert_table_with_metadata.py
import psycopg
import json
from typing import Optional, Iterable, Tuple, Any, Dict
from datetime import datetime
from importlib import resources

from .insert import insert_batch, insert_values

# Read packaged SQL
SQL_INSERT_METADATA = resources.files("timedb").joinpath("sql", "pg_insert_table_with_metadata.sql").read_text(encoding="utf-8")


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
    batch_id,
    tenant_id,
    metadata_rows: Optional[Iterable[Tuple[datetime, dict]]] = None,
) -> None:
    """
    Insert metadata for a run.
    
    Metadata is tenant-specific, so tenant_id is required to support different
    metadata values for different tenants at the same (batch_id, valid_time).
    """

    if metadata_rows is None:
        metadata_rows = []

    # prepare metadata insert tuples:
    # (batch_id, tenant_id, valid_time, metadata_key, value_number, value_string, value_bool, value_time, value_json)
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
                (batch_id, tenant_id, valid_time, metadata_key, v_num, v_str, v_bool, v_time, v_json)
            )

    # upsert metadata into metadata_table
    if meta_inserts:
        with conn.cursor() as cur:
            cur.executemany(
                SQL_INSERT_METADATA,
                meta_inserts,
            )

def insert_batch_with_values_and_metadata(
    conninfo: str,
    *,
    batch_id,
    tenant_id,
    workflow_id: str,
    batch_start_time: datetime,
    batch_finish_time: Optional[datetime],
    value_rows: Iterable[Tuple],  # accepts (tenant_id, valid_time, series_id, value) or (tenant_id, valid_time, valid_time_end, series_id, value)
    known_time: Optional[datetime] = None,
    batch_params: Optional[Dict] = None,
    metadata_rows: Optional[Iterable[Tuple[datetime, dict]]] = None,
) -> None:
    """
    Atomically insert batch, values and metadata.

    This function ensures atomicity: either all operations succeed and are committed,
    or all operations are rolled back if any error occurs. No partial writes are possible.

    - value_rows: iterable of (tenant_id, valid_time, series_id, value) or (tenant_id, valid_time, valid_time_end, series_id, value)
    - metadata_rows: iterable of (valid_time, {metadata_key: value, ...})
    
    Note: metadata is tenant-specific. The tenant_id parameter is used for metadata insertion.
    If value_rows contain multiple tenant_ids, you may need to call insert_metadata separately
    for each tenant, or pass the appropriate tenant_id for the metadata context.
    
    Args:
        tenant_id: Tenant ID for the run entry in runs_table and for metadata insertion
                  (may differ from tenant_ids in value_rows)
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
            # insert run (uses db.insert.insert_batch)
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

            # insert values (uses db.insert.insert_values)
            insert_values(conn, batch_id=batch_id, value_rows=value_rows)

            insert_metadata(conn, batch_id=batch_id, tenant_id=tenant_id, metadata_rows=metadata_rows)

    print("Inserted batch + values + metadata (atomic).")


# Backward compatibility alias
insert_run_with_values_and_metadata = insert_batch_with_values_and_metadata
