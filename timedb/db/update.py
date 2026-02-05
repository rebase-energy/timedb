"""
Concurrency-safe tri-state update API for versioned time-series (overlapping mode).

Key conventions:
 - Updates only apply to overlapping series (flat are immutable facts).
 - At the Python API level, every updatable field is tri-state:
     Omit field => leave unchanged (field not provided)
     None      => explicitly set SQL NULL (clear)
     value     => set to that concrete value
 - No-op updates (canonicalized new == canonicalized current) are skipped.
 - Values must be in the canonical unit for the series.

Updates insert a new row with a new known_time (now()) into the same overlapping table.
The latest value is determined by ORDER BY known_time DESC.
"""
from __future__ import annotations

from typing import Any, Optional, List, Dict, Tuple
import uuid
import datetime as dt

import psycopg
from psycopg.rows import dict_row

_OVERLAPPING_TABLES = {
    "short":  "overlapping_short",
    "medium": "overlapping_medium",
    "long":   "overlapping_long",
}

# -----------------------------------------------------------------------------
# Sentinel for tri-state updates
# -----------------------------------------------------------------------------
_UNSET = object()


# -----------------------------------------------------------------------------
# Canonicalization helpers
# -----------------------------------------------------------------------------
def _normalize_tag(t: str) -> Optional[str]:
    """Normalize a single tag: strip whitespace, lowercase, drop empties."""
    if t is None:
        return None
    s = str(t).strip().lower()
    return s or None


def _canonicalize_tags_input(tags) -> Optional[List[str]]:
    if tags is None:
        return None
    if tags is _UNSET:
        return None
    if isinstance(tags, dict):
        seq = list(tags.keys())
    else:
        try:
            seq = list(tags)
        except TypeError:
            seq = [tags]
    if not seq:
        return None
    uniq = {_normalize_tag(x) for x in seq}
    uniq.discard(None)
    if not uniq:
        return None
    return sorted(uniq)


def _canonicalize_tags_stored(tags) -> Optional[List[str]]:
    if tags is None:
        return None
    seq = list(tags)
    if not seq:
        return None
    uniq = {_normalize_tag(x) for x in seq}
    uniq.discard(None)
    if not uniq:
        return None
    return sorted(uniq)


def _canonicalize_annotation_input(annotation) -> Optional[str]:
    if annotation is None:
        return None
    s = str(annotation).strip()
    return s or None


def _canonicalize_annotation_stored(annotation) -> Optional[str]:
    if annotation is None:
        return None
    s = str(annotation).strip()
    return s or None


def _validate_overlapping_series(conn: psycopg.Connection, series_id: uuid.UUID) -> str:
    """Verify that a series exists and is overlapping.

    Returns the target table name (e.g. 'overlapping_medium').
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT data_class, retention FROM series_table WHERE series_id = %s",
            (series_id,),
        )
        row = cur.fetchone()
    if row is None:
        raise ValueError(f"Series {series_id} not found")
    if row[0] != "overlapping":
        raise ValueError(
            f"Series {series_id} has data_class='{row[0]}'."
            f"Only overlapping support versioned updates."
        )
    table = _OVERLAPPING_TABLES.get(row[1])
    if table is None:
        raise ValueError(f"Unknown retention '{row[1]}' for series {series_id}")
    return table


# -----------------------------------------------------------------------------
# Update function (concurrency-safe)
# -----------------------------------------------------------------------------
def update_records(
    conn: psycopg.Connection,
    *,
    updates: List[Dict[str, Any]],
) -> Dict[str, List]:
    """
    Batch, concurrency-safe versioned updates for overlapping values.

    Updates insert a new row with a new known_time (now()) into the correct
    overlapping table (short/medium/long) based on the series' retention.
    The latest value is determined by ORDER BY known_time DESC.

    Args:
        conn: Database connection
        updates: List of update dictionaries. Each dictionary must contain EITHER:
            Option 1 (by overlapping_id):
            - overlapping_id (int): The overlapping_id of the row to update
            - series_id (uuid.UUID): Series identifier
            - value (float, optional): New value (omit to leave unchanged, None to clear)
            - annotation (str, optional): Annotation text (omit to leave unchanged, None to clear)
            - tags (list[str], optional): Tags (omit to leave unchanged, None to clear)
            - changed_by (str, optional): Who made the change

            Option 2 (by key):
            - batch_id (uuid.UUID): Batch identifier
            - tenant_id (uuid.UUID): Tenant identifier
            - valid_time (datetime): Time the value is valid for
            - series_id (uuid.UUID): Series identifier
            - value (float, optional): New value (omit to leave unchanged, None to clear)
            - annotation (str, optional): Annotation text (omit to leave unchanged, None to clear)
            - tags (list[str], optional): Tags (omit to leave unchanged, None to clear)
            - changed_by (str, optional): Who made the change

    Returns:
        Dictionary with keys:
            - 'updated': List of dicts with update info
            - 'skipped_no_ops': List of dicts for skipped no-op updates
    """
    if not updates:
        return {"updated": [], "skipped_no_ops": []}

    updated: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []

    # Cache series validation + table routing
    series_table_map: Dict[uuid.UUID, str] = {}

    with conn.transaction():
        with conn.cursor(row_factory=dict_row) as cur:
            for u in updates:
                series_id = u.get("series_id")
                if series_id is None:
                    raise ValueError("Each update must include 'series_id'.")
                if isinstance(series_id, str):
                    series_id = uuid.UUID(series_id)

                if series_id not in series_table_map:
                    table = _validate_overlapping_series(conn, series_id)
                    series_table_map[series_id] = table

                target_table = series_table_map[series_id]

                if "overlapping_id" in u:
                    _process_update_by_id(cur, target_table, u, updated, skipped)
                else:
                    _process_update_by_key(cur, target_table, u, updated, skipped)

    return {"updated": updated, "skipped_no_ops": skipped}


def _process_update_by_id(
    cur,
    table: str,
    u: Dict[str, Any],
    updated: List[Dict[str, Any]],
    skipped: List[Dict[str, Any]],
) -> None:
    """Process an update by overlapping_id."""
    overlapping_id = u["overlapping_id"]

    cur.execute(
        f"""
        SELECT overlapping_id, batch_id, tenant_id, valid_time, valid_time_end,
               series_id, value, known_time, annotation, metadata, tags, changed_by
        FROM {table}
        WHERE overlapping_id = %(overlapping_id)s
        """,
        {"overlapping_id": overlapping_id},
    )
    current = cur.fetchone()
    if current is None:
        raise ValueError(f"No row found with overlapping_id={overlapping_id} in {table}")

    # Resolve tri-state fields
    new_value = u.get("value", _UNSET)
    if new_value is _UNSET:
        new_value = current["value"]

    new_annotation = u.get("annotation", _UNSET)
    if new_annotation is _UNSET:
        new_annotation = current["annotation"]
    else:
        new_annotation = _canonicalize_annotation_input(new_annotation)

    new_tags = u.get("tags", _UNSET)
    if new_tags is _UNSET:
        new_tags = current["tags"]
    else:
        new_tags = _canonicalize_tags_input(new_tags)

    new_changed_by = u.get("changed_by", _UNSET)
    if new_changed_by is _UNSET:
        new_changed_by = current["changed_by"]

    # Check if anything actually changed
    has_any_update = "value" in u or "annotation" in u or "tags" in u or "changed_by" in u
    if not has_any_update:
        raise ValueError("No updates supplied: provide at least one of 'value', 'annotation', 'tags', 'changed_by'.")

    current_tags_canonical = _canonicalize_tags_stored(current["tags"])
    current_annotation_canonical = _canonicalize_annotation_stored(current["annotation"])

    if (new_value == current["value"]
            and new_annotation == current_annotation_canonical
            and new_tags == current_tags_canonical
            and new_changed_by == current["changed_by"]):
        skipped.append({"overlapping_id": overlapping_id})
        return

    cur.execute(
        f"""
        INSERT INTO {table} (
            batch_id, tenant_id, series_id, valid_time, valid_time_end,
            value, known_time, annotation, metadata, tags, changed_by
        )
        VALUES (
            %(batch_id)s, %(tenant_id)s, %(series_id)s, %(valid_time)s, %(valid_time_end)s,
            %(value)s, now(), %(annotation)s, %(metadata)s, %(tags)s, %(changed_by)s
        )
        RETURNING overlapping_id
        """,
        {
            "batch_id": current["batch_id"],
            "tenant_id": current["tenant_id"],
            "series_id": current["series_id"],
            "valid_time": current["valid_time"],
            "valid_time_end": current["valid_time_end"],
            "value": new_value,
            "annotation": new_annotation,
            "metadata": current["metadata"],
            "tags": new_tags,
            "changed_by": new_changed_by,
        },
    )
    new_id = cur.fetchone()["overlapping_id"]
    updated.append({
        "overlapping_id": new_id,
        "batch_id": current["batch_id"],
        "tenant_id": current["tenant_id"],
        "valid_time": current["valid_time"],
        "series_id": current["series_id"],
    })


def _process_update_by_key(
    cur,
    table: str,
    u: Dict[str, Any],
    updated: List[Dict[str, Any]],
    skipped: List[Dict[str, Any]],
) -> None:
    """Process an update by (batch_id, tenant_id, valid_time, series_id) key."""
    batch_id = u.get("batch_id")
    tenant_id = u.get("tenant_id")
    valid_time = u.get("valid_time")
    series_id = u.get("series_id")

    if batch_id is None or tenant_id is None or valid_time is None or series_id is None:
        raise ValueError("Each update must contain batch_id, tenant_id, valid_time, and series_id")

    if isinstance(batch_id, str):
        batch_id = uuid.UUID(batch_id)
    if isinstance(tenant_id, str):
        tenant_id = uuid.UUID(tenant_id)
    if isinstance(series_id, str):
        series_id = uuid.UUID(series_id)
    if isinstance(valid_time, str):
        valid_time = dt.datetime.fromisoformat(valid_time.replace('Z', '+00:00'))
    if valid_time.tzinfo is None:
        raise ValueError(f"valid_time must be timezone-aware (timestamptz). Bad update: {u}")

    has_any_update = "value" in u or "annotation" in u or "tags" in u or "changed_by" in u
    if not has_any_update:
        raise ValueError("No updates supplied: provide at least one of 'value', 'annotation', 'tags', 'changed_by'.")

    has_explicit_value = "value" in u

    cur.execute(
        f"""
        SELECT overlapping_id, value, valid_time_end, annotation, metadata, tags, changed_by
        FROM {table}
        WHERE batch_id = %(batch_id)s
          AND tenant_id = %(tenant_id)s
          AND valid_time = %(valid_time)s
          AND series_id = %(series_id)s
        ORDER BY known_time DESC
        LIMIT 1
        """,
        {
            "batch_id": batch_id,
            "tenant_id": tenant_id,
            "valid_time": valid_time,
            "series_id": series_id,
        },
    )
    current = cur.fetchone()

    if current is None and not has_explicit_value:
        raise ValueError(
            f"No current row exists for key in {table}. You must provide 'value'."
        )

    current_value = current["value"] if current else None
    valid_time_end = current["valid_time_end"] if current else None
    metadata = current["metadata"] if current else None
    current_annotation = current["annotation"] if current else None
    current_tags = current["tags"] if current else None
    current_changed_by = current["changed_by"] if current else None

    # Resolve tri-state fields
    new_value = u.get("value", _UNSET)
    if new_value is _UNSET:
        new_value = current_value

    new_annotation = u.get("annotation", _UNSET)
    if new_annotation is _UNSET:
        new_annotation = current_annotation
    else:
        new_annotation = _canonicalize_annotation_input(new_annotation)

    new_tags = u.get("tags", _UNSET)
    if new_tags is _UNSET:
        new_tags = current_tags
    else:
        new_tags = _canonicalize_tags_input(new_tags)

    new_changed_by = u.get("changed_by", _UNSET)
    if new_changed_by is _UNSET:
        new_changed_by = current_changed_by

    current_tags_canonical = _canonicalize_tags_stored(current_tags)
    current_annotation_canonical = _canonicalize_annotation_stored(current_annotation)

    if (new_value == current_value
            and new_annotation == current_annotation_canonical
            and new_tags == current_tags_canonical
            and new_changed_by == current_changed_by):
        skipped.append({
            "batch_id": batch_id,
            "tenant_id": tenant_id,
            "valid_time": valid_time,
            "series_id": series_id,
        })
        return

    cur.execute(
        f"""
        INSERT INTO {table} (
            batch_id, tenant_id, series_id, valid_time, valid_time_end,
            value, known_time, annotation, metadata, tags, changed_by
        )
        VALUES (
            %(batch_id)s, %(tenant_id)s, %(series_id)s, %(valid_time)s, %(valid_time_end)s,
            %(value)s, now(), %(annotation)s, %(metadata)s, %(tags)s, %(changed_by)s
        )
        RETURNING overlapping_id
        """,
        {
            "batch_id": batch_id,
            "tenant_id": tenant_id,
            "series_id": series_id,
            "valid_time": valid_time,
            "valid_time_end": valid_time_end,
            "value": new_value,
            "annotation": new_annotation,
            "metadata": metadata,
            "tags": new_tags,
            "changed_by": new_changed_by,
        },
    )
    new_id = cur.fetchone()["overlapping_id"]
    updated.append({
        "overlapping_id": new_id,
        "batch_id": batch_id,
        "tenant_id": tenant_id,
        "valid_time": valid_time,
        "series_id": series_id,
    })
