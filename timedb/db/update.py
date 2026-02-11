"""
Concurrency-safe tri-state update API for time-series data.

Key conventions:
 - Both flat and overlapping series can be updated.
 - Flat: in-place update (no versioning)
 - Overlapping: creates new row with known_time=now() (versioned)
 - At the Python API level, every updatable field is tri-state:
     Omit field => leave unchanged (field not provided)
     None      => explicitly set SQL NULL (clear)
     value     => set to that concrete value
 - No-op updates (canonicalized new == canonicalized current) are skipped.
 - Values must be in the canonical unit for the series.

For overlapping updates, the latest value is determined by ORDER BY known_time DESC.
"""
from __future__ import annotations

from typing import Any, Optional, List, Dict, Tuple
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


def _get_series_routing(conn: psycopg.Connection, series_id: int) -> Tuple[str, Optional[str]]:
    """Get routing info for a series.

    Returns:
        Tuple of (data_class, table_name).
        - For flat: ("flat", "flat")
        - For overlapping: ("overlapping", "overlapping_medium") etc.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT data_class, retention FROM series_table WHERE series_id = %s",
            (series_id,),
        )
        row = cur.fetchone()
    if row is None:
        raise ValueError(f"Series {series_id} not found")

    data_class = row[0]
    if data_class == "flat":
        return ("flat", "flat")
    elif data_class == "overlapping":
        table = _OVERLAPPING_TABLES.get(row[1])
        if table is None:
            raise ValueError(f"Unknown retention '{row[1]}' for series {series_id}")
        return ("overlapping", table)
    else:
        raise ValueError(f"Unknown data_class '{data_class}' for series {series_id}")


# -----------------------------------------------------------------------------
# Update function (concurrency-safe)
# -----------------------------------------------------------------------------
def update_records(
    conn: psycopg.Connection,
    *,
    updates: List[Dict[str, Any]],
) -> Dict[str, List]:
    """
    Batch, concurrency-safe updates for time-series values.

    - Flat series: in-place update (no versioning)
    - Overlapping series: inserts new row with known_time=now() (versioned)

    Args:
        conn: Database connection
        updates: List of update dictionaries. Required fields vary by data_class:

            For flat series:
            - series_id (int): Series identifier
            - valid_time (datetime): Time the value is valid for
            - value (float, optional): New value
            - annotation (str, optional): Annotation text
            - tags (list[str], optional): Tags
            - changed_by (str, optional): Who made the change

            For overlapping series (flexible lookup):
            - series_id (int): Series identifier
            - valid_time (datetime): Time the value is valid for
            - Plus ONE of these to identify the version:
              - overlapping_id (int): Direct row lookup (fastest)
              - known_time (datetime): Exact version by known_time
              - batch_id (int): Latest version in that batch
              - (none): Latest version overall
            - value (float, optional): New value
            - annotation (str, optional): Annotation text
            - tags (list[str], optional): Tags
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

    # Cache series routing: series_id -> (data_class, table_name)
    series_routing_map: Dict[int, Tuple[str, str]] = {}

    with conn.transaction():
        with conn.cursor(row_factory=dict_row) as cur:
            for u in updates:
                series_id = u.get("series_id")
                if series_id is None:
                    raise ValueError("Each update must include 'series_id'.")
                if isinstance(series_id, str):
                    series_id = int(series_id)

                if series_id not in series_routing_map:
                    routing = _get_series_routing(conn, series_id)
                    series_routing_map[series_id] = routing

                data_class, target_table = series_routing_map[series_id]

                if data_class == "flat":
                    _process_flat_update(cur, u, updated, skipped)
                elif "overlapping_id" in u:
                    _process_update_by_id(cur, target_table, u, updated, skipped)
                else:
                    _process_update_by_key(cur, target_table, u, updated, skipped)

    return {"updated": updated, "skipped_no_ops": skipped}


def _process_flat_update(
    cur,
    u: Dict[str, Any],
    updated: List[Dict[str, Any]],
    skipped: List[Dict[str, Any]],
) -> None:
    """Process an update for a flat series (in-place, no versioning)."""
    valid_time = u.get("valid_time")
    series_id = u.get("series_id")

    if valid_time is None or series_id is None:
        raise ValueError("Flat updates require 'valid_time' and 'series_id'")
    if isinstance(series_id, str):
        series_id = int(series_id)
    if isinstance(valid_time, str):
        valid_time = dt.datetime.fromisoformat(valid_time.replace('Z', '+00:00'))
    if valid_time.tzinfo is None:
        raise ValueError(f"valid_time must be timezone-aware (timestamptz). Bad update: {u}")

    has_any_update = "value" in u or "annotation" in u or "tags" in u or "changed_by" in u
    if not has_any_update:
        raise ValueError("No updates supplied: provide at least one of 'value', 'annotation', 'tags', 'changed_by'.")

    # Lookup existing row
    cur.execute(
        """
        SELECT flat_id, value, annotation, tags, changed_by
        FROM flat
        WHERE series_id = %(series_id)s
          AND valid_time = %(valid_time)s
        """,
        {
            "series_id": series_id,
            "valid_time": valid_time,
        },
    )
    current = cur.fetchone()

    if current is None:
        raise ValueError(
            f"No flat row exists for (series_id={series_id}, valid_time={valid_time}). "
            f"Use insert() to create new data."
        )

    current_value = current["value"]
    current_annotation = current["annotation"]
    current_tags = current["tags"]
    current_changed_by = current["changed_by"]
    flat_id = current["flat_id"]

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

    # Check for no-op
    if (new_value == current_value
            and new_annotation == current_annotation_canonical
            and new_tags == current_tags_canonical
            and new_changed_by == current_changed_by):
        skipped.append({
            "flat_id": flat_id,
            "series_id": series_id,
            "valid_time": valid_time,
        })
        return

    # In-place update (no versioning for flat)
    cur.execute(
        """
        UPDATE flat
        SET value = %(value)s,
            annotation = %(annotation)s,
            tags = %(tags)s,
            changed_by = %(changed_by)s,
            change_time = now()
        WHERE flat_id = %(flat_id)s
        """,
        {
            "flat_id": flat_id,
            "value": new_value,
            "annotation": new_annotation,
            "tags": new_tags,
            "changed_by": new_changed_by,
        },
    )
    updated.append({
        "flat_id": flat_id,
        "series_id": series_id,
        "valid_time": valid_time,
    })


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
        SELECT overlapping_id, batch_id, valid_time, valid_time_end,
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
            batch_id, series_id, valid_time, valid_time_end,
            value, known_time, annotation, metadata, tags, changed_by
        )
        VALUES (
            %(batch_id)s, %(series_id)s, %(valid_time)s, %(valid_time_end)s,
            %(value)s, now(), %(annotation)s, %(metadata)s, %(tags)s, %(changed_by)s
        )
        RETURNING overlapping_id
        """,
        {
            "batch_id": current["batch_id"],
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
    """Process an overlapping update with flexible lookup.

    Lookup priority:
    1. known_time + valid_time: Exact version lookup
    2. batch_id + valid_time: Latest version in that batch
    3. Just valid_time: Latest version overall
    """
    valid_time = u.get("valid_time")
    series_id = u.get("series_id")
    batch_id = u.get("batch_id")
    known_time = u.get("known_time")

    if valid_time is None or series_id is None:
        raise ValueError("Overlapping updates require 'valid_time' and 'series_id'")
    if isinstance(series_id, str):
        series_id = int(series_id)
    if isinstance(valid_time, str):
        valid_time = dt.datetime.fromisoformat(valid_time.replace('Z', '+00:00'))
    if valid_time.tzinfo is None:
        raise ValueError(f"valid_time must be timezone-aware (timestamptz). Bad update: {u}")

    if batch_id is not None and isinstance(batch_id, str):
        batch_id = int(batch_id)
    if known_time is not None:
        if isinstance(known_time, str):
            known_time = dt.datetime.fromisoformat(known_time.replace('Z', '+00:00'))
        if known_time.tzinfo is None:
            raise ValueError(f"known_time must be timezone-aware (timestamptz). Bad update: {u}")

    has_any_update = "value" in u or "annotation" in u or "tags" in u or "changed_by" in u
    if not has_any_update:
        raise ValueError("No updates supplied: provide at least one of 'value', 'annotation', 'tags', 'changed_by'.")

    has_explicit_value = "value" in u

    # Flexible lookup based on what identifiers are provided
    if known_time is not None:
        # Exact version lookup by known_time
        cur.execute(
            f"""
            SELECT overlapping_id, batch_id, value, valid_time_end, annotation, metadata, tags, changed_by
            FROM {table}
            WHERE series_id = %(series_id)s
              AND valid_time = %(valid_time)s
              AND known_time = %(known_time)s
            """,
            {
                "series_id": series_id,
                "valid_time": valid_time,
                "known_time": known_time,
            },
        )
    elif batch_id is not None:
        # Latest version in that batch
        cur.execute(
            f"""
            SELECT overlapping_id, batch_id, value, valid_time_end, annotation, metadata, tags, changed_by
            FROM {table}
            WHERE series_id = %(series_id)s
              AND valid_time = %(valid_time)s
              AND batch_id = %(batch_id)s
            ORDER BY known_time DESC
            LIMIT 1
            """,
            {
                "series_id": series_id,
                "valid_time": valid_time,
                "batch_id": batch_id,
            },
        )
    else:
        # Latest version overall
        cur.execute(
            f"""
            SELECT overlapping_id, batch_id, value, valid_time_end, annotation, metadata, tags, changed_by
            FROM {table}
            WHERE series_id = %(series_id)s
              AND valid_time = %(valid_time)s
            ORDER BY known_time DESC
            LIMIT 1
            """,
            {
                "series_id": series_id,
                "valid_time": valid_time,
            },
        )
    current = cur.fetchone()

    if current is None and not has_explicit_value:
        raise ValueError(
            f"No current row exists for key in {table}. You must provide 'value'."
        )

    # Get batch_id from current row if not provided
    if batch_id is None and current is not None:
        batch_id = current["batch_id"]

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
            "valid_time": valid_time,
            "series_id": series_id,
        })
        return

    cur.execute(
        f"""
        INSERT INTO {table} (
            batch_id, series_id, valid_time, valid_time_end,
            value, known_time, annotation, metadata, tags, changed_by
        )
        VALUES (
            %(batch_id)s, %(series_id)s, %(valid_time)s, %(valid_time_end)s,
            %(value)s, now(), %(annotation)s, %(metadata)s, %(tags)s, %(changed_by)s
        )
        RETURNING overlapping_id
        """,
        {
            "batch_id": batch_id,
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
        "valid_time": valid_time,
        "series_id": series_id,
    })
