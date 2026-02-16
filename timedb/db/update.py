"""
Concurrency-safe tri-state update API for time-series data.

Key conventions:
 - Both flat and overlapping series can be updated.
 - Flat: in-place update (no versioning)
 - Overlapping: creates new row with known_time=now() (versioned)
 - Single-series only: all updates must be for the same series_id
 - At the Python API level, every updatable field is tri-state:
     Omit field => leave unchanged (field not provided)
     None      => explicitly set SQL NULL (clear)
     value     => set to that concrete value
 - Updates are always executed (no no-op detection)
 - Values must be in the canonical unit for the series.

For overlapping updates, the latest value is determined by ORDER BY known_time DESC.
"""
from __future__ import annotations

from typing import Any, Optional, List, Dict, Tuple
import datetime as dt

import psycopg
from psycopg.rows import dict_row

from .series import SeriesRegistry

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


def _canonicalize_tags(tags) -> Optional[List[str]]:
    """
    Canonicalize tags from any input format to sorted list or None.
    
    Handles None, _UNSET, dict, list, or single value.
    """
    if tags is None or tags is _UNSET:
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
    # Normalize and filter out None values
    normalized = [_normalize_tag(x) for x in seq]
    uniq = {tag for tag in normalized if tag is not None}
    if not uniq:
        return None
    return sorted(uniq)


def _canonicalize_annotation(annotation) -> Optional[str]:
    """
    Canonicalize annotation string.
    
    Returns None for None, _UNSET, empty string, or whitespace-only strings.
    """
    if annotation is None or annotation is _UNSET:
        return None
    s = str(annotation).strip()
    return s or None


# -----------------------------------------------------------------------------
# Update function (concurrency-safe)
# -----------------------------------------------------------------------------
def update_records(
    conn: psycopg.Connection,
    registry: SeriesRegistry,
    *,
    updates: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Batch, concurrency-safe updates for time-series values.
    
    **Single-series only**: All updates must be for the same series_id.

    - Flat series: in-place update (no versioning)
    - Overlapping series: inserts new row with known_time=now() (versioned)

    Args:
        conn: Database connection
        registry: SeriesRegistry for fetching series metadata
        updates: List of update dictionaries. All must have the same series_id.
        
            Required fields:
            - series_id (int): Series identifier (same for all updates)
            - valid_time (datetime): Time the value is valid for
            
            Optional fields (tri-state):
            - value (float): New value (omit to keep current)
            - annotation (str): Annotation text (None to clear)
            - tags (list[str]): Tags ([] to clear)
            - changed_by (str): Who made the change
            
            For overlapping series (version identification):
            - known_time (datetime): Exact version by known_time
            - batch_id (int): Latest version in that batch
            - (none): Latest version overall

    Returns:
        List of dicts with update info for each updated record
        
    Raises:
        ValueError: If updates list is empty, contains multiple series_ids, or series not found
    """
    if not updates:
        return []
    
    updated: List[Dict[str, Any]] = []
    
    # Extract and validate single series_id
    first_update = updates[0]
    series_id = first_update.get("series_id")
    if series_id is None:
        raise ValueError("Each update must include 'series_id'")
    if isinstance(series_id, str):
        series_id = int(series_id)
    
    # Validate all updates have the same series_id
    for u in updates:
        u_series_id = u.get("series_id")
        if u_series_id is None:
            raise ValueError("Each update must include 'series_id'")
        if isinstance(u_series_id, str):
            u_series_id = int(u_series_id)
        if u_series_id != series_id:
            raise ValueError(
                f"Single-series updates only. Found multiple series_ids: {series_id}, {u_series_id}"
            )
    
    # Fetch series metadata once (from registry cache or DB)
    info = registry.get(conn, series_id)
    
    is_overlapping = info["overlapping"]
    target_table = info["table"]
    
    # Process all updates (all for same series)
    with conn.transaction():
        with conn.cursor(row_factory=dict_row) as cur:
            for u in updates:
                if not is_overlapping:
                    _process_flat_update(cur, u, updated)
                else:
                    _process_overlapping_update(cur, target_table, u, updated)
    
    return updated


def _process_flat_update(
    cur,
    u: Dict[str, Any],
    updated: List[Dict[str, Any]],
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

    flat_id = current["flat_id"]

    # Resolve tri-state fields
    new_value = u.get("value", _UNSET)
    if new_value is _UNSET:
        new_value = current["value"]

    new_annotation = u.get("annotation", _UNSET)
    if new_annotation is _UNSET:
        new_annotation = current["annotation"]
    else:
        new_annotation = _canonicalize_annotation(new_annotation)

    new_tags = u.get("tags", _UNSET)
    if new_tags is _UNSET:
        new_tags = current["tags"]
    else:
        new_tags = _canonicalize_tags(new_tags)

    new_changed_by = u.get("changed_by", _UNSET)
    if new_changed_by is _UNSET:
        new_changed_by = current["changed_by"]

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


def _process_overlapping_update(
    cur,
    table: str,
    u: Dict[str, Any],
    updated: List[Dict[str, Any]],
) -> None:
    """Process an update for an overlapping series (versioned with known_time).

    Flexible lookup priority:
    1. batch_id + valid_time: Latest version in that batch
    2. known_time + valid_time: Exact version lookup
    3. Just valid_time: Latest version overall (latest known_time, most recent batch)
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
    if batch_id is not None:
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
    elif known_time is not None:
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
    else:
        # Latest version overall (latest known_time, most recent batch)
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
        new_annotation = _canonicalize_annotation(new_annotation)

    new_tags = u.get("tags", _UNSET)
    if new_tags is _UNSET:
        new_tags = current_tags
    else:
        new_tags = _canonicalize_tags(new_tags)

    new_changed_by = u.get("changed_by", _UNSET)
    if new_changed_by is _UNSET:
        new_changed_by = current_changed_by

    # Insert new version (overlapping series are always versioned)
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
