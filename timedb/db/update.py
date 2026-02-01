"""
Concurrency-safe tri-state update API for versioned time-series.

Key conventions:
 - At the Python API level, every updatable field is tri-state:
     Omit field => leave unchanged (field not provided)
     None      => explicitly set SQL NULL (clear)
     value     => set to that concrete value
 - Canonicalization:
     annotation: empty or whitespace-only -> SQL NULL
     tags: empty iterable or tags that normalize away -> SQL NULL
     tags normalized -> strip/lower/dedupe/sort (treated as a set)
 - No-op updates (canonicalized new == canonicalized current) are skipped.
 - Values must be in the canonical unit for the series (unit conversion should happen before calling update_records).

TimescaleDB version: Updates insert a new row with a new known_time (now()).
The latest value is determined by ORDER BY known_time DESC, not by is_current flag.
"""
from __future__ import annotations

from typing import Any, Optional, List, Dict, Tuple
import uuid
import datetime as dt

import psycopg
from psycopg.rows import dict_row

# -----------------------------------------------------------------------------
# Sentinel for tri-state updates
# -----------------------------------------------------------------------------
_UNSET = object()  # sentinel to mean "field not provided / leave unchanged"


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
    """
    Canonicalize tags provided by caller.
      - _UNSET => should not be passed here (handled by caller)
      - None   => explicit clear -> return None
      - empty iterable -> explicit clear -> return None
      - otherwise normalize, dedupe, sort -> list[str]
    Returns:
      - None => canonical "no tags" (SQL NULL)
      - list  => canonical non-empty sorted list of tags
    """
    if tags is None:
        return None
    if tags is _UNSET:
        # defensive: caller should not call this with _UNSET
        return None

    # Accept dict (use keys), list/tuple/set, etc. Scalars also accepted.
    if isinstance(tags, dict):
        seq = list(tags.keys())
    else:
        try:
            seq = list(tags)
        except TypeError:
            # single scalar -> treat as single tag
            seq = [tags]

    if not seq:
        return None

    uniq = {_normalize_tag(x) for x in seq}
    uniq.discard(None)
    if not uniq:
        return None
    return sorted(uniq)


def _canonicalize_tags_stored(tags) -> Optional[List[str]]:
    """
    Canonicalize tags read from DB (defensive).
    DB should store either NULL or a non-empty text[] (constraint prevents empty arrays).
    """
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
    """
    Canonicalize annotation provided by caller:
      - None => explicit clear -> None
      - empty/whitespace-only => canonical None
      - otherwise strip() and return non-empty string
    """
    if annotation is None:
        return None
    s = str(annotation).strip()
    return s or None


def _canonicalize_annotation_stored(annotation) -> Optional[str]:
    """Canonicalize annotation read from DB (defensive)."""
    if annotation is None:
        return None
    s = str(annotation).strip()
    return s or None


# -----------------------------------------------------------------------------
# Update function (concurrency-safe)
# -----------------------------------------------------------------------------
def update_records(
    conn: psycopg.Connection,
    *,
    updates: List[Dict[str, Any]],
) -> Dict[str, List]:
    """
    Batch, concurrency-safe versioned updates for values_table.

    In the TimescaleDB schema, updates insert a new row with a new known_time (now()).
    The latest value is determined by ORDER BY known_time DESC.

    Args:
        conn: Database connection
        updates: List of update dictionaries. Each dictionary must contain EITHER:
            Option 1 (by value_id - simplest, recommended):
            - value_id (int): The value_id of the row to update
            - value (float, optional): New value (omit to leave unchanged, None to clear)
            - annotation (str, optional): New annotation (omit to leave unchanged, None to clear)
            - tags (list[str], optional): New tags (omit to leave unchanged, None or [] to clear)
            - changed_by (str, optional): Who made the change

            Option 2 (by key - for backwards compatibility):
            - batch_id (uuid.UUID): Run identifier
            - tenant_id (uuid.UUID): Tenant identifier
            - valid_time (datetime): Time the value is valid for (must be timezone-aware)
            - series_id (uuid.UUID): Series identifier
            - value (float, optional): New value (omit to leave unchanged, None to clear)
            - annotation (str, optional): New annotation (omit to leave unchanged, None to clear)
            - tags (list[str], optional): New tags (omit to leave unchanged, None or [] to clear)
            - changed_by (str, optional): Who made the change

    Returns:
        Dictionary with keys:
            - 'updated': List of dicts with keys (value_id, batch_id, tenant_id, valid_time, series_id)
            - 'skipped_no_ops': List of dicts with keys (value_id) or (batch_id, tenant_id, valid_time, series_id)

    Rules:
      - For updates by value_id: directly updates the specified row
      - For updates by key: finds current row (latest known_time) and creates new version
      - If canonical new == canonical current => skip (no-op)
      - Otherwise: insert new row with known_time=now() (becomes the "current" value)
      - Note: Values must be in the canonical unit for the series (unit conversion should happen before calling this function).
    """
    if not updates:
        return {"updated": [], "skipped_no_ops": []}

    updated: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []

    # Separate updates by value_id vs by key
    updates_by_value_id: List[Dict[str, Any]] = []
    updates_by_key: List[Dict[str, Any]] = []

    for u in updates:
        if "value_id" in u:
            updates_by_value_id.append(u)
        else:
            updates_by_key.append(u)

    # Process updates by value_id (simpler path)
    if updates_by_value_id:
        _process_updates_by_value_id(conn, updates_by_value_id, updated, skipped)

    # Process updates by key (backwards compatibility)
    if updates_by_key:
        _process_updates_by_key(conn, updates_by_key, updated, skipped)

    return {"updated": updated, "skipped_no_ops": skipped}


def _process_updates_by_value_id(
    conn: psycopg.Connection,
    updates: List[Dict[str, Any]],
    updated: List[Dict[str, Any]],
    skipped: List[Dict[str, Any]],
) -> None:
    """Process updates that specify value_id directly."""
    sql_lock = """
        SELECT value_id, batch_id, tenant_id, valid_time, valid_time_end, series_id, value, annotation, tags, known_time
        FROM values_table
        WHERE value_id = %(value_id)s
        FOR UPDATE
    """

    sql_insert_new = """
        INSERT INTO values_table (
            batch_id, tenant_id, valid_time, valid_time_end, series_id,
            value, annotation, tags,
            known_time, changed_by, change_time
        )
        VALUES (
            %(batch_id)s, %(tenant_id)s, %(valid_time)s, %(valid_time_end)s, %(series_id)s,
            %(value)s, %(annotation)s, %(tags)s,
            now(), %(changed_by)s, now()
        )
        RETURNING value_id
    """

    with conn.transaction():
        with conn.cursor(row_factory=dict_row) as cur:
            for u in updates:
                value_id = u["value_id"]

                # Lock the row
                cur.execute(sql_lock, {"value_id": value_id})
                current = cur.fetchone()

                if current is None:
                    # If value_id lookup fails, try batch_id/run_id fallback
                    # Support both batch_id and run_id for backwards compatibility
                    batch_id = u.get("batch_id") or u.get("run_id")
                    if batch_id:
                        # Find the latest row by known_time for this key
                        sql_lock_by_key = """
                            SELECT value_id, batch_id, tenant_id, valid_time, valid_time_end, series_id, value, annotation, tags, known_time
                            FROM values_table
                            WHERE batch_id = %(batch_id)s
                              AND tenant_id = %(tenant_id)s
                              AND valid_time = %(valid_time)s
                              AND series_id = %(series_id)s
                            ORDER BY known_time DESC
                            LIMIT 1
                            FOR UPDATE
                        """
                        cur.execute(sql_lock_by_key, {
                            "batch_id": batch_id,
                            "tenant_id": u.get("tenant_id"),
                            "valid_time": u.get("valid_time"),
                            "series_id": u.get("series_id")
                        })
                        current = cur.fetchone()

                if current is None:
                    raise ValueError(f"No row found with value_id={value_id} or matching batch_id/tenant_id/valid_time/series_id")

                # Get current values
                current_value = current["value"]
                current_annotation_canon = _canonicalize_annotation_stored(current["annotation"])
                current_tags_canon = _canonicalize_tags_stored(current["tags"])

                # Merge using tri-state semantics
                new_value = u.get("value", _UNSET)
                if new_value is _UNSET:
                    new_value = current_value

                new_annotation = u.get("annotation", _UNSET)
                if new_annotation is _UNSET:
                    new_annotation = current_annotation_canon
                else:
                    new_annotation = _canonicalize_annotation_input(new_annotation)

                new_tags = u.get("tags", _UNSET)
                if new_tags is _UNSET:
                    new_tags = current_tags_canon
                else:
                    new_tags = _canonicalize_tags_input(new_tags)

                # Check if at least one field is being modified
                has_value = "value" in u
                has_annotation = "annotation" in u
                has_tags = "tags" in u

                if not (has_value or has_annotation or has_tags):
                    raise ValueError(
                        "No updates supplied: provide at least one of value/annotation/tags (omit field to leave unchanged)."
                    )

                # No-op detection
                if (new_value == current_value) and (new_annotation == current_annotation_canon) and (new_tags == current_tags_canon):
                    skipped.append({"value_id": value_id})
                    continue

                # Insert new row with known_time=now() (becomes the "current" value)
                cur.execute(sql_insert_new, {
                    "batch_id": current["batch_id"],
                    "tenant_id": current["tenant_id"],
                    "valid_time": current["valid_time"],
                    "valid_time_end": current["valid_time_end"],
                    "series_id": current["series_id"],
                    "value": new_value,
                    "annotation": new_annotation,
                    "tags": new_tags,
                    "changed_by": u.get("changed_by"),
                })
                new_value_id = cur.fetchone()["value_id"]
                updated.append({
                    "value_id": new_value_id,
                    "batch_id": current["batch_id"],
                    "tenant_id": current["tenant_id"],
                    "valid_time": current["valid_time"],
                    "series_id": current["series_id"],
                })


def _process_updates_by_key(
    conn: psycopg.Connection,
    updates: List[Dict[str, Any]],
    updated: List[Dict[str, Any]],
    skipped: List[Dict[str, Any]],
) -> None:
    """Process updates that specify batch_id, tenant_id, valid_time, series_id (backwards compatibility)."""
    # Validate and collapse duplicates (last-write-wins)
    KeyT = Tuple[uuid.UUID, uuid.UUID, dt.datetime, uuid.UUID]
    collapsed: Dict[KeyT, Dict[str, Any]] = {}

    for u in updates:
        # Extract required fields (support both batch_id and run_id for backwards compatibility)
        batch_id = u.get("batch_id") or u.get("run_id")
        tenant_id = u.get("tenant_id")
        valid_time = u.get("valid_time")
        series_id = u.get("series_id")

        if batch_id is None or tenant_id is None or valid_time is None or series_id is None:
            raise ValueError("Each update must contain batch_id (or run_id), tenant_id, valid_time, and series_id")

        # Type conversions
        if isinstance(batch_id, str):
            batch_id = uuid.UUID(batch_id)
        if isinstance(tenant_id, str):
            tenant_id = uuid.UUID(tenant_id)
        if isinstance(series_id, str):
            series_id = uuid.UUID(series_id)

        # Validate timezone-aware datetime
        if isinstance(valid_time, str):
            # Try to parse if string
            valid_time = dt.datetime.fromisoformat(valid_time.replace('Z', '+00:00'))
        if valid_time.tzinfo is None:
            raise ValueError(f"valid_time must be timezone-aware (timestamptz). Bad update: {u}")

        # Check if at least one field is being modified
        has_value = "value" in u
        has_annotation = "annotation" in u
        has_tags = "tags" in u

        if not (has_value or has_annotation or has_tags):
            raise ValueError(
                "No updates supplied: provide at least one of value/annotation/tags (omit field to leave unchanged)."
            )

        # Store the update with _UNSET for omitted fields
        update_dict = {
            "batch_id": batch_id,
            "tenant_id": tenant_id,
            "valid_time": valid_time,
            "series_id": series_id,
            "value": u.get("value", _UNSET),
            "annotation": u.get("annotation", _UNSET),
            "tags": u.get("tags", _UNSET),
            "changed_by": u.get("changed_by"),
        }

        collapsed[(batch_id, tenant_id, valid_time, series_id)] = update_dict

    # Deterministic order reduces deadlock risk
    ordered_items = sorted(
        collapsed.items(),
        key=lambda kv: (str(kv[0][0]), str(kv[0][1]), kv[0][2].isoformat(), str(kv[0][3])),
    )

    # Find the latest row by known_time for this key
    sql_lock_current = """
        SELECT value_id, value, annotation, tags, valid_time_end
        FROM values_table
        WHERE batch_id = %(batch_id)s
          AND tenant_id = %(tenant_id)s
          AND valid_time = %(valid_time)s
          AND series_id = %(series_id)s
        ORDER BY known_time DESC
        LIMIT 1
        FOR UPDATE
    """

    sql_insert_new = """
        INSERT INTO values_table (
            batch_id, tenant_id, valid_time, valid_time_end, series_id,
            value, annotation, tags,
            known_time, changed_by, change_time
        )
        VALUES (
            %(batch_id)s, %(tenant_id)s, %(valid_time)s, %(valid_time_end)s, %(series_id)s,
            %(value)s, %(annotation)s, %(tags)s,
            now(), %(changed_by)s, now()
        )
        RETURNING value_id
    """

    # Run batch inside a transaction to maintain invariants
    with conn.transaction():
        with conn.cursor(row_factory=dict_row) as cur:
            for (batch_id, tenant_id, valid_time, series_id), u in ordered_items:
                # 1) Lock current row for this key (if it exists) - latest by known_time
                cur.execute(sql_lock_current, {
                    "batch_id": batch_id,
                    "tenant_id": tenant_id,
                    "valid_time": valid_time,
                    "series_id": series_id
                })
                current = cur.fetchone()

                # 2) Canonicalize current row defensively
                if current is None:
                    current_value = None
                    current_annotation_canon = None
                    current_tags_canon = None
                    valid_time_end = None
                else:
                    current_value = current["value"]     # may be None
                    current_annotation_canon = _canonicalize_annotation_stored(current["annotation"])
                    current_tags_canon = _canonicalize_tags_stored(current["tags"])
                    valid_time_end = current["valid_time_end"]

                # 3) Merge using tri-state semantics
                # VALUE
                if u["value"] is _UNSET:
                    new_value = current_value
                else:
                    # explicit provided: may be None (clear) or a concrete numeric value
                    new_value = u["value"]

                # ANNOTATION
                if u["annotation"] is _UNSET:
                    new_annotation = current_annotation_canon
                else:
                    # explicit provided: None => clear; string => canonicalize
                    new_annotation = _canonicalize_annotation_input(u["annotation"])

                # TAGS
                if u["tags"] is _UNSET:
                    new_tags = current_tags_canon
                else:
                    # explicit provided: None or empty -> clear (canonical None); else normalized list
                    new_tags = _canonicalize_tags_input(u["tags"])

                # 4) If no current row exists, require `value` to be provided (explicitly, possibly None)
                if current is None and u["value"] is _UNSET:
                    raise ValueError(
                        f"No current row exists for (batch_id={batch_id}, tenant_id={tenant_id}, "
                        f"valid_time={valid_time}, series_id={series_id}). "
                        f"You must provide `value` (explicitly, possibly None) to create the first version."
                    )

                # 5) No-op detection (compare canonicalized forms)
                if (new_value == current_value) and (new_annotation == current_annotation_canon) and (new_tags == current_tags_canon):
                    skipped.append({
                        "batch_id": batch_id,
                        "tenant_id": tenant_id,
                        "valid_time": valid_time,
                        "series_id": series_id,
                    })
                    continue  # nothing to do for this key

                # 6) Insert new row with known_time=now() (becomes the "current" value)
                cur.execute(sql_insert_new, {
                    "batch_id": batch_id,
                    "tenant_id": tenant_id,
                    "valid_time": valid_time,
                    "valid_time_end": valid_time_end,
                    "series_id": series_id,
                    "value": new_value,
                    "annotation": new_annotation,
                    "tags": new_tags,        # None => SQL NULL; list => text[]
                    "changed_by": u["changed_by"],
                })
                value_id = cur.fetchone()["value_id"]
                updated.append({
                    "batch_id": batch_id,
                    "tenant_id": tenant_id,
                    "valid_time": valid_time,
                    "series_id": series_id,
                    "value_id": value_id,
                })
