"""
Series management for TimeDB.

Handles creation, retrieval, and routing of series with their canonical units and labels.
Provides SeriesRegistry for caching series metadata across SDK, API, and DB layer.
"""
import json
from typing import Optional, Dict, List, Any, Tuple
import psycopg


# Table name mapping for overlapping retention tiers
OVERLAPPING_TABLES = {
    "short":  "overlapping_short",
    "medium": "overlapping_medium",
    "long":   "overlapping_long",
}


def get_table_name(overlapping: bool, retention: str) -> str:
    """Return the target table name for a series based on its routing info."""
    if not overlapping:
        return "flat"
    table = OVERLAPPING_TABLES.get(retention)
    if table is None:
        raise ValueError(f"Unknown retention '{retention}'")
    return table


class SeriesRegistry:
    """In-memory cache of series metadata.

    Used by SDK, API, and DB layer to avoid repeated DB round-trips.
    Each entry maps series_id -> {name, unit, labels, description, overlapping, retention, table}.
    """

    def __init__(self):
        self._cache: Dict[int, Dict[str, Any]] = {}

    def resolve(
        self,
        conn: psycopg.Connection,
        *,
        name: Optional[str] = None,
        unit: Optional[str] = None,
        labels: Optional[Dict[str, str]] = None,
        series_id: Optional[int] = None,
    ) -> List[int]:
        """Query series_table, cache results, return matching series_ids.

        Builds WHERE clause with name, unit, labels (@> jsonb), series_id.
        Populates self._cache for each returned row.
        """
        query = "SELECT series_id, name, description, unit, labels, overlapping, retention FROM series_table"
        clauses: list = []
        params: list = []

        if series_id is not None:
            clauses.append("series_id = %s")
            params.append(series_id)
        if name is not None:
            clauses.append("name = %s")
            params.append(name)
        if unit is not None:
            clauses.append("unit = %s")
            params.append(unit)
        if labels:
            clauses.append("labels @> %s::jsonb")
            params.append(json.dumps(labels))

        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY name, unit, series_id"

        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()

        result_ids = []
        for row in rows:
            sid = row[0]
            self._cache[sid] = {
                "name": row[1],
                "description": row[2],
                "unit": row[3],
                "labels": row[4] or {},
                "overlapping": row[5],
                "retention": row[6],
                "table": get_table_name(row[5], row[6]),
            }
            result_ids.append(sid)

        return result_ids

    def ensure_cached(self, conn: psycopg.Connection, series_ids: List[int]) -> None:
        """Query and cache any series_ids not already in cache."""
        missing = [sid for sid in series_ids if sid not in self._cache]
        if not missing:
            return
        with conn.cursor() as cur:
            cur.execute(
                "SELECT series_id, name, description, unit, labels, overlapping, retention "
                "FROM series_table WHERE series_id = ANY(%s)",
                (missing,),
            )
            for row in cur.fetchall():
                sid = row[0]
                self._cache[sid] = {
                    "name": row[1],
                    "description": row[2],
                    "unit": row[3],
                    "labels": row[4] or {},
                    "overlapping": row[5],
                    "retention": row[6],
                    "table": get_table_name(row[5], row[6]),
                }

    def get(self, conn: psycopg.Connection, series_id: int) -> Dict[str, Any]:
        """Get cached info for a series_id, auto-fetching from DB if not cached."""
        self.ensure_cached(conn, [series_id])
        return self._cache[series_id]

    def get_cached(self, series_id: int) -> Dict[str, Any]:
        """Get cached info for a series_id. Raises KeyError if not cached.
        
        Use this when you know the series is already cached (e.g., after resolve()).
        For auto-fetching behavior, use get(conn, series_id) instead.
        """
        return self._cache[series_id]

    def get_routing_single(self, conn: psycopg.Connection, series_id: int) -> Dict[str, Any]:
        """Get routing info for a single series."""
        self.ensure_cached(conn, [series_id])
        return {
            "overlapping": self._cache[series_id]["overlapping"],
            "retention": self._cache[series_id]["retention"],
            "table": self._cache[series_id]["table"],
        }

    @property
    def cache(self) -> Dict[int, Dict[str, Any]]:
        """Direct access to the cache dict."""
        return self._cache


def _make_series_key(name: str, labels: dict) -> Tuple[str, str]:
    """Canonical, hashable key for a (name, labels) pair."""
    return (name, json.dumps(labels, sort_keys=True))


def resolve_series(
    conn: psycopg.Connection,
    identities: List[Tuple[str, Dict[str, Any]]],
    registry: "SeriesRegistry",
) -> Dict[Tuple[str, str], int]:
    """
    Bulk lookup of series_ids for a list of (name, labels_dict) pairs.

    Uses a single ``WHERE name = ANY(%s)`` round-trip, then filters by labels
    in Python.  Populates *registry* with all matching rows.

    Args:
        conn: psycopg connection.
        identities: List of ``(name, labels_dict)`` pairs to resolve.
        registry: SeriesRegistry instance — matching rows are cached into it.

    Returns:
        ``{(name, labels_json_canonical): series_id}`` for every found pair.
        Missing entries are simply absent from the dict; the caller is
        responsible for raising an error on any missing series.
    """
    if not identities:
        return {}

    # Normalize names the same way create_series() does — prevents whitespace misses
    identities = [(name.strip(), labels) for name, labels in identities]

    names = list({name for name, _ in identities})
    with conn.cursor() as cur:
        cur.execute(
            "SELECT series_id, name, description, unit, labels, overlapping, retention "
            "FROM series_table WHERE name = ANY(%s)",
            (names,),
        )
        rows = cur.fetchall()

    # Build a lookup from canonical key → series_id and populate registry
    found: Dict[Tuple[str, str], int] = {}
    for row in rows:
        sid, name, description, unit, labels, overlapping, retention = row
        labels = labels or {}
        registry._cache[sid] = {
            "name": name,
            "description": description,
            "unit": unit,
            "labels": labels,
            "overlapping": overlapping,
            "retention": retention,
            "table": get_table_name(overlapping, retention),
        }
        key = _make_series_key(name, labels)
        found[key] = sid

    return found


def create_series(
    conn: psycopg.Connection,
    series_specs: List[Dict[str, Any]],
) -> List[int]:
    """
    Batch get-or-create for multiple series in one round-trip.

    Returns series_ids in the same order as the input. Each spec is a dict
    with ``name`` (required) and optional ``unit``, ``labels``, ``description``,
    ``overlapping``, ``retention``.

    Uses ``ON CONFLICT (name, labels) DO UPDATE SET series_id =
    series_table.series_id`` — a no-op update that forces conflicting rows to
    appear in ``RETURNING``, preserving get-or-create semantics for all entries
    regardless of whether they already exist.

    Args:
        conn: psycopg connection
        series_specs: List of dicts. Each dict may contain:
            name (str, required), unit (str), labels (dict), description (str),
            overlapping (bool), retention (str).

    Returns:
        List[int]: series_ids in the same order as *series_specs*.

    Raises:
        ValueError: If *series_specs* is empty.
        ValueError: If the same (name, labels) pair appears more than once.
    """
    if not series_specs:
        raise ValueError("series_specs must not be empty")

    normalized = []
    seen: set = set()
    duplicates: set = set()
    for spec in series_specs:
        labels_json = json.dumps(spec.get("labels") or {}, sort_keys=True)
        name = spec["name"].strip()
        unit = (spec.get("unit") or "dimensionless").strip()
        desc = spec.get("description")
        desc = desc.strip() if desc and desc.strip() else None
        key = (name, labels_json)
        if key in seen:
            duplicates.add(key)
        else:
            seen.add(key)
        normalized.append((
            name, unit, labels_json, desc,
            bool(spec.get("overlapping", False)),
            spec.get("retention", "medium"),
        ))

    if duplicates:
        dup_list = ", ".join(f"(name={k[0]!r}, labels={k[1]})" for k in sorted(duplicates))
        raise ValueError(
            f"Duplicate (name, labels) pairs in create_series call: {dup_list}"
        )

    placeholders = ", ".join("(%s, %s, %s::jsonb, %s, %s, %s)" for _ in normalized)
    flat_params = [v for row in normalized for v in row]

    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO series_table (name, unit, labels, description, overlapping, retention) "
            f"VALUES {placeholders} "
            "ON CONFLICT (name, labels) DO UPDATE SET series_id = series_table.series_id "
            "RETURNING series_id, name, labels",
            flat_params,
        )
        lookup = {
            (row[1], json.dumps(row[2] or {}, sort_keys=True)): row[0]
            for row in cur.fetchall()
        }

    return [lookup[(name, labels_json)] for name, _, labels_json, _, _, _ in normalized]


