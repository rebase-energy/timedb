"""
Series management for TimeDB.

Handles creation, retrieval, and routing of series with their canonical units and labels.
Provides SeriesRegistry for caching series metadata across SDK, API, and DB layer.
"""
import json
from typing import Optional, Dict, List, Any
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


def create_series(
    conn: psycopg.Connection,
    *,
    name: str,
    unit: str,
    labels: Optional[Dict[str, str]] = None,
    description: Optional[str] = None,
    overlapping: bool = False,
    retention: str = "medium",
) -> int:
    """
    Create a new series, or return the existing series_id if one already exists
    with the same (name, labels).

    Args:
        conn: psycopg connection
        name: Series name (e.g., 'wind_power')
        unit: Canonical unit (e.g., 'MW', 'dimensionless')
        labels: Optional dict of labels for series differentiation
        description: Optional human-readable description
        overlapping: Whether series stores versioned data (default: False)
        retention: 'short', 'medium', or 'long' (default: 'medium')

    Returns:
        int: The series_id (auto-generated bigserial)
    """
    labels_dict = labels or {}
    labels_json = json.dumps(labels_dict, sort_keys=True)

    with conn.cursor() as cur:
        # Check if series already exists with same (name, labels)
        cur.execute(
            """
            SELECT series_id FROM series_table
            WHERE name = %s AND labels = %s::jsonb
            """,
            (name.strip(), labels_json)
        )
        row = cur.fetchone()
        if row:
            return row[0]

        # Create new series - DB auto-generates series_id via bigserial
        description_value = description.strip() if description and description.strip() else None
        cur.execute(
            """
            INSERT INTO series_table (name, unit, labels, description, overlapping, retention)
            VALUES (%s, %s, %s::jsonb, %s, %s, %s)
            RETURNING series_id
            """,
            (name.strip(), unit.strip(), labels_json, description_value, overlapping, retention)
        )
        return cur.fetchone()[0]
