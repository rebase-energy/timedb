"""
Series management for TimeDB.

Handles creation and retrieval of series with their canonical units and labels.
"""
import json
from typing import Optional, Dict
import psycopg


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


def get_series_info(
    conn: psycopg.Connection,
    series_id: int,
) -> Dict[str, any]:
    """
    Get full metadata for a series.

    Args:
        conn: psycopg connection
        series_id: The series_id to look up

    Returns:
        Dict with keys: name, unit, labels, description, overlapping, retention

    Raises:
        ValueError: If series not found
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT name, unit, labels, description, overlapping, retention
            FROM series_table
            WHERE series_id = %s
            """,
            (series_id,)
        )
        row = cur.fetchone()

    if row is None:
        raise ValueError(f"Series {series_id} not found")

    return {
        'name': row[0],
        'unit': row[1],
        'labels': row[2] or {},
        'description': row[3],
        'overlapping': row[4],
        'retention': row[5],
    }
