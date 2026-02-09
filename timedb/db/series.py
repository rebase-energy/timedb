"""
Series management for TimeDB.

Handles creation and retrieval of series with their canonical units and labels.
"""
import uuid
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
    data_class: str = "flat",
    retention: str = "medium",
) -> uuid.UUID:
    """
    Create a new time series.

    This function always creates a new series. A new series_id is generated for each call.

    Args:
        conn: Database connection
        name: Parameter name (e.g., 'wind_power', 'temperature')
        unit: Canonical unit for the series (e.g., 'MW', 'degC', 'dimensionless')
        labels: Dictionary of labels that differentiate this series (e.g., {"site": "Gotland", "turbine": "T01"})
        description: Optional description of the series
        data_class: 'flat' or 'overlapping' (default: 'flat')
        retention: 'short', 'medium', or 'long' (default: 'medium', only relevant for overlapping)

    Returns:
        The series_id (UUID) for the newly created series

    Raises:
        ValueError: If name or unit is empty, or if data_class/retention are invalid
    """
    if not name or not name.strip():
        raise ValueError("name cannot be empty")
    if not unit or not unit.strip():
        raise ValueError("unit cannot be empty")
    if data_class not in ("flat", "overlapping"):
        raise ValueError(f"data_class must be 'flat' or 'overlapping', got '{data_class}'")
    if retention not in ("short", "medium", "long"):
        raise ValueError(f"retention must be 'short', 'medium', or 'long', got '{retention}'")

    # Normalize labels
    labels_dict = labels or {}
    labels_json = json.dumps(labels_dict, sort_keys=True)

    new_series_id = uuid.uuid4()
    with conn.cursor() as cur:
        description_value = description.strip() if description and description.strip() else None
        cur.execute(
            """
            INSERT INTO series_table (series_id, name, unit, labels, description, data_class, retention)
            VALUES (%s, %s, %s, %s::jsonb, %s, %s, %s)
            """,
            (new_series_id, name.strip(), unit.strip(), labels_json, description_value, data_class, retention)
        )
    return new_series_id


def get_series_info(
    conn: psycopg.Connection,
    series_id: uuid.UUID,
) -> Dict[str, any]:
    """
    Get full metadata for a series.

    Args:
        conn: Database connection
        series_id: The series UUID

    Returns:
        Dictionary with keys: name, unit, labels, description, data_class, retention

    Raises:
        ValueError: If series_id doesn't exist
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT name, unit, labels, description, data_class, retention
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
            'data_class': row[4],
            'retention': row[5],
        }

