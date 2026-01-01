"""
Series management for TimeDB.

Handles creation and retrieval of series with their canonical units.
"""
import uuid
from typing import Optional
import psycopg


def create_series(
    conn: psycopg.Connection,
    *,
    name: str,
    description: Optional[str],
    unit: str,
) -> uuid.UUID:
    """
    Create a new time series.
    
    This function always creates a new series (unlike get_or_create_series which
    may return an existing series). A new series_id is generated for each call.
    
    Args:
        conn: Database connection
        name: Human-readable identifier for the series (e.g., 'wind_power_forecast')
        description: Optional description of the series
        unit: Canonical unit for the series (e.g., 'MW', 'kW', 'MWh', 'dimensionless')
    
    Returns:
        The series_id (UUID) for the newly created series
    
    Raises:
        ValueError: If name or unit is empty
    """
    if not name or not name.strip():
        raise ValueError("name cannot be empty")
    if not unit or not unit.strip():
        raise ValueError("unit cannot be empty")
    
    new_series_id = uuid.uuid4()
    with conn.cursor() as cur:
        # Handle description: strip if provided, otherwise None
        description_value = description.strip() if description and description.strip() else None
        cur.execute(
            """
            INSERT INTO series_table (series_id, series_key, description, series_unit)
            VALUES (%s, %s, %s, %s)
            """,
            (new_series_id, name.strip(), description_value, unit.strip())
        )
    return new_series_id


def get_or_create_series(
    conn: psycopg.Connection,
    *,
    series_key: str,
    series_unit: str,
    series_id: Optional[uuid.UUID] = None,
) -> uuid.UUID:
    """
    Get an existing series or create a new one.
    
    If series_id is provided, verifies it exists and matches series_key/series_unit.
    If series_id is None, looks up by series_key. If not found, creates a new series.
    
    Args:
        conn: Database connection
        series_key: Human-readable identifier for the series (e.g., 'wind_power_forecast')
        series_unit: Canonical unit for the series (e.g., 'MW', 'kW', 'MWh', 'dimensionless')
        series_id: Optional UUID. If provided, verifies it exists and matches.
    
    Returns:
        The series_id (UUID) for the series
    
    Raises:
        ValueError: If series_id is provided but doesn't exist or doesn't match
    """
    with conn.cursor() as cur:
        if series_id is not None:
            # Verify the series exists and matches
            cur.execute(
                """
                SELECT series_key, series_unit
                FROM series_table
                WHERE series_id = %s
                """,
                (series_id,)
            )
            row = cur.fetchone()
            if row is None:
                raise ValueError(f"Series with series_id {series_id} does not exist")
            
            existing_key, existing_unit = row
            if existing_key != series_key or existing_unit != series_unit:
                raise ValueError(
                    f"Series {series_id} exists but has different key/unit: "
                    f"expected ({series_key}, {series_unit}), "
                    f"found ({existing_key}, {existing_unit})"
                )
            
            return series_id
        
        # Look up by series_key
        cur.execute(
            """
            SELECT series_id, series_unit
            FROM series_table
            WHERE series_key = %s
            """,
            (series_key,)
        )
        row = cur.fetchone()
        
        if row is not None:
            existing_id, existing_unit = row
            if existing_unit != series_unit:
                raise ValueError(
                    f"Series with key '{series_key}' exists but has different unit: "
                    f"expected {series_unit}, found {existing_unit}"
                )
            return existing_id
        
        # Create new series
        new_series_id = uuid.uuid4()
        cur.execute(
            """
            INSERT INTO series_table (series_id, series_key, series_unit)
            VALUES (%s, %s, %s)
            """,
            (new_series_id, series_key, series_unit)
        )
        return new_series_id


def get_series_unit(
    conn: psycopg.Connection,
    series_id: uuid.UUID,
) -> str:
    """
    Get the canonical unit for a series.
    
    Args:
        conn: Database connection
        series_id: The series UUID
    
    Returns:
        The canonical unit string (e.g., 'MW', 'dimensionless')
    
    Raises:
        ValueError: If series_id doesn't exist
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT series_unit
            FROM series_table
            WHERE series_id = %s
            """,
            (series_id,)
        )
        row = cur.fetchone()
        if row is None:
            raise ValueError(f"Series with series_id {series_id} does not exist")
        return row[0]


def get_series_info(
    conn: psycopg.Connection,
    series_id: uuid.UUID,
) -> tuple[str, str]:
    """
    Get series_key and series_unit for a series.
    
    Args:
        conn: Database connection
        series_id: The series UUID
    
    Returns:
        Tuple of (series_key, series_unit)
    
    Raises:
        ValueError: If series_id doesn't exist
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT series_key, series_unit
            FROM series_table
            WHERE series_id = %s
            """,
            (series_id,)
        )
        row = cur.fetchone()
        if row is None:
            raise ValueError(f"Series with series_id {series_id} does not exist")
        return row[0], row[1]

