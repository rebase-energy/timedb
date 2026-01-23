"""
Series management for TimeDB.

Handles creation and retrieval of series with their canonical units and labels.
"""
import uuid
import json
from typing import Optional, Dict, FrozenSet, Tuple
import psycopg


def create_series(
    conn: psycopg.Connection,
    *,
    name: str,
    unit: str,
    labels: Optional[Dict[str, str]] = None,
    description: Optional[str] = None,
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
    
    Returns:
        The series_id (UUID) for the newly created series
    
    Raises:
        ValueError: If name or unit is empty
    """
    if not name or not name.strip():
        raise ValueError("name cannot be empty")
    if not unit or not unit.strip():
        raise ValueError("unit cannot be empty")
    
    # Normalize labels
    labels_dict = labels or {}
    labels_json = json.dumps(labels_dict, sort_keys=True)
    
    new_series_id = uuid.uuid4()
    with conn.cursor() as cur:
        description_value = description.strip() if description and description.strip() else None
        cur.execute(
            """
            INSERT INTO series_table (series_id, name, unit, labels, description)
            VALUES (%s, %s, %s, %s::jsonb, %s)
            """,
            (new_series_id, name.strip(), unit.strip(), labels_json, description_value)
        )
    return new_series_id


def get_or_create_series(
    conn: psycopg.Connection,
    *,
    name: str,
    unit: str,
    labels: Optional[Dict[str, str]] = None,
    series_id: Optional[uuid.UUID] = None,
    description: Optional[str] = None,
) -> uuid.UUID:
    """
    Get an existing series or create a new one.
    
    If series_id is provided, verifies it exists and matches name/labels (unit is checked but not part of uniqueness).
    If series_id is None, looks up by (name, labels). If not found, creates a new series.
    
    Note: The unique constraint is on (name, labels) only - unit is NOT part of uniqueness.
    This means you cannot have two series with the same name+labels but different units.
    
    Args:
        conn: Database connection
        name: Parameter name (e.g., 'wind_power')
        unit: Canonical unit for the series (e.g., 'MW', 'dimensionless')
        labels: Dictionary of labels (e.g., {"site": "Gotland", "turbine": "T01"})
        series_id: Optional UUID. If provided, verifies it exists and matches.
        description: Optional description (only used when creating new series)
    
    Returns:
        The series_id (UUID) for the series
    
    Raises:
        ValueError: If series_id is provided but doesn't exist or doesn't match
    """
    # Normalize labels
    labels_dict = labels or {}
    labels_json = json.dumps(labels_dict, sort_keys=True)
    
    with conn.cursor() as cur:
        if series_id is not None:
            # Check if the series exists
            cur.execute(
                """
                SELECT name, unit, labels
                FROM series_table
                WHERE series_id = %s
                """,
                (series_id,)
            )
            row = cur.fetchone()
            if row is None:
                # Series doesn't exist - create it with the provided series_id
                description_value = description.strip() if description and description.strip() else None
                cur.execute(
                    """
                    INSERT INTO series_table (series_id, name, unit, labels, description)
                    VALUES (%s, %s, %s, %s::jsonb, %s)
                    """,
                    (series_id, name, unit, labels_json, description_value)
                )
                return series_id
            
            # Series exists - verify it matches (check name and labels, warn about unit mismatch)
            existing_name, existing_unit, existing_labels = row
            existing_labels_json = json.dumps(existing_labels or {}, sort_keys=True)
            if existing_name != name or existing_labels_json != labels_json:
                raise ValueError(
                    f"Series {series_id} exists but has different attributes: "
                    f"expected (name={name}, labels={labels_dict}), "
                    f"found (name={existing_name}, labels={existing_labels})"
                )
            
            # Warn if unit doesn't match (but still allow it since unit is not part of uniqueness)
            if existing_unit != unit:
                import warnings
                warnings.warn(
                    f"Series {series_id} has unit '{existing_unit}' but you specified '{unit}'. "
                    f"Using existing unit '{existing_unit}'."
                )
            
            return series_id
        
        # Look up by (name, labels) - unit is NOT part of the lookup
        cur.execute(
            """
            SELECT series_id, unit
            FROM series_table
            WHERE name = %s AND labels = %s::jsonb
            """,
            (name, labels_json)
        )
        row = cur.fetchone()
        
        if row is not None:
            existing_series_id, existing_unit = row
            # Warn if unit doesn't match
            if existing_unit != unit:
                import warnings
                warnings.warn(
                    f"Series with name='{name}' and labels={labels_dict} already exists with unit '{existing_unit}'. "
                    f"You specified unit '{unit}'. Using existing series with unit '{existing_unit}'."
                )
            return existing_series_id
        
        # Create new series
        new_series_id = uuid.uuid4()
        description_value = description.strip() if description and description.strip() else None
        cur.execute(
            """
            INSERT INTO series_table (series_id, name, unit, labels, description)
            VALUES (%s, %s, %s, %s::jsonb, %s)
            """,
            (new_series_id, name, unit, labels_json, description_value)
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
            SELECT unit
            FROM series_table
            WHERE series_id = %s
            """,
            (series_id,)
        )
        row = cur.fetchone()
        if row is None:
            raise ValueError(f"Series {series_id} not found")
        return row[0]


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
        Dictionary with keys: name, unit, labels, description
    
    Raises:
        ValueError: If series_id doesn't exist
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT name, unit, labels, description
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
        }


def get_mapping(
    conn: psycopg.Connection,
    *,
    name: Optional[str] = None,
    unit: Optional[str] = None,
    label_filter: Optional[Dict[str, str]] = None,
) -> Dict[FrozenSet[Tuple[str, str]], uuid.UUID]:
    """
    Get a mapping of label sets to series_ids based on search criteria.
    
    This function enables discovery and filtering of series based on their labels.
    The returned dictionary uses frozensets of (key, value) tuples as keys, allowing
    users to distinguish between series with different label combinations.
    
    Note: While unit can be used as a filter, the unique constraint is on (name, labels) only.
    
    Args:
        conn: Database connection
        name: Filter by parameter name (optional, e.g., 'wind_power')
        unit: Filter by unit (optional, e.g., 'MW')
        label_filter: Dictionary of labels to filter by (optional, e.g., {"site": "Gotland"})
                     Series must contain ALL specified labels to match (uses @> operator)
    
    Returns:
        Dictionary mapping frozenset of (label_key, label_value) tuples to series_id
        
    Examples:
        # Broad search - all turbines
        mapping = get_mapping(conn, label_filter={"type": "turbine"})
        # Returns: {
        #     frozenset([("type", "turbine"), ("site", "Gotland"), ("id", "T01")]): uuid1,
        #     frozenset([("type", "turbine"), ("site", "Gotland"), ("id", "T02")]): uuid2,
        #     frozenset([("type", "turbine"), ("site", "Skåne"), ("id", "S01")]): uuid3,
        # }
        
        # Narrower search - turbines at specific site
        mapping = get_mapping(conn, label_filter={"type": "turbine", "site": "Gotland"})
        # Returns: {
        #     frozenset([("type", "turbine"), ("site", "Gotland"), ("id", "T01")]): uuid1,
        #     frozenset([("type", "turbine"), ("site", "Gotland"), ("id", "T02")]): uuid2,
        # }
        
        # Exact match - specific turbine
        mapping = get_mapping(conn, label_filter={"type": "turbine", "site": "Gotland", "id": "T01"})
        # Returns: {
        #     frozenset([("type", "turbine"), ("site", "Gotland"), ("id", "T01")]): uuid1
        # }
        
        # Filter by name and unit
        mapping = get_mapping(conn, name="wind_power", unit="MW")
        
        # Get all series (no filters)
        mapping = get_mapping(conn)
    """
    filters = []
    params = []
    
    if name is not None:
        filters.append("name = %s")
        params.append(name)
    
    if unit is not None:
        filters.append("unit = %s")
        params.append(unit)
    
    if label_filter is not None:
        # Use PostgreSQL's @> operator to check if labels contain all specified key-value pairs
        filters.append("labels @> %s::jsonb")
        params.append(json.dumps(label_filter))
    
    where_clause = ""
    if filters:
        where_clause = "WHERE " + " AND ".join(filters)
    
    sql = f"""
        SELECT series_id, labels
        FROM series_table
        {where_clause}
        ORDER BY name, unit, series_id
    """
    
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    
    # Build the mapping dictionary
    mapping = {}
    for series_id, labels in rows:
        labels_dict = labels or {}
        # Convert labels dict to frozenset of tuples for use as dictionary key
        label_set = frozenset(labels_dict.items())
        mapping[label_set] = series_id
    
    return mapping

