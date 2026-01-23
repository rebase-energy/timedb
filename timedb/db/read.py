import os
from dotenv import load_dotenv
import pandas as pd
import psycopg
from datetime import datetime
from typing import Optional, Literal
import uuid

load_dotenv()


def _build_where_clause(
    tenant_id: Optional[uuid.UUID] = None,
    series_id: Optional[uuid.UUID] = None,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None,
    end_known: Optional[datetime] = None,
    all_versions: bool = False,
) -> tuple[str, dict]:
    """
    Build WHERE clause and parameters for time series queries.
    
    Returns:
        Tuple of (where_clause_string, params_dict)
    """
    filters = []
    params = {}
    
    # Tenant filter (required for multi-tenant, optional for single-tenant)
    if tenant_id is not None:
        filters.append("v.tenant_id = %(tenant_id)s")
        filters.append("b.tenant_id = %(tenant_id)s")
        params["tenant_id"] = tenant_id
    
    # Series filter
    if series_id is not None:
        filters.append("v.series_id = %(series_id)s")
        params["series_id"] = series_id
    
    # Time range filters
    if start_valid is not None:
        filters.append("v.valid_time >= %(start_valid)s")
        params["start_valid"] = start_valid
    if end_valid is not None:
        filters.append("v.valid_time < %(end_valid)s")
        params["end_valid"] = end_valid
    if start_known is not None:
        filters.append("b.known_time >= %(start_known)s")
        params["start_known"] = start_known
    if end_known is not None:
        filters.append("b.known_time < %(end_known)s")
        params["end_known"] = end_known
    
    # is_current filter (only if all_versions is False)
    if not all_versions:
        filters.append("v.is_current = true")
    
    where_clause = ""
    if filters:
        where_clause = "WHERE " + " AND ".join(filters)
    
    return where_clause, params


def read_values_flat(
    conninfo: str,
    *,
    tenant_id: Optional[uuid.UUID] = None,
    series_id: Optional[uuid.UUID] = None,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None,
    end_known: Optional[datetime] = None,
    all_versions: bool = False,
    return_value_id: bool = False,
) -> pd.DataFrame:
    """
    Read time series values in flat mode (latest known_time per valid_time).
    
    Returns the latest version of each (valid_time, series_id) combination,
    based on known_time. This is useful for getting the current state of
    time series data.
    
    Args:
        conninfo: Database connection string
        tenant_id: Tenant UUID (optional)
        series_id: Series UUID (optional, if not provided reads all series)
        start_valid: Start of valid time range (optional)
        end_valid: End of valid time range (optional)
        start_known: Start of known_time range (optional)
        end_known: End of known_time range (optional)
        all_versions: If True, include all versions (not just current). If False, only is_current=True (default: False)
        return_value_id: If True, include value_id column in the result (default: False)
    
    Returns:
        DataFrame with index (valid_time, series_id) and columns (value, name, unit, labels, [value_id if return_value_id=True])
    """
    where_clause, params = _build_where_clause(
        tenant_id=tenant_id,
        series_id=series_id,
        start_valid=start_valid,
        end_valid=end_valid,
        start_known=start_known,
        end_known=end_known,
        all_versions=all_versions,
    )
    
    value_id_col = "v.value_id," if return_value_id else ""
    sql = f"""
    SELECT DISTINCT ON (v.valid_time, COALESCE(v.valid_time_end, v.valid_time), v.series_id)
        v.valid_time,
        {value_id_col}
        v.value,
        v.series_id,
        s.name,
        s.unit,
        s.labels
    FROM values_table v
    JOIN batches_table b ON v.batch_id = b.batch_id AND v.tenant_id = b.tenant_id
    JOIN series_table s ON v.series_id = s.series_id
    {where_clause}
    ORDER BY v.valid_time, COALESCE(v.valid_time_end, v.valid_time), v.series_id, b.known_time DESC;
    """
    
    import warnings
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning, message=".*pandas only supports SQLAlchemy.*")
        with psycopg.connect(conninfo) as conn:
            df = pd.read_sql(sql, conn, params=params)
    
    # Ensure timezone-aware pandas datetimes
    df["valid_time"] = pd.to_datetime(df["valid_time"], utc=True)
    
    # Set index on (valid_time, series_id)
    df = df.set_index(["valid_time", "series_id"]).sort_index()
    
    return df


def read_values_overlapping(
    conninfo: str,
    *,
    tenant_id: Optional[uuid.UUID] = None,
    series_id: Optional[uuid.UUID] = None,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None,
    end_known: Optional[datetime] = None,
    all_versions: bool = False,
) -> pd.DataFrame:
    """
    Read time series values in overlapping mode (all forecast revisions).
    
    Returns all versions of forecasts, showing how predictions evolve over time.
    This is useful for analyzing forecast revisions and backtesting.
    
    Args:
        conninfo: Database connection string
        tenant_id: Tenant UUID (optional)
        series_id: Series UUID (optional, if not provided reads all series)
        start_valid: Start of valid time range (optional)
        end_valid: End of valid time range (optional)
        start_known: Start of known_time range (optional)
        end_known: End of known_time range (optional)
        all_versions: If True, include all versions (not just current). If False, only is_current=True (default: False)
    
    Returns:
        DataFrame with index (known_time, valid_time, series_id) and columns (value, name, unit, labels)
    """
    where_clause, params = _build_where_clause(
        tenant_id=tenant_id,
        series_id=series_id,
        start_valid=start_valid,
        end_valid=end_valid,
        start_known=start_known,
        end_known=end_known,
        all_versions=all_versions,
    )
    
    sql = f"""
    SELECT
        b.known_time,
        v.valid_time,
        v.value,
        v.series_id,
        s.name,
        s.unit,
        s.labels
    FROM values_table v
    JOIN batches_table b ON v.batch_id = b.batch_id AND v.tenant_id = b.tenant_id
    JOIN series_table s ON v.series_id = s.series_id
    {where_clause}
    ORDER BY b.known_time, v.valid_time, v.series_id;
    """
    
    import warnings
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning, message=".*pandas only supports SQLAlchemy.*")
        with psycopg.connect(conninfo) as conn:
            df = pd.read_sql(sql, conn, params=params)
    
    # Ensure timezone-aware pandas datetimes
    df["known_time"] = pd.to_datetime(df["known_time"], utc=True)
    df["valid_time"] = pd.to_datetime(df["valid_time"], utc=True)
    
    # Set index on (known_time, valid_time, series_id)
    df = df.set_index(["known_time", "valid_time", "series_id"]).sort_index()
    
    return df


def read_values_between(
    conninfo: str,
    *,
    tenant_id: Optional[uuid.UUID] = None,
    series_id: Optional[uuid.UUID] = None,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None,
    end_known: Optional[datetime] = None,
    mode: Literal["flat", "overlapping"] = "flat",
    all_versions: bool = False,
) -> pd.DataFrame:
    """
    Read time series values from the database (legacy wrapper function).
    
    This function is kept for backward compatibility. New code should use
    `read_values_flat()` or `read_values_overlapping()` directly.
    """
    if mode == "flat":
        return read_values_flat(
            conninfo,
            tenant_id=tenant_id,
            series_id=series_id,
            start_valid=start_valid,
            end_valid=end_valid,
            start_known=start_known,
            end_known=end_known,
            all_versions=all_versions,
        )
    elif mode == "overlapping":
        return read_values_overlapping(
            conninfo,
            tenant_id=tenant_id,
            series_id=series_id,
            start_valid=start_valid,
            end_valid=end_valid,
            start_known=start_known,
            end_known=end_known,
            all_versions=all_versions,
        )
    else:
        raise ValueError(f"Invalid mode: {mode}. Must be 'flat' or 'overlapping'")


if __name__ == "__main__":
    conninfo = os.environ["DATABASE_URL"]
    df = read_values_flat(conninfo)
    print(df)
