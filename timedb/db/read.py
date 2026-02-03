import os
from dotenv import load_dotenv
import pandas as pd
import psycopg
from datetime import datetime
from typing import Optional, Literal, List, Union
import uuid

load_dotenv()


def _build_where_clause(
    tenant_id: Optional[uuid.UUID] = None,
    series_ids: Optional[Union[uuid.UUID, List[uuid.UUID]]] = None,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None,
    end_known: Optional[datetime] = None,
    time_col: str = "v.valid_time",
    known_time_col: str = "v.known_time",
) -> tuple[str, dict]:
    """
    Build WHERE clause and parameters for time series queries.

    Args:
        time_col: Column name for the time dimension (e.g., 'v.valid_time', 'v.time', 'v.bucket_time')
        known_time_col: Column name for known_time (e.g., 'v.known_time', 'v.generated_at')

    Returns:
        Tuple of (where_clause_string, params_dict)
    """
    filters = []
    params = {}

    if tenant_id is not None:
        filters.append("v.tenant_id = %(tenant_id)s")
        params["tenant_id"] = tenant_id

    if series_ids is not None:
        if isinstance(series_ids, uuid.UUID):
            id_list = [series_ids]
        elif isinstance(series_ids, (list, tuple, set)):
            id_list = list(series_ids)
        else:
            raise ValueError("series_ids must be a UUID or an iterable of UUIDs")
        filters.append("v.series_id = ANY(%(series_ids)s)")
        params["series_ids"] = id_list

    if start_valid is not None:
        filters.append(f"{time_col} >= %(start_valid)s")
        params["start_valid"] = start_valid
    if end_valid is not None:
        filters.append(f"{time_col} < %(end_valid)s")
        params["end_valid"] = end_valid

    if start_known is not None:
        filters.append(f"{known_time_col} >= %(start_known)s")
        params["start_known"] = start_known
    if end_known is not None:
        filters.append(f"{known_time_col} < %(end_known)s")
        params["end_known"] = end_known

    where_clause = ""
    if filters:
        where_clause = "WHERE " + " AND ".join(filters)

    return where_clause, params


def read_actuals(
    conninfo: str,
    *,
    tenant_id: Optional[uuid.UUID] = None,
    series_ids: Optional[Union[uuid.UUID, List[uuid.UUID]]] = None,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
) -> pd.DataFrame:
    """
    Read actual (fact) values from the actuals table.

    Returns immutable fact data (meter readings, measurements).

    Args:
        conninfo: Database connection string
        tenant_id: Tenant UUID (optional)
        series_ids: Series UUID or list of UUIDs (optional)
        start_valid: Start of time range (optional)
        end_valid: End of time range (optional)

    Returns:
        DataFrame with index (valid_time, series_id) and columns (value, name, unit, labels)
    """
    where_clause, params = _build_where_clause(
        tenant_id=tenant_id,
        series_ids=series_ids,
        start_valid=start_valid,
        end_valid=end_valid,
        time_col="v.valid_time",
        known_time_col="v.inserted_at",  # actuals don't have known_time
    )

    sql = f"""
    SELECT
        v.valid_time,
        v.value,
        v.series_id,
        s.name,
        s.unit,
        s.labels
    FROM actuals v
    JOIN series_table s ON v.series_id = s.series_id
    {where_clause}
    ORDER BY v.valid_time, v.series_id;
    """

    import warnings
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning, message=".*pandas only supports SQLAlchemy.*")
        with psycopg.connect(conninfo) as conn:
            df = pd.read_sql(sql, conn, params=params)

    if len(df) == 0:
        return df

    df["valid_time"] = pd.to_datetime(df["valid_time"], utc=True)
    df = df.set_index(["valid_time", "series_id"]).sort_index()
    return df


def read_projections_latest(
    conninfo: str,
    *,
    tenant_id: Optional[uuid.UUID] = None,
    series_ids: Optional[Union[uuid.UUID, List[uuid.UUID]]] = None,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None,
    end_known: Optional[datetime] = None,
) -> pd.DataFrame:
    """
    Read latest projection values from the latest_projection_curve view.

    Returns the latest value for each (valid_time, series_id) combination,
    pre-computed by the continuous aggregates.

    Args:
        conninfo: Database connection string
        tenant_id: Tenant UUID (optional)
        series_ids: Series UUID or list of UUIDs (optional)
        start_valid: Start of valid time range (optional)
        end_valid: End of valid time range (optional)
        start_known: Start of generated_at range (optional)
        end_known: End of generated_at range (optional)

    Returns:
        DataFrame with index (valid_time, series_id) and columns (value, name, unit, labels)
    """
    where_clause, params = _build_where_clause(
        tenant_id=tenant_id,
        series_ids=series_ids,
        start_valid=start_valid,
        end_valid=end_valid,
        start_known=start_known,
        end_known=end_known,
        time_col="v.bucket_time",
        known_time_col="v.generated_at",
    )

    sql = f"""
    SELECT
        v.bucket_time as valid_time,
        v.value,
        v.series_id,
        s.name,
        s.unit,
        s.labels
    FROM latest_projection_curve v
    JOIN series_table s ON v.series_id = s.series_id
    {where_clause}
    ORDER BY v.bucket_time, v.series_id;
    """

    import warnings
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning, message=".*pandas only supports SQLAlchemy.*")
        with psycopg.connect(conninfo) as conn:
            df = pd.read_sql(sql, conn, params=params)

    if len(df) == 0:
        return df

    df["valid_time"] = pd.to_datetime(df["valid_time"], utc=True)
    df = df.set_index(["valid_time", "series_id"]).sort_index()
    return df


def read_projections_all(
    conninfo: str,
    *,
    tenant_id: Optional[uuid.UUID] = None,
    series_ids: Optional[Union[uuid.UUID, List[uuid.UUID]]] = None,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None,
    end_known: Optional[datetime] = None,
) -> pd.DataFrame:
    """
    Read all projection versions from the all_projections_raw view.

    Returns all versions of forecasts, showing how predictions evolve over time.
    Useful for analyzing forecast revisions and backtesting.

    Args:
        conninfo: Database connection string
        tenant_id: Tenant UUID (optional)
        series_ids: Series UUID or list of UUIDs (optional)
        start_valid: Start of valid time range (optional)
        end_valid: End of valid time range (optional)
        start_known: Start of known_time range (optional)
        end_known: End of known_time range (optional)

    Returns:
        DataFrame with index (known_time, valid_time, series_id) and columns (value, name, unit, labels)
    """
    where_clause, params = _build_where_clause(
        tenant_id=tenant_id,
        series_ids=series_ids,
        start_valid=start_valid,
        end_valid=end_valid,
        start_known=start_known,
        end_known=end_known,
        time_col="v.valid_time",
        known_time_col="v.known_time",
    )

    sql = f"""
    SELECT
        v.known_time,
        v.valid_time,
        v.value,
        v.series_id,
        s.name,
        s.unit,
        s.labels
    FROM all_projections_raw v
    JOIN series_table s ON v.series_id = s.series_id
    {where_clause}
    ORDER BY v.known_time, v.valid_time, v.series_id;
    """

    import warnings
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning, message=".*pandas only supports SQLAlchemy.*")
        with psycopg.connect(conninfo) as conn:
            df = pd.read_sql(sql, conn, params=params)

    if len(df) == 0:
        return df

    df["known_time"] = pd.to_datetime(df["known_time"], utc=True)
    df["valid_time"] = pd.to_datetime(df["valid_time"], utc=True)
    df = df.set_index(["known_time", "valid_time", "series_id"]).sort_index()
    return df


# Legacy wrappers for backward compatibility
def read_values_flat(
    conninfo: str,
    *,
    tenant_id: Optional[uuid.UUID] = None,
    series_ids: Optional[Union[uuid.UUID, List[uuid.UUID]]] = None,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None,
    end_known: Optional[datetime] = None,
    return_value_id: bool = False,
) -> pd.DataFrame:
    """Legacy wrapper. Reads latest projections from latest_projection_curve."""
    return read_projections_latest(
        conninfo,
        tenant_id=tenant_id,
        series_ids=series_ids,
        start_valid=start_valid,
        end_valid=end_valid,
        start_known=start_known,
        end_known=end_known,
    )


def read_values_overlapping(
    conninfo: str,
    *,
    tenant_id: Optional[uuid.UUID] = None,
    series_ids: Optional[Union[uuid.UUID, List[uuid.UUID]]] = None,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None,
    end_known: Optional[datetime] = None,
) -> pd.DataFrame:
    """Legacy wrapper. Reads all projection versions from all_projections_raw."""
    return read_projections_all(
        conninfo,
        tenant_id=tenant_id,
        series_ids=series_ids,
        start_valid=start_valid,
        end_valid=end_valid,
        start_known=start_known,
        end_known=end_known,
    )


def read_values_between(
    conninfo: str,
    *,
    tenant_id: Optional[uuid.UUID] = None,
    series_ids: Optional[Union[uuid.UUID, List[uuid.UUID]]] = None,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None,
    end_known: Optional[datetime] = None,
    mode: Literal["flat", "overlapping"] = "flat",
) -> pd.DataFrame:
    """Legacy wrapper function."""
    if mode == "flat":
        return read_projections_latest(
            conninfo,
            tenant_id=tenant_id,
            series_ids=series_ids,
            start_valid=start_valid,
            end_valid=end_valid,
            start_known=start_known,
            end_known=end_known,
        )
    elif mode == "overlapping":
        return read_projections_all(
            conninfo,
            tenant_id=tenant_id,
            series_ids=series_ids,
            start_valid=start_valid,
            end_valid=end_valid,
            start_known=start_known,
            end_known=end_known,
        )
    else:
        raise ValueError(f"Invalid mode: {mode}. Must be 'flat' or 'overlapping'")


if __name__ == "__main__":
    conninfo = os.environ["DATABASE_URL"]
    df = read_projections_latest(conninfo)
    print(df)
