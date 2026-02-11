import os
from dotenv import load_dotenv
import pandas as pd
import psycopg
from datetime import datetime
from typing import Optional, List, Union

load_dotenv()


def _build_where_clause(
    series_ids: Optional[Union[int, List[int]]] = None,
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

    if series_ids is not None:
        if isinstance(series_ids, int):
            id_list = [series_ids]
        elif isinstance(series_ids, (list, tuple, set)):
            id_list = list(series_ids)
        else:
            raise ValueError("series_ids must be an int or an iterable of ints")
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


def read_flat(
    conninfo: str,
    *,
    series_ids: Optional[Union[int, List[int]]] = None,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
) -> pd.DataFrame:
    """
    Read flat (fact) values from the flat table.

    Returns immutable fact data (meter readings, measurements).

    Args:
        conninfo: Database connection string
        series_ids: Series ID or list of IDs (optional)
        start_valid: Start of time range (optional)
        end_valid: End of time range (optional)

    Returns:
        DataFrame with index (valid_time, series_id) and columns (value, name, unit, labels)
    """
    where_clause, params = _build_where_clause(
        series_ids=series_ids,
        start_valid=start_valid,
        end_valid=end_valid,
        time_col="v.valid_time",
        known_time_col="v.inserted_at",  # flat don't have known_time
    )

    sql = f"""
    SELECT
        v.valid_time,
        v.value,
        v.series_id,
        s.name,
        s.unit,
        s.labels
    FROM flat v
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


def read_overlapping_latest(
    conninfo: str,
    *,
    series_ids: Optional[Union[int, List[int]]] = None,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None,
    end_known: Optional[datetime] = None,
) -> pd.DataFrame:
    """
    Read latest overlapping values from the overlapping table.

    Returns the latest value for each (valid_time, series_id) combination,
    determined by the most recent known_time via DISTINCT ON.

    Args:
        conninfo: Database connection string
        series_ids: Series ID or list of IDs (optional)
        start_valid: Start of valid time range (optional)
        end_valid: End of valid time range (optional)
        start_known: Start of known_time range (optional)
        end_known: End of known_time range (optional)

    Returns:
        DataFrame with index (valid_time, series_id) and columns (value, name, unit, labels)
    """
    where_clause, params = _build_where_clause(
        series_ids=series_ids,
        start_valid=start_valid,
        end_valid=end_valid,
        start_known=start_known,
        end_known=end_known,
        time_col="v.valid_time",
        known_time_col="v.known_time",
    )

    sql = f"""
    SELECT DISTINCT ON (v.series_id, v.valid_time)
        v.valid_time,
        v.value,
        v.series_id,
        s.name,
        s.unit,
        s.labels
    FROM all_overlapping_raw v
    JOIN series_table s ON v.series_id = s.series_id
    {where_clause}
    ORDER BY v.series_id, v.valid_time, v.known_time DESC;
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


def read_overlapping_all(
    conninfo: str,
    *,
    series_ids: Optional[Union[int, List[int]]] = None,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None,
    end_known: Optional[datetime] = None,
) -> pd.DataFrame:
    """
    Read all overlapping versions from the overlapping table.

    Returns all versions of forecasts, showing how predictions evolve over time.
    Useful for analyzing forecast revisions and backtesting.

    Args:
        conninfo: Database connection string
        series_ids: Series ID or list of IDs (optional)
        start_valid: Start of valid time range (optional)
        end_valid: End of valid time range (optional)
        start_known: Start of known_time range (optional)
        end_known: End of known_time range (optional)

    Returns:
        DataFrame with index (known_time, valid_time, series_id) and columns (value, name, unit, labels)
    """
    where_clause, params = _build_where_clause(
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
    FROM all_overlapping_raw v
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


# Combined read functions (query both flat and overlapping)
def read_values_flat(
    conninfo: str,
    *,
    series_ids: Optional[Union[int, List[int]]] = None,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None,
    end_known: Optional[datetime] = None,
    return_value_id: bool = False,
) -> pd.DataFrame:
    """Read latest values from both flat and overlapping.

    In flat mode, returns one value per (valid_time, series_id):
    - Flat: immutable facts (one value per timestamp)
    - Overlapping: latest version by known_time
    """
    # Read flat (skip if filtering by known_time since flat don't have it)
    df_flat = pd.DataFrame()
    if start_known is None and end_known is None:
        df_flat = read_flat(
            conninfo,
            series_ids=series_ids,
            start_valid=start_valid,
            end_valid=end_valid,
        )

    # Read latest overlapping
    df_overlapping = read_overlapping_latest(
        conninfo,
        series_ids=series_ids,
        start_valid=start_valid,
        end_valid=end_valid,
        start_known=start_known,
        end_known=end_known,
    )

    # Combine results
    if len(df_flat) == 0:
        return df_overlapping
    elif len(df_overlapping) == 0:
        return df_flat
    else:
        return pd.concat([df_flat, df_overlapping]).sort_index()


def read_values_overlapping(
    conninfo: str,
    *,
    series_ids: Optional[Union[int, List[int]]] = None,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None,
    end_known: Optional[datetime] = None,
) -> pd.DataFrame:
    """Read all versions from both flat and overlapping.

    In overlapping mode, returns all versions with known_time:
    - Flat: single version per timestamp (inserted_at as known_time)
    - Overlapping: all versions with their known_time
    """
    # Read flat (skip if filtering by known_time)
    df_flat = pd.DataFrame()
    if start_known is None and end_known is None:
        df_flat_raw = read_flat(
            conninfo,
            series_ids=series_ids,
            start_valid=start_valid,
            end_valid=end_valid,
        )
        # Add known_time level for flat using inserted_at from the table
        if len(df_flat_raw) > 0:
            # Query flat with inserted_at to use as known_time
            where_clause, params = _build_where_clause(
                series_ids=series_ids,
                start_valid=start_valid,
                end_valid=end_valid,
                time_col="v.valid_time",
                known_time_col="v.inserted_at",
            )
            sql = f"""
            SELECT
                v.inserted_at as known_time,
                v.valid_time,
                v.value,
                v.series_id,
                s.name,
                s.unit,
                s.labels
            FROM flat v
            JOIN series_table s ON v.series_id = s.series_id
            {where_clause}
            ORDER BY v.inserted_at, v.valid_time, v.series_id;
            """
            import warnings
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=UserWarning, message=".*pandas only supports SQLAlchemy.*")
                with psycopg.connect(conninfo) as conn:
                    df_flat = pd.read_sql(sql, conn, params=params)
            if len(df_flat) > 0:
                df_flat["known_time"] = pd.to_datetime(df_flat["known_time"], utc=True)
                df_flat["valid_time"] = pd.to_datetime(df_flat["valid_time"], utc=True)
                df_flat = df_flat.set_index(["known_time", "valid_time", "series_id"]).sort_index()

    # Read all overlapping versions
    df_overlapping = read_overlapping_all(
        conninfo,
        series_ids=series_ids,
        start_valid=start_valid,
        end_valid=end_valid,
        start_known=start_known,
        end_known=end_known,
    )

    # Combine results
    if len(df_flat) == 0:
        return df_overlapping
    elif len(df_overlapping) == 0:
        return df_flat
    else:
        return pd.concat([df_flat, df_overlapping]).sort_index()


if __name__ == "__main__":
    conninfo = os.environ["DATABASE_URL"]
    df = read_overlapping_latest(conninfo)
    print(df)
