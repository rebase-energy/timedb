import os
import warnings
from contextlib import contextmanager
import pandas as pd
import psycopg
from datetime import datetime, timedelta
from typing import Optional, Union


@contextmanager
def _ensure_conn(conninfo_or_conn):
    """Yield a psycopg Connection, creating one only if given a string."""
    if isinstance(conninfo_or_conn, str):
        with psycopg.connect(conninfo_or_conn) as conn:
            yield conn
    else:
        yield conninfo_or_conn


def _build_where_clause(
    series_id: int,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None,
    end_known: Optional[datetime] = None,
) -> tuple[str, dict]:
    """
    Build WHERE clause and parameters for time series queries.

    Returns:
        Tuple of (where_clause_string, params_dict)
    """
    filters = []
    params = {}

    filters.append("v.series_id = %(series_id)s")
    params["series_id"] = series_id

    if start_valid is not None:
        filters.append("v.valid_time >= %(start_valid)s")
        params["start_valid"] = start_valid
    if end_valid is not None:
        filters.append("v.valid_time < %(end_valid)s")
        params["end_valid"] = end_valid

    if start_known is not None:
        filters.append("v.knowledge_time >= %(start_known)s")
        params["start_known"] = start_known
    if end_known is not None:
        filters.append("v.knowledge_time < %(end_known)s")
        params["end_known"] = end_known

    where_clause = ""
    if filters:
        where_clause = "WHERE " + " AND ".join(filters)

    return where_clause, params


def read_flat(
    conninfo: Union[psycopg.Connection, str],
    *,
    series_id: int,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None,
    end_known: Optional[datetime] = None,
) -> pd.DataFrame:
    """
    Read flat (fact) values from the flat table.

    Args:
        conninfo: Database connection or connection string
        series_id: Series ID (required)
        start_valid: Start of time range (optional)
        end_valid: End of time range (optional)
        start_known: Start of knowledge_time range (optional)
        end_known: End of knowledge_time range (optional)

    Returns:
        DataFrame with index (valid_time) and columns (value)
    """
    where_clause, params = _build_where_clause(
        series_id=series_id,
        start_valid=start_valid,
        end_valid=end_valid,
        start_known=start_known,
        end_known=end_known,
    )

    sql = f"""
    SELECT v.valid_time, v.value
    FROM flat v
    {where_clause}
    ORDER BY v.valid_time;
    """

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning, message=".*pandas only supports SQLAlchemy.*")
        with _ensure_conn(conninfo) as conn:
            df = pd.read_sql(
                sql,
                conn,
                params=params,
                dtype={"value": "float64"},
                parse_dates={"valid_time": {"utc": True}},
            )

    if len(df) == 0:
        return df

    df = df.set_index("valid_time")
    return df


def read_overlapping_latest(
    conninfo: Union[psycopg.Connection, str],
    *,
    series_id: int,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None,
    end_known: Optional[datetime] = None,
) -> pd.DataFrame:
    """
    Read latest overlapping values from the overlapping table.

    Returns the latest value for each valid_time,
    determined by the most recent knowledge_time via DISTINCT ON.

    Args:
        conninfo: Database connection or connection string
        series_id: Series ID (required)
        start_valid: Start of valid time range (optional)
        end_valid: End of valid time range (optional)
        start_known: Start of knowledge_time range (optional)
        end_known: End of knowledge_time range (optional)

    Returns:
        DataFrame with index (valid_time) and columns (value)
    """
    where_clause, params = _build_where_clause(
        series_id=series_id,
        start_valid=start_valid,
        end_valid=end_valid,
        start_known=start_known,
        end_known=end_known,
    )

    sql = f"""
    SELECT DISTINCT ON (v.valid_time)
        v.valid_time, v.value
    FROM all_overlapping_raw v
    {where_clause}
    ORDER BY v.valid_time, v.knowledge_time DESC;
    """

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning, message=".*pandas only supports SQLAlchemy.*")
        with _ensure_conn(conninfo) as conn:
            df = pd.read_sql(
                sql,
                conn,
                params=params,
                dtype={"value": "float64"},
                parse_dates={"valid_time": {"utc": True}},
            )

    if len(df) == 0:
        return df

    df = df.set_index("valid_time")
    return df


def read_overlapping_relative(
    conninfo: Union[psycopg.Connection, str],
    *,
    series_id: int,
    window_length: timedelta,
    issue_offset: timedelta,
    start_window: datetime,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
) -> pd.DataFrame:
    """
    Read overlapping values using a per-window knowledge_time cutoff.

    For each valid_time, computes the window it belongs to (aligned to
    start_window with period window_length), then returns the latest forecast
    with knowledge_time <= window_start + issue_offset.

    Args:
        conninfo: Database connection or connection string
        series_id: Series ID (required)
        window_length: Length of each window (e.g., timedelta(hours=24))
        issue_offset: Offset from window_start for the knowledge_time cutoff.
                      Negative means before the window starts
                      (e.g., timedelta(hours=-12) = 12h before window start).
        start_window: Origin for window alignment (required).
        start_valid: Start of valid time range (optional)
        end_valid: End of valid time range (optional)

    Returns:
        DataFrame with index (valid_time) and columns (value)
    """
    where_clause, params = _build_where_clause(
        series_id=series_id,
        start_valid=start_valid,
        end_valid=end_valid,
    )
    params.update({
        "start_window": start_window,
        "window_secs": window_length.total_seconds(),
        "issue_offset_secs": issue_offset.total_seconds(),
    })

    sql = f"""
    WITH windowed AS (
        SELECT
            v.valid_time,
            v.value,
            v.knowledge_time,
            -- Cutoff: latest allowed knowledge_time for the window containing this valid_time
            %(start_window)s
            + make_interval(secs => floor(
                extract(epoch from (v.valid_time - %(start_window)s))
                / %(window_secs)s
              ) * %(window_secs)s
            )
            + make_interval(secs => %(issue_offset_secs)s)
            AS cutoff_time
        FROM all_overlapping_raw v
        {where_clause}
    )
    SELECT DISTINCT ON (valid_time)
        valid_time, value
    FROM windowed
    WHERE knowledge_time <= cutoff_time
    ORDER BY valid_time, knowledge_time DESC;
    """

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning, message=".*pandas only supports SQLAlchemy.*")
        with _ensure_conn(conninfo) as conn:
            df = pd.read_sql(
                sql,
                conn,
                params=params,
                dtype={"value": "float64"},
                parse_dates={"valid_time": {"utc": True}},
            )

    if len(df) == 0:
        return df

    df = df.set_index("valid_time")
    return df


def read_overlapping_all(
    conninfo: Union[psycopg.Connection, str],
    *,
    series_id: int,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None,
    end_known: Optional[datetime] = None,
) -> pd.DataFrame:
    """
    Read all overlapping versions from the overlapping table.

    Returns all versions of forecasts, showing how predictions evolve over time.

    Args:
        conninfo: Database connection or connection string
        series_id: Series ID (required)
        start_valid: Start of valid time range (optional)
        end_valid: End of valid time range (optional)
        start_known: Start of knowledge_time range (optional)
        end_known: End of knowledge_time range (optional)

    Returns:
        DataFrame with index (knowledge_time, valid_time) and columns (value)
    """
    where_clause, params = _build_where_clause(
        series_id=series_id,
        start_valid=start_valid,
        end_valid=end_valid,
        start_known=start_known,
        end_known=end_known,
    )

    sql = f"""
    SELECT v.knowledge_time, v.valid_time, v.value
    FROM all_overlapping_raw v
    {where_clause}
    ORDER BY v.knowledge_time, v.valid_time;
    """

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning, message=".*pandas only supports SQLAlchemy.*")
        with _ensure_conn(conninfo) as conn:
            df = pd.read_sql(
                sql,
                conn,
                params=params,
                dtype={"value": "float64"},
                parse_dates={"knowledge_time": {"utc": True}, "valid_time": {"utc": True}},
            )

    if len(df) == 0:
        return df

    df = df.set_index(["knowledge_time", "valid_time"])
    return df


if __name__ == "__main__":
    conninfo = os.environ["DATABASE_URL"]
    # Example: df = read_overlapping_latest(conninfo, series_id=1)
    print("Run with series_id parameter")
