"""
High-level SDK for TimeDB (TimescaleDB version).

Provides a simple interface for working with TimeDB, including automatic
DataFrame conversion for time series data. All numeric values are treated as
dimensionless floats - unit conversion is the user's responsibility.

The SDK exposes two main classes:
- TimeDataClient: Main entry point for database operations
- SeriesCollection: Fluent API for series filtering and operations

Data model:
- Flat: Immutable fact data (meter readings, measurements). Stored in 'flat' table.
- Overlapping: Versioned estimates (forecasts). Stored in 'overlapping' table
  (list-partitioned by retention) with known_time versioning.
"""
import os
from contextlib import contextmanager
from typing import Optional, List, Tuple, NamedTuple, Dict, Union, Any
from datetime import datetime, timezone
from itertools import repeat
import pandas as pd

from . import db
from .db.series import SeriesRegistry
import psycopg
from psycopg import errors
from psycopg_pool import ConnectionPool


class InsertResult(NamedTuple):
    """Result from insert containing the IDs that were used."""
    batch_id: Optional[int]
    workflow_id: Optional[str]
    series_id: int


class IncompatibleUnitError(ValueError):
    """Raised when units cannot be converted to each other."""
    pass


class SeriesCollection:
    """
    A lazy collection of time series that matches a set of filters.

    SeriesCollection provides a fluent, chainable API for filtering and
    operating on one or more time series without manually managing series IDs.

    The collection resolves which series match the filters only when an
    operation like .read(), .insert(), or .update_records() is called.
    This allows building complex queries progressively.

    Filtering:
        Series are filtered by name, unit, series_id, and labels. You can chain
        multiple .where() calls to add additional label filters.

    Operations:
        Once filtered, the collection supports:
        - read(): Retrieve time series data
        - insert(): Add new data points
        - update_records(): Update existing records
        - count(): Count matching series
        - list_labels(): List unique label values

    Examples:
        >>> from timedb import TimeDataClient
        >>> client = TimeDataClient()

        >>> # Single series with label filter
        >>> client.series('wind_power').where(site='offshore_1').read()

        >>> # Multiple filters (chained)
        >>> client.series(unit='MW').where(site='offshore_1', turbine='T01').read()

        >>> # Direct lookup by series_id
        >>> client.series(series_id=123).read()

        >>> # Count matching series
        >>> count = client.series('wind_power').count()
    """

    def __init__(
        self,
        conninfo: str,
        name: Optional[str] = None,
        unit: Optional[str] = None,
        label_filters: Optional[Dict[str, str]] = None,
        series_id: Optional[int] = None,
        _registry: Optional[SeriesRegistry] = None,
        _pool: Optional[ConnectionPool] = None,
    ):
        self._conninfo = conninfo
        self._name = name
        self._unit = unit
        self._label_filters = label_filters or {}
        self._series_id = series_id
        self._registry = _registry or SeriesRegistry()
        self._resolved = False
        self._pool = _pool

    def where(self, **labels) -> 'SeriesCollection':
        """
        Add additional label filters to narrow down the collection.

        Creates a new SeriesCollection with combined filters. This method is
        chainable and does not modify the original collection (immutable).

        Args:
            **labels: Key-value pairs for label filtering.
                     Example: where(site='offshore_1', turbine='T01')

        Returns:
            SeriesCollection: New collection with combined filters applied

        Example:
            >>> coll = client.series('wind_power')
            >>> # Add filters progressively
            >>> coll = coll.where(site='offshore_1')
            >>> coll = coll.where(turbine='T01')
            >>> df = coll.read()  # Only applies both filters at read time
        """
        new_filters = {**self._label_filters, **labels}
        return SeriesCollection(
            conninfo=self._conninfo,
            name=self._name,
            unit=self._unit,
            label_filters=new_filters,
            series_id=self._series_id,
            _registry=self._registry,
            _pool=self._pool,
        )

    def _resolve_ids(self) -> List[int]:
        """
        Resolve series IDs that match the current filters.
        Delegates to SeriesRegistry for DB query and caching.
        """
        if not self._resolved:
            with _get_connection(self._pool, self._conninfo) as conn:
                self._registry.resolve(
                    conn, name=self._name, unit=self._unit,
                    labels=self._label_filters if self._label_filters else None,
                    series_id=self._series_id,
                )
            self._resolved = True

        # If series_id is specified, return just that (if it exists in cache)
        if self._series_id is not None:
            return [self._series_id] if self._series_id in self._registry.cache else []
        
        # Filter cached entries by label_filters (in-memory sub-filtering for .where())
        if not self._label_filters:
            return list(self._registry.cache.keys())

        matching_ids = []
        filter_items = self._label_filters.items()
        for sid, meta in self._registry.cache.items():
            labels = meta.get("labels", {})
            if all(labels.get(k) == v for k, v in filter_items):
                matching_ids.append(sid)
        return matching_ids

    def _get_single_id(self) -> int:
        """Get a single series ID. Raises error if filters match multiple series."""
        ids = self._resolve_ids()
        if len(ids) == 0:
            raise ValueError(
                f"No series found matching filters: name={self._name}, "
                f"unit={self._unit}, labels={self._label_filters}"
            )
        if len(ids) > 1:
            raise ValueError(
                f"Multiple series ({len(ids)}) match the filters. "
                f"Use more specific filters or call bulk operations. "
                f"Filters: name={self._name}, unit={self._unit}, labels={self._label_filters}"
            )
        return ids[0]

    def _get_series_routing(self, series_id: int) -> Dict[str, Any]:
        """Get routing info (overlapping, retention, table) for single series."""
        with _get_connection(self._pool, self._conninfo) as conn:
            return self._registry.get_routing_single(conn, series_id)

    def insert(
        self,
        df: pd.DataFrame,
        workflow_id: Optional[str] = None,
        batch_start_time: Optional[datetime] = None,
        batch_finish_time: Optional[datetime] = None,
        known_time: Optional[datetime] = None,
        batch_params: Optional[dict] = None,
    ) -> InsertResult:
        """
        Insert time series data for this collection.

        **Single-series inserts only.** DataFrame must have fixed columns:
        - Point-in-time: [valid_time, value]
        - Intervals: [valid_time, valid_time_end, value]

        Automatically routes data to the correct table based on the series' overlapping flag:
        - flat (overlapping=False): inserts into 'flat' table (immutable facts, upsert on conflict)
        - overlapping (overlapping=True): inserts into 'overlapping_{tier}' table with batch and known_time

        Args:
            df: DataFrame with columns [valid_time, value] or [valid_time, valid_time_end, value]
            workflow_id: Workflow identifier (optional)
            batch_start_time: Start time (optional)
            batch_finish_time: Finish time (optional)
            known_time: Time of knowledge (optional, used for overlapping)
            batch_params: Batch parameters (optional)

        Returns:
            InsertResult with batch_id, workflow_id, series_id

        Raises:
            ValueError: If collection matches multiple series (use more specific filters)
            ValueError: If DataFrame doesn't have the required columns
        """
        series_id = self._get_single_id()
        routing = self._get_series_routing(series_id)
        series_unit = self._registry.get_cached(series_id)["unit"]

        return _insert(
            df=df,
            workflow_id=workflow_id,
            batch_start_time=batch_start_time,
            batch_finish_time=batch_finish_time,
            known_time=known_time,
            batch_params=batch_params,
            series_id=series_id,
            routing=routing,
            series_unit=series_unit,
            _pool=self._pool,
            _registry=self._registry,
        )

    def update_records(self, updates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Update records for this collection.
        
        **Single-series only.** Collection must resolve to exactly one series.
        
        Supports both flat and overlapping series:
        - **Flat**: In-place update (no versioning) by (series_id, valid_time)
        - **Overlapping**: Creates new version with known_time=now(). Three lookup methods:
          1. known_time + valid_time: Exact version lookup
          2. batch_id + valid_time: Latest version in that batch
          3. Just valid_time: Latest version overall
        
        Args:
            updates: List of update dicts. Each must include:
                - valid_time (datetime): Required
                - value (float, optional): New value (omit to keep current)
                - annotation (str, optional): Text annotation (None to clear)
                - tags (list[str], optional): Tags ([] to clear)
                - changed_by (str, optional): User identifier
                For overlapping only:
                - batch_id (int, optional): Target specific batch
                - known_time (datetime, optional): Target specific version
        
        Returns:
            List of dicts with update info for each updated record
        
        Raises:
            ValueError: If collection matches multiple series or no series
        
        Example:
            >>> td.series("temperature").where(site="A").update_records([
            ...     {"valid_time": dt, "value": 25.0, "annotation": "Corrected"}
            ... ])
        """
        if not updates:
            return []

        series_id = self._get_single_id()

        # Add series_id to all updates
        filled_updates = []
        for u in updates:
            u_copy = u.copy()
            if "series_id" not in u_copy:
                u_copy["series_id"] = series_id
            filled_updates.append(u_copy)

        return _update_records(filled_updates, _pool=self._pool, _registry=self._registry)

    def read(
        self,
        start_valid: Optional[datetime] = None,
        end_valid: Optional[datetime] = None,
        start_known: Optional[datetime] = None,
        end_known: Optional[datetime] = None,
        versions: bool = False,
        as_pint: bool = False,
    ) -> pd.DataFrame:
        """
        Read time series data for this collection.

        **Single-series reads only.** Collection must resolve to exactly one series.

        Automatically reads from the correct table based on the series' overlapping flag:
        - flat (overlapping=False): reads from 'flat' table (no versioning)
        - overlapping (overlapping=True): reads latest values or all versions (if versions=True)

        Args:
            start_valid: Start of valid time range (optional)
            end_valid: End of valid time range (optional)
            start_known: Start of known_time range (optional, overlapping only)
            end_known: End of known_time range (optional, overlapping only)
            versions: If True, return all overlapping revisions (default: False)
            as_pint: If True, return value column as pint dtype with series unit (default: False).
                Requires pint and pint-pandas: pip install pint pint-pandas

        Returns:
            DataFrame with index (valid_time,) or (known_time, valid_time) for versions,
            and column (value). If as_pint=True, value column has pint dtype.

        Raises:
            ValueError: If collection matches multiple series or no series
            ImportError: If as_pint=True but pint/pint-pandas not installed

        Example:
            >>> # Single series read
            >>> df = td.series("wind_power").where(site="Gotland", turbine="T01").read()
            >>>
            >>> # Read with pint units
            >>> df = td.series("wind_power").where(turbine="T01").read(as_pint=True)
            >>> print(df["value"].dtype)  # pint[MW]
        """
        series_ids = self._resolve_ids()

        if len(series_ids) == 0:
            raise ValueError(
                f"No series found matching filters: name={self._name}, "
                f"unit={self._unit}, labels={self._label_filters}"
            )

        if len(series_ids) > 1:
            # Show which series matched to help user debug
            matched_series = []
            for sid in series_ids[:5]:  # Show first 5
                meta = self._registry.get_cached(sid)
                labels_str = ", ".join(f"{k}={v}" for k, v in meta.get("labels", {}).items())
                matched_series.append(f"{meta['name']} ({labels_str})" if labels_str else meta['name'])

            series_list = ", ".join(matched_series)
            if len(series_ids) > 5:
                series_list += f", ... ({len(series_ids) - 5} more)"

            raise ValueError(
                f"Collection matches {len(series_ids)} series. "
                f"Single-series reads only. Use more specific filters to match exactly one series.\n"
                f"Matched series: [{series_list}]\n"
                f"Tip: Use .where(label_key='value') to narrow down."
            )

        series_id = series_ids[0]
        meta = self._registry.get_cached(series_id)
        is_overlapping = meta["overlapping"]

        if not is_overlapping:
            df = _read_flat(
                series_id=series_id,
                start_valid=start_valid,
                end_valid=end_valid,
                _pool=self._pool,
            )
        elif versions:
            df = _read_overlapping_all(
                series_id=series_id,
                start_valid=start_valid,
                end_valid=end_valid,
                start_known=start_known,
                end_known=end_known,
                _pool=self._pool,
            )
        else:
            df = _read_overlapping_latest(
                series_id=series_id,
                start_valid=start_valid,
                end_valid=end_valid,
                start_known=start_known,
                end_known=end_known,
                _pool=self._pool,
            )

        if as_pint and len(df) > 0:
            try:
                import pint
                import pint_pandas  # noqa: F401
            except ImportError:
                raise ImportError(
                    "as_pint=True requires pint and pint-pandas. "
                    "Install with: pip install pint pint-pandas"
                )
            ureg = pint.application_registry.get()
            pa = pint_pandas.PintArray.from_1darray_quantity(
                pint.Quantity(df["value"].values, ureg(meta["unit"]))
            )
            df["value"] = pa

        return df

    def list_labels(self, label_key: str) -> List[str]:
        """List all unique values for a specific label key in this collection."""
        ids = set(self._resolve_ids())
        values = set()
        for sid in ids:
            meta = self._registry.get_cached(sid)
            if meta:
                labels = meta.get("labels", {})
                if label_key in labels:
                    values.add(labels[label_key])
        return sorted(list(values))

    def list_series(self) -> List[Dict[str, Any]]:
        """List all series matching the current filters with their metadata.

        Returns:
            List of dicts, each containing:
            - series_id: int
            - name: str
            - unit: str
            - labels: dict
            - description: str (optional)
            - overlapping: bool
            - retention: str

        Example:
            >>> client.series('wind_power').where(site='Gotland').list_series()
            [
                {'series_id': 1, 'name': 'wind_power', 'unit': 'MW',
                 'labels': {'turbine': 'T01', 'site': 'Gotland', 'type': 'onshore'},
                 'description': 'Onshore wind turbine power output',
                 'overlapping': False, 'retention': 'medium'},
                ...
            ]
        """
        ids = self._resolve_ids()
        result = []
        for sid in ids:
            meta = self._registry.get_cached(sid)
            if meta:
                result.append({
                    "series_id": sid,
                    "name": meta["name"],
                    "unit": meta["unit"],
                    "labels": meta["labels"],
                    "description": meta.get("description"),
                    "overlapping": meta["overlapping"],
                    "retention": meta["retention"],
                })
        return result

    def count(self) -> int:
        """Count how many series match the current filters."""
        return len(self._resolve_ids())

    def __repr__(self) -> str:
        return (
            f"SeriesCollection(name={self._name!r}, unit={self._unit!r}, "
            f"series_id={self._series_id!r}, labels={self._label_filters!r}, resolved={self._resolved})"
        )


class TimeDataClient:
    """
    High-level client for TimeDB with fluent API for series selection.

    The TimeDataClient provides the main entry point for working with timedb.
    It supports:

    - Creating and deleting database schema
    - Creating new time series with labels and metadata
    - Building fluent queries to filter, read, and update series data

    Example:
        >>> from timedb import TimeDataClient
        >>> import pandas as pd
        >>> from datetime import datetime, timezone

        >>> # Create client and schema
        >>> td = TimeDataClient()
        >>> td.create()

        >>> # Create a series
        >>> td.create_series('wind_power', unit='MW', labels={'site': 'offshore_1'})

        >>> # Insert and read data using fluent API
        >>> df = pd.DataFrame({
        ...     'valid_time': [datetime.now(timezone.utc)],
        ...     'wind_power': [100.0]
        ... })
        >>> td.series('wind_power').where(site='offshore_1').insert(df)
        >>> result = td.series('wind_power').where(site='offshore_1').read()

    Environment:
        Requires TIMEDB_DSN or DATABASE_URL environment variable
        to connect to the PostgreSQL database.
    """

    def __init__(self, conninfo: Optional[str] = None, min_size: int = 2, max_size: int = 10):
        self._conninfo = conninfo or _get_conninfo()
        self._pool = ConnectionPool(self._conninfo, min_size=min_size, max_size=max_size, open=True)

    def close(self):
        """Close the connection pool."""
        self._pool.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def series(
        self,
        name: Optional[str] = None,
        unit: Optional[str] = None,
        series_id: Optional[int] = None,
    ) -> SeriesCollection:
        """
        Start building a series collection by name, unit, and/or series_id.

        Creates a lazy SeriesCollection that can be further filtered using
        .where() to add label-based filters. The collection resolves to the
        actual series only when an operation like .read() or .insert() is called.

        Args:
            name: Optional series name to filter by (e.g., 'wind_power')
            unit: Optional unit to filter by (e.g., 'MW')
            series_id: Optional series_id for direct lookup (e.g., 123)

        Returns:
            SeriesCollection: A lazy collection that can be further filtered
                with .where() and then used for read/insert/update operations

        Example:
            >>> client = TimeDataClient()
            >>> # Get a specific series by name and labels
            >>> client.series('wind_power').where(site='offshore_1').read()
            >>> # Get all series with unit 'MW'
            >>> client.series(unit='MW').read()
            >>> # Get series by ID (if you know it)
            >>> client.series(series_id=123).read()
        """
        return SeriesCollection(
            conninfo=self._conninfo,
            name=name,
            unit=unit,
            series_id=series_id,
            _pool=self._pool,
        )

    def create(
        self,
        retention=None,
        *,
        retention_short: str = "6 months",
        retention_medium: str = "3 years",
        retention_long: str = "5 years",
    ) -> None:
        """
        Create database schema (TimescaleDB version).

        Args:
            retention: Shorthand to set the default (medium) retention period.
                       An int is interpreted as years (e.g., 5 → "5 years").
                       A string is used as-is (e.g., "18 months").
            retention_short: Retention for overlapping_short (default: "6 months")
            retention_medium: Retention for overlapping_medium (default: "3 years")
            retention_long: Retention for overlapping_long (default: "5 years")
        """
        if retention is not None:
            if isinstance(retention, int):
                retention_medium = f"{retention} years"
            else:
                retention_medium = str(retention)
        _create(
            retention_short=retention_short,
            retention_medium=retention_medium,
            retention_long=retention_long,
        )

    def delete(self) -> None:
        """Delete database schema."""
        _delete()

    def create_series(
        self,
        name: str,
        unit: str = "dimensionless",
        labels: Optional[Dict[str, str]] = None,
        description: Optional[str] = None,
        overlapping: bool = False,
        retention: str = "medium",
    ) -> int:
        """
        Create a new time series with metadata and labels.

        Creates a new series in the database with the specified configuration.
        Each series has a unique name+labels combination.

        Args:
            name (str):
                Series name/identifier (e.g., 'wind_power', 'solar_irradiance').
                Human-readable identifier for the measurement.

            unit (str, default="dimensionless"):
                Canonical unit for the series. Examples:

                - 'MW' - megawatts (power)
                - 'kWh' - kilowatt-hours (energy)
                - 'C' - celsius (temperature)
                - 'dimensionless' - unitless values

            labels (dict, optional):
                Dictionary of key-value labels to differentiate series with same
                name. Example: {"site": "Gotland", "turbine": "T01"}
                Enables filtering and organization of related series.

            description (str, optional):
                Human-readable description of the series and its contents.

            overlapping (bool, default=False):
                Whether this series stores versioned/revised data:

                - False: Flat/immutable facts (e.g., meter readings, historical data)
                - True: Versioned/revised data (e.g., forecasts, estimates)
                  with known_time tracking for changes over time

            retention (str, default="medium"):
                Data retention policy (overlapping series only):

                - 'short': 6 months (fast queries on recent data)
                - 'medium': 3 years (balanced for forecasts)
                - 'long': 5 years (historical archival)

        Returns:
            int: The unique series_id for this series, used in read/write operations

        Raises:
            ValueError: If series with same name+labels already exists

        Example:
            >>> client = TimeDataClient()
            >>> # Create a flat series for meter readings
            >>> series_id = client.create_series(
            ...     name='power_consumption',
            ...     unit='kWh',
            ...     labels={'building': 'main', 'floor': '3'},
            ...     description='Power consumption for floor 3 of main building',
            ... )
            >>> # Create an overlapping series for weather forecasts
            >>> series_id = client.create_series(
            ...     name='wind_speed',
            ...     unit='m/s',
            ...     labels={'site': 'offshore_1'},
            ...     overlapping=True,
            ...     retention='medium'
            ... )
        """
        return _create_series(
            name=name,
            unit=unit,
            labels=labels,
            description=description,
            overlapping=overlapping,
            retention=retention,
        )

# =============================================================================
# Internal helper functions (not part of public API)
# =============================================================================

def _get_conninfo() -> str:
    """Get database connection string from environment variables."""
    conninfo = os.environ.get("TIMEDB_DSN") or os.environ.get("DATABASE_URL")
    if not conninfo:
        raise ValueError(
            "Database connection not configured. Set TIMEDB_DSN or DATABASE_URL environment variable."
        )
    return conninfo


@contextmanager
def _get_connection(_pool: Optional[ConnectionPool] = None, conninfo: Optional[str] = None):
    """
    Context manager that yields a database connection.
    
    Uses connection pool if available, otherwise creates a new connection.
    
    Args:
        _pool: Optional connection pool
        conninfo: Optional connection string (fetched via _get_conninfo() if not provided)
    
    Yields:
        psycopg.Connection
    """
    if _pool is not None:
        with _pool.connection() as conn:
            yield conn
    else:
        if conninfo is None:
            conninfo = _get_conninfo()
        with psycopg.connect(conninfo) as conn:
            yield conn


def _resolve_pint_values(value_series: pd.Series, series_unit: str) -> pd.Series:
    """Strip pint dtype from value series, converting units if needed.

    Rules:
    - Plain float64 (no pint dtype): pass through unchanged, no unit check
    - Pint dimensionless: treat as series unit, strip to plain float64
    - Pint with unit: convert to series_unit if compatible, error if not
    - Same unit: strip to plain float64 (no conversion needed)

    Returns:
        Plain float64 Series (no pint dtype)

    Raises:
        IncompatibleUnitError: if units are not convertible
        ImportError: if pint array detected but pint not installed
    """
    dtype = value_series.dtype

    # No pint dtype → pass through (backward compatible, no unit check)
    if not hasattr(dtype, 'units'):
        return value_series

    # Pint dtype detected — need pint for conversion
    try:
        import pint
    except ImportError:
        raise ImportError(
            "Pint array detected but 'pint' is not installed. "
            "Install with: pip install pint"
        )

    source_unit = str(dtype.units)

    # Extract magnitudes (numpy array, no copy if already float64)
    magnitudes = value_series.values.quantity.magnitude

    # Dimensionless or same unit → no conversion needed
    ureg = pint.application_registry.get()
    if ureg.dimensionless == ureg.Unit(source_unit) or source_unit == series_unit:
        return pd.Series(magnitudes, index=value_series.index, name=value_series.name)

    # Check compatibility and convert
    try:
        factor = ureg.Quantity(1, source_unit).to(series_unit).magnitude
    except pint.DimensionalityError:
        raise IncompatibleUnitError(
            f"Cannot convert '{source_unit}' to series unit '{series_unit}'. "
            f"Units are not dimensionally compatible."
        )

    converted = magnitudes * factor
    return pd.Series(converted, index=value_series.index, name=value_series.name)


def _validate_df_columns(df: pd.DataFrame) -> bool:
    """Validate DataFrame column structure. Returns True if interval data, False if point-in-time."""
    cols = list(df.columns)
    if len(cols) == 2:
        if cols != ['valid_time', 'value']:
            raise ValueError(
                f"For point-in-time data, DataFrame must have columns ['valid_time', 'value']. "
                f"Found: {cols}"
            )
        return False
    elif len(cols) == 3:
        if cols != ['valid_time', 'valid_time_end', 'value']:
            raise ValueError(
                f"For interval data, DataFrame must have columns ['valid_time', 'valid_time_end', 'value']. "
                f"Found: {cols}"
            )
        return True
    else:
        raise ValueError(
            f"DataFrame must have 2 columns ['valid_time', 'value'] or 3 columns ['valid_time', 'valid_time_end', 'value']. "
            f"Found {len(cols)} columns: {cols}"
        )


def _dataframe_to_value_rows(
    df: pd.DataFrame,
    series_id: int,
) -> List[Tuple]:
    """
    Convert a pandas DataFrame to TimeDB value_rows format.
    All numeric values are treated as dimensionless floats.

    DataFrame must have fixed columns:
    - Point-in-time: [valid_time, value]
    - Intervals: [valid_time, valid_time_end, value]

    Args:
        df: DataFrame with columns [valid_time, value] or [valid_time, valid_time_end, value]
        series_id: Series ID for all rows

    Returns:
        List of tuples:
        - Point-in-time: (valid_time, series_id, value)
        - Interval: (valid_time, valid_time_end, series_id, value)
    """
    has_intervals = _validate_df_columns(df)

    # Validate timezone on the column dtype or first value (once, not per-row)
    vt_series = df['valid_time']
    if hasattr(vt_series.dtype, 'tz'):
        if vt_series.dtype.tz is None:
            raise ValueError("valid_time must be timezone-aware. Found timezone-naive datetime.")
    elif len(vt_series) > 0:
        first_vt = vt_series.iloc[0]
        if isinstance(first_vt, (pd.Timestamp, datetime)) and first_vt.tzinfo is None:
            raise ValueError("valid_time must be timezone-aware. Found timezone-naive datetime.")

    if has_intervals:
        vte_series = df['valid_time_end']
        if hasattr(vte_series.dtype, 'tz'):
            if vte_series.dtype.tz is None:
                raise ValueError("valid_time_end must be timezone-aware. Found timezone-naive datetime.")
        elif len(vte_series) > 0:
            first_vte = vte_series.iloc[0]
            if isinstance(first_vte, (pd.Timestamp, datetime)) and first_vte.tzinfo is None:
                raise ValueError("valid_time_end must be timezone-aware. Found timezone-naive datetime.")

    # Convert time columns to numpy arrays once (much faster than repeated .iloc access)
    vt_array = df['valid_time'].values
    vte_array = df['valid_time_end'].values if has_intervals else None
    n_rows = len(df)
    
    # Convert datetime64 to Python datetime objects (psycopg3 requires this)
    vt_list = [pd.Timestamp(x).to_pydatetime() for x in vt_array]
    vte_list = [pd.Timestamp(x).to_pydatetime() for x in vte_array] if has_intervals else None

    # Convert value column to float array (vectorized operation)
    values = df['value'].astype(float).values
    
    # Convert NaN to None (vectorized check with list comprehension)
    values_clean = [None if pd.isna(v) else v for v in values]
    
    if has_intervals:
        # Use zip with itertools.repeat - avoids creating intermediate list
        rows = list(zip(vt_list, vte_list, repeat(series_id, n_rows), values_clean))
    else:
        rows = list(zip(vt_list, repeat(series_id, n_rows), values_clean))

    return rows


def _create(
    retention_short: str = "6 months",
    retention_medium: str = "3 years",
    retention_long: str = "5 years",
) -> None:
    """Create or update the database schema (TimescaleDB version)."""
    conninfo = _get_conninfo()
    db.create.create_schema(
        conninfo,
        retention_short=retention_short,
        retention_medium=retention_medium,
        retention_long=retention_long,
    )


def _create_series(
    name: str,
    unit: str = "dimensionless",
    labels: Optional[Dict[str, str]] = None,
    description: Optional[str] = None,
    overlapping: bool = False,
    retention: str = "medium",
) -> int:
    """
    Create a new time series.

    Args:
        name: Parameter name (e.g., 'wind_power', 'temperature')
        unit: Canonical unit for the series
        labels: Dictionary of labels
        description: Optional description
        overlapping: Whether this series stores versioned data (default: False)
        retention: 'short', 'medium', or 'long' (default: 'medium')

    Returns:
        The series_id (int) for the newly created series
    """
    conninfo = _get_conninfo()

    try:
        with psycopg.connect(conninfo) as conn:
            return db.series.create_series(
                conn,
                name=name,
                unit=unit,
                labels=labels,
                description=description,
                overlapping=overlapping,
                retention=retention,
            )
    except (errors.UndefinedTable, errors.UndefinedObject) as e:
        raise ValueError(
            "TimeDB tables do not exist. Please create the schema first by running:\n"
            "  td.create()"
        ) from e


def _delete() -> None:
    """Delete all TimeDB tables and views."""
    conninfo = _get_conninfo()
    db.delete.delete_schema(conninfo)


def _read_flat(
    series_id: int,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    _pool: Optional[ConnectionPool] = None,
) -> pd.DataFrame:
    """Read flat values for a single series."""
    conninfo = _get_conninfo()

    with _get_connection(_pool, conninfo) as conn:
        df = db.read.read_flat(
            conn,
            series_id=series_id,
            start_valid=start_valid,
            end_valid=end_valid,
        )

    return df


def _read_overlapping_latest(
    series_id: int,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None,
    end_known: Optional[datetime] = None,
    _pool: Optional[ConnectionPool] = None,
) -> pd.DataFrame:
    """Read latest overlapping values for a single series."""
    conninfo = _get_conninfo()

    with _get_connection(_pool, conninfo) as conn:
        df = db.read.read_overlapping_latest(
            conn,
            series_id=series_id,
            start_valid=start_valid,
            end_valid=end_valid,
            start_known=start_known,
            end_known=end_known,
        )

    return df


def _read_overlapping_all(
    series_id: int,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None,
    end_known: Optional[datetime] = None,
    _pool: Optional[ConnectionPool] = None,
) -> pd.DataFrame:
    """Read all overlapping versions for a single series."""
    conninfo = _get_conninfo()

    with _get_connection(_pool, conninfo) as conn:
        df = db.read.read_overlapping_all(
            conn,
            series_id=series_id,
            start_valid=start_valid,
            end_valid=end_valid,
            start_known=start_known,
            end_known=end_known,
        )

    return df


def _insert(
    df: pd.DataFrame,
    workflow_id: Optional[str] = None,
    batch_start_time: Optional[datetime] = None,
    batch_finish_time: Optional[datetime] = None,
    known_time: Optional[datetime] = None,
    batch_params: Optional[dict] = None,
    series_id: int = None,
    routing: Optional[Dict[str, Any]] = None,
    series_unit: str = "dimensionless",
    _pool: Optional[ConnectionPool] = None,
    _registry: Optional[SeriesRegistry] = None,
) -> InsertResult:
    """
    Insert time series data from a pandas DataFrame.

    DataFrame must have columns [valid_time, value] or [valid_time, valid_time_end, value].
    Routes data to the correct table based on series overlapping flag and retention.
    Supports single-series inserts only.
    Requires series to exist — use td.create_series() first.
    """
    conninfo = _get_conninfo()

    if workflow_id is None:
        workflow_id = "sdk-workflow"
    if batch_start_time is None:
        batch_start_time = datetime.now(timezone.utc)
    elif batch_start_time.tzinfo is None:
        raise ValueError("batch_start_time must be timezone-aware")

    if series_id is None:
        raise ValueError("series_id must be provided")

    if routing is None:
        raise ValueError("routing must be provided")

    # Validate columns before accessing df['value'] to give a clear error message
    _validate_df_columns(df)

    # Resolve pint units before row conversion (only copy if pint conversion happened)
    resolved = _resolve_pint_values(df['value'], series_unit)
    if resolved is not df['value']:
        df = df.copy()
        df['value'] = resolved

    value_rows = _dataframe_to_value_rows(
        df=df,
        series_id=series_id,
    )

    try:
        with _get_connection(_pool, conninfo) as conn:
            batch_id = db.insert.insert_values(
                conninfo=conn,
                workflow_id=workflow_id,
                batch_start_time=batch_start_time,
                batch_finish_time=batch_finish_time,
                value_rows=value_rows,
                known_time=known_time,
                batch_params=batch_params,
                series_id=series_id,
                routing=routing,
            )
    except (errors.UndefinedTable, errors.UndefinedObject) as e:
        raise ValueError(
            "TimeDB tables do not exist. Please create the schema first by running:\n"
            "  td.create()"
        ) from e

    return InsertResult(
        batch_id=batch_id,
        workflow_id=workflow_id,
        series_id=series_id,
    )



def _update_records(
    updates: List[Dict[str, Any]],
    _pool: Optional[ConnectionPool] = None,
    _registry: Optional[SeriesRegistry] = None,
) -> List[Dict[str, Any]]:
    """
    Update records (flat or overlapping).

    Wrapper around db.update.update_records that handles the database connection.
    """
    registry = _registry or SeriesRegistry()
    with _get_connection(_pool) as conn:
        return db.update.update_records(conn, registry, updates=updates)
