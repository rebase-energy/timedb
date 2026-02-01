"""
High-level SDK for TimeDB (TimescaleDB version).

Provides a simple interface for working with TimeDB, including automatic
DataFrame conversion for time series data with unit handling using Pint Quantity objects.

The SDK exposes two main classes:
- TimeDataClient: Main entry point for database operations
- SeriesCollection: Fluent API for series filtering and operations

Key differences from the standard PostgreSQL version:
- No is_current flag - latest values determined by known_time
- known_time is stored directly on values_table (denormalized from batches)
- Two read methods: read() for latest values, read_overlapping() for all revisions
"""
import os
import uuid
from typing import Optional, List, Tuple, NamedTuple, Dict, Union, Any, FrozenSet
from datetime import datetime, timezone
import pandas as pd

from dotenv import load_dotenv, find_dotenv
from . import db
from .units import (
    convert_to_canonical_unit,
    convert_quantity_to_canonical_unit,
    IncompatibleUnitError,
    extract_unit_from_quantity,
    extract_value_from_quantity,
    is_pint_pandas_series,
    extract_unit_from_pint_pandas_series,
)
import pint
import psycopg
from psycopg import errors

load_dotenv(find_dotenv())

# Default tenant ID for single-tenant installations (all zeros UUID)
DEFAULT_TENANT_ID = uuid.UUID('00000000-0000-0000-0000-000000000000')


class InsertResult(NamedTuple):
    """Result from insert_batch containing the IDs that were used."""
    batch_id: uuid.UUID
    workflow_id: str
    series_ids: Dict[str, uuid.UUID]  # Maps name to series_id
    tenant_id: uuid.UUID


class SeriesCollection:
    """
    A lazy collection of time series that matches a set of filters.

    This class acts as a proxy for one or more series, allowing fluent
    filtering and operations without requiring the user to manage series IDs.
    Resolution of series IDs happens just-in-time when an action is performed.
    """

    def __init__(
        self,
        conninfo: str,
        name: Optional[str] = None,
        unit: Optional[str] = None,
        label_filters: Optional[Dict[str, str]] = None,
        _id_cache: Optional[Dict[FrozenSet[Tuple[str, str]], uuid.UUID]] = None,
    ):
        """
        Initialize a SeriesCollection.

        Args:
            conninfo: Database connection string
            name: Series name filter (optional)
            unit: Series unit filter (optional)
            label_filters: Dictionary of label filters (optional)
            _id_cache: Internal cache of resolved series IDs (private)
        """
        self._conninfo = conninfo
        self._name = name
        self._unit = unit
        self._label_filters = label_filters or {}
        self._id_cache = _id_cache or {}
        self._resolved = False

    def where(self, **labels) -> 'SeriesCollection':
        """
        Add additional label filters to narrow down the collection.

        Returns a new SeriesCollection with the combined filters (immutable).

        Args:
            **labels: Label key-value pairs to filter by

        Returns:
            New SeriesCollection with additional filters

        Example:
            # Start broad
            wind = td.series("wind_power")

            # Narrow down step by step
            gotland_wind = wind.where(site="Gotland")
            onshore_gotland = gotland_wind.where(type="onshore")

            # Or chain them
            specific = wind.where(site="Gotland").where(turbine="T01")
        """
        # Combine existing filters with new ones
        new_filters = {**self._label_filters, **labels}

        return SeriesCollection(
            conninfo=self._conninfo,
            name=self._name,
            unit=self._unit,
            label_filters=new_filters,
            _id_cache=self._id_cache.copy(),  # Share cache across filters
        )

    def _resolve_ids(self) -> List[uuid.UUID]:
        """
        Resolve series IDs that match the current filters.

        This is called just-in-time when an action is performed.
        Uses internal cache to avoid repeated database queries.

        Returns:
            List of series UUIDs that match the filters
        """
        if not self._resolved:
            # Query `series_table` directly for discovery (no SDK-level get_mapping)
            import psycopg
            import json
            with psycopg.connect(self._conninfo) as conn:
                with conn.cursor() as cur:
                    sql = "SELECT series_id, labels FROM series_table"
                    clauses: list = []
                    params: list = []
                    if self._name:
                        clauses.append("name = %s")
                        params.append(self._name)
                    if self._unit:
                        clauses.append("unit = %s")
                        params.append(self._unit)
                    if self._label_filters:
                        clauses.append("labels @> %s::jsonb")
                        params.append(json.dumps(self._label_filters))
                    if clauses:
                        sql += " WHERE " + " AND ".join(clauses)
                    sql += " ORDER BY name, unit, series_id"
                    cur.execute(sql, params)
                    rows = cur.fetchall()

            # Update cache with newly resolved IDs (map frozenset(label tuples) -> series_id)
            for series_id, labels in rows:
                label_set = frozenset((k, v) for k, v in (labels or {}).items())
                self._id_cache[label_set] = series_id
            self._resolved = True

        # Return list of IDs that match current filters
        if not self._label_filters:
            # No label filters - return all cached IDs
            return list(self._id_cache.values())

        # Filter cached IDs by current label filters
        matching_ids = []
        filter_set = set(self._label_filters.items())

        for label_set, series_id in self._id_cache.items():
            # Check if all filter labels are present in this series
            if filter_set.issubset(label_set):
                matching_ids.append(series_id)

        return matching_ids

    def _get_single_id(self) -> uuid.UUID:
        """
        Get a single series ID. Raises error if filters match multiple series.

        Returns:
            Single series UUID

        Raises:
            ValueError: If filters match zero or multiple series
        """
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

    def insert_batch(
        self,
        df: pd.DataFrame,
        tenant_id: Optional[uuid.UUID] = None,
        batch_id: Optional[uuid.UUID] = None,
        workflow_id: Optional[str] = None,
        batch_start_time: Optional[datetime] = None,
        batch_finish_time: Optional[datetime] = None,
        valid_time_col: str = 'valid_time',
        valid_time_end_col: Optional[str] = None,
        known_time: Optional[datetime] = None,
        batch_params: Optional[dict] = None,
    ) -> InsertResult:
        """
        Insert time series data for this collection.

        If the collection matches a single series, inserts data for that series.
        If it matches multiple series, the DataFrame must contain columns matching
        the series names or you must provide explicit column-to-series mapping.

        Args:
            df: DataFrame with time series data
            tenant_id: Tenant UUID (optional)
            batch_id: Batch UUID (optional)
            workflow_id: Workflow identifier (optional)
            batch_start_time: Start time (optional)
            batch_finish_time: Finish time (optional)
            valid_time_col: Valid time column name
            valid_time_end_col: Valid time end column (for intervals)
            known_time: Time of knowledge (optional)
            batch_params: Batch parameters (optional)

        Returns:
            InsertResult with batch_id, workflow_id, series_ids, tenant_id

        Examples:
            # Single series (filters resolve to one series)
            td.series("wind_power").where(site="Gotland", turbine="T01").insert_batch(df)

            # Multiple series (DataFrame columns must match)
            gotland = td.series("wind_power").where(site="Gotland")
            gotland.insert_batch(df)  # df should have columns for each turbine
        """
        # Resolve series IDs just-in-time
        series_ids = self._resolve_ids()

        if len(series_ids) == 0:
            raise ValueError(
                f"No series found matching filters: name={self._name}, "
                f"unit={self._unit}, labels={self._label_filters}"
            )

        if len(series_ids) == 1:
            # Single series - simple case
            series_id = series_ids[0]

            # Detect series info from DataFrame
            series_info = _detect_series_from_dataframe(
                df=df,
                valid_time_col=valid_time_col,
                valid_time_end_col=valid_time_end_col,
            )

            if len(series_info) != 1:
                raise ValueError(
                    f"DataFrame has {len(series_info)} series columns, "
                    f"but collection resolves to 1 series. "
                    f"Expected exactly 1 data column (excluding time columns)."
                )

            # Get the single data column name
            col_name = list(series_info.keys())[0]

            # Use the global insert_batch with explicit series_id
            return _insert_batch(
                df=df,
                tenant_id=tenant_id,
                batch_id=batch_id,
                workflow_id=workflow_id,
                batch_start_time=batch_start_time,
                batch_finish_time=batch_finish_time,
                valid_time_col=valid_time_col,
                valid_time_end_col=valid_time_end_col,
                known_time=known_time,
                batch_params=batch_params,
                series_ids={col_name: series_id},
            )

        else:
            # Multiple series - DataFrame should have columns for each
            # For now, delegate to global insert_batch
            # It will auto-detect or use name_overrides if needed
            return _insert_batch(
                df=df,
                tenant_id=tenant_id,
                batch_id=batch_id,
                workflow_id=workflow_id,
                batch_start_time=batch_start_time,
                batch_finish_time=batch_finish_time,
                valid_time_col=valid_time_col,
                valid_time_end_col=valid_time_end_col,
                known_time=known_time,
                batch_params=batch_params,
            )

    def update_records(self, updates: List[Dict[str, Any]]) -> Dict[str, List]:
        """
        Collection-scoped wrapper around the module-level `update_records` helper.

        Behavior:
        - If the collection resolves to a single series and an update does not
          include `series_id`, it will be filled automatically from the collection.
        - For key-based updates (not using `value_id`), `tenant_id` will default
          to `DEFAULT_TENANT_ID` when not provided (consistent with module-level helper).
        - Updates that specify `value_id` are forwarded unchanged.

        Args:
            updates: List of update dictionaries (see module-level `update_records` docstring)

        Returns:
            The same dictionary structure as `update_records`:
            {'updated': [...], 'skipped_no_ops': [...]}.
        """
        if not updates:
            return {"updated": [], "skipped_no_ops": []}

        # Resolve series IDs according to current collection filters
        series_ids = self._resolve_ids()
        if len(series_ids) == 0:
            raise ValueError(
                f"No series found matching filters: name={self._name}, unit={self._unit}, labels={self._label_filters}"
            )

        single_series = len(series_ids) == 1
        filled_updates: List[Dict[str, Any]] = []

        for u in updates:
            u_copy = u.copy()

            # If value_id is provided, forward the update unchanged
            if "value_id" in u_copy:
                filled_updates.append(u_copy)
                continue

            # Key-based updates must include a series_id. If the collection resolves
            # to a single series and no series_id is provided, fill it in for the user.
            if "series_id" not in u_copy:
                if single_series:
                    u_copy["series_id"] = series_ids[0]
                else:
                    raise ValueError(
                        "For collections matching multiple series, each update must include 'series_id' or use 'value_id'."
                    )

            # Default tenant_id for key-based updates when omitted
            if "tenant_id" not in u_copy:
                u_copy["tenant_id"] = DEFAULT_TENANT_ID

            filled_updates.append(u_copy)

        # Delegate to module-level update_records which handles DB connection and execution
        return _update_records(filled_updates)

    def read(
        self,
        tenant_id: Optional[uuid.UUID] = None,
        start_valid: Optional[datetime] = None,
        end_valid: Optional[datetime] = None,
        start_known: Optional[datetime] = None,
        end_known: Optional[datetime] = None,
        return_mapping: bool = False,
    ) -> Union[pd.DataFrame, Tuple[pd.DataFrame, Dict[uuid.UUID, str]]]:
        """
        Read time series data for this collection (latest values only).

        Returns the latest value for each (valid_time, series_id) combination,
        determined by the most recent known_time.

        Args:
            tenant_id: Tenant UUID filter (optional)
            start_valid: Start of valid time range (optional)
            end_valid: End of valid time range (optional)
            start_known: Start of known_time range (optional)
            end_known: End of known_time range (optional)
            return_mapping: Return (DataFrame, mapping_dict) if True

        Returns:
            DataFrame (or tuple of DataFrame and mapping dict)

        Examples:
            # Read single series
            df = td.series("wind_power").where(site="Gotland", turbine="T01").read()

            # Read all matching series
            df = td.series("wind_power").where(site="Gotland").read()
        """
        # Resolve series IDs just-in-time
        series_ids = self._resolve_ids()

        if len(series_ids) == 0:
            raise ValueError(
                f"No series found matching filters: name={self._name}, "
                f"unit={self._unit}, labels={self._label_filters}"
            )

        return _read(
            series_ids=series_ids,
            tenant_id=tenant_id,
            start_valid=start_valid,
            end_valid=end_valid,
            start_known=start_known,
            end_known=end_known,
            return_mapping=return_mapping,
        )

    def read_overlapping(
        self,
        tenant_id: Optional[uuid.UUID] = None,
        start_valid: Optional[datetime] = None,
        end_valid: Optional[datetime] = None,
        start_known: Optional[datetime] = None,
        end_known: Optional[datetime] = None,
        return_mapping: bool = False,
    ) -> Union[pd.DataFrame, Tuple[pd.DataFrame, Dict[uuid.UUID, str]]]:
        """
        Read time series data in overlapping mode (all forecast revisions).

        Returns all versions of forecasts, showing how predictions evolve over time.
        This is useful for analyzing forecast revisions and backtesting.

        Args:
            tenant_id: Tenant UUID filter (optional)
            start_valid: Start of valid time range (optional)
            end_valid: End of valid time range (optional)
            start_known: Start of known_time range (optional)
            end_known: End of known_time range (optional)
            return_mapping: Return (DataFrame, mapping_dict) if True

        Returns:
            DataFrame with (known_time, valid_time) MultiIndex
            (or tuple of DataFrame and mapping dict if return_mapping=True)

        Examples:
            # Read all forecast revisions for a specific series
            df = td.series("forecast").where(type="power").read_overlapping()

            # Read with time filters
            df = td.series("forecast").read_overlapping(
                start_valid=datetime(2025, 1, 1, tzinfo=timezone.utc),
                end_valid=datetime(2025, 1, 4, tzinfo=timezone.utc)
            )
        """
        # Resolve series IDs just-in-time
        series_ids = self._resolve_ids()

        if len(series_ids) == 0:
            raise ValueError(
                f"No series found matching filters: name={self._name}, "
                f"unit={self._unit}, labels={self._label_filters}"
            )

        return _read_overlapping(
            series_ids=series_ids,
            tenant_id=tenant_id,
            start_valid=start_valid,
            end_valid=end_valid,
            start_known=start_known,
            end_known=end_known,
            return_mapping=return_mapping,
        )

    def list_labels(self, label_key: str) -> List[str]:
        """
        List all unique values for a specific label key in this collection.

        Useful for exploration and discovering what series exist.

        Args:
            label_key: The label key to list values for

        Returns:
            List of unique label values

        Example:
            # See all sites with wind power
            sites = td.series("wind_power").list_labels("site")
            # ['Gotland', 'Skane', ...]

            # See all turbines at Gotland
            turbines = td.series("wind_power").where(site="Gotland").list_labels("turbine")
            # ['T01', 'T02', ...]
        """
        # Force resolution to get all matching series
        self._resolve_ids()

        # Extract label values from cache
        values = set()
        for label_set in self._id_cache.keys():
            for key, value in label_set:
                if key == label_key:
                    values.add(value)

        return sorted(list(values))

    def count(self) -> int:
        """
        Count how many series match the current filters.

        Returns:
            Number of matching series

        Example:
            # How many wind turbines at Gotland?
            count = td.series("wind_power").where(site="Gotland").count()
        """
        return len(self._resolve_ids())

    def __repr__(self) -> str:
        """String representation for debugging."""
        return (
            f"SeriesCollection(name={self._name!r}, unit={self._unit!r}, "
            f"labels={self._label_filters!r}, resolved={self._resolved})"
        )


class TimeDataClient:
    """
    High-level client for TimeDB with fluent API for series selection.

    This class provides a clean interface for working with time series
    without requiring users to manage series IDs directly.
    """

    def __init__(self):
        """Initialize the TimeDB client."""
        self._conninfo = _get_conninfo()

    def series(
        self,
        name: Optional[str] = None,
        unit: Optional[str] = None,
    ) -> SeriesCollection:
        """
        Start building a series collection by name and/or unit.

        Returns a SeriesCollection that can be further filtered using .where().

        Args:
            name: Series name (e.g., 'wind_power')
            unit: Series unit (e.g., 'MW')

        Returns:
            SeriesCollection that can be filtered and used for operations

        Examples:
            # Get a collection of all wind power series
            wind = td.series("wind_power")

            # Filter by unit
            wind_mw = td.series("wind_power", unit="MW")

            # Further filter by labels
            gotland = wind.where(site="Gotland")

            # Chain filters
            specific = wind.where(site="Gotland").where(turbine="T01")

            # Perform actions
            specific.insert_batch(df)
            df = gotland.read()
        """
        return SeriesCollection(
            conninfo=self._conninfo,
            name=name,
            unit=unit,
        )

    def create(self) -> None:
        """Create database schema (TimescaleDB version)."""
        _create()

    def delete(self) -> None:
        """Delete database schema."""
        _delete()

    def create_series(
        self,
        name: str,
        unit: str = "dimensionless",
        labels: Optional[Dict[str, str]] = None,
        description: Optional[str] = None,
    ) -> uuid.UUID:
        """Create a new series (delegates to internal function)."""
        return _create_series(
            name=name,
            unit=unit,
            labels=labels,
            description=description,
        )

    def update_records(self, updates: List[Dict[str, Any]]) -> Dict[str, List]:
        """
        Instance-level wrapper around the module-level `update_records` helper.

        Allows calling `td = TimeDataClient()` and then `td.update_records(updates=[...])`.
        This preserves the same behavior as the module-level function but makes the
        usage consistent with the rest of the client-based API.
        """
        return _update_records(updates)


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


def _detect_series_from_dataframe(
    df: pd.DataFrame,
    valid_time_col: str = 'valid_time',
    valid_time_end_col: Optional[str] = None,
) -> Dict[str, str]:
    """
    Detect series from DataFrame columns that contain Pint Quantity objects or pint-pandas Series.

    For each column (except time columns):
    - If it's a pint-pandas Series (dtype="pint[unit]"), extract unit from dtype
    - If it contains Pint Quantity objects, use the unit from the first non-null value as canonical
    - If it contains regular values, treat as dimensionless

    Args:
        df: DataFrame with time series data
        valid_time_col: Column name for valid_time (default: 'valid_time')
        valid_time_end_col: Column name for valid_time_end (None for point-in-time)

    Returns:
        Dictionary mapping name (column name) to unit (canonical unit string)

    Raises:
        ValueError: If no series columns found or if units are inconsistent within a column
    """
    exclude_cols = {valid_time_col}
    if valid_time_end_col is not None:
        exclude_cols.add(valid_time_end_col)

    series_cols = [col for col in df.columns if col not in exclude_cols]

    if not series_cols:
        raise ValueError("No series columns found in DataFrame (excluding time columns)")

    series_info = {}

    for col in series_cols:
        # Check if this is a pint-pandas Series (check dtype first)
        if is_pint_pandas_series(df[col]):
            # Extract unit from dtype (e.g., "pint[MW]" -> "MW")
            unit = extract_unit_from_pint_pandas_series(df[col])
            if unit is None:
                raise ValueError(f"Column '{col}' has pint dtype but unit extraction failed")
            series_info[col] = unit
            continue

        # Not a pint-pandas Series - check for Pint Quantity objects
        # Find first non-null value in this column
        first_value = None
        for val in df[col]:
            if pd.notna(val):
                first_value = val
                break

        if first_value is None:
            raise ValueError(
                f"Column '{col}' has no non-null values. Cannot determine unit."
            )

        # Extract unit from Pint Quantity
        unit = extract_unit_from_quantity(first_value)

        if unit is None:
            # Not a Pint Quantity - check if all values are non-Quantity
            # If mixed (some Quantity, some not), raise error
            has_quantity = False
            for val in df[col]:
                if pd.notna(val) and isinstance(val, pint.Quantity):
                    has_quantity = True
                    break

            if has_quantity:
                raise ValueError(
                    f"Column '{col}' has mixed Pint Quantity and regular values. "
                    "All values in a column must be either Pint Quantities or regular values."
                )

            # All values are regular (not Pint Quantities) - treat as dimensionless
            unit = "dimensionless"
        else:
            # This column contains Pint Quantities
            # Use the first value's unit as the canonical unit
            # Validate that all values in the column have compatible units
            for val in df[col]:
                if pd.notna(val):
                    if not isinstance(val, pint.Quantity):
                        raise ValueError(
                            f"Column '{col}' has mixed Pint Quantity and regular values. "
                            "All values in a column must be Pint Quantities if the first value is a Quantity."
                        )

                    val_unit = extract_unit_from_quantity(val)
                    if val_unit is not None and val_unit != unit:
                        # Try to see if they're compatible (can convert)
                        try:
                            from .units import validate_unit_compatibility
                            validate_unit_compatibility(val_unit, unit)
                            # If compatible, use the canonical unit (already set)
                        except IncompatibleUnitError:
                            raise ValueError(
                                f"Column '{col}' has inconsistent units: "
                                f"found {unit} and {val_unit} which are incompatible"
                            )

        series_info[col] = unit

    return series_info


def _dataframe_to_value_rows(
    df: pd.DataFrame,
    tenant_id: uuid.UUID,
    series_mapping: Dict[str, uuid.UUID],  # Maps name to series_id
    units: Dict[str, str],  # Maps name to canonical unit
    valid_time_col: str = 'valid_time',
    valid_time_end_col: Optional[str] = None,
) -> List[Tuple]:
    """
    Convert a pandas DataFrame with Pint Quantity columns to TimeDB value_rows format.

    Each column (except time columns) becomes a separate series. Units are extracted
    from Pint Quantity objects, and values are converted to canonical units.

    Args:
        df: DataFrame with time series data (columns should be Pint Quantity objects)
        tenant_id: Tenant UUID
        series_mapping: Dictionary mapping name (column name) to series_id
        units: Dictionary mapping name to canonical unit
        valid_time_col: Column name for valid_time (default: 'valid_time')
        valid_time_end_col: Column name for valid_time_end (None for point-in-time)

    Returns:
        List of tuples in TimeDB format (values already converted to canonical unit):
        - Point-in-time: (tenant_id, valid_time, series_id, value)
        - Interval: (tenant_id, valid_time, valid_time_end, series_id, value)

    Raises:
        IncompatibleUnitError: If unit conversion fails due to dimensionality mismatch
    """
    if valid_time_col not in df.columns:
        raise ValueError(f"Column '{valid_time_col}' not found in DataFrame")

    exclude_cols = {valid_time_col}
    if valid_time_end_col is not None:
        exclude_cols.add(valid_time_end_col)

    series_cols = [col for col in df.columns if col not in exclude_cols]

    if not series_cols:
        raise ValueError("No series columns found in DataFrame (excluding time columns)")

    # Determine if we have interval values
    has_intervals = valid_time_end_col is not None and valid_time_end_col in df.columns

    # Pre-compute which columns are pint-pandas Series for efficiency
    pint_pandas_cols = {
        col: extract_unit_from_pint_pandas_series(df[col])
        for col in series_cols
        if is_pint_pandas_series(df[col])
    }

    # Convert to TimeDB format with unit conversion
    rows = []
    for _, row in df.iterrows():
        valid_time = row[valid_time_col]

        # Ensure timezone-aware
        if isinstance(valid_time, pd.Timestamp):
            if valid_time.tzinfo is None:
                raise ValueError(f"valid_time must be timezone-aware. Found timezone-naive datetime.")
        elif isinstance(valid_time, datetime):
            if valid_time.tzinfo is None:
                raise ValueError(f"valid_time must be timezone-aware. Found timezone-naive datetime.")

        valid_time_end = None
        if has_intervals:
            valid_time_end = row[valid_time_end_col]
            if isinstance(valid_time_end, pd.Timestamp):
                if valid_time_end.tzinfo is None:
                    raise ValueError(f"valid_time_end must be timezone-aware. Found timezone-naive datetime.")
            elif isinstance(valid_time_end, datetime):
                if valid_time_end.tzinfo is None:
                    raise ValueError(f"valid_time_end must be timezone-aware. Found timezone-naive datetime.")

        # Process each series column
        for name in series_cols:
            series_id = series_mapping[name]
            canonical_unit = units[name]

            value = row[name]

            # Handle NaN/None values
            if pd.isna(value):
                converted_value = None
            else:
                # Convert to canonical unit
                try:
                    # Check if value is a Pint Quantity (from pint-pandas Series or Pint Quantity objects)
                    if isinstance(value, pint.Quantity):
                        # Direct conversion from Quantity - handles offset units properly
                        # This works for both pint-pandas Series values and regular Pint Quantity objects
                        converted_value = convert_quantity_to_canonical_unit(value, canonical_unit)
                    else:
                        # Regular value - extract unit if available
                        # This handles the case where the column is not pint-pandas but might have units
                        submitted_value = extract_value_from_quantity(value)
                        submitted_unit = extract_unit_from_quantity(value)
                        converted_value = convert_to_canonical_unit(
                            value=submitted_value,
                            submitted_unit=submitted_unit,
                            canonical_unit=canonical_unit,
                        )
                except IncompatibleUnitError:
                    raise
                except Exception as e:
                    raise ValueError(
                        f"Unit conversion error for series '{name}', value {value}: {e}"
                    ) from e

            if has_intervals:
                rows.append((
                    tenant_id,
                    valid_time,
                    valid_time_end,
                    series_id,
                    converted_value
                ))
            else:
                rows.append((
                    tenant_id,
                    valid_time,
                    series_id,
                    converted_value
                ))

    return rows


def _create() -> None:
    """
    Create or update the database schema (TimescaleDB version).

    This function creates the TimeDB tables. It's safe to run multiple times.
    Uses connection string from TIMEDB_DSN or DATABASE_URL environment variable.
    """
    conninfo = _get_conninfo()
    db.create.create_schema(conninfo)


def _create_series(
    name: str,
    unit: str = "dimensionless",
    labels: Optional[Dict[str, str]] = None,
    description: Optional[str] = None,
) -> uuid.UUID:
    """
    Create a new time series.

    This function creates a new series with the specified name, unit, labels, and description.
    A new series_id is generated and returned. The combination of name + labels
    must be unique (unit is NOT part of the uniqueness constraint).

    Args:
        name: Parameter name (e.g., 'wind_power', 'temperature')
        unit: Canonical unit for the series (e.g., 'MW', 'degC', 'dimensionless')
              Defaults to 'dimensionless' if not provided
        labels: Dictionary of labels that differentiate this series
                (e.g., {"site": "Gotland", "turbine": "T01"})
        description: Optional description of the series

    Returns:
        The series_id (UUID) for the newly created series

    Raises:
        ValueError: If name or unit is empty, or if database tables don't exist
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
            )
    except (errors.UndefinedTable, errors.UndefinedObject) as e:
        error_msg = str(e)
        if "series_table" in error_msg or "does not exist" in error_msg:
            raise ValueError(
                "TimeDB tables do not exist. Please create the schema first by running:\n"
                "  td.create()"
            ) from None
        raise
    except Exception as e:
        error_msg = str(e)
        if "series_table" in error_msg or "does not exist" in error_msg:
            raise ValueError(
                "TimeDB tables do not exist. Please create the schema first by running:\n"
                "  td.create()"
            ) from None
        raise


def _delete() -> None:
    """
    Delete all TimeDB tables and views.

    WARNING: This will delete all data! Use with caution.
    Uses connection string from TIMEDB_DSN or DATABASE_URL environment variable.
    """
    conninfo = _get_conninfo()
    db.delete.delete_schema(conninfo)


def _read(
    series_ids: Optional[List[uuid.UUID]] = None,
    tenant_id: Optional[uuid.UUID] = None,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None,
    end_known: Optional[datetime] = None,
    return_mapping: bool = False,
) -> Union[pd.DataFrame, Tuple[pd.DataFrame, Dict[uuid.UUID, str]]]:
    """
    Read time series values from TimeDB into a pandas DataFrame.

    Returns the latest value for each (valid_time, series_id) combination,
    determined by the most recent known_time.

    Args:
        series_ids: Filter by series IDs (optional)
        tenant_id: Filter by tenant ID (optional, defaults to zeros UUID for single-tenant)
        start_valid: Start of valid time range (optional)
        end_valid: End of valid time range (optional)
        start_known: Start of known_time range (optional)
        end_known: End of known_time range (optional)
        return_mapping: If True, return both DataFrame and mapping dict (default: False)

    Returns:
        If return_mapping=False:
            DataFrame with:
            - Index: valid_time
            - Columns: series names (one column per series_id)
            - Each column has pint-pandas dtype (e.g., dtype="pint[MW]") based on unit

        If return_mapping=True:
            Tuple of (DataFrame, mapping_dict) where mapping_dict maps series_id -> name
    """
    conninfo = _get_conninfo()

    if tenant_id is None:
        tenant_id = DEFAULT_TENANT_ID

    # Get raw data from db layer
    df = db.read.read_values_flat(
        conninfo,
        tenant_id=tenant_id,
        series_ids=series_ids,
        start_valid=start_valid,
        end_valid=end_valid,
        start_known=start_known,
        end_known=end_known,
    )

    # If no data, return empty DataFrame with proper structure
    if len(df) == 0:
        if return_mapping:
            return pd.DataFrame(index=pd.DatetimeIndex([], name="valid_time", tz="UTC")), {}
        else:
            return pd.DataFrame(index=pd.DatetimeIndex([], name="valid_time", tz="UTC"))

    # Reset index to get valid_time and series_id as columns
    df_reset = df.reset_index()

    # Create mappings
    name_map = df_reset[['series_id', 'name']].drop_duplicates().set_index('series_id')['name'].to_dict()
    unit_map = df_reset[['series_id', 'unit']].drop_duplicates().set_index('series_id')['unit'].to_dict()

    # Pivot the data: valid_time as index, series_id as columns
    df_pivoted = df_reset.pivot_table(
        index='valid_time',
        columns='series_id',
        values='value',
        aggfunc='first'  # In case of duplicates (shouldn't happen with DISTINCT ON)
    )

    # Sort index
    df_pivoted = df_pivoted.sort_index()

    # Convert each column to pint-pandas dtype based on unit
    for col_series_id in df_pivoted.columns:
        unit = unit_map.get(col_series_id)
        if unit:
            pint_dtype = f"pint[{unit}]"
            df_pivoted[col_series_id] = df_pivoted[col_series_id].astype(pint_dtype)

    # Return based on return_mapping flag
    if return_mapping:
        df_pivoted.columns.name = "series_id"
        return df_pivoted, name_map
    else:
        # Rename columns from series_id to name
        df_pivoted.rename(columns=name_map, inplace=True)
        df_pivoted.columns.name = "name"
        return df_pivoted


def _read_overlapping(
    series_ids: Optional[List[uuid.UUID]] = None,
    tenant_id: Optional[uuid.UUID] = None,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None,
    end_known: Optional[datetime] = None,
    return_mapping: bool = False,
) -> Union[pd.DataFrame, Tuple[pd.DataFrame, Dict[uuid.UUID, str]]]:
    """
    Read time series values in overlapping mode (all forecast revisions).

    Returns all versions of forecasts, showing how predictions evolve over time.
    This is useful for analyzing forecast revisions and backtesting.

    Args:
        series_ids: Filter by series IDs (optional)
        tenant_id: Filter by tenant ID (optional, defaults to zeros UUID for single-tenant)
        start_valid: Start of valid time range (optional)
        end_valid: End of valid time range (optional)
        start_known: Start of known_time range (optional)
        end_known: End of known_time range (optional)
        return_mapping: If True, return both DataFrame and mapping dict (default: False)

    Returns:
        If return_mapping=False:
            DataFrame with:
            - Index: (known_time, valid_time) - double index
            - Columns: series names (one column per series)
            - Each column has pint-pandas dtype (e.g., dtype="pint[MW]") based on unit

        If return_mapping=True:
            Tuple of (DataFrame, mapping_dict) where:
            - DataFrame has series_id as columns (not renamed to name)
            - Index: (known_time, valid_time) - double index
            - mapping_dict maps series_id -> name
    """
    conninfo = _get_conninfo()

    if tenant_id is None:
        tenant_id = DEFAULT_TENANT_ID

    # Get raw data from db layer
    df = db.read.read_values_overlapping(
        conninfo,
        tenant_id=tenant_id,
        series_ids=series_ids,
        start_valid=start_valid,
        end_valid=end_valid,
        start_known=start_known,
        end_known=end_known,
    )

    # If no data, return empty DataFrame with proper structure
    if len(df) == 0:
        empty_index = pd.MultiIndex.from_tuples([], names=["known_time", "valid_time"])
        if return_mapping:
            return pd.DataFrame(index=empty_index), {}
        else:
            return pd.DataFrame(index=empty_index)

    # Reset index to get known_time, valid_time, and series_id as columns
    df_reset = df.reset_index()

    # Create mappings
    name_map = df_reset[['series_id', 'name']].drop_duplicates().set_index('series_id')['name'].to_dict()
    unit_map = df_reset[['series_id', 'unit']].drop_duplicates().set_index('series_id')['unit'].to_dict()

    # Pivot the data: (known_time, valid_time) as index, series_id as columns
    df_pivoted = df_reset.pivot_table(
        index=['known_time', 'valid_time'],
        columns='series_id',
        values='value',
        aggfunc='first'  # In case of duplicates (shouldn't happen)
    )

    # Sort index
    df_pivoted = df_pivoted.sort_index()

    # Convert each column to pint-pandas dtype based on unit
    for col_series_id in df_pivoted.columns:
        unit = unit_map.get(col_series_id)
        if unit:
            pint_dtype = f"pint[{unit}]"
            df_pivoted[col_series_id] = df_pivoted[col_series_id].astype(pint_dtype)

    # Return based on return_mapping flag
    if return_mapping:
        df_pivoted.columns.name = "series_id"
        return df_pivoted, name_map
    else:
        # Rename columns from series_id to name
        df_pivoted.rename(columns=name_map, inplace=True)
        df_pivoted.columns.name = "name"
        return df_pivoted


def _insert_batch(
    df: pd.DataFrame,
    tenant_id: Optional[uuid.UUID] = None,
    batch_id: Optional[uuid.UUID] = None,
    workflow_id: Optional[str] = None,
    batch_start_time: Optional[datetime] = None,
    batch_finish_time: Optional[datetime] = None,
    valid_time_col: str = 'valid_time',
    valid_time_end_col: Optional[str] = None,
    known_time: Optional[datetime] = None,
    batch_params: Optional[dict] = None,
    name_overrides: Optional[Dict[str, str]] = None,
    series_ids: Optional[Dict[str, uuid.UUID]] = None,
    series_descriptions: Optional[Dict[str, str]] = None,
) -> InsertResult:
    """
    Insert a batch with time series data from a pandas DataFrame.

    This function automatically:
    - Detects series from DataFrame columns (each column except time columns becomes a series)
    - Extracts units from Pint Quantity objects or pint-pandas Series in each column
    - Creates/gets series with name = column name (or override) and unit from Pint
    - Converts all values to canonical units before storage
    - Inserts both the batch metadata and the time series values atomically.

    Args:
        df: DataFrame containing time series data (required)
        tenant_id: Tenant UUID (optional, defaults to zeros UUID for single-tenant installations)
        batch_id: Unique identifier for the batch (optional, auto-generated if not provided)
        workflow_id: Workflow identifier (optional, defaults to "sdk-workflow" if not provided)
        batch_start_time: Start time of the batch (optional, defaults to datetime.now(timezone.utc))
        batch_finish_time: Optional finish time of the batch (must be timezone-aware if provided)
        valid_time_col: Column name for valid_time (default: 'valid_time')
        valid_time_end_col: Column name for valid_time_end for interval values (optional)
        known_time: Time of knowledge - when the data was known/available (optional)
        batch_params: Optional dictionary of batch parameters (will be stored as JSONB)
        name_overrides: Optional dict mapping column names to custom name values
        series_ids: Optional dict mapping column names to series_id UUIDs
        series_descriptions: Optional dict mapping column names to descriptions

    Returns:
        InsertResult: Named tuple containing (batch_id, workflow_id, series_ids, tenant_id).

    Raises:
        IncompatibleUnitError: If unit conversion fails due to dimensionality mismatch
        ValueError: If DataFrame structure is invalid or units are inconsistent
    """
    conninfo = _get_conninfo()

    # Auto-generate missing IDs
    if tenant_id is None:
        tenant_id = DEFAULT_TENANT_ID

    if batch_id is None:
        batch_id = uuid.uuid4()

    if workflow_id is None:
        workflow_id = "sdk-workflow"

    if batch_start_time is None:
        batch_start_time = datetime.now(timezone.utc)
    elif batch_start_time.tzinfo is None:
        raise ValueError("batch_start_time must be timezone-aware")

    # Detect series from DataFrame (extract units from Pint Quantities)
    series_info = _detect_series_from_dataframe(
        df=df,
        valid_time_col=valid_time_col,
        valid_time_end_col=valid_time_end_col,
    )

    # Get or create series for each detected series
    series_mapping = {}  # Maps column name to series_id
    units = {}    # Maps column name to canonical unit

    if name_overrides is None:
        name_overrides = {}
    if series_ids is None:
        series_ids = {}
    if series_descriptions is None:
        series_descriptions = {}

    with psycopg.connect(conninfo) as conn:
        for col_name, canonical_unit in series_info.items():
            # Use override if provided, otherwise use column name
            name = name_overrides.get(col_name, col_name)

            # Check if series_id is provided for this series
            # Try both column name and name as keys
            provided_series_id = series_ids.get(col_name) or series_ids.get(name)

            if provided_series_id is not None:
                # Use provided series_id - look up its metadata from database
                try:
                    series_info_db = db.series.get_series_info(conn, provided_series_id)
                    series_id = provided_series_id
                    # Use the unit from the database, not from the DataFrame
                    canonical_unit = series_info_db['unit']
                except ValueError as e:
                    raise ValueError(f"series_id {provided_series_id} not found in database") from e
            else:
                # Create a new series using create_series
                # Get description if provided (try both column name and name)
                description = series_descriptions.get(col_name) or series_descriptions.get(name)

                series_id = db.series.create_series(
                    conn,
                    name=name,
                    description=description,
                    unit=canonical_unit,
                )

            series_mapping[col_name] = series_id
            units[col_name] = canonical_unit

    # Convert DataFrame to value_rows (with unit conversion)
    value_rows = _dataframe_to_value_rows(
        df=df,
        tenant_id=tenant_id,
        series_mapping=series_mapping,
        units=units,
        valid_time_col=valid_time_col,
        valid_time_end_col=valid_time_end_col,
    )

    # Insert batch with values
    try:
        db.insert.insert_batch_with_values(
            conninfo=conninfo,
            batch_id=batch_id,
            tenant_id=tenant_id,
            workflow_id=workflow_id,
            batch_start_time=batch_start_time,
            batch_finish_time=batch_finish_time,
            value_rows=value_rows,
            known_time=known_time,
            batch_params=batch_params,
        )
    except (errors.UndefinedTable, errors.UndefinedObject) as e:
        error_msg = str(e)
        if "batches_table" in error_msg or "values_table" in error_msg or "series_table" in error_msg or "does not exist" in error_msg:
            raise ValueError(
                "TimeDB tables do not exist. Please create the schema first by running:\n"
                "  td.create()"
            ) from None
        raise
    except Exception as e:
        error_msg = str(e)
        if "batches_table" in error_msg or "values_table" in error_msg or "series_table" in error_msg or "does not exist" in error_msg:
            raise ValueError(
                "TimeDB tables do not exist. Please create the schema first by running:\n"
                "  td.create()"
            ) from None
        raise

    # Build series_ids dict (using final name, not column names)
    final_series_ids = {}
    for col_name, series_id in series_mapping.items():
        name = name_overrides.get(col_name, col_name)
        final_series_ids[name] = series_id

    # Return the IDs that were used (including auto-generated ones)
    return InsertResult(
        batch_id=batch_id,
        workflow_id=workflow_id,
        series_ids=final_series_ids,
        tenant_id=tenant_id,
    )


def _update_records(
    updates: List[Dict[str, Any]],
) -> Dict[str, List]:
    """
    Update time series records (values, annotations, tags).

    This is a convenience wrapper around db.update.update_records that handles
    the database connection internally.

    Args:
        updates: List of update dictionaries. Each dictionary must contain EITHER:
            Option 1 (by value_id - simplest, recommended):
            - value_id (int): The value_id of the row to update
            - value (float, optional): New value (omit to leave unchanged, None to clear)
            - annotation (str, optional): New annotation (omit to leave unchanged, None to clear)
            - tags (list[str], optional): New tags (omit to leave unchanged, None or [] to clear)
            - changed_by (str, optional): Who made the change

            Option 2 (by key - for backwards compatibility):
            - batch_id (uuid.UUID): Run identifier
            - tenant_id (uuid.UUID): Tenant identifier (optional, defaults to DEFAULT_TENANT_ID)
            - valid_time (datetime): Time the value is valid for (must be timezone-aware)
            - series_id (uuid.UUID): Series identifier
            - value (float, optional): New value (omit to leave unchanged, None to clear)
            - annotation (str, optional): New annotation (omit to leave unchanged, None to clear)
            - tags (list[str], optional): New tags (omit to leave unchanged, None or [] to clear)
            - changed_by (str, optional): Who made the change

    Returns:
        Dictionary with keys:
            - 'updated': List of dicts with keys (value_id, batch_id, tenant_id, valid_time, series_id)
            - 'skipped_no_ops': List of dicts with keys (value_id) or (batch_id, tenant_id, valid_time, series_id)
    """
    conninfo = _get_conninfo()

    # Set default tenant_id for updates by key if not provided
    for update_dict in updates:
        if "value_id" not in update_dict:
            if "tenant_id" not in update_dict:
                update_dict["tenant_id"] = DEFAULT_TENANT_ID

    with psycopg.connect(conninfo) as conn:
        return db.update.update_records(conn, updates=updates)
