"""
High-level SDK for TimeDB.

Provides a simple interface for working with TimeDB, including automatic
DataFrame conversion for time series data with unit handling using Pint Quantity objects.
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
            # Query database for matching series
            with psycopg.connect(self._conninfo) as conn:
                mapping = db.series.get_mapping(
                    conn,
                    name=self._name,
                    unit=self._unit,
                    label_filter=self._label_filters if self._label_filters else None,
                )
            
            # Update cache with newly resolved IDs
            self._id_cache.update(mapping)
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
            return insert_batch(
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
            return insert_batch(
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
    
    def read(
        self,
        tenant_id: Optional[uuid.UUID] = None,
        start_valid: Optional[datetime] = None,
        end_valid: Optional[datetime] = None,
        return_mapping: bool = False,
        all_versions: bool = False,
        return_value_id: bool = False,
        tags_and_annotations: bool = False,
    ) -> Union[pd.DataFrame, Tuple[pd.DataFrame, Dict[uuid.UUID, str]]]:
        """
        Read time series data for this collection.
        
        If collection matches a single series, returns data for that series.
        If multiple series, returns data for all matching series.
        
        Args:
            tenant_id: Tenant UUID filter (optional)
            start_valid: Start of valid time range (optional)
            end_valid: End of valid time range (optional)
            return_mapping: Return (DataFrame, mapping_dict) if True
            all_versions: Include all versions if True
            return_value_id: Include value_id column if True
            tags_and_annotations: Include tags and annotation columns if True
        
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
        
        if len(series_ids) == 1:
            # Single series - use read() with series_id
            return read(
                series_id=series_ids[0],
                tenant_id=tenant_id,
                start_valid=start_valid,
                end_valid=end_valid,
                return_mapping=return_mapping,
                all_versions=all_versions,
                return_value_id=return_value_id,
                tags_and_annotations=tags_and_annotations,
            )
        else:
            # Multiple series - read without series_id filter
            # This will return all series, then we filter to our IDs
            # For now, just read all and filter
            df_all = read(
                series_id=None,
                tenant_id=tenant_id,
                start_valid=start_valid,
                end_valid=end_valid,
                return_mapping=True,
                all_versions=all_versions,
                return_value_id=return_value_id,
                tags_and_annotations=tags_and_annotations,
            )
            
            if isinstance(df_all, tuple):
                df, mapping = df_all
                # Filter to only our series (columns are UUIDs)
                our_cols = [col for col in df.columns if isinstance(col, uuid.UUID) and col in series_ids]
                df_filtered = df[our_cols]
                
                if return_mapping:
                    filtered_mapping = {col: mapping[col] for col in our_cols}
                    return df_filtered, filtered_mapping
                else:
                    # Rename columns to names
                    df_filtered.rename(columns=mapping, inplace=True)
                    df_filtered.columns.name = "name"
                    return df_filtered
            else:
                # Shouldn't happen, but handle it
                return df_all
    
    def read_values_overlapping(
        self,
        tenant_id: Optional[uuid.UUID] = None,
        start_valid: Optional[datetime] = None,
        end_valid: Optional[datetime] = None,
        start_known: Optional[datetime] = None,
        end_known: Optional[datetime] = None,
        all_versions: bool = False,
        return_mapping: bool = False,
        units: bool = True,
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
            all_versions: Include all versions if True
            return_mapping: Return (DataFrame, mapping_dict) if True
            units: Return pint-pandas DataFrame with units if True
        
        Returns:
            DataFrame with (known_time, valid_time) MultiIndex
            (or tuple of DataFrame and mapping dict if return_mapping=True)
        
        Examples:
            # Read all forecast revisions for a specific series
            df = td.series("forecast").where(type="power").read_values_overlapping()
            
            # Read with time filters
            df = td.series("forecast").read_values_overlapping(
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
        
        if len(series_ids) == 1:
            # Single series - use global read_values_overlapping with series_id
            return read_values_overlapping(
                series_id=series_ids[0],
                tenant_id=tenant_id,
                start_valid=start_valid,
                end_valid=end_valid,
                start_known=start_known,
                end_known=end_known,
                all_versions=all_versions,
                return_mapping=return_mapping,
                units=units,
            )
        else:
            # Multiple series - read without series_id filter
            df_all = read_values_overlapping(
                series_id=None,
                tenant_id=tenant_id,
                start_valid=start_valid,
                end_valid=end_valid,
                start_known=start_known,
                end_known=end_known,
                all_versions=all_versions,
                return_mapping=True,
                units=units,
            )
            
            if isinstance(df_all, tuple):
                df, mapping = df_all
                # Filter to only our series (columns are UUIDs)
                our_cols = [col for col in df.columns if isinstance(col, uuid.UUID) and col in series_ids]
                df_filtered = df[our_cols]
                
                if return_mapping:
                    filtered_mapping = {col: mapping[col] for col in our_cols}
                    return df_filtered, filtered_mapping
                else:
                    # Rename columns to names
                    df_filtered.rename(columns=mapping, inplace=True)
                    df_filtered.columns.name = "name"
                    return df_filtered
            else:
                return df_all
    
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
            # ['Gotland', 'Skåne', ...]
            
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
        """Create database schema."""
        create()
    
    def delete(self) -> None:
        """Delete database schema."""
        delete()
    
    def create_series(
        self,
        name: str,
        unit: str = "dimensionless",
        labels: Optional[Dict[str, str]] = None,
        description: Optional[str] = None,
    ) -> uuid.UUID:
        """Create a new series (delegates to global function)."""
        return create_series(
            name=name,
            unit=unit,
            labels=labels,
            description=description,
        )


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


def create() -> None:
    """
    Create or update the database schema.
    
    This function creates the TimeDB tables. It's safe to run multiple times.
    Uses connection string from TIMEDB_DSN or DATABASE_URL environment variable.
    """
    conninfo = _get_conninfo()
    db.create.create_schema(conninfo)


def create_series(
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
    
    Example:
        # Create a new series with labels
        series_id = td.create_series(
            name="wind_power",
            unit="MW",
            labels={"site": "Gotland", "turbine": "T01"},
            description="Wind power output for turbine T01 at Gotland"
        )
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
    except Exception as e:
        error_msg = str(e)
        if "series_table" in error_msg or "does not exist" in error_msg:
            raise ValueError(
                "TimeDB tables do not exist. Please create the schema first by running:\n"
                "  td.create()"
            ) from None
        raise


def get_mapping(
    name: Optional[str] = None,
    unit: Optional[str] = None,
    label_filter: Optional[Dict[str, str]] = None,
) -> Dict[FrozenSet[Tuple[str, str]], uuid.UUID]:
    """
    Get a mapping of label sets to series_ids based on search criteria.
    
    This function enables discovery and filtering of series based on their labels.
    The returned dictionary uses frozensets of (key, value) tuples as keys, allowing
    you to distinguish between series with different label combinations.
    
    Note: While unit can be used as a filter, the unique constraint is on (name, labels) only.
    
    Args:
        name: Filter by parameter name (optional, e.g., 'wind_power')
        unit: Filter by unit (optional, e.g., 'MW')
        label_filter: Dictionary of labels to filter by (optional, e.g., {"site": "Gotland"})
                     Series must contain ALL specified labels to match
    
    Returns:
        Dictionary mapping frozenset of (label_key, label_value) tuples to series_id
    
    Examples:
        # Broad search - all turbines
        mapping = td.get_mapping(label_filter={"type": "turbine"})
        # Returns: {
        #     frozenset([("type", "turbine"), ("site", "Gotland"), ("id", "T01")]): uuid1,
        #     frozenset([("type", "turbine"), ("site", "Gotland"), ("id", "T02")]): uuid2,
        #     frozenset([("type", "turbine"), ("site", "Skåne"), ("id", "S01")]): uuid3,
        # }
        
        # Narrower search - turbines at specific site
        mapping = td.get_mapping(label_filter={"type": "turbine", "site": "Gotland"})
        # Returns only turbines at Gotland
        
        # Exact match - specific turbine
        mapping = td.get_mapping(
            name="wind_power",
            label_filter={"type": "turbine", "site": "Gotland", "id": "T01"}
        )
        # Returns: {
        #     frozenset([("type", "turbine"), ("site", "Gotland"), ("id", "T01")]): uuid1
        # }
        
        # Get all series (no filters)
        all_series = td.get_mapping()
    """
    conninfo = _get_conninfo()
    
    try:
        with psycopg.connect(conninfo) as conn:
            return db.series.get_mapping(
                conn,
                name=name,
                unit=unit,
                label_filter=label_filter,
            )
    except (errors.UndefinedTable, errors.UndefinedObject) as e:
        error_msg = str(e)
        if "series_table" in error_msg or "does not exist" in error_msg:
            raise ValueError(
                "TimeDB tables do not exist. Please create the schema first by running:\n"
                "  td.create()"
            ) from None
        raise


def delete() -> None:
    """
    Delete all TimeDB tables and views.
    
    WARNING: This will delete all data! Use with caution.
    Uses connection string from TIMEDB_DSN or DATABASE_URL environment variable.
    """
    conninfo = _get_conninfo()
    db.delete.delete_schema(conninfo)


def check_api(
    host: str = "127.0.0.1",
    port: int = 8000,
) -> bool:
    """
    Check if the TimeDB API server is running and display API information.
    
    This function checks if the API server is responding and prints detailed
    information about the API including available endpoints.
    
    Args:
        host: Host to check (default: "127.0.0.1")
        port: Port to check (default: 8000)
    
    Returns:
        True if the API server is running and responding, False otherwise
    
    Example:
        if td.check_api():
            # API is running and information was printed
            pass
        else:
            # API is not running, error message was printed
            pass
    """
    try:
        import requests
    except ImportError:
        print("❌ Error: 'requests' library not installed. Install with: pip install requests")
        return False
    
    try:
        response = requests.get(f"http://{host}:{port}/", timeout=2)
        response.raise_for_status()
        api_info = response.json()
        
        print("✓ API is running")
        print(f"  Name: {api_info['name']}")
        print(f"  Version: {api_info['version']}")
        print(f"\nAvailable endpoints:")
        for endpoint, description in api_info['endpoints'].items():
            print(f"  - {endpoint}: {description}")
        
        return True
    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
        print("❌ API is not running!")
        print(f"   Please start it by running: td.start_api_background()")
        print(f"   Or in a terminal: timedb api")
        return False
    except Exception as e:
        print(f"❌ Error getting API information: {e}")
        return False


def start_api(
    host: str = "127.0.0.1",
    port: int = 8000,
    reload: bool = False,
) -> None:
    """
    Start the TimeDB REST API server.
    
    This function starts the FastAPI server using uvicorn. The server will run
    until interrupted (Ctrl+C) or the process is terminated.
    
    Args:
        host: Host to bind to (default: "127.0.0.1")
        port: Port to bind to (default: 8000)
        reload: Enable auto-reload for development (default: False)
    
    Example:
        # Start API server
        td.start_api()
        
        # Start with custom host/port
        td.start_api(host="0.0.0.0", port=8080)
        
        # Start with auto-reload (development)
        td.start_api(reload=True)
    
    Note:
        This function blocks until the server is stopped. For non-blocking
        execution in notebooks, use start_api_background() instead.
    """
    try:
        import uvicorn
    except ImportError as e:
        raise ImportError(
            "FastAPI dependencies not installed. Install with: pip install fastapi uvicorn[standard]"
        ) from e
    
    print(f"Starting TimeDB API server on http://{host}:{port}")
    print(f"API docs available at http://{host}:{port}/docs")
    print("Press Ctrl+C to stop the server")
    
    uvicorn.run(
        "timedb.api:app",
        host=host,
        port=port,
        reload=reload,
    )


def start_api_background(
    host: str = "127.0.0.1",
    port: int = 8000,
    reload: bool = False,
    wait_seconds: float = 2.0,
) -> bool:
    """
    Start the TimeDB REST API server in a background thread.
    
    This function starts the FastAPI server in a daemon thread, allowing
    the calling code to continue execution. Useful for notebooks and scripts
    where you want to start the server and continue working.
    
    Args:
        host: Host to bind to (default: "127.0.0.1")
        port: Port to bind to (default: 8000)
        reload: Enable auto-reload for development (default: False)
        wait_seconds: Seconds to wait after starting thread before checking if server is up (default: 2.0)
    
    Returns:
        True if the server started successfully, False otherwise
    
    Example:
        # Start API server in background
        if td.start_api_background():
            print("API server started")
        else:
            print("Failed to start API server")
    
    Note:
        The server runs in a daemon thread, so it will stop when the main
        process exits. To stop the server manually, restart the kernel/process.
    """
    import threading
    import time
    
    # Silently check if server is already running (without printing)
    try:
        import requests
        response = requests.get(f"http://{host}:{port}/", timeout=1)
        if response.status_code == 200:
            # Server is already running, use check_api to print info
            check_api(host, port)
            return True
    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout, ImportError):
        pass  # Server is not running, which is expected
    
    # Start the server in a background thread
    print(f"Starting API server in background thread on http://{host}:{port}...")
    api_thread = threading.Thread(
        target=start_api,
        args=(host, port, reload),
        daemon=True,  # Thread will stop when main process exits
    )
    api_thread.start()
    
    # Wait a moment for server to start
    time.sleep(wait_seconds)
    
    # Check if server started successfully and print info
    if check_api(host, port):
        print("✓ API server started successfully")
        print(f"   Server running at http://{host}:{port}")
        print(f"   API docs available at http://{host}:{port}/docs")
        return True
    else:
        print("❌ API server failed to start")
        print("   Try starting it manually:")
        print("   - In a terminal: timedb api")
        print("   - Or in Python: td.start_api()")
        return False


def read(
    series_id: Optional[uuid.UUID] = None,
    tenant_id: Optional[uuid.UUID] = None,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    return_mapping: bool = False,
    all_versions: bool = False,
    return_value_id: bool = False,
    tags_and_annotations: bool = False,
) -> Union[pd.DataFrame, Tuple[pd.DataFrame, Dict[uuid.UUID, str]]]:
    """
    Read time series values from TimeDB into a pandas DataFrame.
    
    Args:
        series_id: Filter by series ID (optional)
        tenant_id: Filter by tenant ID (optional, defaults to zeros UUID for single-tenant)
        start_valid: Start of valid time range (optional)
        end_valid: End of valid time range (optional)
        return_mapping: If True, return both DataFrame and mapping dict (default: False)
        all_versions: If True, include all versions (not just current). If False, only is_current=True (default: False)
        return_value_id: If True, include value_id column in the result (default: False)
        tags_and_annotations: If True, include tags and annotation columns (default: False)
    
    Returns:
        If return_mapping=False:
            DataFrame with:
            - Index: valid_time (or (valid_time, value_id) MultiIndex when all_versions=True and return_value_id=True)
            - Columns: series_id (one column per series_id)
            - Each column has pint-pandas dtype (e.g., dtype="pint[MW]") based on unit
            - If return_value_id=True and single series (and not all_versions): includes 'value_id' column
            - If all_versions=True: includes 'changed_by' column
            - If tags_and_annotations=True: includes 'tags' and 'annotation' columns
        
        If return_mapping=True:
            Tuple of (DataFrame, mapping_dict) where mapping_dict maps series_id -> name
    """
    conninfo = _get_conninfo()
    
    if tenant_id is None:
        tenant_id = DEFAULT_TENANT_ID
    
    # Build SQL query with series_id support
    filters = ["v.tenant_id = %(tenant_id)s", "r.tenant_id = %(tenant_id)s"]
    if not all_versions:
        filters.append("v.is_current = true")
    params = {"tenant_id": tenant_id}
    
    if series_id is not None:
        filters.append("v.series_id = %(series_id)s")
        params["series_id"] = series_id
    
    if start_valid is not None:
        filters.append("v.valid_time >= %(start_valid)s")
        params["start_valid"] = start_valid
    
    if end_valid is not None:
        filters.append("v.valid_time < %(end_valid)s")
        params["end_valid"] = end_valid
    
    where_clause = "WHERE " + " AND ".join(filters)
    
    # Include value_id in SELECT if requested
    value_id_col = "v.value_id," if return_value_id else ""
    
    # Include changed_by and change_time when all_versions=True
    changed_by_col = "v.changed_by," if all_versions else ""
    change_time_col = "v.change_time," if all_versions else ""
    
    # Include tags and annotation when tags_and_annotations=True
    tags_annotation_cols = ""
    if tags_and_annotations:
        tags_annotation_cols = "v.tags, v.annotation,"
    
    # Include series metadata (name, unit) in SELECT
    # When all_versions=True, don't use DISTINCT ON so we get all versions
    # When all_versions=False, use DISTINCT ON to get only the latest version per (valid_time, series_id)
    if all_versions:
        # Get all versions - order by valid_time, series_id, and known_time DESC
        sql = f"""
            SELECT
                v.valid_time,
                {value_id_col}
                {changed_by_col}
                {change_time_col}
                {tags_annotation_cols}
                v.value,
                v.series_id,
                s.name,
                s.unit
            FROM values_table v
            JOIN batches_table r ON v.batch_id = r.batch_id AND v.tenant_id = r.tenant_id
            JOIN series_table s ON v.series_id = s.series_id
            {where_clause}
            ORDER BY v.valid_time, COALESCE(v.valid_time_end, v.valid_time), v.series_id, r.known_time DESC;
        """
    else:
        # Use DISTINCT ON to get only the latest version per (valid_time, series_id)
        sql = f"""
            SELECT DISTINCT ON (v.valid_time, COALESCE(v.valid_time_end, v.valid_time), v.series_id)
                v.valid_time,
                {value_id_col}
                {tags_annotation_cols}
                v.value,
                v.series_id,
                s.name,
                s.unit
            FROM values_table v
            JOIN batches_table r ON v.batch_id = r.batch_id AND v.tenant_id = r.tenant_id
            JOIN series_table s ON v.series_id = s.series_id
            {where_clause}
            ORDER BY v.valid_time, COALESCE(v.valid_time_end, v.valid_time), v.series_id, r.known_time DESC;
        """
    
    try:
        # Use psycopg connection directly and suppress pandas warning
        # pandas works fine with psycopg connections, the warning is just about official support
        # This avoids SQLAlchemy driver issues (psycopg2 vs psycopg3)
        import warnings
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning, message=".*pandas only supports SQLAlchemy.*")
            with psycopg.connect(conninfo) as conn:
                df = pd.read_sql(sql, conn, params=params)
    except (errors.UndefinedTable, errors.UndefinedObject) as e:
        # Check if it's a table-related error
        error_msg = str(e)
        if "values_table" in error_msg or "batches_table" in error_msg or "series_table" in error_msg or "does not exist" in error_msg:
            raise ValueError(
                "TimeDB tables do not exist. Please create the schema first by running:\n"
                "  td.create()"
            ) from None
        raise
    except Exception as e:
        # Handle case where pandas wraps the psycopg error
        error_msg = str(e)
        if "values_table" in error_msg or "batches_table" in error_msg or "series_table" in error_msg or "does not exist" in error_msg:
            raise ValueError(
                "TimeDB tables do not exist. Please create the schema first by running:\n"
                "  td.create()"
            ) from None
        raise
    
    # Ensure timezone-aware pandas datetimes
    df["valid_time"] = pd.to_datetime(df["valid_time"], utc=True)
    
    # If no data, return empty DataFrame with proper structure
    if len(df) == 0:
        if return_mapping:
            return pd.DataFrame(index=pd.DatetimeIndex([], name="valid_time", tz="UTC")), {}
        else:
            return pd.DataFrame(index=pd.DatetimeIndex([], name="valid_time", tz="UTC"))
    
    # Handle value_id if requested - store it separately before pivoting
    value_id_data = None
    if return_value_id and 'value_id' in df.columns:
        # Store value_id mapping: (valid_time, series_id) -> value_id
        value_id_data = df.set_index(['valid_time', 'series_id'])['value_id'].to_dict()
    
    # Handle changed_by, change_time, tags, and annotation - store separately before pivoting
    changed_by_data = None
    change_time_data = None
    tags_data = None
    annotation_data = None
    if all_versions:
        if 'changed_by' in df.columns:
            # Store changed_by mapping: (valid_time, value_id, series_id) -> changed_by
            # When all_versions=True, we need value_id to distinguish versions
            if return_value_id and 'value_id' in df.columns:
                changed_by_data = df.set_index(['valid_time', 'value_id', 'series_id'])['changed_by'].to_dict()
            else:
                changed_by_data = df.set_index(['valid_time', 'series_id'])['changed_by'].to_dict()
        if 'change_time' in df.columns:
            # Store change_time mapping: (valid_time, value_id, series_id) -> change_time
            if return_value_id and 'value_id' in df.columns:
                change_time_data = df.set_index(['valid_time', 'value_id', 'series_id'])['change_time'].to_dict()
            else:
                change_time_data = df.set_index(['valid_time', 'series_id'])['change_time'].to_dict()
    if tags_and_annotations:
        if 'tags' in df.columns:
            if return_value_id and 'value_id' in df.columns and all_versions:
                tags_data = df.set_index(['valid_time', 'value_id', 'series_id'])['tags'].to_dict()
            else:
                tags_data = df.set_index(['valid_time', 'series_id'])['tags'].to_dict()
        if 'annotation' in df.columns:
            if return_value_id and 'value_id' in df.columns and all_versions:
                annotation_data = df.set_index(['valid_time', 'value_id', 'series_id'])['annotation'].to_dict()
            else:
                annotation_data = df.set_index(['valid_time', 'series_id'])['annotation'].to_dict()
    
    # Create mappings of series_id to unit and name for dtype assignment and mapping
    unit_map = df[['series_id', 'unit']].drop_duplicates().set_index('series_id')['unit'].to_dict()
    name_map = df[['series_id', 'name']].drop_duplicates().set_index('series_id')['name'].to_dict()
    
    # When all_versions=True and return_value_id=True, use MultiIndex (valid_time, value_id) to preserve multiple versions
    if all_versions and return_value_id and 'value_id' in df.columns:
        # Use MultiIndex (valid_time, value_id) to preserve all versions
        # Pivot with (valid_time, value_id) as index
        df_pivoted = df.pivot_table(
            index=['valid_time', 'value_id'],
            columns='series_id',
            values='value',
            aggfunc='first'  # Shouldn't have duplicates with (valid_time, value_id, series_id)
        )
        
        # Sort index
        df_pivoted = df_pivoted.sort_index()
        
        # Don't add value_id as a column - it's already in the MultiIndex
        # Add changed_by, change_time, tags, and annotation as columns if available
        if len(df_pivoted.columns) == 1:
            series_id_val = df_pivoted.columns[0]
            # Add changed_by column
            if changed_by_data:
                changed_by_values = []
                for idx in df_pivoted.index:
                    valid_time, value_id = idx
                    # Use (valid_time, value_id, series_id) if all_versions, else (valid_time, series_id)
                    if return_value_id and all_versions:
                        changed_by = changed_by_data.get((valid_time, value_id, series_id_val))
                    else:
                        changed_by = changed_by_data.get((valid_time, series_id_val))
                    changed_by_values.append(changed_by)
                df_pivoted['changed_by'] = changed_by_values
            
            # Add change_time column
            if change_time_data:
                change_time_values = []
                for idx in df_pivoted.index:
                    valid_time, value_id = idx
                    if return_value_id and all_versions:
                        change_time = change_time_data.get((valid_time, value_id, series_id_val))
                    else:
                        change_time = change_time_data.get((valid_time, series_id_val))
                    change_time_values.append(change_time)
                df_pivoted['change_time'] = change_time_values
            
            # Add tags and annotation columns
            if tags_and_annotations:
                if tags_data:
                    tags_values = []
                    for idx in df_pivoted.index:
                        valid_time, value_id = idx
                        if return_value_id and all_versions:
                            tags = tags_data.get((valid_time, value_id, series_id_val))
                        else:
                            tags = tags_data.get((valid_time, series_id_val))
                        tags_values.append(tags)
                    df_pivoted['tags'] = tags_values
                if annotation_data:
                    annotation_values = []
                    for idx in df_pivoted.index:
                        valid_time, value_id = idx
                        if return_value_id and all_versions:
                            annotation = annotation_data.get((valid_time, value_id, series_id_val))
                        else:
                            annotation = annotation_data.get((valid_time, series_id_val))
                        annotation_values.append(annotation)
                    df_pivoted['annotation'] = annotation_values
    else:
        # Normal pivot: valid_time as index, series_id as columns
        df_pivoted = df.pivot_table(
            index='valid_time',
            columns='series_id',
            values='value',
            aggfunc='first'  # In case of duplicates (shouldn't happen with DISTINCT ON when all_versions=False)
        )
        
        # Sort index
        df_pivoted = df_pivoted.sort_index()
        
        # Add value_id as a column if requested
        # For single series, add as regular column. For multiple series, we'd need MultiIndex columns
        # For simplicity, we'll support it for single series case (most common use case)
        if return_value_id and value_id_data and len(df_pivoted.columns) == 1:
            # Single series - add value_id as a column
            series_id_val = df_pivoted.columns[0]
            value_ids = []
            for valid_time in df_pivoted.index:
                value_id = value_id_data.get((valid_time, series_id_val))
                value_ids.append(value_id)
            df_pivoted['value_id'] = value_ids
        
        # Add tags and annotation columns if requested
        if tags_and_annotations and len(df_pivoted.columns) == 1:
            series_id_val = df_pivoted.columns[0]
            if tags_data:
                tags_values = []
                for valid_time in df_pivoted.index:
                    tags = tags_data.get((valid_time, series_id_val))
                    tags_values.append(tags)
                df_pivoted['tags'] = tags_values
            if annotation_data:
                annotation_values = []
                for valid_time in df_pivoted.index:
                    annotation = annotation_data.get((valid_time, series_id_val))
                    annotation_values.append(annotation)
                df_pivoted['annotation'] = annotation_values
    
    # Convert each column to pint-pandas dtype based on unit
    # Skip metadata columns (value_id, changed_by, change_time, tags, annotation)
    skip_columns = {'value_id', 'changed_by', 'change_time', 'tags', 'annotation'}
    for col_series_id in df_pivoted.columns:
        if col_series_id in skip_columns:
            continue  # Skip metadata columns
        unit = unit_map.get(col_series_id)
        if unit:
            # Convert to pint-pandas dtype
            pint_dtype = f"pint[{unit}]"
            df_pivoted[col_series_id] = df_pivoted[col_series_id].astype(pint_dtype)
    
    # Return based on return_mapping flag
    if return_mapping:
        # Return DataFrame with series_id columns and the mapping
        # Keep the column index name as "series_id" since columns are series_id
        df_pivoted.columns.name = "series_id"
        return df_pivoted, name_map
    else:
        # Rename columns from series_id to name using the mapping
        df_pivoted.rename(columns=name_map, inplace=True)
        # Update the column index name to "name" since columns are now name
        df_pivoted.columns.name = "name"
        return df_pivoted


def read_values_flat(
    series_id: Optional[uuid.UUID] = None,
    tenant_id: Optional[uuid.UUID] = None,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None,
    end_known: Optional[datetime] = None,
    all_versions: bool = False,
    return_mapping: bool = False,
    units: bool = False,
    return_value_id: bool = False,
) -> Union[pd.DataFrame, Tuple[pd.DataFrame, Dict[uuid.UUID, str]]]:
    """
    Read time series values in flat mode (latest known_time per valid_time).
    
    Returns the latest version of each (valid_time, series_id) combination,
    based on known_time. This is useful for getting the current state of
    time series data.
    
    Args:
        series_id: Series UUID (optional, if not provided reads all series)
        tenant_id: Tenant UUID (optional, defaults to zeros UUID for single-tenant installations)
        start_valid: Start of valid time range (optional)
        end_valid: End of valid time range (optional)
        start_known: Start of known_time range (optional)
        end_known: End of known_time range (optional)
        all_versions: If True, include all versions (not just current). If False, only is_current=True (default: False)
        return_mapping: If True, return both DataFrame and mapping dict (default: False)
        units: If True, return pint-pandas DataFrame with units. If False, return normal pandas DataFrame without unit column (default: False)
    
    Returns:
        If return_mapping=False:
            DataFrame with:
            - Index: valid_time
            - Columns: name (one column per series)
            - If units=True: Each column has pint-pandas dtype (e.g., dtype="pint[MW]")
            - If units=False: Normal pandas DataFrame without unit information
        
        If return_mapping=True:
            Tuple of (DataFrame, mapping_dict) where:
            - DataFrame has series_id as columns (not renamed to name)
            - mapping_dict maps series_id -> name
    """
    conninfo = _get_conninfo()
    
    # Use default tenant if not provided
    if tenant_id is None:
        tenant_id = DEFAULT_TENANT_ID
    
    # Get raw data from db layer
    df = db.read.read_values_flat(
        conninfo,
        tenant_id=tenant_id,
        series_id=series_id,
        start_valid=start_valid,
        end_valid=end_valid,
        start_known=start_known,
        end_known=end_known,
        all_versions=all_versions,
        return_value_id=return_value_id,
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
    
    # Handle value_id if requested - store it separately before pivoting
    value_id_data = None
    if return_value_id and 'value_id' in df_reset.columns:
        # Store value_id mapping: (valid_time, series_id) -> value_id
        value_id_data = df_reset.set_index(['valid_time', 'series_id'])['value_id'].to_dict()
    
    # Pivot the data: valid_time as index, series_id as columns
    df_pivoted = df_reset.pivot_table(
        index='valid_time',
        columns='series_id',
        values='value',
        aggfunc='first'  # In case of duplicates (shouldn't happen with DISTINCT ON)
    )
    
    # Sort index
    df_pivoted = df_pivoted.sort_index()
    
    # Add value_id as a column if requested
    # For single series, add as regular column. For multiple series, we'd need MultiIndex columns
    # For simplicity, we'll support it for single series case (most common use case)
    if return_value_id and value_id_data and len(df_pivoted.columns) == 1:
        # Single series - add value_id as a column
        series_id_val = df_pivoted.columns[0]
        value_ids = []
        for valid_time in df_pivoted.index:
            value_id = value_id_data.get((valid_time, series_id_val))
            value_ids.append(value_id)
        df_pivoted['value_id'] = value_ids
    
    # Handle units if requested
    if units:
        # Convert each column to pint-pandas dtype based on unit
        unit_map = df_reset[['series_id', 'unit']].drop_duplicates().set_index('series_id')['unit'].to_dict()
        for col_series_id in df_pivoted.columns:
            if col_series_id == 'value_id':
                continue  # Skip value_id column
            unit = unit_map.get(col_series_id)
            if unit:
                # Convert to pint-pandas dtype
                pint_dtype = f"pint[{unit}]"
                df_pivoted[col_series_id] = df_pivoted[col_series_id].astype(pint_dtype)
    
    # Return based on return_mapping flag
    if return_mapping:
        # Return DataFrame with series_id columns and the mapping
        # Keep the column index name as "series_id" since columns are series_id
        df_pivoted.columns.name = "series_id"
        return df_pivoted, name_map
    else:
        # Rename columns from series_id to name using the mapping
        # But keep value_id column as-is if it exists
        cols_to_rename = {sid: name_map[sid] for sid in df_pivoted.columns if sid != 'value_id' and sid in name_map}
        df_pivoted.rename(columns=cols_to_rename, inplace=True)
        # Update the column index name to "name" since columns are now name (except value_id)
        if 'value_id' not in df_pivoted.columns:
            df_pivoted.columns.name = "name"
        return df_pivoted


def read_values_overlapping(
    series_id: Optional[uuid.UUID] = None,
    tenant_id: Optional[uuid.UUID] = None,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None,
    end_known: Optional[datetime] = None,
    all_versions: bool = False,
    return_mapping: bool = False,
    units: bool = False,
) -> Union[pd.DataFrame, Tuple[pd.DataFrame, Dict[uuid.UUID, str]]]:
    """
    Read time series values in overlapping mode (all forecast revisions).
    
    Returns all versions of forecasts, showing how predictions evolve over time.
    This is useful for analyzing forecast revisions and backtesting.
    
    Args:
        series_id: Series UUID (optional, if not provided reads all series)
        tenant_id: Tenant UUID (optional, defaults to zeros UUID for single-tenant installations)
        start_valid: Start of valid time range (optional)
        end_valid: End of valid time range (optional)
        start_known: Start of known_time range (optional)
        end_known: End of known_time range (optional)
        all_versions: If True, include all versions (not just current). If False, only is_current=True (default: False)
        return_mapping: If True, return both DataFrame and mapping dict (default: False)
        units: If True, return pint-pandas DataFrame with units. If False, return normal pandas DataFrame without unit column (default: False)
    
    Returns:
        If return_mapping=False:
            DataFrame with:
            - Index: (known_time, valid_time) - double index
            - Columns: name (one column per series)
            - If units=True: Each column has pint-pandas dtype (e.g., dtype="pint[MW]")
            - If units=False: Normal pandas DataFrame without unit information
        
        If return_mapping=True:
            Tuple of (DataFrame, mapping_dict) where:
            - DataFrame has series_id as columns (not renamed to name)
            - Index: (known_time, valid_time) - double index
            - mapping_dict maps series_id -> name
    
    Examples:
        # Read all forecast revisions for a series
        df = td.read_values_overlapping(
            series_id=my_series_id,
            start_valid=datetime(2025, 1, 1, tzinfo=timezone.utc),
            end_valid=datetime(2025, 1, 4, tzinfo=timezone.utc)
        )
        
        # Read only forecasts made in a specific time range
        df = td.read_values_overlapping(
            series_id=my_series_id,
            start_known=datetime(2025, 1, 1, tzinfo=timezone.utc),
            end_known=datetime(2025, 1, 2, tzinfo=timezone.utc)
        )
    """
    conninfo = _get_conninfo()
    
    # Use default tenant if not provided
    if tenant_id is None:
        tenant_id = DEFAULT_TENANT_ID
    
    # Get raw data from db layer
    df = db.read.read_values_overlapping(
        conninfo,
        tenant_id=tenant_id,
        series_id=series_id,
        start_valid=start_valid,
        end_valid=end_valid,
        start_known=start_known,
        end_known=end_known,
        all_versions=all_versions,
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
    
    # Pivot the data: (known_time, valid_time) as index, series_id as columns
    df_pivoted = df_reset.pivot_table(
        index=['known_time', 'valid_time'],
        columns='series_id',
        values='value',
        aggfunc='first'  # In case of duplicates (shouldn't happen)
    )
    
    # Sort index
    df_pivoted = df_pivoted.sort_index()
    
    # Handle units if requested
    if units:
        # Convert each column to pint-pandas dtype based on unit
        unit_map = df_reset[['series_id', 'unit']].drop_duplicates().set_index('series_id')['unit'].to_dict()
        for col_series_id in df_pivoted.columns:
            unit = unit_map.get(col_series_id)
            if unit:
                # Convert to pint-pandas dtype
                pint_dtype = f"pint[{unit}]"
                df_pivoted[col_series_id] = df_pivoted[col_series_id].astype(pint_dtype)
    
    # Return based on return_mapping flag
    if return_mapping:
        # Return DataFrame with series_id columns and the mapping
        # Keep the column index name as "series_id" since columns are series_id
        df_pivoted.columns.name = "series_id"
        return df_pivoted, name_map
    else:
        # Rename columns from series_id to name using the mapping
        df_pivoted.rename(columns=name_map, inplace=True)
        # Update the column index name to "name" since columns are now name
        df_pivoted.columns.name = "name"
        return df_pivoted


def insert_batch(
    df: pd.DataFrame,
    tenant_id: Optional[uuid.UUID] = None,
    batch_id: Optional[uuid.UUID] = None,
    workflow_id: Optional[str] = None,
    batch_start_time: Optional[datetime] = None,  # Defaults to datetime.now(timezone.utc) if None
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
    - Inserts both the run metadata and the time series values atomically.
    
    Args:
        df: DataFrame containing time series data (required)
              Columns can be:
              - Pint Quantity objects (e.g., `power_vals * ureg.kW`)
              - pint-pandas Series with dtype="pint[unit]" (e.g., `pd.Series([1.2], dtype="pint[MW]")`)
              - Regular numeric columns (treated as dimensionless)
        tenant_id: Tenant UUID (optional, defaults to zeros UUID for single-tenant installations)
        batch_id: Unique identifier for the batch (optional, auto-generated if not provided)
        workflow_id: Workflow identifier (optional, defaults to "sdk-workflow" if not provided)
        batch_start_time: Start time of the batch (optional, defaults to datetime.now(timezone.utc))
        batch_finish_time: Optional finish time of the batch (must be timezone-aware if provided)
        valid_time_col: Column name for valid_time (default: 'valid_time')
        valid_time_end_col: Column name for valid_time_end for interval values (optional)
        known_time: Time of knowledge - when the data was known/available (optional, 
                    defaults to inserted_at in database if not provided)
        batch_params: Optional dictionary of batch parameters (will be stored as JSONB)
        name_overrides: Optional dict mapping column names to custom name values
                            (if not provided, column names are used as name)
        series_ids: Optional dict mapping column names (or name) to series_id UUIDs.
                   If provided for a series, that series_id will be used and no new series will be created.
                   If not provided for a series, a new series will be created using create_series.
        series_descriptions: Optional dict mapping column names (or name) to descriptions.
                           Used when creating new series (when series_id is not provided).
                           If not provided, description will be None for new series.
    
    Returns:
        InsertResult: Named tuple containing (batch_id, workflow_id, series_ids, tenant_id).
                      series_ids is a dict mapping name to series_id.
    
    Raises:
        IncompatibleUnitError: If unit conversion fails due to dimensionality mismatch
        ValueError: If DataFrame structure is invalid or units are inconsistent
    
    Examples:
        # Option 1: Using Pint Quantity objects
        import pint
        ureg = pint.UnitRegistry()
        
        df = pd.DataFrame({
            "valid_time": times,
            "power": power_vals_kW * ureg.kW,              # Series with kW unit
            "wind_speed": wind_vals_m_s * (ureg.meter / ureg.second),  # Series with m/s unit
            "temperature": temp_vals_C * ureg.degC          # Series with degC unit
        })
        
        # Option 2: Using pint-pandas Series
        df = pd.DataFrame({
            "valid_time": times,
            "power": pd.Series(power_vals, dtype="pint[MW]"),      # Series with MW unit
            "wind_speed": pd.Series(wind_vals, dtype="pint[m/s]"), # Series with m/s unit
        })
        
        # Insert - automatically creates series from columns
        result = td.insert_batch(df=df)
        # result.series_ids = {
        #     'power': <uuid>,
        #     'wind_speed': <uuid>,
        #     'temperature': <uuid>  # if using Option 1
        # }
        
        # With custom series keys
        result = td.insert_batch(
            df=df,
            name_overrides={
                'power': 'wind_power_forecast',
                'wind_speed': 'wind_speed_measured'
            }
        )
        
        # Interval values
        df_intervals = pd.DataFrame({
            "valid_time": start_times,
            "valid_time_end": end_times,
            "energy": energy_vals_MWh * ureg.MWh
        })
        result = td.insert_batch(
            df=df_intervals,
            valid_time_end_col='valid_time_end'
        )
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
        # Check if it's a table-related error
        error_msg = str(e)
        if "batches_table" in error_msg or "values_table" in error_msg or "series_table" in error_msg or "does not exist" in error_msg:
            raise ValueError(
                "TimeDB tables do not exist. Please create the schema first by running:\n"
                "  td.create()"
            ) from None
        raise
    except Exception as e:
        # Handle case where the error might be wrapped
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


def update_records(
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
    
    Examples:
        # Update by value_id (simplest)
        result = td.update_records([{
            "value_id": 123,
            "value": 25.5,
            "annotation": "Corrected value",
            "tags": ["reviewed"],
        }])
        
        # Update by key (backwards compatible)
        result = td.update_records([{
            "batch_id": batch_id,
            "valid_time": datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc),
            "series_id": series_id,
            "value": 25.5,
        }])
    """
    conninfo = _get_conninfo()
    
    # Set default tenant_id for updates by key if not provided
    for update_dict in updates:
        if "value_id" not in update_dict:
            if "tenant_id" not in update_dict:
                update_dict["tenant_id"] = DEFAULT_TENANT_ID
    
    with psycopg.connect(conninfo) as conn:
        return db.update.update_records(conn, updates=updates)
