"""
High-level SDK for TimeDB (TimescaleDB version).

Provides a simple interface for working with TimeDB, including automatic
DataFrame conversion for time series data with unit handling using Pint Quantity objects.

The SDK exposes two main classes:
- TimeDataClient: Main entry point for database operations
- SeriesCollection: Fluent API for series filtering and operations

Data model:
- Flat: Immutable fact data (meter readings, measurements). Stored in 'flat' table.
- Overlapping: Versioned estimates (forecasts). Stored in 'overlapping' table
  (list-partitioned by retention) with known_time versioning.
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
    """Result from insert containing the IDs that were used."""
    batch_id: uuid.UUID
    workflow_id: str
    series_ids: Dict[str, uuid.UUID]  # Maps name to series_id


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
        _meta_cache: Optional[Dict[uuid.UUID, Dict[str, str]]] = None,
    ):
        self._conninfo = conninfo
        self._name = name
        self._unit = unit
        self._label_filters = label_filters or {}
        self._id_cache = _id_cache or {}
        self._meta_cache = _meta_cache or {}  # series_id -> {data_class, retention}
        self._resolved = False

    def where(self, **labels) -> 'SeriesCollection':
        """
        Add additional label filters to narrow down the collection.

        Returns a new SeriesCollection with the combined filters (immutable).
        """
        new_filters = {**self._label_filters, **labels}
        return SeriesCollection(
            conninfo=self._conninfo,
            name=self._name,
            unit=self._unit,
            label_filters=new_filters,
            _id_cache=self._id_cache.copy(),
            _meta_cache=self._meta_cache.copy(),
        )

    def _resolve_ids(self) -> List[uuid.UUID]:
        """
        Resolve series IDs that match the current filters.
        Also caches data_class and retention for routing.
        """
        if not self._resolved:
            import psycopg
            import json
            with psycopg.connect(self._conninfo) as conn:
                with conn.cursor() as cur:
                    sql = "SELECT series_id, labels, data_class, retention FROM series_table"
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

            for series_id, labels, data_class, retention in rows:
                label_set = frozenset((k, v) for k, v in (labels or {}).items())
                self._id_cache[label_set] = series_id
                self._meta_cache[series_id] = {
                    "data_class": data_class,
                    "retention": retention,
                }
            self._resolved = True

        if not self._label_filters:
            return list(self._id_cache.values())

        matching_ids = []
        filter_set = set(self._label_filters.items())
        for label_set, series_id in self._id_cache.items():
            if filter_set.issubset(label_set):
                matching_ids.append(series_id)
        return matching_ids

    def _get_single_id(self) -> uuid.UUID:
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

    def _get_data_classes(self) -> set:
        """Get the set of data_classes for all resolved series."""
        ids = self._resolve_ids()
        return {self._meta_cache[sid]["data_class"] for sid in ids if sid in self._meta_cache}

    def _get_series_routing(self) -> Dict[uuid.UUID, Dict[str, str]]:
        """Get routing info (data_class, retention) for all resolved series."""
        ids = self._resolve_ids()
        return {sid: self._meta_cache[sid] for sid in ids if sid in self._meta_cache}

    def insert(
        self,
        df: pd.DataFrame,
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

        Automatically routes data to the correct table based on the series' data_class:
        - flat: inserts into 'flat' table (immutable facts, upsert on conflict)
        - overlapping: inserts into 'overlapping_{tier}' table with batch and known_time

        Args:
            df: DataFrame with time series data
            batch_id: Batch UUID (optional, auto-generated)
            workflow_id: Workflow identifier (optional)
            batch_start_time: Start time (optional)
            batch_finish_time: Finish time (optional)
            valid_time_col: Valid time column name
            valid_time_end_col: Valid time end column (for intervals)
            known_time: Time of knowledge (optional, used for overlapping)
            batch_params: Batch parameters (optional)

        Returns:
            InsertResult with batch_id, workflow_id, series_ids
        """
        series_ids = self._resolve_ids()

        if len(series_ids) == 0:
            raise ValueError(
                f"No series found matching filters: name={self._name}, "
                f"unit={self._unit}, labels={self._label_filters}"
            )

        series_routing = self._get_series_routing()

        if len(series_ids) == 1:
            series_id = series_ids[0]
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
            col_name = list(series_info.keys())[0]
            return _insert(
                df=df,
                batch_id=batch_id,
                workflow_id=workflow_id,
                batch_start_time=batch_start_time,
                batch_finish_time=batch_finish_time,
                valid_time_col=valid_time_col,
                valid_time_end_col=valid_time_end_col,
                known_time=known_time,
                batch_params=batch_params,
                series_ids={col_name: series_id},
                series_routing=series_routing,
            )
        else:
            return _insert(
                df=df,
                batch_id=batch_id,
                workflow_id=workflow_id,
                batch_start_time=batch_start_time,
                batch_finish_time=batch_finish_time,
                valid_time_col=valid_time_col,
                valid_time_end_col=valid_time_end_col,
                known_time=known_time,
                batch_params=batch_params,
                series_routing=series_routing,
            )

    # Keep insert_batch as alias for backward compatibility
    insert_batch = insert

    def update_records(self, updates: List[Dict[str, Any]]) -> Dict[str, List]:
        """
        Update records for series in this collection.

        Supports both flat and overlapping series:

        **Flat series**: In-place update (no versioning).
        - Key: (series_id, valid_time)
        - Updateable fields: value, annotation, tags, changed_by

        **Overlapping series**: Creates a new version with known_time=now().
        Lookup priority (use what you have):
        - known_time + valid_time: Exact version lookup
        - batch_id + valid_time: Latest version in that batch
        - Just valid_time: Latest version overall

        Args:
            updates: List of update dicts. Each dict must include:
                - valid_time: Required for both flat and overlapping
                - series_id: Required if collection matches multiple series
                Optional fields:
                - value: New value (if omitted, keeps current value)
                - annotation: Text annotation (None to clear)
                - tags: List of tags ([] to clear)
                - changed_by: User identifier
                For overlapping only:
                - batch_id: Target specific batch
                - known_time: Target specific version

        Returns:
            Dict with 'updated' and 'skipped_no_ops' lists.
        """
        if not updates:
            return {"updated": [], "skipped_no_ops": []}

        series_ids = self._resolve_ids()
        if len(series_ids) == 0:
            raise ValueError(
                f"No series found matching filters: name={self._name}, unit={self._unit}, labels={self._label_filters}"
            )

        single_series = len(series_ids) == 1
        filled_updates: List[Dict[str, Any]] = []

        for u in updates:
            u_copy = u.copy()

            if "overlapping_id" in u_copy:
                if "series_id" not in u_copy and single_series:
                    u_copy["series_id"] = series_ids[0]
                filled_updates.append(u_copy)
                continue

            if "series_id" not in u_copy:
                if single_series:
                    u_copy["series_id"] = series_ids[0]
                else:
                    raise ValueError(
                        "For collections matching multiple series, each update must include 'series_id'."
                    )

            filled_updates.append(u_copy)

        return _update_records(filled_updates)

    def read(
        self,
        start_valid: Optional[datetime] = None,
        end_valid: Optional[datetime] = None,
        start_known: Optional[datetime] = None,
        end_known: Optional[datetime] = None,
        versions: bool = False,
        return_mapping: bool = False,
    ) -> Union[pd.DataFrame, Tuple[pd.DataFrame, Dict[uuid.UUID, str]]]:
        """
        Read time series data for this collection.

        Automatically reads from the correct table based on the series' data_class:
        - flat: reads from 'flat' table (no versioning)
        - overlapping: reads from 'latest_overlapping_curve' (default) or
          'all_overlapping_raw' (if versions=True)

        Args:
            start_valid: Start of valid time range (optional)
            end_valid: End of valid time range (optional)
            start_known: Start of known_time range (optional, overlapping only)
            end_known: End of known_time range (optional, overlapping only)
            versions: If True, return all overlapping revisions (default: False)
            return_mapping: Return (DataFrame, mapping_dict) if True

        Returns:
            DataFrame (or tuple of DataFrame and mapping dict)
        """
        series_ids = self._resolve_ids()

        if len(series_ids) == 0:
            raise ValueError(
                f"No series found matching filters: name={self._name}, "
                f"unit={self._unit}, labels={self._label_filters}"
            )

        data_classes = self._get_data_classes()

        if data_classes == {"flat"}:
            return _read_flat(
                series_ids=series_ids,
                start_valid=start_valid,
                end_valid=end_valid,
                return_mapping=return_mapping,
            )
        elif data_classes == {"overlapping"}:
            if versions:
                return _read_overlapping_all(
                    series_ids=series_ids,
                    start_valid=start_valid,
                    end_valid=end_valid,
                    start_known=start_known,
                    end_known=end_known,
                    return_mapping=return_mapping,
                )
            else:
                return _read_overlapping_latest(
                    series_ids=series_ids,
                    start_valid=start_valid,
                    end_valid=end_valid,
                    start_known=start_known,
                    end_known=end_known,
                    return_mapping=return_mapping,
                )
        else:
            # Mixed flat and overlapping - read both and merge
            flat_ids = [sid for sid in series_ids if self._meta_cache[sid]["data_class"] == "flat"]
            overlapping_ids = [sid for sid in series_ids if self._meta_cache[sid]["data_class"] == "overlapping"]

            dfs = []
            mappings = {}

            if flat_ids:
                result = _read_flat(
                    series_ids=flat_ids,
                    start_valid=start_valid,
                    end_valid=end_valid,
                    return_mapping=True,
                )
                dfs.append(result[0])
                mappings.update(result[1])

            if overlapping_ids:
                if versions:
                    result = _read_overlapping_all(
                        series_ids=overlapping_ids,
                        start_valid=start_valid,
                        end_valid=end_valid,
                        start_known=start_known,
                        end_known=end_known,
                        return_mapping=True,
                    )
                else:
                    result = _read_overlapping_latest(
                        series_ids=overlapping_ids,
                        start_valid=start_valid,
                        end_valid=end_valid,
                        start_known=start_known,
                        end_known=end_known,
                        return_mapping=True,
                    )
                dfs.append(result[0])
                mappings.update(result[1])

            if not dfs:
                empty_index = pd.DatetimeIndex([], name="valid_time", tz="UTC")
                if return_mapping:
                    return pd.DataFrame(index=empty_index), {}
                return pd.DataFrame(index=empty_index)

            combined = pd.concat(dfs, axis=1)
            if return_mapping:
                return combined, mappings
            combined.rename(columns=mappings, inplace=True)
            combined.columns.name = "name"
            return combined

    # Keep read_overlapping as convenience alias
    def read_overlapping(self, **kwargs) -> Union[pd.DataFrame, Tuple[pd.DataFrame, Dict[uuid.UUID, str]]]:
        """Read all overlapping revisions. Alias for read(versions=True)."""
        return self.read(versions=True, **kwargs)

    def list_labels(self, label_key: str) -> List[str]:
        """List all unique values for a specific label key in this collection."""
        self._resolve_ids()
        values = set()
        for label_set in self._id_cache.keys():
            for key, value in label_set:
                if key == label_key:
                    values.add(value)
        return sorted(list(values))

    def count(self) -> int:
        """Count how many series match the current filters."""
        return len(self._resolve_ids())

    def __repr__(self) -> str:
        return (
            f"SeriesCollection(name={self._name!r}, unit={self._unit!r}, "
            f"labels={self._label_filters!r}, resolved={self._resolved})"
        )


class TimeDataClient:
    """
    High-level client for TimeDB with fluent API for series selection.
    """

    def __init__(self):
        self._conninfo = _get_conninfo()

    def series(
        self,
        name: Optional[str] = None,
        unit: Optional[str] = None,
    ) -> SeriesCollection:
        """
        Start building a series collection by name and/or unit.

        Returns a SeriesCollection that can be further filtered using .where().
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
        data_class: str = "flat",
        retention: str = "medium",
    ) -> uuid.UUID:
        """
        Create a new series.

        Args:
            name: Parameter name (e.g., 'wind_power')
            unit: Canonical unit (e.g., 'MW', 'dimensionless')
            labels: Dictionary of labels (e.g., {"site": "Gotland"})
            description: Optional description
            data_class: 'flat' or 'overlapping' (default: 'flat')
            retention: 'short', 'medium', or 'long' (default: 'medium', only for overlapping)

        Returns:
            The series_id (UUID) for the newly created series
        """
        return _create_series(
            name=name,
            unit=unit,
            labels=labels,
            description=description,
            data_class=data_class,
            retention=retention,
        )

    def update_records(self, updates: List[Dict[str, Any]]) -> Dict[str, List]:
        """
        Update records for flat or overlapping series.

        Flat series: In-place update by (series_id, valid_time).
        Overlapping series: Creates new version. Lookup by known_time, batch_id, or latest.

        See SeriesCollection.update_records for full documentation.
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

    Returns:
        Dictionary mapping name (column name) to unit (canonical unit string)
    """
    exclude_cols = {valid_time_col}
    if valid_time_end_col is not None:
        exclude_cols.add(valid_time_end_col)

    series_cols = [col for col in df.columns if col not in exclude_cols]

    if not series_cols:
        raise ValueError("No series columns found in DataFrame (excluding time columns)")

    series_info = {}

    for col in series_cols:
        if is_pint_pandas_series(df[col]):
            unit = extract_unit_from_pint_pandas_series(df[col])
            if unit is None:
                raise ValueError(f"Column '{col}' has pint dtype but unit extraction failed")
            series_info[col] = unit
            continue

        first_value = None
        for val in df[col]:
            if pd.notna(val):
                first_value = val
                break

        if first_value is None:
            raise ValueError(
                f"Column '{col}' has no non-null values. Cannot determine unit."
            )

        unit = extract_unit_from_quantity(first_value)

        if unit is None:
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
            unit = "dimensionless"
        else:
            for val in df[col]:
                if pd.notna(val):
                    if not isinstance(val, pint.Quantity):
                        raise ValueError(
                            f"Column '{col}' has mixed Pint Quantity and regular values. "
                            "All values in a column must be Pint Quantities if the first value is a Quantity."
                        )
                    val_unit = extract_unit_from_quantity(val)
                    if val_unit is not None and val_unit != unit:
                        try:
                            from .units import validate_unit_compatibility
                            validate_unit_compatibility(val_unit, unit)
                        except IncompatibleUnitError:
                            raise ValueError(
                                f"Column '{col}' has inconsistent units: "
                                f"found {unit} and {val_unit} which are incompatible"
                            )

        series_info[col] = unit

    return series_info


def _dataframe_to_value_rows(
    df: pd.DataFrame,
    series_mapping: Dict[str, uuid.UUID],
    units: Dict[str, str],
    valid_time_col: str = 'valid_time',
    valid_time_end_col: Optional[str] = None,
) -> List[Tuple]:
    """
    Convert a pandas DataFrame to TimeDB value_rows format.

    Returns:
        List of tuples:
        - Point-in-time: (tenant_id, valid_time, series_id, value)
        - Interval: (tenant_id, valid_time, valid_time_end, series_id, value)
    """
    if valid_time_col not in df.columns:
        raise ValueError(f"Column '{valid_time_col}' not found in DataFrame")

    exclude_cols = {valid_time_col}
    if valid_time_end_col is not None:
        exclude_cols.add(valid_time_end_col)

    series_cols = [col for col in df.columns if col not in exclude_cols]

    if not series_cols:
        raise ValueError("No series columns found in DataFrame (excluding time columns)")

    has_intervals = valid_time_end_col is not None and valid_time_end_col in df.columns

    pint_pandas_cols = {
        col: extract_unit_from_pint_pandas_series(df[col])
        for col in series_cols
        if is_pint_pandas_series(df[col])
    }

    rows = []
    for _, row in df.iterrows():
        valid_time = row[valid_time_col]

        if isinstance(valid_time, pd.Timestamp):
            if valid_time.tzinfo is None:
                raise ValueError("valid_time must be timezone-aware. Found timezone-naive datetime.")
        elif isinstance(valid_time, datetime):
            if valid_time.tzinfo is None:
                raise ValueError("valid_time must be timezone-aware. Found timezone-naive datetime.")

        valid_time_end = None
        if has_intervals:
            valid_time_end = row[valid_time_end_col]
            if isinstance(valid_time_end, pd.Timestamp):
                if valid_time_end.tzinfo is None:
                    raise ValueError("valid_time_end must be timezone-aware. Found timezone-naive datetime.")
            elif isinstance(valid_time_end, datetime):
                if valid_time_end.tzinfo is None:
                    raise ValueError("valid_time_end must be timezone-aware. Found timezone-naive datetime.")

        for name in series_cols:
            series_id = series_mapping[name]
            canonical_unit = units[name]

            value = row[name]

            if pd.isna(value):
                converted_value = None
            else:
                try:
                    if isinstance(value, pint.Quantity):
                        converted_value = convert_quantity_to_canonical_unit(value, canonical_unit)
                    else:
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
                rows.append((DEFAULT_TENANT_ID, valid_time, valid_time_end, series_id, converted_value))
            else:
                rows.append((DEFAULT_TENANT_ID, valid_time, series_id, converted_value))

    return rows


def _create() -> None:
    """Create or update the database schema (TimescaleDB version)."""
    conninfo = _get_conninfo()
    db.create.create_schema(conninfo)


def _create_series(
    name: str,
    unit: str = "dimensionless",
    labels: Optional[Dict[str, str]] = None,
    description: Optional[str] = None,
    data_class: str = "flat",
    retention: str = "medium",
) -> uuid.UUID:
    """
    Create a new time series.

    Args:
        name: Parameter name (e.g., 'wind_power', 'temperature')
        unit: Canonical unit for the series
        labels: Dictionary of labels
        description: Optional description
        data_class: 'flat' or 'overlapping' (default: 'flat')
        retention: 'short', 'medium', or 'long' (default: 'medium')

    Returns:
        The series_id (UUID) for the newly created series
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
                data_class=data_class,
                retention=retention,
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
    """Delete all TimeDB tables and views."""
    conninfo = _get_conninfo()
    db.delete.delete_schema(conninfo)


def _read_flat(
    series_ids: Optional[List[uuid.UUID]] = None,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    return_mapping: bool = False,
) -> Union[pd.DataFrame, Tuple[pd.DataFrame, Dict[uuid.UUID, str]]]:
    """Read flat values and pivot into a wide DataFrame."""
    conninfo = _get_conninfo()

    df = db.read.read_flat(
        conninfo,
        tenant_id=DEFAULT_TENANT_ID,
        series_ids=series_ids,
        start_valid=start_valid,
        end_valid=end_valid,
    )

    if len(df) == 0:
        if return_mapping:
            return pd.DataFrame(index=pd.DatetimeIndex([], name="valid_time", tz="UTC")), {}
        else:
            return pd.DataFrame(index=pd.DatetimeIndex([], name="valid_time", tz="UTC"))

    df_reset = df.reset_index()

    name_map = df_reset[['series_id', 'name']].drop_duplicates().set_index('series_id')['name'].to_dict()
    unit_map = df_reset[['series_id', 'unit']].drop_duplicates().set_index('series_id')['unit'].to_dict()

    df_pivoted = df_reset.pivot_table(
        index='valid_time',
        columns='series_id',
        values='value',
        aggfunc='first',
    )
    df_pivoted = df_pivoted.sort_index()

    for col_series_id in df_pivoted.columns:
        unit = unit_map.get(col_series_id)
        if unit:
            df_pivoted[col_series_id] = df_pivoted[col_series_id].astype(f"pint[{unit}]")

    if return_mapping:
        df_pivoted.columns.name = "series_id"
        return df_pivoted, name_map
    else:
        df_pivoted.rename(columns=name_map, inplace=True)
        df_pivoted.columns.name = "name"
        return df_pivoted


def _read_overlapping_latest(
    series_ids: Optional[List[uuid.UUID]] = None,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None,
    end_known: Optional[datetime] = None,
    return_mapping: bool = False,
) -> Union[pd.DataFrame, Tuple[pd.DataFrame, Dict[uuid.UUID, str]]]:
    """Read latest overlapping values and pivot into a wide DataFrame."""
    conninfo = _get_conninfo()

    df = db.read.read_overlapping_latest(
        conninfo,
        tenant_id=DEFAULT_TENANT_ID,
        series_ids=series_ids,
        start_valid=start_valid,
        end_valid=end_valid,
        start_known=start_known,
        end_known=end_known,
    )

    if len(df) == 0:
        if return_mapping:
            return pd.DataFrame(index=pd.DatetimeIndex([], name="valid_time", tz="UTC")), {}
        else:
            return pd.DataFrame(index=pd.DatetimeIndex([], name="valid_time", tz="UTC"))

    df_reset = df.reset_index()

    name_map = df_reset[['series_id', 'name']].drop_duplicates().set_index('series_id')['name'].to_dict()
    unit_map = df_reset[['series_id', 'unit']].drop_duplicates().set_index('series_id')['unit'].to_dict()

    df_pivoted = df_reset.pivot_table(
        index='valid_time',
        columns='series_id',
        values='value',
        aggfunc='first',
    )
    df_pivoted = df_pivoted.sort_index()

    for col_series_id in df_pivoted.columns:
        unit = unit_map.get(col_series_id)
        if unit:
            df_pivoted[col_series_id] = df_pivoted[col_series_id].astype(f"pint[{unit}]")

    if return_mapping:
        df_pivoted.columns.name = "series_id"
        return df_pivoted, name_map
    else:
        df_pivoted.rename(columns=name_map, inplace=True)
        df_pivoted.columns.name = "name"
        return df_pivoted


def _read_overlapping_all(
    series_ids: Optional[List[uuid.UUID]] = None,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None,
    end_known: Optional[datetime] = None,
    return_mapping: bool = False,
) -> Union[pd.DataFrame, Tuple[pd.DataFrame, Dict[uuid.UUID, str]]]:
    """Read all overlapping versions and pivot into a wide DataFrame."""
    conninfo = _get_conninfo()

    df = db.read.read_overlapping_all(
        conninfo,
        tenant_id=DEFAULT_TENANT_ID,
        series_ids=series_ids,
        start_valid=start_valid,
        end_valid=end_valid,
        start_known=start_known,
        end_known=end_known,
    )

    if len(df) == 0:
        empty_index = pd.MultiIndex.from_tuples([], names=["known_time", "valid_time"])
        if return_mapping:
            return pd.DataFrame(index=empty_index), {}
        else:
            return pd.DataFrame(index=empty_index)

    df_reset = df.reset_index()

    name_map = df_reset[['series_id', 'name']].drop_duplicates().set_index('series_id')['name'].to_dict()
    unit_map = df_reset[['series_id', 'unit']].drop_duplicates().set_index('series_id')['unit'].to_dict()

    df_pivoted = df_reset.pivot_table(
        index=['known_time', 'valid_time'],
        columns='series_id',
        values='value',
        aggfunc='first',
    )
    df_pivoted = df_pivoted.sort_index()

    for col_series_id in df_pivoted.columns:
        unit = unit_map.get(col_series_id)
        if unit:
            df_pivoted[col_series_id] = df_pivoted[col_series_id].astype(f"pint[{unit}]")

    if return_mapping:
        df_pivoted.columns.name = "series_id"
        return df_pivoted, name_map
    else:
        df_pivoted.rename(columns=name_map, inplace=True)
        df_pivoted.columns.name = "name"
        return df_pivoted


def _insert(
    df: pd.DataFrame,
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
    series_routing: Optional[Dict[uuid.UUID, Dict[str, str]]] = None,
) -> InsertResult:
    """
    Insert a batch with time series data from a pandas DataFrame.

    Routes data to the correct table based on series data_class and retention.
    """
    conninfo = _get_conninfo()

    if batch_id is None:
        batch_id = uuid.uuid4()
    if workflow_id is None:
        workflow_id = "sdk-workflow"
    if batch_start_time is None:
        batch_start_time = datetime.now(timezone.utc)
    elif batch_start_time.tzinfo is None:
        raise ValueError("batch_start_time must be timezone-aware")

    series_info = _detect_series_from_dataframe(
        df=df,
        valid_time_col=valid_time_col,
        valid_time_end_col=valid_time_end_col,
    )

    series_mapping = {}
    units = {}

    if name_overrides is None:
        name_overrides = {}
    if series_ids is None:
        series_ids = {}
    if series_descriptions is None:
        series_descriptions = {}
    if series_routing is None:
        series_routing = {}

    with psycopg.connect(conninfo) as conn:
        for col_name, canonical_unit in series_info.items():
            name = name_overrides.get(col_name, col_name)
            provided_series_id = series_ids.get(col_name) or series_ids.get(name)

            if provided_series_id is not None:
                try:
                    series_info_db = db.series.get_series_info(conn, provided_series_id)
                    series_id = provided_series_id
                    canonical_unit = series_info_db['unit']
                    # Cache routing info if not already present
                    if series_id not in series_routing:
                        series_routing[series_id] = {
                            "data_class": series_info_db['data_class'],
                            "retention": series_info_db['retention'],
                        }
                except ValueError as e:
                    raise ValueError(f"series_id {provided_series_id} not found in database") from e
            else:
                description = series_descriptions.get(col_name) or series_descriptions.get(name)
                series_id = db.series.create_series(
                    conn,
                    name=name,
                    description=description,
                    unit=canonical_unit,
                )
                # New series defaults: look up what was created
                if series_id not in series_routing:
                    info = db.series.get_series_info(conn, series_id)
                    series_routing[series_id] = {
                        "data_class": info['data_class'],
                        "retention": info['retention'],
                    }

            series_mapping[col_name] = series_id
            units[col_name] = canonical_unit

    value_rows = _dataframe_to_value_rows(
        df=df,
        series_mapping=series_mapping,
        units=units,
        valid_time_col=valid_time_col,
        valid_time_end_col=valid_time_end_col,
    )

    try:
        db.insert.insert_batch_with_values(
            conninfo=conninfo,
            batch_id=batch_id,
            tenant_id=DEFAULT_TENANT_ID,
            workflow_id=workflow_id,
            batch_start_time=batch_start_time,
            batch_finish_time=batch_finish_time,
            value_rows=value_rows,
            known_time=known_time,
            batch_params=batch_params,
            series_routing=series_routing,
        )
    except (errors.UndefinedTable, errors.UndefinedObject) as e:
        error_msg = str(e)
        if any(t in error_msg for t in ["batches_table", "flat", "overlapping_", "series_table", "does not exist"]):
            raise ValueError(
                "TimeDB tables do not exist. Please create the schema first by running:\n"
                "  td.create()"
            ) from None
        raise
    except Exception as e:
        error_msg = str(e)
        if any(t in error_msg for t in ["batches_table", "flat", "overlapping_", "series_table", "does not exist"]):
            raise ValueError(
                "TimeDB tables do not exist. Please create the schema first by running:\n"
                "  td.create()"
            ) from None
        raise

    final_series_ids = {}
    for col_name, series_id in series_mapping.items():
        name = name_overrides.get(col_name, col_name)
        final_series_ids[name] = series_id

    return InsertResult(
        batch_id=batch_id,
        workflow_id=workflow_id,
        series_ids=final_series_ids,
    )


# Keep _insert_batch as alias for backward compatibility
_insert_batch = _insert


def _update_records(
    updates: List[Dict[str, Any]],
) -> Dict[str, List]:
    """
    Update overlapping records (flat are immutable).

    Wrapper around db.update.update_records that handles the database connection.
    """
    conninfo = _get_conninfo()

    for update_dict in updates:
        update_dict["tenant_id"] = DEFAULT_TENANT_ID

    with psycopg.connect(conninfo) as conn:
        return db.update.update_records(conn, updates=updates)
