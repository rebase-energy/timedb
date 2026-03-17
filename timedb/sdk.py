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
  (list-partitioned by retention) with knowledge_time versioning.
"""
import atexit
import os
import uuid
import warnings
from contextlib import contextmanager
from typing import Optional, List, Tuple, NamedTuple, Dict, Union, Any
from datetime import datetime, timedelta, timezone, time
import pandas as pd
import polars as pl
import pyarrow as pa

from . import db
from .db.series import SeriesRegistry
from timedatamodel import TimeSeriesPolars, DataShape, TimeSeriesType
import psycopg
from psycopg import errors
from psycopg_pool import ConnectionPool


class InsertResult(NamedTuple):
    """Result from insert containing the batch_id and series_id."""
    batch_id: uuid.UUID
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
        >>> client.get_series('wind_power').where(site='offshore_1').read()

        >>> # Multiple filters (chained)
        >>> client.get_series(unit='MW').where(site='offshore_1', turbine='T01').read()

        >>> # Direct lookup by series_id
        >>> client.get_series(series_id=123).read()

        >>> # Count matching series
        >>> count = client.get_series('wind_power').count()
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
            >>> coll = client.get_series('wind_power')
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
        data: Union[pd.DataFrame, "TimeSeries"],
        workflow_id: Optional[str] = None,
        batch_start_time: Optional[datetime] = None,
        batch_finish_time: Optional[datetime] = None,
        knowledge_time: Optional[datetime] = None,
        batch_params: Optional[dict] = None,
    ) -> InsertResult:
        """
        Insert time series data for this collection.

        **Single-series inserts only.**  Accepts either:

        - A :class:`~timedb.timeseries.TimeSeries` with shape ``SIMPLE`` or
          ``VERSIONED``.  The series' ``unit`` is compared against the stored
          unit and a conversion factor is applied automatically when they differ.
        - A ``pd.DataFrame`` with columns ``[valid_time, value]``,
          ``[valid_time, valid_time_end, value]``,
          ``[knowledge_time, valid_time, value]``, or
          ``[knowledge_time, valid_time, valid_time_end, value]``.

        Automatically routes data to the correct table based on the series'
        overlapping flag:

        - flat (``overlapping=False``): inserts into ``flat`` table (upsert on
          conflict by ``(series_id, valid_time)``).
        - overlapping (``overlapping=True``): appends into ``overlapping_{tier}``
          table (no conflict resolution).

        Both flat and overlapping series support per-row ``knowledge_time``
        values.  Pass a DataFrame with a ``knowledge_time`` column (or a
        ``VERSIONED`` :class:`~timedb.timeseries.TimeSeries`) to store a
        different knowledge_time per row.  The ``knowledge_time`` keyword
        argument is a convenience shortcut that broadcasts a single value to
        all rows; passing it together with a ``knowledge_time`` column raises
        :class:`ValueError`.

        Args:
            data: TimeSeries or DataFrame with time series data
            workflow_id: Workflow identifier (optional)
            batch_start_time: Start time (optional)
            batch_finish_time: Finish time (optional)
            knowledge_time: Time of knowledge broadcast to all rows (optional).
                Defaults to ``now()`` when neither this kwarg nor a
                ``knowledge_time`` column is present in the data.
            batch_params: Batch parameters (optional)

        Returns:
            InsertResult with batch_id (uuid.UUID), workflow_id, series_id.
            One batch is always created per insert() call regardless of how
            many unique knowledge_times are in the data.

        Raises:
            ValueError: If collection matches multiple series (use more specific filters)
            ValueError: If input data doesn't have the required columns or shape
            ValueError: If knowledge_time kwarg and a knowledge_time column are both provided
        """
        series_id = self._get_single_id()
        routing = self._get_series_routing(series_id)
        series_unit = self._registry.get_cached(series_id)["unit"]

        table, shape = _normalize_insert_input(data, series_unit, knowledge_time=knowledge_time)

        return _insert(
            table=table,
            shape=shape,
            series_id=series_id,
            routing=routing,
            workflow_id=workflow_id,
            batch_start_time=batch_start_time,
            batch_finish_time=batch_finish_time,
            batch_params=batch_params,
            _pool=self._pool,
        )

    def update_records(self, updates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Update records for this collection.

        **Single-series only.** Collection must resolve to exactly one series.

        Supports both flat and overlapping series:
        - **Flat**: In-place update (no versioning) by (series_id, valid_time)
        - **Overlapping**: Appends a correction row, preserving the original knowledge_time
          and stamping change_time=now().
          Lookup priority:
          - knowledge_time + valid_time: Exact version lookup
          - valid_time only: Latest version overall

        .. note::
            Values must be in the series' canonical unit. Unlike ``insert()``,
            no automatic unit conversion is performed here.

        Args:
            updates: List of update dicts. Each item must include ``valid_time``.
                Optional fields are ``value`` (new value, must be in the series'
                canonical unit), ``annotation`` (text annotation; set to ``None``
                to clear), ``tags`` (set to ``[]`` to clear), ``changed_by``
                (user identifier), and ``knowledge_time`` (target specific
                version, overlapping only).

        Returns:
            List of dicts with update info for each updated record

        Raises:
            ValueError: If collection matches multiple series or no series

        Example:
            >>> td.get_series("temperature").where(site="A").update_records([
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
        overlapping: bool = False,
        include_updates: bool = False,
    ) -> "TimeSeries":
        """
        Read time series data for this collection.

        Returns a :class:`~timedb.timeseries.TimeSeries` instance.
        Call ``.to_pandas()`` on it to get a ``pd.DataFrame`` with the
        conventional index.

        **Single-series reads only.** Collection must resolve to exactly one series.

        Args:
            start_valid: Start of valid time range (optional)
            end_valid: End of valid time range (optional)
            start_known: Start of knowledge_time range (optional)
            end_known: End of knowledge_time range (optional)
            overlapping: Controls whether forecast history is exposed (default: False).

                - False: one row per valid_time with the **latest** value — the most
                  recent forecast run wins; corrections within that run are resolved to
                  the latest change_time. Index: ``[valid_time]``
                - True: one row per (knowledge_time, valid_time) showing how all forecast
                  runs compare against each other; corrections within each run are
                  resolved (only the latest change_time is shown). Raises ValueError for
                  flat series. Index: ``[knowledge_time, valid_time]``
            include_updates: If True, expose the correction chain:

                - Combined with ``overlapping=False`` (default): returns all corrections
                  for the currently winning forecast run, hiding knowledge_time.
                  Works for both flat and overlapping series.
                  Index: ``[valid_time, change_time]``, columns: ``[value, changed_by, annotation]``

                - Combined with ``overlapping=True``: returns the full bi-temporal matrix —
                  every model run and every correction ever made. Raises ValueError for flat.
                  Index: ``[knowledge_time, change_time, valid_time]``

        Returns:
            DataFrame with an index and columns that depend on the flag combination:

            +--------------+-----------------+-------------------------------------+
            | overlapping  | include_updates | Index                               |
            +==============+=================+=====================================+
            | False        | False (default) | [valid_time]                        |
            +--------------+-----------------+-------------------------------------+
            | False        | True            | [valid_time, change_time]           |
            +--------------+-----------------+-------------------------------------+
            | True         | False           | [knowledge_time, valid_time]        |
            +--------------+-----------------+-------------------------------------+
            | True         | True            | [knowledge_time, change_time,       |
            |              |                 |  valid_time]                        |
            +--------------+-----------------+-------------------------------------+

        Raises:
            ValueError: If collection matches multiple series, no series, or
                ``overlapping=True`` is used with a flat series

        Example:
            >>> # Latest forecast — the single best value per timestamp
            >>> df = td.get_series("wind_power").where(site="Gotland").read()
            >>>
            >>> # Who edited the numbers we're currently using, and when?
            >>> df = td.get_series("wind_power").read(include_updates=True)
            >>>
            >>> # Compare all forecast model runs against each other
            >>> df = td.get_series("wind_power").read(overlapping=True)
            >>>
            >>> # Full bi-temporal dump: every run and every correction
            >>> df = td.get_series("wind_power").read(overlapping=True, include_updates=True)
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

        if overlapping:
            if not is_overlapping:
                raise ValueError(
                    "overlapping=True is not supported for flat series. "
                    "Flat series have a single value per timestamp with no forecast runs."
                )
            if include_updates:
                table = _read_overlapping_with_updates(
                    series_id=series_id,
                    routing_table=meta["table"],
                    start_valid=start_valid,
                    end_valid=end_valid,
                    start_known=start_known,
                    end_known=end_known,
                    _pool=self._pool,
                )
            else:
                table = _read_overlapping(
                    series_id=series_id,
                    routing_table=meta["table"],
                    start_valid=start_valid,
                    end_valid=end_valid,
                    start_known=start_known,
                    end_known=end_known,
                    _pool=self._pool,
                )
        elif include_updates:
            if is_overlapping:
                table = _read_overlapping_latest_with_updates(
                    series_id=series_id,
                    routing_table=meta["table"],
                    start_valid=start_valid,
                    end_valid=end_valid,
                    start_known=start_known,
                    end_known=end_known,
                    _pool=self._pool,
                )
            else:
                table = _read_flat_with_updates(
                    series_id=series_id,
                    start_valid=start_valid,
                    end_valid=end_valid,
                    start_known=start_known,
                    end_known=end_known,
                    _pool=self._pool,
                )
        else:
            if is_overlapping:
                table = _read_overlapping_latest(
                    series_id=series_id,
                    routing_table=meta["table"],
                    start_valid=start_valid,
                    end_valid=end_valid,
                    start_known=start_known,
                    end_known=end_known,
                    _pool=self._pool,
                )
            else:
                table = _read_flat(
                    series_id=series_id,
                    start_valid=start_valid,
                    end_valid=end_valid,
                    start_known=start_known,
                    end_known=end_known,
                    _pool=self._pool,
                )

        ts_type = TimeSeriesType.OVERLAPPING if is_overlapping else TimeSeriesType.FLAT
        ts = TimeSeriesPolars.from_polars(
            pl.from_arrow(table),
            name=meta.get("name"),
            unit=meta.get("unit", "dimensionless"),
            labels=meta.get("labels") or {},
            description=meta.get("description"),
            timeseries_type=ts_type,
        )

        return ts

    def read_relative(
        self,
        window_length: Optional[timedelta] = None,
        issue_offset: Optional[timedelta] = None,
        start_window: Optional[datetime] = None,
        start_valid: Optional[datetime] = None,
        end_valid: Optional[datetime] = None,
        *,
        days_ahead: Optional[int] = None,
        time_of_day: Optional[time] = None,
    ) -> "TimeSeries":
        """
        Read overlapping series using a per-window knowledge_time cutoff.

        For each valid_time, determines which window it belongs to (aligned to
        start_window with period window_length), then returns the latest forecast
        with knowledge_time <= window_start + issue_offset.

        Only valid for overlapping (versioned) series.

        **Low-level mode** — full control over window shape and offset:

        Args:
            window_length: Length of each window (e.g., timedelta(hours=24))
            issue_offset: Offset from window_start for the knowledge_time cutoff.
                          Negative means before the window starts
                          (e.g., timedelta(hours=-12) = 12h before window start).
            start_window: Origin for window alignment. Defaults to start_valid.
                          Required if start_valid is not provided.
            start_valid: Start of valid time range (optional)
            end_valid: End of valid time range (optional)

        **Daily shorthand mode** — fixed 1-day windows, human-friendly cutoff:

        Args:
            days_ahead: Calendar days before the window the forecast must be issued.
                        0 = same-day cutoff, 1 = day-ahead, 2 = two-days-ahead, etc.
            time_of_day: Latest time of day on the issue day (datetime.time, UTC).
                         E.g., time(6, 0) means "by 06:00 on the issue day".
            start_valid: Start of valid time range. Also sets window alignment
                         (midnight of this date). Required in daily mode.
            end_valid: End of valid time range (optional)

        Returns:
            TimeSeries with columns (valid_time, value).

        Raises:
            ValueError: If collection matches no series, multiple series,
                        series is not overlapping, or required parameters are missing/mixed.

        Examples:
            >>> from datetime import datetime, timedelta, time, timezone
            >>> # Low-level: arbitrary window length
            >>> df = td.get_series("wind_forecast").read_relative(
            ...     window_length=timedelta(hours=24),
            ...     issue_offset=timedelta(hours=-12),
            ...     start_window=datetime(2026, 2, 1, tzinfo=timezone.utc),
            ... )
            >>> # Daily shorthand: day-ahead, issued by 06:00
            >>> df = td.get_series("wind_forecast").read_relative(
            ...     days_ahead=1,
            ...     time_of_day=time(6, 0),
            ...     start_valid=datetime(2026, 2, 1, tzinfo=timezone.utc),
            ...     end_valid=datetime(2026, 2, 28, tzinfo=timezone.utc),
            ... )
        """
        using_daily    = days_ahead is not None or time_of_day is not None
        using_explicit = window_length is not None or issue_offset is not None

        if using_daily and using_explicit:
            raise ValueError(
                "Cannot mix (days_ahead, time_of_day) with (window_length, issue_offset). Use one set."
            )

        if using_daily:
            if days_ahead is None or time_of_day is None:
                raise ValueError("Both days_ahead and time_of_day must be provided together.")
            if start_valid is None:
                raise ValueError("start_valid is required when using days_ahead/time_of_day.")
            window_length = timedelta(days=1)
            issue_offset = (
                timedelta(
                    hours=time_of_day.hour,
                    minutes=time_of_day.minute,
                    seconds=time_of_day.second,
                    microseconds=time_of_day.microsecond,
                )
                - timedelta(days=days_ahead)
            )
            start_window = start_valid.replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            if window_length is None or issue_offset is None:
                raise ValueError("Both window_length and issue_offset are required.")
            start_window = start_window if start_window is not None else start_valid
            if start_window is None:
                raise ValueError(
                    "start_window is required when start_valid is not provided. "
                    "Pass start_window to set the window alignment origin."
                )

        series_id = self._get_single_id()
        meta = self._registry.get_cached(series_id)

        if not meta["overlapping"]:
            raise ValueError(
                f"read_relative() is only supported for overlapping (versioned) series. "
                f"Series '{meta['name']}' is a flat series. Use read() instead."
            )

        table = _read_overlapping_relative(
            series_id=series_id,
            routing_table=meta["table"],
            window_length=window_length,
            issue_offset=issue_offset,
            start_window=start_window,
            start_valid=start_valid,
            end_valid=end_valid,
            _pool=self._pool,
        )
        return TimeSeriesPolars.from_polars(
            pl.from_arrow(table),
            name=meta.get("name"),
            unit=meta.get("unit", "dimensionless"),
            labels=meta.get("labels") or {},
            description=meta.get("description"),
            timeseries_type=TimeSeriesType.OVERLAPPING,
        )

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
            >>> client.get_series('wind_power').where(site='Gotland').list_series()
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

    def list_batches(self) -> List[Dict[str, Any]]:
        """List all batches that contain data for this series.

        **Single-series only.**  Results are ordered by ``inserted_at`` DESC
        (most recent batch first).

        Returns:
            List of dicts, each containing:

            - **batch_id** (uuid.UUID): Unique batch identifier
            - **workflow_id** (str or None): Workflow tag set at insert time
            - **batch_start_time** (datetime or None): User-supplied batch start
            - **batch_finish_time** (datetime or None): User-supplied batch finish
            - **batch_params** (dict or None): Arbitrary metadata from insert
            - **inserted_at** (datetime): When the batch was created in the DB

        Example:
            >>> sc.list_batches()
            [
                {'batch_id': UUID('...'), 'workflow_id': 'sdk-workflow',
                 'batch_start_time': None, 'batch_finish_time': None,
                 'batch_params': None, 'inserted_at': datetime(...)},
                ...
            ]

        Raises:
            ValueError: If collection matches zero or multiple series.
        """
        series_id = self._get_single_id()
        routing = self._get_series_routing(series_id)
        return _list_batches(series_id=series_id, routing=routing, _pool=self._pool)

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
        >>> from timedatamodel import TimeSeriesPolars

        >>> # Create client and schema
        >>> td = TimeDataClient()
        >>> td.create()

        >>> # Create a series
        >>> td.create_series('wind_power', unit='MW', labels={'site': 'offshore_1'})

        >>> # Insert data using TimeSeries
        >>> ts = TimeSeriesPolars.from_pandas(
        ...     pd.DataFrame({'valid_time': [datetime.now(timezone.utc)], 'value': [100.0]}),
        ...     unit='MW',
        ... )
        >>> td.get_series('wind_power').where(site='offshore_1').insert(ts)

        >>> # Read data — returns a TimeSeries
        >>> ts_result = td.get_series('wind_power').where(site='offshore_1').read()
        >>> df = ts_result.to_pandas()  # pd.DataFrame with valid_time index

    Environment:
        Requires TIMEDB_DSN or DATABASE_URL environment variable
        to connect to the PostgreSQL database.
    """

    def __init__(self, conninfo: Optional[str] = None, min_size: int = 2, max_size: int = 10):
        self._conninfo = conninfo or _get_conninfo()
        self._pool = ConnectionPool(self._conninfo, min_size=min_size, max_size=max_size, open=True)
        atexit.register(self.close)

    def close(self):
        """Close the connection pool."""
        if not self._pool.closed:
            self._pool.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def get_series(
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
            >>> client.get_series('wind_power').where(site='offshore_1').read()
            >>> # Get all series with unit 'MW'
            >>> client.get_series(unit='MW').read()
            >>> # Get series by ID (if you know it)
            >>> client.get_series(series_id=123).read()
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
            conninfo=self._conninfo,
            retention_short=retention_short,
            retention_medium=retention_medium,
            retention_long=retention_long,
        )

    def delete(self) -> None:
        """Delete database schema."""
        _delete(conninfo=self._conninfo)

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
                  with knowledge_time tracking for changes over time

            retention (str, default="medium"):
                Data retention policy (overlapping series only):

                - 'short': 6 months (fast queries on recent data)
                - 'medium': 3 years (balanced for forecasts)
                - 'long': 5 years (historical archival)

        Returns:
            int: The series_id for this series. If a series with the same name+labels
                already exists, the existing series_id is returned (get-or-create semantics).

        Raises:
            ValueError: If the timedb schema has not been created yet (run td.create() first)

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
            conninfo=self._conninfo,
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

    # Same unit → no conversion needed
    ureg = pint.application_registry.get()
    if source_unit == series_unit:
        return pd.Series(magnitudes, index=value_series.index, name=value_series.name)
    if ureg.dimensionless == ureg.Unit(source_unit):
        if series_unit != "dimensionless":
            warnings.warn(
                f"Inserting a pint dimensionless array into series with unit "
                f"'{series_unit}'. Values will be stored as-is without conversion. "
                f"Use a pint array with the correct unit to enable automatic conversion.",
                UserWarning,
                stacklevel=4,
            )
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


def _validate_df_columns(df: pd.DataFrame) -> None:
    """Validate DataFrame column structure.

    Accepted column sets:

    - ``[valid_time, value]``
    - ``[valid_time, valid_time_end, value]``
    - ``[knowledge_time, valid_time, value]``
    - ``[knowledge_time, valid_time, valid_time_end, value]``
    """
    cols = list(df.columns)
    valid_sets = [
        ['valid_time', 'value'],
        ['valid_time', 'valid_time_end', 'value'],
        ['knowledge_time', 'valid_time', 'value'],
        ['knowledge_time', 'valid_time', 'valid_time_end', 'value'],
    ]
    if cols not in valid_sets:
        raise ValueError(
            f"DataFrame columns must be one of: {valid_sets}. "
            f"Found {len(cols)} columns: {cols}"
        )


def _check_df_timezone(df: pd.DataFrame) -> None:
    """Raise ValueError if any timestamp column contains timezone-naive datetimes."""
    for col in ['knowledge_time', 'valid_time', 'valid_time_end']:
        if col not in df.columns:
            continue
        s = df[col]
        if hasattr(s.dtype, 'tz'):
            if s.dtype.tz is None:
                raise ValueError(
                    f"{col} must be timezone-aware. Found timezone-naive datetime."
                )
        elif len(s) > 0:
            first = s.iloc[0]
            if isinstance(first, (pd.Timestamp, datetime)) and first.tzinfo is None:
                raise ValueError(
                    f"{col} must be timezone-aware. Found timezone-naive datetime."
                )


def _resolve_polars_units(df: pl.DataFrame, ts_unit: str, series_unit: str) -> pl.DataFrame:
    """Scale the ``value`` column when *ts_unit* and *series_unit* differ.

    Rules:
    - Same unit → return unchanged.
    - ``ts_unit == "dimensionless"`` → return unchanged, warn if *series_unit* is not dimensionless.
    - Compatible units → multiply ``value`` by the pint conversion factor.
    - Incompatible units → raise :class:`IncompatibleUnitError`.
    """
    if ts_unit == series_unit:
        return df
    if ts_unit == "dimensionless":
        if series_unit != "dimensionless":
            warnings.warn(
                f"Inserting a dimensionless TimeSeries into series with unit "
                f"'{series_unit}'. Values will be stored as-is without conversion. "
                f"Set the unit on your TimeSeries to enable automatic conversion.",
                UserWarning,
                stacklevel=4,
            )
        return df
    try:
        import pint
    except ImportError:
        raise ImportError(
            "pint is required for automatic unit conversion. "
            "Install with: pip install pint"
        )
    ureg = pint.application_registry.get()
    try:
        factor = float(ureg.Quantity(1, ts_unit).to(series_unit).magnitude)
    except pint.DimensionalityError:
        raise IncompatibleUnitError(
            f"Cannot convert '{ts_unit}' to '{series_unit}'. "
            f"Units are not dimensionally compatible."
        )
    return df.with_columns(pl.col("value") * factor)


def _normalize_insert_input(
    data: Union[pd.DataFrame, "TimeSeries"],
    series_unit: str,
    knowledge_time: Optional[datetime] = None,
) -> Tuple[pa.Table, DataShape]:
    """Convert DataFrame or TimeSeries insert input to a validated ``(pa.Table, DataShape)``.

    For :class:`~timedb.timeseries.TimeSeries` inputs, calls
    :meth:`~timedb.timeseries.TimeSeries.validate_for_insert` and applies unit
    conversion if needed.  For ``pd.DataFrame`` inputs, performs column
    validation, timezone checks, pint stripping, and Arrow conversion.

    After conversion the table is guaranteed to contain a ``knowledge_time``
    column (baked in Python, never NULL).  If ``knowledge_time`` is provided
    as a keyword argument *and* the data already contains a ``knowledge_time``
    column a :class:`ValueError` is raised to prevent ambiguity.

    Returns:
        ``(pa.Table, DataShape)`` tuple ready to pass to :func:`_insert`.
    """
    if isinstance(data, TimeSeriesPolars):
        df, shape = data.validate_for_insert()
        df = _resolve_polars_units(df, ts_unit=data.unit, series_unit=series_unit)
        table = df.to_arrow()
    else:
        # --- pd.DataFrame path ---
        _validate_df_columns(data)
        _check_df_timezone(data)

        resolved = _resolve_pint_values(data['value'], series_unit)
        if resolved is not data['value']:
            data = data.copy()
            data['value'] = resolved

        # from_pandas infers VERSIONED when knowledge_time column is present
        ts = TimeSeriesPolars.from_pandas(data)
        table = ts.df.to_arrow()
        shape = ts.shape

    # Ambiguity check: KT in data AND as kwarg is not allowed
    if "knowledge_time" in table.schema.names and knowledge_time is not None:
        raise ValueError(
            "Ambiguous knowledge_time: the data already contains a 'knowledge_time' column "
            "and knowledge_time was also passed as a keyword argument. "
            "Remove the kwarg or the column."
        )

    # Bake knowledge_time into the table if not already present
    if "knowledge_time" not in table.schema.names:
        kt = knowledge_time if knowledge_time is not None else datetime.now(timezone.utc)
        kt_arr = pa.array([kt] * len(table), type=pa.timestamp("us", tz="UTC"))
        table = table.append_column("knowledge_time", kt_arr)

    return table, shape


def _create(
    retention_short: str = "6 months",
    retention_medium: str = "3 years",
    retention_long: str = "5 years",
    *,
    conninfo: Optional[str] = None,
) -> None:
    """Create or update the database schema (TimescaleDB version)."""
    if conninfo is None:
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
    conninfo: Optional[str] = None,
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
        conninfo: Optional connection string (falls back to env vars if not provided)

    Returns:
        The series_id (int) for the newly created series
    """
    if conninfo is None:
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


def _delete(conninfo: Optional[str] = None) -> None:
    """Delete all TimeDB tables and views."""
    if conninfo is None:
        conninfo = _get_conninfo()
    db.delete.delete_schema(conninfo)


def _read_flat(
    series_id: int,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None,
    end_known: Optional[datetime] = None,
    _pool: Optional[ConnectionPool] = None,
) -> pa.Table:
    """Read flat values for a single series."""
    conninfo = _get_conninfo()

    with _get_connection(_pool, conninfo) as conn:
        table = db.read.read_flat(
            conn,
            series_id=series_id,
            start_valid=start_valid,
            end_valid=end_valid,
            start_known=start_known,
            end_known=end_known,
        )

    return table


def _read_overlapping_latest(
    series_id: int,
    routing_table: str,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None,
    end_known: Optional[datetime] = None,
    _pool: Optional[ConnectionPool] = None,
) -> pa.Table:
    """Read latest overlapping values for a single series."""
    conninfo = _get_conninfo()

    with _get_connection(_pool, conninfo) as conn:
        table = db.read.read_overlapping_latest(
            conn,
            series_id=series_id,
            table=routing_table,
            start_valid=start_valid,
            end_valid=end_valid,
            start_known=start_known,
            end_known=end_known,
        )

    return table


def _read_overlapping_with_updates(
    series_id: int,
    routing_table: str,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None,
    end_known: Optional[datetime] = None,
    _pool: Optional[ConnectionPool] = None,
) -> pa.Table:
    """Read all overlapping versions for a single series."""
    conninfo = _get_conninfo()

    with _get_connection(_pool, conninfo) as conn:
        table = db.read.read_overlapping_with_updates(
            conn,
            series_id=series_id,
            table=routing_table,
            start_valid=start_valid,
            end_valid=end_valid,
            start_known=start_known,
            end_known=end_known,
        )

    return table


def _read_overlapping(
    series_id: int,
    routing_table: str,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None,
    end_known: Optional[datetime] = None,
    _pool: Optional[ConnectionPool] = None,
) -> pa.Table:
    """Read overlapping forecast history (latest correction per knowledge_time × valid_time)."""
    conninfo = _get_conninfo()

    with _get_connection(_pool, conninfo) as conn:
        table = db.read.read_overlapping(
            conn,
            series_id=series_id,
            table=routing_table,
            start_valid=start_valid,
            end_valid=end_valid,
            start_known=start_known,
            end_known=end_known,
        )

    return table


def _read_overlapping_latest_with_updates(
    series_id: int,
    routing_table: str,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None,
    end_known: Optional[datetime] = None,
    _pool: Optional[ConnectionPool] = None,
) -> pa.Table:
    """Read all corrections for the winning knowledge_time per valid_time."""
    conninfo = _get_conninfo()

    with _get_connection(_pool, conninfo) as conn:
        table = db.read.read_overlapping_latest_with_updates(
            conn,
            series_id=series_id,
            table=routing_table,
            start_valid=start_valid,
            end_valid=end_valid,
            start_known=start_known,
            end_known=end_known,
        )

    return table


def _read_flat_with_updates(
    series_id: int,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    start_known: Optional[datetime] = None,
    end_known: Optional[datetime] = None,
    _pool: Optional[ConnectionPool] = None,
) -> pa.Table:
    """Read flat values with change_time, changed_by, annotation columns."""
    conninfo = _get_conninfo()

    with _get_connection(_pool, conninfo) as conn:
        table = db.read.read_flat_with_updates(
            conn,
            series_id=series_id,
            start_valid=start_valid,
            end_valid=end_valid,
            start_known=start_known,
            end_known=end_known,
        )

    return table


def _read_overlapping_relative(
    series_id: int,
    routing_table: str,
    window_length: timedelta,
    issue_offset: timedelta,
    start_window: datetime,
    start_valid: Optional[datetime] = None,
    end_valid: Optional[datetime] = None,
    _pool: Optional[ConnectionPool] = None,
) -> pa.Table:
    """Read overlapping values with per-window knowledge_time cutoff."""
    conninfo = _get_conninfo()

    with _get_connection(_pool, conninfo) as conn:
        table = db.read.read_overlapping_relative(
            conn,
            series_id=series_id,
            table=routing_table,
            window_length=window_length,
            issue_offset=issue_offset,
            start_window=start_window,
            start_valid=start_valid,
            end_valid=end_valid,
        )

    return table


def _insert(
    table: pa.Table,
    shape: DataShape,
    series_id: int,
    routing: Dict[str, Any],
    workflow_id: Optional[str] = None,
    batch_start_time: Optional[datetime] = None,
    batch_finish_time: Optional[datetime] = None,
    batch_params: Optional[dict] = None,
    _pool: Optional[ConnectionPool] = None,
) -> InsertResult:
    """
    Insert an Arrow table of time series data into the database.

    Accepts a ``(pa.Table, DataShape)`` produced by :func:`_normalize_insert_input`
    and routes it to the correct Postgres table via :func:`db.insert.insert_table`.
    The table must already contain a ``knowledge_time`` column (guaranteed by
    :func:`_normalize_insert_input`).  Both flat and overlapping series accept
    SIMPLE and VERSIONED shapes; per-row knowledge_times are stored directly on
    each row in the target table.
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

    try:
        with _get_connection(_pool, conninfo) as conn:
            result = db.insert.insert_table(
                conn,
                table=table,
                workflow_id=workflow_id,
                batch_start_time=batch_start_time,
                batch_finish_time=batch_finish_time,
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
        batch_id=result,
        workflow_id=workflow_id,
        series_id=series_id,
    )



def _list_batches(
    series_id: int,
    routing: Dict[str, Any],
    _pool: Optional[ConnectionPool] = None,
) -> List[Dict[str, Any]]:
    """List batches for a series. Wrapper around db.read.read_batches_for_series."""
    conninfo = _get_conninfo()
    with _get_connection(_pool, conninfo) as conn:
        return db.read.read_batches_for_series(conn, series_id=series_id, routing=routing)


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
