"""
High-level SDK for TimeDB.

Provides a simple interface for working with TimeDB, including automatic
DataFrame conversion for time series data. Optional unit conversion via pint
is applied automatically when a ``unit=`` kwarg or TimeSeries unit differs
from the series' canonical unit.

The SDK exposes two main classes:
- TimeDataClient: Main entry point for database operations
- SeriesCollection: Fluent API for series filtering and operations

Data model:
- Flat: Immutable fact data (meter readings, measurements). Stored in 'flat' table.
- Overlapping: Versioned estimates (forecasts). Stored in 'overlapping' table
  (list-partitioned by retention) with knowledge_time versioning.
"""
import atexit
import uuid
from time import perf_counter
from typing import Optional, List, Dict, Union, Any
from datetime import datetime, timedelta, timezone, time as dt_time
import pandas as pd
import polars as pl
import pyarrow as pa

from . import db, profiling, insert_pipeline, read_pipeline, write_pipeline
from .connection import _get_connection, _get_pg_conninfo, _get_ch_url
from .db.series import SeriesRegistry
from .types import RunContext, InsertResult, IncompatibleUnitError
from timedatamodel import TimeSeries, TimeSeriesType

try:
    from uuid import uuid7
except ImportError:
    from uuid6 import uuid7
import clickhouse_connect
import psycopg
from psycopg import errors
from psycopg_pool import ConnectionPool


class SeriesCollection:
    """
    A lazy collection of time series that matches a set of filters.

    SeriesCollection provides a fluent, chainable API for filtering and
    operating on one or more time series without manually managing series IDs.

    The collection resolves which series match the filters only when an
    operation like .read() or .insert() is called.
    This allows building complex queries progressively.

    Filtering:
        Series are filtered by name, unit, series_id, and labels. You can chain
        multiple .where() calls to add additional label filters.

    Operations:
        Once filtered, the collection supports:
        - read(): Retrieve time series data
        - insert(): Add new data points

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
        _ch_client=None,
    ):
        self._conninfo = conninfo
        self._name = name
        self._unit = unit
        self._label_filters = label_filters or {}
        self._series_id = series_id
        self._registry = _registry or SeriesRegistry()
        self._resolved = False
        self._pool = _pool
        self._ch_client = _ch_client

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
            _ch_client=self._ch_client,
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
        data: Union[pd.DataFrame, pl.DataFrame, "TimeSeries"],
        workflow_id: Optional[str] = None,
        run_start_time: Optional[datetime] = None,
        run_finish_time: Optional[datetime] = None,
        knowledge_time: Optional[datetime] = None,
        run_params: Optional[dict] = None,
        unit: Optional[str] = None,
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
            run_start_time: Start time (optional)
            run_finish_time: Finish time (optional)
            knowledge_time: Time of knowledge broadcast to all rows (optional).
                Defaults to ``now()`` when neither this kwarg nor a
                ``knowledge_time`` column is present in the data.
            run_params: Run parameters (optional)
            unit: Unit of the incoming ``pd.DataFrame`` values (optional).
                When provided, values are converted from this unit to the
                series' canonical unit before insert.  Ignored for
                :class:`~timedatamodel.TimeSeries` (which carries its own unit).

        Returns:
            InsertResult with run_id (uuid.UUID), workflow_id, series_id.
            One run is always created per insert() call regardless of how
            many unique knowledge_times are in the data.

        Raises:
            ValueError: If collection matches multiple series (use more specific filters)
            ValueError: If input data doesn't have the required columns or shape
            ValueError: If knowledge_time kwarg and a knowledge_time column are both provided
        """
        series_id = self._get_single_id()
        routing = self._get_series_routing(series_id)
        series_unit = self._registry.get_cached(series_id)["unit"]

        return _insert(
            data=data,
            series_unit=series_unit,
            series_id=series_id,
            routing=routing,
            knowledge_time=knowledge_time,
            workflow_id=workflow_id,
            run_start_time=run_start_time,
            run_finish_time=run_finish_time,
            run_params=run_params,
            data_unit=unit,
            ch_client=self._ch_client,
            _pool=self._pool,
        )

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

        Returns a :class:`~timedatamodel.timeseries_polars.TimeSeries` instance.
        Call ``.to_pandas()`` on it to get a ``pd.DataFrame`` with the
        conventional index.

        **Single-series reads only.** Collection must resolve to exactly one series.

        Args:
            start_valid: Start of valid time range (inclusive).
            end_valid: End of valid time range (exclusive).
            start_known: Start of knowledge_time range (inclusive).
            end_known: End of knowledge_time range (exclusive).
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

        if overlapping and not is_overlapping:
            raise ValueError(
                "overlapping=True is not supported for flat series. "
                "Flat series have a single value per timestamp with no forecast runs."
            )

        table = read_pipeline._read_multi(
            self._ch_client,
            {series_id: meta},
            start_valid=start_valid,
            end_valid=end_valid,
            start_known=start_known,
            end_known=end_known,
            overlapping=overlapping,
            include_updates=include_updates,
        )
        # Drop series_id column — single-series result doesn't need it
        table = db.read._drop_series_id(table)

        ts_type = TimeSeriesType.OVERLAPPING if is_overlapping else TimeSeriesType.FLAT
        _prof = profiling.is_enabled()
        _t0 = perf_counter() if _prof else 0.0
        ts = TimeSeries.from_polars(
            pl.from_arrow(table),
            name=meta.get("name"),
            unit=meta.get("unit", "dimensionless"),
            labels=meta.get("labels") or {},
            description=meta.get("description"),
            timeseries_type=ts_type,
        )
        if _prof: profiling._record(profiling.PHASE_READ_TO_POLARS, perf_counter() - _t0)

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
        time_of_day: Optional[dt_time] = None,
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
                         E.g., dt_time(6, 0) means "by 06:00 on the issue day".
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
            ...     time_of_day=dt_time(6, 0),
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
            start_window = start_valid.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
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

        table = read_pipeline._read_relative_multi(
            self._ch_client,
            {series_id: meta},
            window_length=window_length,
            issue_offset=issue_offset,
            start_window=start_window,
            start_valid=start_valid,
            end_valid=end_valid,
        )
        # Drop series_id column — single-series result doesn't need it
        table = db.read._drop_series_id(table)

        _prof = profiling.is_enabled()
        _t0 = perf_counter() if _prof else 0.0
        ts = TimeSeries.from_polars(
            pl.from_arrow(table),
            name=meta.get("name"),
            unit=meta.get("unit", "dimensionless"),
            labels=meta.get("labels") or {},
            description=meta.get("description"),
            timeseries_type=TimeSeriesType.OVERLAPPING,
        )
        if _prof: profiling._record(profiling.PHASE_READ_TO_POLARS, perf_counter() - _t0)
        return ts

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

    def list_runs(self) -> List[Dict[str, Any]]:
        """List all runs that contain data for this series.

        **Single-series only.**  Results are ordered by ``inserted_at`` DESC
        (most recent run first).

        Returns:
            List of dicts, each containing:

            - **run_id** (uuid.UUID): Unique run identifier
            - **workflow_id** (str or None): Workflow tag set at insert time
            - **run_start_time** (datetime or None): User-supplied run start
            - **run_finish_time** (datetime or None): User-supplied run finish
            - **run_params** (dict or None): Arbitrary metadata from insert
            - **inserted_at** (datetime): When the run was created in the DB

        Example:
            >>> sc.list_runs()
            [
                {'run_id': UUID('...'), 'workflow_id': 'sdk-workflow',
                 'run_start_time': None, 'run_finish_time': None,
                 'run_params': None, 'inserted_at': datetime(...)},
                ...
            ]

        Raises:
            ValueError: If collection matches zero or multiple series.
        """
        series_id = self._get_single_id()
        routing = self._get_series_routing(series_id)
        return _list_runs(ch_client=self._ch_client, series_id=series_id, routing=routing)

    def count(self) -> int:
        """Count how many series match the current filters."""
        return len(self._resolve_ids())

    def __repr__(self) -> str:
        return (
            f"SeriesCollection(name={self._name!r}, unit={self._unit!r}, "
            f"series_id={self._series_id!r}, labels={self._label_filters!r}, resolved={self._resolved})"
        )


_WRITE_RUN_RESERVED_COLS = frozenset({
    # Columns that map to native runs_table fields when used in run_cols
    "workflow_id", "run_start_time", "run_finish_time",
})

_WRITE_RESERVED_COLS = frozenset({
    # Required data columns
    "valid_time", "value",
    # Optional passthrough columns (forwarded to DB)
    "knowledge_time", "valid_time_end", "change_time", "changed_by", "annotation",
    # TimeDB-internal columns that appear in round-trip DataFrames (read → write)
    "series_id", "run_id",
    # Series metadata — not a label dimension
    "unit",
    # Run metadata columns — never auto-inferred as label dimensions
    *_WRITE_RUN_RESERVED_COLS,
})


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
        >>> from timedb import TimeSeries

        >>> # Create client and schema
        >>> td = TimeDataClient()
        >>> td.create()

        >>> # Create a series
        >>> td.create_series('wind_power', unit='MW', labels={'site': 'offshore_1'})

        >>> # Insert data using TimeSeries
        >>> ts = TimeSeries.from_pandas(
        ...     pd.DataFrame({'valid_time': [datetime.now(timezone.utc)], 'value': [100.0]}),
        ...     unit='MW',
        ... )
        >>> td.get_series('wind_power').where(site='offshore_1').insert(ts)

        >>> # Read data — returns a TimeSeries
        >>> ts_result = td.get_series('wind_power').where(site='offshore_1').read()
        >>> df = ts_result.to_pandas()  # pd.DataFrame with valid_time index

    Environment:
        Requires TIMEDB_PG_DSN (PostgreSQL) and TIMEDB_CH_URL (ClickHouse)
        environment variables.
    """

    def __init__(
        self,
        pg_conninfo: Optional[str] = None,
        ch_url: Optional[str] = None,
        min_size: int = 2,
        max_size: int = 10,
    ):
        self._conninfo = pg_conninfo or _get_pg_conninfo()
        self._ch_url = ch_url or _get_ch_url()
        self._ch_client = clickhouse_connect.get_client(dsn=self._ch_url)
        self._pool = ConnectionPool(self._conninfo, min_size=min_size, max_size=max_size, open=True)
        atexit.register(self.close)

    def close(self):
        """Close the connection pool and ClickHouse client."""
        if not self._pool.closed:
            self._pool.close()
        self._ch_client.close()

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
            _ch_client=self._ch_client,
        )

    def create(self) -> None:
        """Create database schema in PostgreSQL (series_table) and ClickHouse (values tables)."""
        _create(pg_conninfo=self._conninfo, ch_url=self._ch_url)

    def delete(self) -> None:
        """Delete database schema from PostgreSQL and ClickHouse."""
        _delete(pg_conninfo=self._conninfo, ch_url=self._ch_url)

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
        Get-or-create a single time series.

        Args:
            name (str): Series name/identifier (e.g., ``'wind_power'``).

            unit (str, default="dimensionless"):
                Canonical unit. Examples: ``'MW'``, ``'kWh'``, ``'degC'``,
                ``'dimensionless'``.

            labels (dict, optional):
                Key-value labels to differentiate series with the same name.
                Example: ``{"site": "Gotland", "turbine": "T01"}``

            description (str, optional): Human-readable description.

            overlapping (bool, default=False):
                ``False`` for immutable facts; ``True`` for versioned forecasts
                with ``knowledge_time`` tracking.

            retention (str, default="medium"):
                Data retention policy: ``'short'`` (6 months),
                ``'medium'`` (3 years), or ``'long'`` (5 years).
                Only applies to overlapping series.

        Returns:
            int: The ``series_id``. If a series with the same name+labels already
            exists, its existing id is returned (get-or-create semantics).

        Raises:
            ValueError: If the timedb schema has not been created yet.

        Example:
            >>> client = TimeDataClient()
            >>> series_id = client.create_series(
            ...     'wind_power', unit='MW',
            ...     labels={'site': 'Gotland'}, overlapping=True,
            ... )
        """
        return _create_series(
            name=name,
            unit=unit,
            labels=labels,
            description=description,
            overlapping=overlapping,
            retention=retention,
            conninfo=self._conninfo,
        )

    def create_series_many(self, series: List[Dict[str, Any]]) -> List[int]:
        """
        Bulk get-or-create multiple series in one round-trip.

        Args:
            series (list[dict]): Each dict may contain:

                - **name** (str, required): Series name.
                - **unit** (str, default ``"dimensionless"``): Canonical unit.
                - **labels** (dict, optional): Key-value label dict.
                - **description** (str, optional): Human-readable description.
                - **overlapping** (bool, default ``False``): Versioned data flag.
                - **retention** (str, default ``"medium"``): ``'short'``,
                  ``'medium'``, or ``'long'``.

        Returns:
            List[int]: series_ids in the same order as the input.

        Example:
            >>> ids = client.create_series_many([
            ...     {"name": "wind_power", "unit": "MW", "labels": {"turbine": "T01"}},
            ...     {"name": "wind_power", "unit": "MW", "labels": {"turbine": "T02"}},
            ... ])
        """
        return _create_series_many(series, conninfo=self._conninfo)

    def write(
        self,
        df: Union[pd.DataFrame, pl.DataFrame],
        name_col: Optional[str] = None,
        label_cols: Optional[List[str]] = None,
        run_cols: Optional[List[str]] = None,
        series_col: Optional[str] = None,
        *,
        knowledge_time: Optional[datetime] = None,
        unit: Optional[str] = None,
        workflow_id: Optional[str] = None,
        run_start_time: Optional[datetime] = None,
        run_finish_time: Optional[datetime] = None,
        run_params: Optional[dict] = None,
    ) -> List[InsertResult]:
        """
        Insert multi-series data in long/tidy format.

        Series routing is controlled by either *name_col*/*label_cols* (resolve
        by name and labels) or *series_col* (route by integer series ID
        directly, bypassing resolution).  The two modes are mutually exclusive.

        All series must already exist — unknown identities raise a
        :class:`ValueError` before any data is written.

        The DataFrame must contain ``valid_time`` and ``value`` columns in
        addition to routing columns.  Optional passthrough columns
        (``valid_time_end``, ``change_time``, ``changed_by``, ``annotation``)
        are forwarded to the database unchanged if present.

        Args:
            df: Long-format DataFrame (Pandas or Polars) containing routing
                and data columns.
            name_col: Column whose values map to ``series_table.name``.
                Defaults to ``"name"`` when *series_col* is not set.
                Mutually exclusive with *series_col*.
            label_cols: Columns whose values map to ``series_table.labels``.
                If ``None`` (default), inferred as all columns not in
                :data:`_WRITE_RESERVED_COLS`, not *name_col*, and not
                *run_cols*.  Pass ``[]`` explicitly for series with no labels.
                Mutually exclusive with *series_col*.
            series_col: Column whose values are integer ``series_table.series_id``
                values.  When set, bypasses name/label resolution and only
                validates that the IDs exist.  Mutually exclusive with
                *name_col* and *label_cols*.
            run_cols: Columns that define run identity (provenance).  Each
                unique combination of values becomes a distinct run in the
                database.  Three routing rules apply:

                - Columns named ``workflow_id``, ``run_start_time``, or
                  ``run_finish_time`` map to the corresponding native
                  ``runs_table`` fields, overriding the same-named kwargs.
                - All other columns are packed into ``run_params`` JSON,
                  merged on top of any global *run_params* kwarg.
                - Any field absent from *run_cols* falls back to the
                  corresponding kwarg (or its default).

                Example — one run per model::

                    td.write(df, run_cols=["model"], workflow_id="nightly")
            knowledge_time: Broadcast knowledge_time for all rows (mutually
                exclusive with a ``knowledge_time`` column in *df*).
            unit: Unit of the incoming values.  When provided, values are
                converted from this unit to each series' canonical unit via pint.
            workflow_id: Global run workflow identifier (default for all
                runs when *run_cols* does not include ``workflow_id``).
            run_start_time: Global run start time.
            run_finish_time: Global run finish time.
            run_params: Global run params dict — base for any per-run
                merge when *run_cols* contains unreserved columns.

        Returns:
            List of :class:`InsertResult`, one per unique (series_id, run_id)
            combination.  Without *run_cols* this is one per series; with
            *run_cols* it is N×M where N is the number of unique run
            combinations and M is the number of series.

        Raises:
            ValueError: If any (name, labels) combination or series ID has no
                matching series.
            ValueError: If *series_col* is used together with *name_col* or
                *label_cols*.
            ValueError: If ``valid_time`` or ``value`` columns are missing.
            ValueError: If *run_cols* contains columns not present in *df*.
            ValueError: If *run_cols* overlaps with *label_cols*.
            IncompatibleUnitError: If *unit* is incompatible with any series unit.

        Example:
            >>> # df columns: ['site', 'turbine_id', 'metric', 'valid_time', 'value']
            >>> td.write(df, name_col="metric", label_cols=["site", "turbine_id"])
        """
        all_cols = list(df.columns)

        # ── Mutual exclusivity: series_col vs name_col/label_cols ─────────
        if series_col is not None:
            if name_col is not None:
                raise ValueError(
                    "series_col and name_col are mutually exclusive. "
                    "Use series_col to route by series ID, or name_col/label_cols to route by name and labels."
                )
            if label_cols is not None:
                raise ValueError(
                    "series_col and label_cols are mutually exclusive. "
                    "Use series_col to route by series ID, or name_col/label_cols to route by name and labels."
                )
            if series_col not in all_cols:
                raise ValueError(
                    f"Series column {series_col!r} not found in DataFrame. "
                    f"Available columns: {all_cols}"
                )
            label_cols = []
        else:
            name_col = name_col or "name"
            if name_col not in all_cols:
                raise ValueError(
                    f"Name column {name_col!r} not found in DataFrame. "
                    f"Rename your column to 'name' or pass name_col='your_column'."
                )

        # Validate unit column / kwarg mutual exclusion
        if "unit" in all_cols and unit is not None:
            raise ValueError(
                "Cannot pass both a 'unit' column in the DataFrame and the unit= kwarg. "
                "Use the column for per-row units, or the kwarg to broadcast a single unit."
            )

        # Validate run_cols exist in the DataFrame
        if run_cols is not None:
            missing_run_cols = [c for c in run_cols if c not in all_cols]
            if missing_run_cols:
                raise ValueError(
                    f"run_cols column(s) not found in DataFrame: {missing_run_cols}. "
                    f"Available columns: {all_cols}"
                )

        if series_col is None and label_cols is None:
            exclude = _WRITE_RESERVED_COLS | set(run_cols or [])
            label_cols = [c for c in all_cols if c not in exclude and c != name_col]

        # Validate no overlap between run_cols and label_cols
        if run_cols is not None:
            overlap = set(run_cols) & set(label_cols)
            if overlap:
                raise ValueError(
                    f"run_cols and label_cols cannot overlap. "
                    f"Column(s) {sorted(overlap)} appear in both. "
                    f"Columns cannot serve dual roles as run dimension and series label."
                )

        return write_pipeline._write(
            df,
            name_col=name_col,
            label_cols=label_cols,
            run_cols=run_cols,
            series_col=series_col,
            knowledge_time=knowledge_time,
            data_unit=unit,
            workflow_id=workflow_id,
            run_start_time=run_start_time,
            run_finish_time=run_finish_time,
            run_params=run_params,
            make_run_context=_make_run_context,
            ch_client=self._ch_client,
            _pool=self._pool,
        )

    def read(
        self,
        manifest: Union[pd.DataFrame, pl.DataFrame],
        name_col: Optional[str] = None,
        label_cols: Optional[List[str]] = None,
        series_col: Optional[str] = None,
        *,
        start_valid: Optional[datetime] = None,
        end_valid: Optional[datetime] = None,
        start_known: Optional[datetime] = None,
        end_known: Optional[datetime] = None,
        overlapping: bool = False,
        include_updates: bool = False,
    ) -> pl.DataFrame:
        """
        Read multi-series data using a manifest DataFrame.

        The manifest specifies which series to read using the same routing
        columns as :meth:`write`: either *name_col*/*label_cols* (resolve by
        name and labels) or *series_col* (route by integer series ID).
        The two modes are mutually exclusive.

        All series must already exist — unknown identities raise a
        :class:`ValueError`.

        Args:
            manifest: DataFrame (Pandas or Polars) specifying which series to
                read.  Each unique combination of routing columns identifies
                one series.
            name_col: Column whose values map to ``series_table.name``.
                Defaults to ``"name"`` when *series_col* is not set.
                Mutually exclusive with *series_col*.
            label_cols: Columns whose values map to ``series_table.labels``.
                If ``None`` (default), inferred as all columns not in
                *name_col* and not reserved columns.
                Pass ``[]`` explicitly for series with no labels.
                Mutually exclusive with *series_col*.
            series_col: Column whose values are integer ``series_table.series_id``
                values.  Bypasses name/label resolution.
                Mutually exclusive with *name_col* and *label_cols*.
            start_valid: Start of valid time range (inclusive).
            end_valid: End of valid time range (exclusive).
            start_known: Start of knowledge_time range (inclusive).
            end_known: End of knowledge_time range (exclusive).
            overlapping: Controls whether forecast history is exposed.

                - False (default): one row per (series_id, valid_time) with the
                  latest value.
                - True: one row per (series_id, knowledge_time, valid_time).
                  Overlapping series return all versions; flat series return one
                  version per valid_time with knowledge_time populated.
            include_updates: If True, expose the correction chain.

        Returns:
            ``pl.DataFrame`` in long/tidy format with columns:
            ``[name_col, *label_cols, "unit", "series_id", <data_columns>]``.

            Data columns depend on *overlapping* and *include_updates*:

            +--------------+-----------------+-------------------------------------+
            | overlapping  | include_updates | Data columns                        |
            +==============+=================+=====================================+
            | False        | False (default) | valid_time, value                   |
            +--------------+-----------------+-------------------------------------+
            | False        | True            | valid_time, change_time, value,     |
            |              |                 | changed_by, annotation              |
            +--------------+-----------------+-------------------------------------+
            | True         | False           | knowledge_time, valid_time, value   |
            +--------------+-----------------+-------------------------------------+
            | True         | True            | knowledge_time, change_time,        |
            |              |                 | valid_time, value, changed_by,      |
            |              |                 | annotation                          |
            +--------------+-----------------+-------------------------------------+

            For mixed flat + overlapping reads with ``overlapping=True``, both
            flat and overlapping series include ``knowledge_time``.

        Raises:
            ValueError: If *series_col* is used together with *name_col* or
                *label_cols*.
            ValueError: If no series match the manifest.

        Example:
            >>> manifest = pl.DataFrame({
            ...     "metric": ["wind_power", "wind_power", "solar_power"],
            ...     "site": ["Gotland", "Oslo", "Gotland"],
            ... })
            >>> df = td.read(manifest, name_col="metric", label_cols=["site"],
            ...              start_valid=datetime(2026, 3, 1, tzinfo=timezone.utc))
        """
        return read_pipeline._read(
            manifest,
            name_col=name_col,
            label_cols=label_cols,
            series_col=series_col,
            start_valid=start_valid,
            end_valid=end_valid,
            start_known=start_known,
            end_known=end_known,
            overlapping=overlapping,
            include_updates=include_updates,
            ch_client=self._ch_client,
            _pool=self._pool,
        )

    def read_relative(
        self,
        manifest: Union[pd.DataFrame, pl.DataFrame],
        name_col: Optional[str] = None,
        label_cols: Optional[List[str]] = None,
        series_col: Optional[str] = None,
        *,
        window_length: Optional[timedelta] = None,
        issue_offset: Optional[timedelta] = None,
        start_window: Optional[datetime] = None,
        start_valid: Optional[datetime] = None,
        end_valid: Optional[datetime] = None,
        days_ahead: Optional[int] = None,
        time_of_day: Optional[dt_time] = None,
    ) -> pl.DataFrame:
        """
        Read multi-series overlapping data with per-window knowledge_time cutoff.

        Same manifest routing as :meth:`read`.  All matched series must be
        overlapping (versioned) — flat series raise :class:`ValueError`.

        **Low-level mode** — full control over window shape and offset:

        Args:
            manifest: DataFrame specifying which series to read.
            name_col: Column mapping to series name.
            label_cols: Columns mapping to series labels.
            series_col: Column with integer series IDs.
            window_length: Length of each window (e.g., ``timedelta(hours=24)``).
            issue_offset: Offset from window_start for the knowledge_time cutoff.
            start_window: Origin for window alignment.  Defaults to *start_valid*.
            start_valid: Start of valid time range (inclusive).
            end_valid: End of valid time range (exclusive).

        **Daily shorthand mode** — fixed 1-day windows:

        Args:
            days_ahead: Calendar days before the window the forecast must be issued.
            time_of_day: Latest time of day on the issue day (UTC).
            start_valid: Start of valid time range (required in daily mode).
            end_valid: End of valid time range (optional).

        Returns:
            ``pl.DataFrame`` with columns
            ``[name_col, *label_cols, "unit", "series_id", valid_time, value]``.

        Raises:
            ValueError: If any matched series is flat.
            ValueError: If parameters are missing or modes are mixed.

        Example:
            >>> df = td.read_relative(
            ...     manifest, name_col="metric", label_cols=["site"],
            ...     days_ahead=1, time_of_day=dt_time(6, 0),
            ...     start_valid=datetime(2026, 3, 1, tzinfo=timezone.utc),
            ... )
        """
        return read_pipeline._read_relative(
            manifest,
            name_col=name_col,
            label_cols=label_cols,
            series_col=series_col,
            window_length=window_length,
            issue_offset=issue_offset,
            start_window=start_window,
            start_valid=start_valid,
            end_valid=end_valid,
            days_ahead=days_ahead,
            time_of_day=time_of_day,
            ch_client=self._ch_client,
            _pool=self._pool,
        )


# =============================================================================
# Internal helper functions (not part of public API)
# =============================================================================

def _create(pg_conninfo: Optional[str] = None, ch_url: Optional[str] = None) -> None:
    """Create the database schema in PostgreSQL and ClickHouse."""
    db.create.create_schema(
        pg_conninfo or _get_pg_conninfo(),
        ch_url or _get_ch_url(),
    )


def _create_series_many(
    series_specs: List[Dict[str, Any]],
    conninfo: Optional[str] = None,
) -> List[int]:
    """Core bulk get-or-create. Single DB round-trip."""
    if conninfo is None:
        conninfo = _get_pg_conninfo()
    try:
        with psycopg.connect(conninfo) as conn:
            return db.series.create_series(conn, series_specs)
    except (errors.UndefinedTable, errors.UndefinedObject) as e:
        raise ValueError(
            "TimeDB tables do not exist. Please create the schema first by running:\n"
            "  td.create()"
        ) from e


def _create_series(
    name: str,
    unit: str = "dimensionless",
    labels: Optional[Dict[str, str]] = None,
    description: Optional[str] = None,
    overlapping: bool = False,
    retention: str = "medium",
    conninfo: Optional[str] = None,
) -> int:
    """Single-series convenience wrapper around _create_series_many."""
    return _create_series_many(
        [{"name": name, "unit": unit, "labels": labels,
          "description": description, "overlapping": overlapping,
          "retention": retention}],
        conninfo=conninfo,
    )[0]


def _delete(pg_conninfo: Optional[str] = None, ch_url: Optional[str] = None) -> None:
    """Delete all TimeDB tables from PostgreSQL and ClickHouse."""
    db.delete.delete_schema(
        pg_conninfo or _get_pg_conninfo(),
        ch_url or _get_ch_url(),
    )


# NOTE: Multi-series read/write orchestration moved to read_pipeline.py and write_pipeline.py
# _read_multi, _read_relative_multi, _resolve_manifest, _build_read_result, _read, _read_relative → read_pipeline
# _write, _build_multi_run_contexts → write_pipeline


def _make_run_context(
    workflow_id: Optional[str],
    run_start_time: Optional[datetime],
    run_finish_time: Optional[datetime],
    run_params: Optional[dict],
) -> RunContext:
    """Create a RunContext with a fresh UUIDv7 run_id and sensible defaults."""
    start = run_start_time if run_start_time is not None else datetime.now(timezone.utc)
    if start.tzinfo is None:
        raise ValueError("run_start_time must be timezone-aware")
    return RunContext(
        run_id=str(uuid7()),
        workflow_id=workflow_id if workflow_id is not None else "sdk-workflow",
        run_start_time=start,
        run_finish_time=run_finish_time,
        run_params=run_params,
    )


def _insert(
    data: Union[pd.DataFrame, "TimeSeries"],
    series_unit: str,
    series_id: int,
    routing: Dict[str, Any],
    knowledge_time: Optional[datetime] = None,
    workflow_id: Optional[str] = None,
    run_start_time: Optional[datetime] = None,
    run_finish_time: Optional[datetime] = None,
    run_params: Optional[dict] = None,
    data_unit: Optional[str] = None,
    ch_client=None,
    _pool: Optional[ConnectionPool] = None,
) -> InsertResult:
    """
    Normalize, decorate, and insert time series data into the database.

    Generates a UUIDv7 ``run_id`` here (SDK layer owns the ID), then delegates
    to :func:`insert_pipeline.normalize_insert_input` which stamps ``run_id``,
    ``series_id``, and ``knowledge_time`` into the Polars DataFrame in a single
    Rust pass before converting to Arrow.  The resulting ``pa.Table`` contains
    all columns and is passed directly to :func:`db.insert.insert_table`.
    """
    if series_id is None:
        raise ValueError("series_id must be provided")
    if routing is None:
        raise ValueError("routing must be provided")

    run_ctx = _make_run_context(workflow_id, run_start_time, run_finish_time, run_params)

    _t_normalize = perf_counter() if profiling._enabled else 0.0
    table = insert_pipeline.normalize_insert_input(
        data,
        series_unit,
        series_id=series_id,
        run_id=run_ctx.run_id,
        knowledge_time=knowledge_time,
        data_unit=data_unit,
    )
    if profiling._enabled:
        profiling._record(profiling.PHASE_INSERT_NORMALIZE, perf_counter() - _t_normalize)

    db.insert.insert_table(ch_client, table=table, routing=routing, run_ctx=run_ctx)

    return InsertResult(
        run_id=uuid.UUID(run_ctx.run_id),
        workflow_id=run_ctx.workflow_id,
        series_id=series_id,
    )


def _list_runs(
    ch_client,
    series_id: int,
    routing: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """List runs for a series. Wrapper around db.read.read_runs_for_series."""
    return db.read.read_runs_for_series(ch_client, series_id=series_id, routing=routing)
