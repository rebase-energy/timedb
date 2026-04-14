SDK Usage
=========

The timedb SDK provides a high-level Python interface for working with time series data. It uses a fluent API to manage series with automatic unit conversion, label filtering, and versioning support.

Overview
--------

The SDK centers around two main concepts:

- **TimeDataClient**: Main entry point for database operations
- **SeriesCollection**: Fluent API for filtering and operating on series

Time series in TimeDB fall into two categories:

- **Flat series**: Immutable fact data (meter readings, measurements)
- **Overlapping series**: Versioned estimates (forecasts, predictions) with ``knowledge_time`` tracking

Getting Started
---------------

Import the package and start using it directly:

.. code-block:: python

   import timedb as td
   import pandas as pd
   from datetime import datetime, timezone, timedelta
   from timedb import TimeSeries

Module-level functions (``td.create()``, ``td.delete()``, ``td.create_series()``, ``td.create_series_many()``, ``td.get_series()``) use a lazy default client that reads the database connection from environment variables automatically. This is the recommended approach for most use cases.

Explicit Client (Advanced)
~~~~~~~~~~~~~~~~~~~~~~~~~~

For custom connection settings (pool size, direct connection string, etc.), instantiate the client directly:

.. code-block:: python

   from timedb import TimeDataClient

   td = TimeDataClient(conninfo="postgresql://user:pass@host/db", min_size=4, max_size=20)
   td.get_series("wind_power").read()

   # Or as a context manager:
   with TimeDataClient() as td:
       td.create()

Database Connection
-------------------

The SDK uses environment variables for database connection:

- ``TIMEDB_DSN`` (preferred)
- ``DATABASE_URL`` (alternative)

You can also use a ``.env`` file in your project root:

.. code-block:: text

   TIMEDB_DSN=postgresql://user:password@localhost:5432/timedb

Schema Management
-----------------

Creating the Schema
~~~~~~~~~~~~~~~~~~~

Before using the SDK, create the database schema:

.. code-block:: python

   td.create()

This creates all necessary tables. It's safe to run multiple times.

You can customize retention periods:

.. code-block:: python

   td.create(retention=5)  # 5 years default retention
   # Or with explicit control:
   td.create(
       retention_short="6 months",
       retention_medium="3 years",
       retention_long="5 years"
   )

Deleting the Schema
~~~~~~~~~~~~~~~~~~~

To delete all tables and data (use with caution):

.. code-block:: python

   td.delete()

**WARNING**: This will delete all data!

Creating Series
---------------

Before inserting data, create a series with ``create_series()``:

.. code-block:: python

   series_id = td.create_series(
       "wind_power",
       unit="MW",
       labels={"site": "offshore_1", "type": "forecast"},
       overlapping=True  # False (default) for facts
   )

Parameters:

- **name**: Series name string (e.g., ``"wind_power"``).
- **unit**: Canonical unit (e.g., ``"MW"``, ``"degC"``, ``"dimensionless"``)
- **labels**: Optional dict of labels for filtering (e.g., ``{"site": "A", "type": "forecast"}``)
- **description**: Optional text description
- **overlapping**: ``False`` (default) for immutable facts, ``True`` for versioned forecasts
- **retention**: ``"short"``, ``"medium"`` (default), or ``"long"`` (only for overlapping)

Returns the ``series_id`` integer. The fluent API handles series resolution automatically
via ``.where()``.

Bulk Series Creation
~~~~~~~~~~~~~~~~~~~~

Use ``create_series_many()`` to create many series in a single database round-trip:

.. code-block:: python

   series_ids = td.create_series_many([
       {"name": "wind_power", "unit": "MW",
        "labels": {"turbine": f"T{i:02d}"}, "overlapping": True}
       for i in range(10)
   ])
   print(series_ids)  # [1, 2, 3, ..., 10]

Each dict supports the same keys as ``create_series()``. ``name`` is required; all
other keys are optional with the same defaults. The function is idempotent: calling
it again with the same ``(name, labels)`` pairs returns the existing ``series_id``
values without error.

Returns a list of ``series_id`` integers in the same order as the input list.

Targeted Ingestion
------------------

Use ``SeriesCollection.insert()`` when you need to write to a single, known series —
patching a bad data segment, backfilling a gap, running a one-off correction, or
exploring data interactively. The series is resolved exactly once via ``.where()``
filters before any data reaches the database.

.. list-table::
   :header-rows: 1
   :widths: 28 36 36

   * -
     - Targeted Ingestion (``insert``)
     - Production Ingestion (``write*``)
   * - **Targets**
     - One specific series
     - Many series at once
   * - **Typical use**
     - Patch bad data, backfill, explore
     - ETL pipelines, scheduled loads
   * - **Data shape**
     - ``TimeSeries`` or ``pd.DataFrame``
     - Long / wide / MultiIndex DataFrame
   * - **Routing**
     - Explicit ``.where()`` filters
     - Inferred from columns (vectorized join)
   * - **Transaction**
     - One per ``insert()`` call
     - One for all series combined

First, select a series by name (and optionally filter by unit or labels):

.. code-block:: python

   # Select by name
   td.get_series("wind_power")

   # Optionally filter by unit
   td.get_series("power", unit="MW")

   # Optionally filter by labels
   td.get_series("wind_power").where(site="offshore_1", type="forecast")

Then insert data:

.. code-block:: python

   from timedb import TimeSeries

   ts = TimeSeries.from_pandas(
       pd.DataFrame({
           "valid_time": [datetime(2025, 1, 1, i, tzinfo=timezone.utc) for i in range(24)],
           "value": [100.0 + i * 2 for i in range(24)]
       }),
       unit="MW",
   )

   result = td.get_series("wind_power").where(site="offshore_1").insert(ts)
   # result.run_id  — uuid.UUID
   # result.series_id — int

.. note::

   A plain ``pd.DataFrame`` is also accepted directly (backward-compatible) — the SDK
   converts it internally. ``TimeSeries`` is preferred because it carries unit metadata
   that the SDK can validate.

To set a single ``knowledge_time`` for all rows, pass it as a keyword argument:

.. code-block:: python

   result = td.get_series("wind_power").where(site="offshore_1").insert(
       ts,
       knowledge_time=datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc),
   )

To store a different ``knowledge_time`` per row — for both flat and overlapping series
— include a ``knowledge_time`` column in the DataFrame / ``TimeSeries`` (VERSIONED
insert). Passing the kwarg *and* a column simultaneously raises ``ValueError``:

.. code-block:: python

   import pandas as pd
   from timedb import TimeSeries

   ts_versioned = TimeSeries.from_pandas(
       pd.DataFrame({
           "knowledge_time": [kt1] * 24 + [kt2] * 24,
           "valid_time":     times1 + times2,
           "value":          values1 + values2,
       }),
       unit="MW",
   )
   result = td.get_series("wind_power").where(site="offshore_1").insert(ts_versioned)
   print(result.run_id)  # uuid.UUID — one run per insert() call

.. note::

   One run is created per ``insert()`` call regardless of how many unique
   ``knowledge_time`` values are in the data. The ``knowledge_time`` column is stored
   per-row in the flat and overlapping tables and does not affect the run count.

Full insert signature:

.. code-block:: python

   result = td.get_series("name").where(...).insert(
       ts,                     # TimeSeries or pd.DataFrame
       knowledge_time=None,    # Broadcast knowledge_time; mutually exclusive with column
       unit=None,              # Incoming unit for DataFrame path (pint conversion applied)
       workflow_id=None,       # Optional, defaults to "sdk-workflow"
       run_start_time=None,  # Optional, defaults to now()
       run_finish_time=None, # Optional
       run_params=None,      # Optional dict of metadata
   )

Production Ingestion
--------------------

For scheduled pipelines and bulk data loads that cover many series at once, use the
global ``write()`` method. It routes through the same engine as targeted ingestion
and shares the same guarantees:

Multi-Series Write Methods
~~~~~~~~~~~~~~~~~~~~~~~~~~

TimeDB provides a single multi-series write method. It:

- Accepts **Pandas or Polars** DataFrames
- Resolves all series in **one database round-trip**
- Writes all partitions in **one transaction**
- Raises ``ValueError`` or ``IncompatibleUnitError`` **before** any DB write

``write()`` — long / tidy format
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

One row per value. Every row encodes its full series identity in columns.

**Required structure:**

.. code-block:: text

   name        | site    | turbine | valid_time               | value
   ------------|---------|---------|--------------------------|------
   wind_power  | Gotland | T01     | 2025-01-01 00:00:00+00:00 |  4.2
   wind_power  | Gotland | T02     | 2025-01-01 00:00:00+00:00 |  3.8
   wind_speed  | Gotland | T01     | 2025-01-01 00:00:00+00:00 |  9.1
   wind_speed  | Gotland | T02     | 2025-01-01 00:00:00+00:00 |  8.7
   ...

**Auto-detection rules** for ``label_cols`` when omitted: every column not in the
reserved set and not ``name_col`` becomes a label. The reserved set is:
``valid_time``, ``value``, ``knowledge_time``, ``valid_time_end``, ``change_time``,
``changed_by``, ``annotation``, ``series_id``, ``run_id``, ``unit``.

**Pattern 1 — broadcast** ``knowledge_time`` **(kwarg):** same timestamp for every row:

.. code-block:: python

   import pandas as pd
   from datetime import datetime, timezone

   t = datetime(2025, 1, 1, tzinfo=timezone.utc)

   df = pd.DataFrame([
       {"name": "wind_power", "site": "Gotland", "turbine": "T01", "valid_time": t, "value": 4.2},
       {"name": "wind_power", "site": "Gotland", "turbine": "T02", "valid_time": t, "value": 3.8},
       {"name": "wind_speed", "site": "Gotland", "turbine": "T01", "valid_time": t, "value": 9.1},
       {"name": "wind_speed", "site": "Gotland", "turbine": "T02", "valid_time": t, "value": 8.7},
   ])
   results = td.write(df, knowledge_time=datetime(2025, 1, 1, 6, tzinfo=timezone.utc))

**Pattern 2 — per-row** ``knowledge_time`` **(column):** include it in the DataFrame to
assign a different ``knowledge_time`` to each row (e.g. two forecast runs in one call).
The ``knowledge_time=`` kwarg must be omitted when the column is present.

.. code-block:: text

   name        | site    | turbine | valid_time                | knowledge_time            | value
   ------------|---------|---------|---------------------------|---------------------------|------
   wind_power  | Gotland | T01     | 2025-01-01 00:00:00+00:00 | 2025-01-01 06:00:00+00:00 |  4.2
   wind_power  | Gotland | T01     | 2025-01-01 01:00:00+00:00 | 2025-01-01 18:00:00+00:00 |  4.5
   ...

**Pattern 3 — per-row** ``knowledge_time`` **+** ``change_time`` **(corrections):**
include ``change_time`` to stamp the exact moment a correction was applied. On upserts
(``series_id + valid_time`` already exists) PostgreSQL's ``DEFAULT now()`` only fires on
INSERT; TimeDB forces ``change_time = now()`` in the UPDATE clause unless you supply the
column yourself.

.. code-block:: text

   name        | site    | turbine | valid_time                | knowledge_time            | change_time               | value
   ------------|---------|---------|---------------------------|---------------------------|---------------------------|------
   wind_power  | Gotland | T01     | 2025-01-01 00:00:00+00:00 | 2025-03-02 10:00:00+00:00 | 2025-03-02 09:00:00+00:00 |  4.8
   wind_power  | Gotland | T01     | 2025-01-01 00:15:00+00:00 | 2025-03-02 10:00:00+00:00 | 2025-03-02 09:00:00+00:00 |  5.1
   ...

**Pattern 4 — multiple runs per call** ``(run_cols)``**:** pass ``run_cols`` to
commit multiple provenance runs in one atomic transaction. Every unique combination of
values in those columns becomes a distinct run record.

*With unreserved columns* (e.g. ``model``) — values are packed into ``run_params`` JSON
on each run record:

.. code-block:: text

   name        | site    | turbine | model   | valid_time                | value
   ------------|---------|---------|---------|---------------------------|------
   wind_power  | Gotland | T01     | ECMWF   | 2025-01-01 00:00:00+00:00 |  4.2
   wind_power  | Gotland | T01     | GFS     | 2025-01-01 00:00:00+00:00 |  3.9
   wind_power  | Gotland | T02     | ECMWF   | 2025-01-01 00:00:00+00:00 |  3.8
   wind_power  | Gotland | T02     | GFS     | 2025-01-01 00:00:00+00:00 |  3.5
   ...

.. code-block:: python

   results = td.write(
       df,
       run_cols=["model"],          # one run per unique model value
       knowledge_time=datetime(2025, 1, 1, 6, tzinfo=timezone.utc),
       workflow_id="nightly-forecast",
   )
   # run_params = {"model": "ECMWF"} stored for the ECMWF run, etc.
   # len(results) == num_series × num_models

*With reserved columns* (``workflow_id``, ``run_start_time``, ``run_finish_time``) —
values are written directly to the native ``runs`` table fields, not packed into
``run_params``:

.. code-block:: text

   name        | site    | turbine | workflow_id       | valid_time                | value
   ------------|---------|---------|-------------------|---------------------------|------
   wind_power  | Gotland | T01     | run-2025-01-01-06 | 2025-01-01 00:00:00+00:00 |  4.2
   wind_power  | Gotland | T01     | run-2025-01-01-18 | 2025-01-01 12:00:00+00:00 |  4.5
   ...

.. code-block:: python

   results = td.write(df, run_cols=["workflow_id"])
   # Two distinct runs, each with its own workflow_id stored natively in runs.

**Pattern 5 — per-row unit conversion** ``(unit column)``**:** include a ``"unit"``
column to apply different conversion factors to individual rows in the same payload.
Every row declares its own incoming unit; TimeDB resolves the factor for each unique
``(series, unit)`` combination via a vectorized join — pint is only invoked O(unique
combos) times regardless of DataFrame size.

.. code-block:: text

   name        | site    | turbine | valid_time                | unit | value
   ------------|---------|---------|---------------------------|------|------
   wind_power  | Gotland | T01     | 2025-01-01 00:00:00+00:00 | MW   |  4.2
   wind_power  | Gotland | T01     | 2025-01-01 01:00:00+00:00 | kW   | 4200
   wind_power  | Gotland | T02     | 2025-01-01 00:00:00+00:00 | MW   |  3.8
   ...

.. code-block:: python

   results = td.write(df, knowledge_time=datetime(2025, 1, 1, 6, tzinfo=timezone.utc))
   # Series registered as "MW" — kW rows are multiplied by 0.001 automatically.
   # Passing unit= kwarg at the same time as a unit column raises ValueError.

**Pattern 6 — direct series ID routing** ``(series_col)``**:** when you already know
the integer ``series_id`` values (e.g. from a previous ``create_series()`` call or a
read→write round-trip), bypass name/label resolution entirely:

.. code-block:: text

   series_id | valid_time                | value
   ----------|---------------------------|------
   42        | 2025-01-01 00:00:00+00:00 |  4.2
   42        | 2025-01-01 01:00:00+00:00 |  4.5
   17        | 2025-01-01 00:00:00+00:00 |  9.1
   ...

.. code-block:: python

   results = td.write(df, series_col="series_id")
   # Validates that series IDs 42 and 17 exist, then inserts directly.
   # Mutually exclusive with name_col and label_cols.

All other options (``unit``, ``run_cols``, ``knowledge_time``, etc.) work
identically in both routing modes.

Full signature:

.. code-block:: python

   results = td.write(
       df,                       # pd.DataFrame or pl.DataFrame
       name_col=None,            # defaults to "name"; mutually exclusive with series_col
       label_cols=None,          # None → inferred; [] → no labels; mutually exclusive with series_col
       run_cols=None,          # columns that define run identity (provenance)
       series_col=None,          # route by series ID; mutually exclusive with name_col/label_cols
       *,
       knowledge_time=None,      # broadcast; mutually exclusive with column
       unit=None,                # broadcast unit; mutually exclusive with unit column
       workflow_id=None,         # default workflow_id (overridden by run_cols)
       run_start_time=None,
       run_finish_time=None,
       run_params=None,        # base dict; per-run unreserved cols merged on top
   )  # -> List[InsertResult], one per unique (series_id, run_id)

Also available as a ``TimeDataClient`` method: ``td_client.write(...)``.

Returns a list of :class:`~timedb.InsertResult` objects, one per unique
``(series_id, run_id)`` pair. Without ``run_cols`` this is one per series;
with ``run_cols`` it is N×M (N unique run combinations × M series).

Raises:

- ``ValueError``: if ``name_col`` is not found in the DataFrame
- ``ValueError``: if any ``(name, labels)`` combination has no matching series
- ``ValueError``: if ``valid_time`` or ``value`` columns are missing
- ``ValueError``: if ``run_cols`` contains columns not present in the DataFrame
- ``ValueError``: if ``run_cols`` overlaps with ``label_cols``
- ``ValueError``: if both a ``"unit"`` column and the ``unit=`` kwarg are provided
- ``IncompatibleUnitError``: if any incoming unit is incompatible with a series unit

Passthrough Columns
^^^^^^^^^^^^^^^^^^^

Optional audit columns present in the input DataFrame are forwarded to the database
unchanged. Supported passthrough columns: ``knowledge_time``, ``change_time``,
``changed_by``, ``annotation``, ``valid_time_end``.

``change_time`` is particularly important for corrections: on an upsert
(``series_id + valid_time`` already exists), PostgreSQL's ``DEFAULT now()`` only fires
on INSERT — not on ``ON CONFLICT DO UPDATE``. TimeDB forces ``change_time = now()``
in the UPDATE clause unless you supply the column yourself. See Example 3 in the
``write()`` section above for full usage.

Unit Conversion
~~~~~~~~~~~~~~~

Pass ``unit=`` to ``insert()`` to declare the unit of the incoming DataFrame values.
TimeDB uses pint to compute a scalar conversion factor from your unit to the series'
canonical unit before writing:

.. code-block:: python

   # Series registered as "MW" — values in kW are multiplied by 0.001 automatically
   td.get_series("wind_power").where(site="Gotland").insert(df, unit="kW")

If the units are dimensionally incompatible (e.g., ``"kg"`` into an ``"MW"`` series),
:class:`~timedb.IncompatibleUnitError` is raised before any data is written.

``TimeSeries`` objects carry their own unit; pass them directly to ``insert()``
and conversion is applied automatically — no ``unit=`` kwarg needed:

.. code-block:: python

   ts = TimeSeries.from_pandas(df, unit="kW")
   td.get_series("wind_power").where(site="Gotland").insert(ts)  # converts kW → MW

Interval-Based Time Series
~~~~~~~~~~~~~~~~~~~~~~~~~~~

For interval-based data (e.g., energy over a time period):

.. code-block:: python

   from timedb import TimeSeries

   start_times = pd.date_range(
       start=datetime.now(timezone.utc),
       periods=24,
       freq='h'
   )
   end_times = start_times + timedelta(hours=1)

   ts_intervals = TimeSeries.from_pandas(
       pd.DataFrame({
           "valid_time": start_times,
           "valid_time_end": end_times,
           "value": [100.0, 105.0, 110.0] * 8,
       }),
       unit="MWh",
   )

   td.get_series("energy").where(...).insert(ts_intervals)

Reading Data
------------

Use the fluent API to read data. Start by selecting a series:

.. code-block:: python

   # Select series by name — returns a TimeSeries
   ts = td.get_series("temperature").read()
   df = ts.to_pandas()  # pd.DataFrame with valid_time index, value column

   # Select by name and filter by labels
   ts = td.get_series("temperature").where(location="room_1").read()

   # Multiple filters
   ts = td.get_series("wind_power").where(site="Gotland", turbine="T01").read()

   # Time range filtering
   ts = td.get_series("power").read(
       start_valid=datetime(2025, 1, 1, tzinfo=timezone.utc),
       end_valid=datetime(2025, 1, 2, tzinfo=timezone.utc),
   )

``read()`` returns a :class:`~timedatamodel.TimeSeries`. Inspect
metadata directly (``ts.name``, ``ts.unit``, ``ts.shape``, ``ts.labels``, ``len(ts)``),
then call ``.to_pandas()`` to get a ``pd.DataFrame``:

- **Index**: ``valid_time`` (with UTC timezone) for ``SIMPLE`` shape; MultiIndex for other shapes
- **Column**: ``value`` (float64)

Full read signature:

.. code-block:: python

   ts = td.get_series("name").read(
       start_valid=None,     # Optional, start of valid time range
       end_valid=None,       # Optional, end of valid time range
       start_known=None,     # Optional, start of knowledge_time (overlapping only)
       end_known=None,       # Optional, end of knowledge_time (overlapping only)
       overlapping=False,    # If True, return VERSIONED shape (overlapping only)
       include_updates=False,  # If True, include correction chain (CORRECTED/AUDIT shape)
   )
   df = ts.to_pandas()       # pd.DataFrame; index depends on shape (see DataShape)

Reading with Units
~~~~~~~~~~~~~~~~~~

The ``TimeSeries`` returned by ``read()`` carries the series' canonical unit in ``ts.unit``.
To work with pint quantities, convert to pandas first and apply pint manually:

.. code-block:: python

   import pint
   ts = td.get_series("power").read()
   df = ts.to_pandas()
   # ts.unit == "MW"
   ureg = pint.UnitRegistry()
   qty = df["value"].values * ureg(ts.unit)  # pint Quantity (numpy array * unit)

Reading Latest Values (Default)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

By default, ``read()`` returns the latest version for overlapping series as a
``SIMPLE``-shape ``TimeSeries``:

.. code-block:: python

   # Read latest forecast values — returns a TimeSeries (SIMPLE shape)
   ts_latest = td.get_series("wind_forecast").read(
       start_valid=datetime(2025, 1, 1, tzinfo=timezone.utc),
       end_valid=datetime(2025, 1, 2, tzinfo=timezone.utc),
   )
   df_latest = ts_latest.to_pandas()  # index: valid_time

Reading Forecast History and Audit Log
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Two boolean flags control what ``read()`` returns. They are orthogonal — each answers
a distinct question:

- **``overlapping``** (*False* by default): *"Which forecast run is each value from?"*
  When ``True``, returns a ``VERSIONED``-shape ``TimeSeries`` with one row per
  forecast-run × valid_time. Only valid for overlapping series.
- **``include_updates``** (*False* by default): *"Who edited these numbers, and when?"*
  When ``True``, returns a ``CORRECTED`` or ``AUDIT``-shape ``TimeSeries`` with
  ``change_time``, ``changed_by``, and ``annotation`` columns. Works for both flat
  and overlapping series.

The four combinations:

.. list-table::
   :header-rows: 1
   :widths: 18 18 30 20 14

   * - ``overlapping``
     - ``include_updates``
     - Returned ``DataShape``
     - ``.to_pandas()`` index
     - Works for
   * - ``False`` (default)
     - ``False`` (default)
     - ``SIMPLE``
     - ``valid_time``
     - flat + overlapping
   * - ``False``
     - ``True``
     - ``CORRECTED``
     - ``valid_time, change_time``
     - flat + overlapping
   * - ``True``
     - ``False``
     - ``VERSIONED``
     - ``knowledge_time, valid_time``
     - overlapping only
   * - ``True``
     - ``True``
     - ``AUDIT``
     - ``knowledge_time, change_time, valid_time``
     - overlapping only

.. code-block:: python

   # Latest value per valid_time (default) — SIMPLE shape
   ts_latest = td.get_series("wind_forecast").read()
   df_latest = ts_latest.to_pandas()

   # History: one row per forecast run × valid_time — VERSIONED shape
   ts_history = td.get_series("wind_forecast").read(
       overlapping=True,
       start_valid=datetime(2025, 1, 1, tzinfo=timezone.utc),
       end_valid=datetime(2025, 1, 2, tzinfo=timezone.utc),
   )
   df_history = ts_history.to_pandas()
   # df_history.index: (knowledge_time, valid_time)

   # Filter by knowledge_time range (when forecasts were made)
   ts_history = td.get_series("wind_forecast").read(
       overlapping=True,
       start_known=datetime(2024, 12, 1, tzinfo=timezone.utc),
       end_known=datetime(2025, 1, 15, tzinfo=timezone.utc),
   )

   # Corrections for the currently-winning forecast run only — CORRECTED shape
   ts_corrected = td.get_series("wind_forecast").read(include_updates=True)
   df_corrected = ts_corrected.to_pandas()
   # df_corrected.index: (valid_time, change_time) — knowledge_time NOT included

   # Full audit log — every correction ever written — AUDIT shape
   ts_audit = td.get_series("wind_forecast").read(overlapping=True, include_updates=True)
   df_audit = ts_audit.to_pandas()
   # df_audit.index: (knowledge_time, change_time, valid_time)

``overlapping=True`` on a flat series raises ``ValueError``.

Example: Analyzing forecast revisions:

.. code-block:: python

   import timedb as td
   import pandas as pd
   from datetime import datetime, timezone, timedelta
   from timedb import TimeSeries

   base_time = datetime(2025, 1, 1, tzinfo=timezone.utc)

   # Insert 4 forecast runs in a single call using a VERSIONED TimeSeries
   rows = []
   for i in range(4):
       kt = base_time + timedelta(days=i)
       for j in range(72):
           rows.append({"knowledge_time": kt,
                        "valid_time": kt + timedelta(hours=j),
                        "value": 100.0 + i * 10})

   ts_versioned = TimeSeries.from_pandas(pd.DataFrame(rows), unit="MW")
   result = td.get_series("power").insert(ts_versioned)
   print(result.run_id)  # uuid.UUID — one run for all 4 forecast runs

   # Read forecast history — VERSIONED shape, one row per (knowledge_time, valid_time)
   ts_history = td.get_series("power").read(
       start_valid=base_time,
       end_valid=base_time + timedelta(days=5),
       overlapping=True,
   )
   df_history = ts_history.to_pandas()
   print(df_history.index.get_level_values("knowledge_time").nunique())  # 4

Reading with Per-Window Cutoffs (Overlapping Only)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For backtesting or day-ahead simulation, ``read_relative()`` returns — for each time
window — the latest forecast whose ``knowledge_time`` is at or before a per-window
cutoff. This simulates "what forecast was actually available at decision time" rather
than using the globally latest version.

**Low-level mode** — full control over window length and offset:

.. code-block:: python

   from datetime import datetime, timedelta, timezone

   ts = td.get_series("wind_forecast").where(model="nwp").read_relative(
       window_length=timedelta(hours=24),
       issue_offset=timedelta(hours=-12),  # 12h before each window start
       start_window=datetime(2026, 2, 1, tzinfo=timezone.utc),
   )
   df = ts.to_pandas()

Parameters:

- **window_length** (timedelta): Length of each window (e.g., ``timedelta(hours=24)``)
- **issue_offset** (timedelta): Offset from window start for the ``knowledge_time`` cutoff.
  Negative = before the window (e.g., ``timedelta(hours=-12)`` = 12h before the window starts).
- **start_window** (datetime): Window alignment origin. Defaults to ``start_valid`` if not provided.
- **start_valid** (datetime, optional): Start of valid time range
- **end_valid** (datetime, optional): End of valid time range

**Daily shorthand mode** — fixed 1-day windows with a human-friendly cutoff
(mirrors `Energy Quantified's instances.relative() <https://energyquantified-python.readthedocs.io/en/latest/reference/reference.html#energyquantified.api.InstancesAPI.relative>`_):

.. code-block:: python

   from datetime import datetime, time, timezone

   # Day-ahead: latest forecast issued by 06:00 on the day before each calendar day
   ts = td.get_series("wind_forecast").where(model="nwp").read_relative(
       days_ahead=1,
       time_of_day=time(6, 0),
       start_valid=datetime(2026, 2, 1, tzinfo=timezone.utc),
       end_valid=datetime(2026, 2, 28, tzinfo=timezone.utc),
   )
   df = ts.to_pandas()

Parameters:

- **days_ahead** (int): Calendar days before the window the forecast must be issued.
  ``0`` = same-day cutoff, ``1`` = day-ahead, ``2`` = two-days-ahead, etc.
- **time_of_day** (datetime.time): Latest time of day on the issue day (UTC).
  E.g., ``time(6, 0)`` means "by 06:00 on the issue day".
- **start_valid** (datetime): Start of valid time range. Also sets window alignment
  (midnight of this date). Required in daily mode.
- **end_valid** (datetime, optional): End of valid time range.

The two parameter sets are mutually exclusive — mixing them raises ``ValueError``.

**How the daily shorthand maps internally:**

.. code-block:: python

   window_length = timedelta(days=1)
   issue_offset  = timedelta(hours=time_of_day.hour, minutes=time_of_day.minute) - timedelta(days=days_ahead)
   start_window  = start_valid.replace(hour=0, minute=0, second=0, microsecond=0)

Example with ``days_ahead=1, time_of_day=time(6, 0)``:
Window Jan 2 00:00 + (6h − 24h) = **Jan 1 06:00** — only forecasts issued by then qualify.

Returns a ``TimeSeries`` with ``SIMPLE`` shape — identical to ``read()``. Call ``.to_pandas()`` for a DataFrame with ``valid_time`` index and ``value`` column.

Multi-Series Read
-----------------

For bulk reads across many series at once, use the module-level ``td.read()`` method.
It mirrors ``td.write()``: you pass a **manifest DataFrame** specifying which series
to read, and get back a long-format ``pl.DataFrame`` with data for all matched series.

``read()`` — manifest-based multi-series read
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The manifest contains routing columns (name + labels, or series IDs) — exactly like
the input to ``write()``, but without the data columns:

.. code-block:: python

   import polars as pl
   from datetime import datetime, timezone

   manifest = pl.DataFrame({
       "name": ["wind_power", "wind_power", "solar_power"],
       "site": ["Gotland", "Oslo", "Gotland"],
   })

   df = td.read(
       manifest,
       name_col="name",
       label_cols=["site"],
       start_valid=datetime(2026, 3, 1, tzinfo=timezone.utc),
       end_valid=datetime(2026, 4, 1, tzinfo=timezone.utc),
   )

Returns a ``pl.DataFrame`` with columns:
``[name, site, unit, series_id, valid_time, value]``.

**Fast path — route by series ID:**

.. code-block:: python

   fast = pl.DataFrame({"sid": [1, 2, 3]})
   df = td.read(fast, series_col="sid")

**With overlapping history:**

.. code-block:: python

   df = td.read(manifest, name_col="name", label_cols=["site"], overlapping=True)
   # Columns: name | site | unit | series_id | knowledge_time | valid_time | value
   # Both flat and overlapping series include knowledge_time; only overlapping series return multiple versions per valid_time.

**With correction chain:**

.. code-block:: python

   df = td.read(manifest, name_col="name", label_cols=["site"], include_updates=True)
   # Adds: change_time, changed_by, annotation columns

Full signature:

.. code-block:: python

   df = td.read(
       manifest,                 # pl.DataFrame or pd.DataFrame (routing columns only)
       name_col=None,            # defaults to "name"; mutually exclusive with series_col
       label_cols=None,          # None → inferred; [] → no labels; mutually exclusive with series_col
       series_col=None,          # route by series ID; mutually exclusive with name_col/label_cols
       *,
       start_valid=None,         # valid time range (inclusive/exclusive)
       end_valid=None,
       start_known=None,         # knowledge_time range (overlapping only)
       end_known=None,
       overlapping=False,        # True → all forecast versions for overlapping series
       include_updates=False,    # True → correction chain
   )  # -> pl.DataFrame

Also available as ``td_client.read(...)``.

``read_relative()`` — manifest-based relative read
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Multi-series version of ``SeriesCollection.read_relative()``. Same manifest routing,
same window parameters. All matched series must be overlapping.

.. code-block:: python

   from datetime import time

   df = td.read_relative(
       manifest,
       name_col="name",
       label_cols=["site"],
       days_ahead=1,
       time_of_day=time(6, 0),
       start_valid=datetime(2026, 3, 1, tzinfo=timezone.utc),
       end_valid=datetime(2026, 3, 31, tzinfo=timezone.utc),
   )
   # Returns: name | site | unit | series_id | valid_time | value

Full signature:

.. code-block:: python

   df = td.read_relative(
       manifest,
       name_col=None, label_cols=None, series_col=None,
       *,
       # Low-level mode:
       window_length=None, issue_offset=None, start_window=None,
       # Daily shorthand mode (mutually exclusive with low-level):
       days_ahead=None, time_of_day=None,
       # Shared:
       start_valid=None, end_valid=None,
   )  # -> pl.DataFrame

Getting Series Metadata
~~~~~~~~~~~~~~~~~~~~~~~

List unique label values and count series in a collection:

.. code-block:: python

   collection = td.get_series("wind_power").where(site="Gotland")

   # Count matching series
   num_series = collection.count()  # Returns: 3

   # List all unique values for a label
   turbines = collection.list_labels("turbine")  # Returns: ["T01", "T02", "T03"]

   # List all matching series with full metadata
   series_list = collection.list_series()
   # Returns: [
   #   {'series_id': 1, 'name': 'wind_power', 'unit': 'MW',
   #    'labels': {'site': 'Gotland', 'turbine': 'T01'},
   #    'description': None, 'overlapping': True, 'retention': 'medium'},
   #   ...
   # ]

   # Filter progressively
   t01_collection = collection.where(turbine="T01")

Getting Run History
~~~~~~~~~~~~~~~~~~~

Use ``list_runs()`` to inspect all runs written for a series.  Each insert
call creates one run, so this gives you a full history of data loads ordered
by most recent first.  Single-series only — use ``.where()`` to narrow down
to a single series if needed.

.. code-block:: python

   runs = td.get_series("wind_forecast").where(site="offshore_1").list_runs()
   # [
   #   {'run_id': UUID('...'), 'workflow_id': 'forecast-run-v2',
   #    'run_start_time': datetime(...), 'run_finish_time': datetime(...),
   #    'run_params': {'model': 'nwp'}, 'inserted_at': datetime(...)},
   #   {'run_id': UUID('...'), 'workflow_id': 'forecast-run-v1',
   #    'run_start_time': None, 'run_finish_time': None,
   #    'run_params': None, 'inserted_at': datetime(...)},
   #   ...
   # ]

Each dict contains:

- **run_id** (uuid.UUID): Unique run identifier (UUIDv7, embeds insert timestamp)
- **workflow_id** (str or None): Workflow tag set at insert time
- **run_start_time** / **run_finish_time** (datetime or None): User-supplied time range
- **run_params** (dict or None): Arbitrary metadata from insert
- **inserted_at** (datetime): When the run was written to the DB

Error Handling
--------------

Common errors and how to handle them:

.. code-block:: python

   from timedb import IncompatibleUnitError

   try:
       td.create()
   except Exception as e:
       print(f"Schema creation failed: {e}")

   try:
       result = td.get_series("power").insert(df)
   except ValueError as e:
       if "No series found" in str(e):
           print("Series doesn't exist. Create it first with td.create_series()")
       elif "does not exist" in str(e):
           print("Tables not created. Run td.create() first.")
       else:
           raise
   except IncompatibleUnitError as e:
       print(f"Unit mismatch: {e}")

Key exceptions:

- ``ValueError``: Series not found, DataFrame format invalid, or schema not created
- ``IncompatibleUnitError``: Unit conversion failed due to dimensionality mismatch

Best Practices
--------------

1. **Always use timezone-aware datetimes**: All time columns must have UTC timezone

   .. code-block:: python

      from datetime import datetime, timezone
      dt = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)  # Good
      dt = datetime(2025, 1, 1, 12, 0)  # Raises error

2. **Create schema first**: Always run ``td.create()`` before inserting data

3. **Create series before inserting**: Use ``td.create_series()`` before any insert or write call

4. **Use labels for filtering**: Organize data with meaningful labels for easy retrieval

   .. code-block:: python

      td.create_series(
          "wind_power",
          unit="MW",
          labels={"site": "Gotland", "turbine": "T01"}
      )

5. **Use knowledge_time for versioning**: Set ``knowledge_time`` to record when data was created or known. Works for both flat and overlapping series

   .. code-block:: python

      td.get_series("forecast").insert(
          data=df,
          knowledge_time=datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)
      )

6. **Use workflow_id for tracking**: Tag data with meaningful workflow IDs to track data sources

   .. code-block:: python

      td.get_series("power").insert(data=df, workflow_id="forecast-v2")
      td.write(long_df, knowledge_time=kt, workflow_id="nwp-pipeline-v3")

7. **Choose the right ingestion path**: use ``insert()`` for targeted writes to one
   known series (patches, corrections, exploration); use ``write()`` for production
   bulk loads across many series.

Complete Example
----------------

A complete workflow from setup to analysis:

.. code-block:: python

   import timedb as td
   import pandas as pd
   from datetime import datetime, timezone, timedelta
   from timedb import TimeSeries

   # 1. Create schema
   td.delete()  # Clean slate
   td.create()

   # 2. Create series with metadata
   td.create_series(
       "wind_power",
       unit="MW",
       labels={"site": "Gotland", "type": "offshore"},
       overlapping=True,  # Versioned forecasts
       retention="medium"
   )

   base_time = datetime(2025, 1, 1, tzinfo=timezone.utc)
   times = [base_time + timedelta(hours=i) for i in range(24)]

   # 3. Insert two forecast runs in a single call (VERSIONED TimeSeries)
   revised_time = base_time + timedelta(hours=6)
   rows = (
       [{"knowledge_time": base_time,    "valid_time": t, "value": 100.0 + i * 2} for i, t in enumerate(times)] +
       [{"knowledge_time": revised_time, "valid_time": t, "value": 105.0 + i * 2} for i, t in enumerate(times)]
   )
   ts_versioned = TimeSeries.from_pandas(pd.DataFrame(rows), unit="MW")
   result = td.get_series("wind_power").where(site="Gotland").insert(
       ts_versioned, workflow_id="forecast-run-1"
   )
   print(result.run_id)  # uuid.UUID — one run for both forecast runs

   # 4. Read latest forecast — SIMPLE shape TimeSeries
   ts_latest = td.get_series("wind_power").where(site="Gotland").read()
   print(ts_latest)              # rich repr with metadata
   df_latest = ts_latest.to_pandas()
   print(df_latest.head())

   # 5. Read forecast history — VERSIONED shape
   ts_history = td.get_series("wind_power").where(site="Gotland").read(overlapping=True)
   df_history = ts_history.to_pandas()
   print(f"Forecast runs: {df_history.index.get_level_values('knowledge_time').nunique()}")

   # ── Production ingestion: write many series at once ───────────────────────
   # Create 5 turbines × 2 metrics = 10 series in one call
   TURBINES = [f"T{i:02d}" for i in range(1, 6)]
   td.create_series_many([
       {"name": metric, "unit": "dimensionless",
        "labels": {"turbine": t, "site": "Gotland"}, "overlapping": True}
       for t in TURBINES for metric in ["wind_power", "wind_speed"]
   ])

   # Long DataFrame: one row per value, routing info in columns
   long_df = pd.DataFrame(...)  # columns: name, site, turbine, valid_time, value

   # Write all 10 series in one round-trip, one transaction
   results = td.write(
       long_df,
       knowledge_time=datetime(2025, 1, 1, 6, tzinfo=timezone.utc),
   )
   print(f"{len(results)} series written, run_id={results[0].run_id}")


