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
   from timedatamodel.timeseries_arrow import TimeSeries

Module-level functions (``td.create()``, ``td.delete()``, ``td.create_series()``, ``td.get_series()``) use a lazy default client that reads the database connection from environment variables automatically. This is the recommended approach for most use cases.

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
       name="wind_power",
       unit="MW",
       labels={"site": "offshore_1", "type": "forecast"},
       overlapping=True  # False (default) for facts
   )

Parameters:

- **name**: Series identifier (e.g., "wind_power", "temperature")
- **unit**: Canonical unit (e.g., "MW", "degC", "dimensionless")
- **labels**: Optional dict of labels for filtering (e.g., ``{"site": "A", "type": "forecast"}``)
- **description**: Optional text description
- **overlapping**: ``False`` (default) for immutable facts, ``True`` for versioned forecasts
- **retention**: ``"short"``, ``"medium"`` (default), or ``"long"`` (only for overlapping)

The function returns the ``series_id`` (integer) which can be used directly if needed, but the fluent API handles this automatically via the ``.where()`` filter.

Inserting Data
--------------

Use the fluent ``SeriesCollection`` API to insert data. First, select a series by name (and optionally by unit):

.. code-block:: python

   # Select by name
   td.get_series("wind_power")

   # Optionally filter by unit
   td.get_series("power", unit="MW")

   # Optionally filter by labels
   td.get_series("wind_power").where(site="offshore_1", type="forecast")

Then insert data:

.. code-block:: python

   from timedatamodel.timeseries_arrow import TimeSeries

   ts = TimeSeries.from_pandas(
       pd.DataFrame({
           "valid_time": [datetime(2025, 1, 1, i, tzinfo=timezone.utc) for i in range(24)],
           "value": [100.0 + i * 2 for i in range(24)]
       }),
       unit="MW",
   )

   result = td.get_series("wind_power").where(site="offshore_1").insert(ts)
   # result.series_id, result.batch_id

.. note::

   A plain ``pd.DataFrame`` is also accepted directly (backward-compatible) — the SDK
   converts it internally. ``TimeSeries`` is preferred because it carries unit metadata
   that the SDK can validate.

For overlapping (versioned) series, pass ``knowledge_time`` as a keyword argument:

.. code-block:: python

   result = td.get_series("wind_power").where(site="offshore_1").insert(
       ts,
       knowledge_time=datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc),
   )

Alternatively, include a ``knowledge_time`` column in the DataFrame / ``TimeSeries`` to
insert multiple forecast runs in a single call — each unique ``knowledge_time`` becomes
its own batch:

.. code-block:: python

   import pandas as pd
   from timedatamodel.timeseries_arrow import TimeSeries

   ts_versioned = TimeSeries.from_pandas(
       pd.DataFrame({
           "knowledge_time": [kt1] * 24 + [kt2] * 24,
           "valid_time":     times1 + times2,
           "value":          values1 + values2,
       }),
       unit="MW",
   )
   result = td.get_series("wind_power").where(site="offshore_1").insert(ts_versioned)
   print(result.batch_ids)  # list of created batch IDs

Full insert signature:

.. code-block:: python

   result = td.get_series("name").where(...).insert(
       ts,                  # TimeSeries or pd.DataFrame
       workflow_id=None,    # Optional, defaults to "sdk-workflow"
       batch_start_time=None,  # Optional, defaults to now()
       batch_finish_time=None,  # Optional
       knowledge_time=None,    # Time of knowledge (SIMPLE overlapping inserts)
       batch_params=None,      # Optional dict of metadata
   )

Using pint Units
~~~~~~~~~~~~~~~~

If your DataFrame has pint-pandas dtypes, timedb automatically validates and converts units on insert.
Install the optional pint dependencies first: ``pip install timedb[pint]``

.. code-block:: python

   import pint_pandas  # noqa

   df = pd.DataFrame({
       "valid_time": [datetime(2025, 1, 1, i, tzinfo=timezone.utc) for i in range(24)],
       "value": pd.Series([500.0] * 24, dtype="pint[kW]"),
   })

   # kW is automatically converted to MW (the series' canonical unit)
   td.get_series("power").insert(df)

.. note::

   Pass pint-typed DataFrames **directly** to ``insert()`` — not through
   ``TimeSeries.from_pandas()``. PyArrow does not support pint dtypes, so
   ``TimeSeries.from_pandas()`` would strip the unit information before
   the SDK can validate it. The plain-DataFrame path keeps pint handling intact.

Rules:

- **Plain float64**: passed through unchanged, no unit check
- **Pint with same unit**: stripped to float64, no conversion
- **Pint with compatible unit**: converted to series unit (e.g., kW → MW)
- **Pint dimensionless**: treated as series unit
- **Pint with incompatible unit**: raises ``IncompatibleUnitError`` (e.g., kg into MW series)

Interval-Based Time Series
~~~~~~~~~~~~~~~~~~~~~~~~~~~

For interval-based data (e.g., energy over a time period):

.. code-block:: python

   from timedatamodel.timeseries_arrow import TimeSeries

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

``read()`` returns a :class:`~timedatamodel.timeseries_arrow.TimeSeries`. Inspect
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
       as_pint=False,        # Deprecated; use ts.to_pandas() instead
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
   qty = df["value"].values * ureg(ts.unit)  # pint Quantity array

``as_pint=True`` is deprecated but still works for backward compatibility.
Requires ``pip install timedb[pint]``.

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
   from timedatamodel.timeseries_arrow import TimeSeries

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
   print(result.batch_ids)  # [1, 2, 3, 4]

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

   df = td.get_series("wind_forecast").where(model="nwp").read_relative(
       window_length=timedelta(hours=24),
       issue_offset=timedelta(hours=-12),  # 12h before each window start
       start_window=datetime(2026, 2, 1, tzinfo=timezone.utc),
   )

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
   df = td.get_series("wind_forecast").where(model="nwp").read_relative(
       days_ahead=1,
       time_of_day=time(6, 0),
       start_valid=datetime(2026, 2, 1, tzinfo=timezone.utc),
       end_valid=datetime(2026, 2, 28, tzinfo=timezone.utc),
   )

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

Updating Records
----------------

Update records for both flat and overlapping series. Flat series are updated in-place.
For overlapping series, each update inserts a correction row that **preserves the
original** ``knowledge_time`` and stamps ``change_time = now()``, so the forecast
run identity is unchanged while the full correction history remains queryable:

.. code-block:: python

   updates = [
       {
           "valid_time": datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc),
           "value": 150.0,
           "annotation": "Manually corrected",
           "tags": ["reviewed"],
       }
   ]

   result = td.get_series("wind_forecast").where(site="offshore_1").update_records(updates)

``update_records()`` is single-series only — the same constraint as ``read()`` and ``insert()``.
The collection must resolve to exactly one series; use ``.where()`` filters to narrow it down if needed.

The function returns a list of updated records:

.. code-block:: python

   [
       {
           "series_id": 123,
           "valid_time": datetime(...),
           "value": 150.0,
           ...
       },
       ...
   ]

For overlapping series, you can target a specific forecast run by including:

- ``knowledge_time``: Target the exact forecast run by knowledge_time
- ``batch_id``: Target the latest version in a specific batch
- (neither): Target the globally latest version

Tri-state field semantics:

- **Omit a field**: Leave it unchanged
- **Set to None**: Explicitly clear the field
- **Set to a value**: Update to that value

Example:

.. code-block:: python

   # Clear annotation, update value, leave tags unchanged
   result = td.get_series("power").update_records([
       {
           "valid_time": datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc),
           "value": 200.0,
           "annotation": None,
       }
   ])

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

3. **Create series before inserting**: Use ``td.create_series()`` before ``td.get_series().insert()``

4. **Use labels for filtering**: Organize data with meaningful labels for easy retrieval

   .. code-block:: python

      td.create_series(
          name="wind_power",
          unit="MW",
          labels={"site": "Gotland", "turbine": "T01"}
      )

5. **Use knowledge_time for versioning**: For overlapping series, always set ``knowledge_time`` to indicate when data was created

   .. code-block:: python

      td.get_series("forecast").insert(
          df=df,
          knowledge_time=datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)
      )

6. **Use workflow_id for tracking**: Tag data with meaningful workflow IDs to track data sources

   .. code-block:: python

      td.get_series("power").insert(
          df=df,
          workflow_id="forecast-v2"
      )

Complete Example
----------------

A complete workflow from setup to analysis:

.. code-block:: python

   import timedb as td
   import pandas as pd
   from datetime import datetime, timezone, timedelta
   from timedatamodel.timeseries_arrow import TimeSeries

   # 1. Create schema
   td.delete()  # Clean slate
   td.create()

   # 2. Create series with metadata
   td.create_series(
       name="wind_power",
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
   print(result.batch_ids)  # [1, 2]

   # 4. Read latest forecast — SIMPLE shape TimeSeries
   ts_latest = td.get_series("wind_power").where(site="Gotland").read()
   print(ts_latest)              # rich repr with metadata
   df_latest = ts_latest.to_pandas()
   print(df_latest.head())

   # 5. Read forecast history — VERSIONED shape
   ts_history = td.get_series("wind_power").where(site="Gotland").read(overlapping=True)
   df_history = ts_history.to_pandas()
   print(f"Forecast runs: {df_history.index.get_level_values('knowledge_time').nunique()}")


.. toctree::
    :hidden:

    api_reference
