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

   df = pd.DataFrame({
       "valid_time": [datetime(2025, 1, 1, i, tzinfo=timezone.utc) for i in range(24)],
       "value": [100.0 + i * 2 for i in range(24)]
   })

   result = td.get_series("wind_power").where(site="offshore_1").insert(df)
   # result.series_id, result.batch_id

For overlapping (versioned) series, use ``knowledge_time`` to indicate when the data was created:

.. code-block:: python

   result = td.get_series("wind_power").where(site="offshore_1").insert(
       df=df,
       knowledge_time=datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc)
   )

Full insert signature:

.. code-block:: python

   result = td.get_series("name").where(...).insert(
       df=pd.DataFrame(...),  # Columns: [valid_time, value] or [valid_time, valid_time_end, value]
       workflow_id=None,  # Optional, defaults to "sdk-workflow"
       batch_start_time=None,  # Optional, defaults to now()
       batch_finish_time=None,  # Optional
       knowledge_time=None,  # Time of knowledge (overlapping only)
       batch_params=None,  # Optional dict of metadata
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
   td.get_series("power").insert(df=df)

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

   start_times = pd.date_range(
       start=datetime.now(timezone.utc),
       periods=24,
       freq='H'
   )
   end_times = start_times + timedelta(hours=1)

   df_intervals = pd.DataFrame({
       "valid_time": start_times,
       "valid_time_end": end_times,
       "value": [100.0, 105.0, 110.0] * 8
   })

   td.get_series("energy").where(...).insert(df=df_intervals)

Reading Data
------------

Use the fluent API to read data. Start by selecting a series:

.. code-block:: python

   # Select series by name
   df = td.get_series("temperature").read()

   # Select by name and filter by labels
   df = td.get_series("temperature").where(location="room_1").read()

   # Multiple filters
   df = td.get_series("wind_power").where(site="Gotland", turbine="T01").read()

   # Time range filtering
   df = td.get_series("power").read(
       start_valid=datetime(2025, 1, 1, tzinfo=timezone.utc),
       end_valid=datetime(2025, 1, 2, tzinfo=timezone.utc),
   )

The returned DataFrame has:

- **Index**: ``valid_time`` (with UTC timezone)
- **Column**: ``value`` (float64, or pint dtype if ``as_pint=True``)

Full read signature:

.. code-block:: python

   df = td.get_series("name").read(
       start_valid=None,  # Optional, start of valid time range
       end_valid=None,  # Optional, end of valid time range
       start_known=None,  # Optional, start of knowledge_time (overlapping only)
       end_known=None,  # Optional, end of knowledge_time (overlapping only)
       versions=False,  # If True, return all versions (overlapping only)
       as_pint=False,  # If True, return value column with pint dtype
   )

Reading with Units
~~~~~~~~~~~~~~~~~~

Use ``as_pint=True`` to get the value column as a pint-pandas dtype with the series' canonical unit:

.. code-block:: python

   df = td.get_series("power").read(as_pint=True)
   print(df["value"].dtype)  # pint[MW]

   # Unit-aware arithmetic works naturally
   df["value"] * 2  # still in MW
   df["value"].pint.to("kW")  # convert to kW

Requires ``pip install timedb[pint]``.

Reading Latest Values (Default)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

By default, ``read()`` returns the latest version for overlapping series:

.. code-block:: python

   # Read latest forecast values
   df_latest = td.get_series("wind_forecast").read(
       start_valid=datetime(2025, 1, 1, tzinfo=timezone.utc),
       end_valid=datetime(2025, 1, 2, tzinfo=timezone.utc),
   )

Reading All Versions (Overlapping Only)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For overlapping series, use ``versions=True`` to see the complete revision history:

.. code-block:: python

   # Read all versions with knowledge_time
   df_versions = td.get_series("wind_forecast").read(
       start_valid=datetime(2025, 1, 1, tzinfo=timezone.utc),
       end_valid=datetime(2025, 1, 2, tzinfo=timezone.utc),
       versions=True
   )

   # Filter by knowledge_time range (when forecasts were made)
   df_versions = td.get_series("wind_forecast").read(
       versions=True,
       start_known=datetime(2024, 12, 1, tzinfo=timezone.utc),
       end_known=datetime(2025, 1, 15, tzinfo=timezone.utc),
   )

The result has a MultiIndex with ``(knowledge_time, valid_time)`` to show both when the forecast was made and what it applies to.

Example: Analyzing forecast revisions:

.. code-block:: python

   import timedb as td
   import pandas as pd
   from datetime import datetime, timezone, timedelta

   # Insert multiple forecast revisions
   base_time = datetime(2025, 1, 1, tzinfo=timezone.utc)

   for i in range(4):
       knowledge_time = base_time + timedelta(days=i)
       times = [knowledge_time + timedelta(hours=j) for j in range(72)]

       df = pd.DataFrame({
           "valid_time": times,
           "value": [100.0 + i*10 for _ in range(72)]
       })

       td.get_series("power").insert(df, knowledge_time=knowledge_time)

   # Read all revisions
   df_all = td.get_series("power").read(
       start_valid=base_time,
       end_valid=base_time + timedelta(days=5),
       versions=True
   )

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

The returned DataFrame has index ``valid_time`` and column ``value``, identical to ``read()``.

Getting Series Metadata
~~~~~~~~~~~~~~~~~~~~~~~

List unique label values and count series in a collection:

.. code-block:: python

   collection = td.get_series("wind_power").where(site="Gotland")

   # Count matching series
   num_series = collection.count()  # Returns: 3

   # List all unique values for a label
   turbines = collection.list_labels("turbine")  # Returns: ["T01", "T02", "T03"]

   # Filter progressively
   t01_collection = collection.where(turbine="T01")

Updating Records
----------------

Update records for both flat and overlapping series. Flat series are updated in-place, while overlapping series create new versions with ``knowledge_time=now()``:

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

For collections matching a single series, ``series_id`` is optional. For multiple series, include it:

.. code-block:: python

   # For multi-series collection
   updates = [
       {
           "series_id": 123,
           "valid_time": datetime(...),
           "value": 150.0,
       }
   ]

   result = td.get_series("wind_power").where(site="Gotland").update_records(updates)

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

For overlapping series, the system creates a new version with the current ``knowledge_time``. You can target specific versions by including:

- ``knowledge_time``: Target a specific version
- ``batch_id``: Target records from a specific batch

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

   # 3. Insert forecast data
   base_time = datetime(2025, 1, 1, tzinfo=timezone.utc)
   times = [base_time + timedelta(hours=i) for i in range(24)]

   df = pd.DataFrame({
       "valid_time": times,
       "value": [100.0 + i * 2 for i in range(24)]
   })

   result = td.get_series("wind_power").where(site="Gotland").insert(
       df=df,
       knowledge_time=base_time,
       workflow_id="forecast-run-1"
   )

   # 4. Insert revised forecast (6 hours later)
   revised_time = base_time + timedelta(hours=6)
   df_revised = pd.DataFrame({
       "valid_time": times,
       "value": [105.0 + i * 2 for i in range(24)]
   })

   td.get_series("wind_power").where(site="Gotland").insert(
       df=df_revised,
       knowledge_time=revised_time,
       workflow_id="forecast-run-1"
   )

   # 5. Read latest forecast
   df_latest = td.get_series("wind_power").where(site="Gotland").read()
   print(df_latest.head())

   # 6. Read all forecast revisions for analysis
   df_versions = td.get_series("wind_power").where(site="Gotland").read(versions=True)
   print(f"Total revisions: {df_versions.index.get_level_values(0).nunique()}")
