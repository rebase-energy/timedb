SDK Usage
=========

The timedb SDK provides a high-level Python interface for working with time series data. It handles unit conversion, series management, and DataFrame operations automatically.

Getting Started
---------------

Import the SDK:

.. code-block:: python

   import timedb as td
   import pandas as pd
   import pint

Database Connection
-------------------

The SDK uses environment variables for database connection:

- ``TIMEDB_DSN`` (preferred)
- ``DATABASE_URL`` (alternative)

You can also use a ``.env`` file in your project root.

Schema Management
-----------------

Creating the Schema
~~~~~~~~~~~~~~~~~~~

Before using the SDK, create the database schema:

.. code-block:: python

   td.create()

This creates all necessary tables. It's safe to run multiple times.

Deleting the Schema
~~~~~~~~~~~~~~~~~~~

To delete all tables and data (use with caution):

.. code-block:: python

   td.delete()

**WARNING**: This will delete all data!

Inserting Data
--------------

The main function for inserting data is ``insert_batch()``. It automatically:

- Detects series from DataFrame columns
- Extracts units from Pint Quantity objects or pint-pandas Series
- Creates/gets series with appropriate units
- Converts all values to canonical units before storage

Basic Example
~~~~~~~~~~~~~

.. code-block:: python

   import timedb as td
   import pandas as pd
   import pint
   from datetime import datetime, timezone, timedelta

   # Create unit registry
   ureg = pint.UnitRegistry()

   # Create sample data with Pint Quantity objects
   times = pd.date_range(
       start=datetime.now(timezone.utc),
       periods=24,
       freq='H',
       tz='UTC'
   )

   # Create DataFrame with Pint Quantity columns
   df = pd.DataFrame({
       "valid_time": times,
       "power": [100.0, 105.0, 110.0] * 8 * ureg.kW,
       "temperature": [20.0, 21.0, 22.0] * 8 * ureg.degC,
   })

   # Insert the data
   result = td.insert_batch(df=df)

   # result.series_ids contains the mapping of series_key to series_id
   print(result.series_ids)
   # {'power': UUID('...'), 'temperature': UUID('...')}

Using pint-pandas Series
~~~~~~~~~~~~~~~~~~~~~~~~

You can also use pint-pandas Series with dtype annotations:

.. code-block:: python

   df = pd.DataFrame({
       "valid_time": times,
       "power": pd.Series([100.0, 105.0, 110.0] * 8, dtype="pint[MW]"),
       "wind_speed": pd.Series([5.0, 6.0, 7.0] * 8, dtype="pint[m/s]"),
   })

   result = td.insert_batch(df=df)

Custom Series Keys
~~~~~~~~~~~~~~~~~~

Override default series keys (column names):

.. code-block:: python

   result = td.insert_batch(
       df=df,
       series_key_overrides={
           'power': 'wind_power_forecast',
           'temperature': 'ambient_temperature'
       }
   )

Interval Values
~~~~~~~~~~~~~~~

For interval-based time series (e.g., energy over a time period):

.. code-block:: python

   start_times = pd.date_range(
       start=datetime.now(timezone.utc),
       periods=24,
       freq='H',
       tz='UTC'
   )
   end_times = start_times + timedelta(hours=1)

   df_intervals = pd.DataFrame({
       "valid_time": start_times,
       "valid_time_end": end_times,
       "energy": [100.0, 105.0, 110.0] * 8 * ureg.MWh
   })

   result = td.insert_batch(
       df=df_intervals,
       valid_time_end_col='valid_time_end'
   )

Advanced Options
~~~~~~~~~~~~~~~~

Full function signature:

.. code-block:: python

   result = td.insert_batch(
       df=pd.DataFrame(...),
       run_id=None,  # Optional, auto-generated if not provided
       workflow_id=None,  # Optional, defaults to "sdk-workflow"
       run_start_time=None,  # Optional, defaults to now()
       run_finish_time=None,  # Optional
       valid_time_col='valid_time',  # Column name for valid_time
       valid_time_end_col=None,  # Column name for valid_time_end
       known_time=None,  # Time of knowledge (for backfills)
       run_params=None,  # Optional dict of run parameters
       series_key_overrides=None,  # Optional dict mapping column names to series_key
   )

Reading Data
------------

The SDK provides reading capabilities through the fluent SeriesCollection API.

Basic Read
~~~~~~~~~~

Read the latest time series values using a SeriesCollection:

.. code-block:: python

   df = td.series("temperature").read(
       start_valid=datetime(2025, 1, 1, tzinfo=timezone.utc),
       end_valid=datetime(2025, 1, 2, tzinfo=timezone.utc),
   )

Function signature:

.. code-block:: python

   df = td.series("temperature").read(
       start_valid=None,  # Optional, start of valid time range
       end_valid=None,  # Optional, end of valid time range
       start_known=None,  # Optional, start of known_time range (overlapping only)
       end_known=None,  # Optional, end of known_time range (overlapping only)
       versions=False,  # If True, include all versions (overlapping only)
       return_mapping=False,  # If True, return (DataFrame, mapping_dict)
   )

Returns a DataFrame with:

- Index: ``valid_time``
- Columns: ``series_key`` (one column per series)
- Each column has pint-pandas dtype based on the series unit

Examples:

.. code-block:: python

   # Read specific series by name
   df = td.series("temperature").read()

   # Read with time range
   df = td.series("power").read(
       start_valid=datetime(2025, 1, 1, tzinfo=timezone.utc),
       end_valid=datetime(2025, 1, 2, tzinfo=timezone.utc),
   )

   # Get mapping of series_id to series_key
   df, mapping = td.series("temperature").read(return_mapping=True)

Reading All Versions (Overlapping Series Only)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For overlapping series (forecasts, estimates with revisions), read all historical versions:

.. code-block:: python

   # Read all versions as separate rows
   df = td.series("wind_forecast").read(versions=True)

   # With time filters
   df = td.series("wind_forecast").read(
       versions=True,
       start_valid=datetime(2025, 1, 1, tzinfo=timezone.utc),
       end_valid=datetime(2025, 1, 2, tzinfo=timezone.utc),
       start_known=datetime(2024, 12, 1, tzinfo=timezone.utc),
       end_known=datetime(2025, 1, 15, tzinfo=timezone.utc),
   )

This is useful for:

- Auditing how forecast predictions changed over time
- Analyzing forecast revisions and backtesting
- Viewing complete version history with known_time tracking

Example: Analyzing forecast revisions:

.. code-block:: python

   # Create multiple forecasts at different known_times
   base_time = datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc)

   for i in range(4):
       known_time = base_time + timedelta(days=i)
       valid_times = [known_time + timedelta(hours=j) for j in range(72)]

       df = pd.DataFrame({
           "valid_time": valid_times,
           "power": generate_forecast(valid_times) * ureg.MW
       })

       result = td.insert_batch(
           df=df,
           known_time=known_time,  # When forecast was made
           workflow_id=f"forecast-run-{i}"
       )

   # Read all forecast revisions (overlapping mode)
   df_overlapping = td.series("power").read(
       versions=True,
       start_valid=base_time,
       end_valid=base_time + timedelta(days=5)
   )

Updating Records
----------------

Update existing overlapping records (flat series are immutable):

.. code-block:: python

   updates = [
       {
           'batch_id': batch_id,
           'valid_time': datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc),
           'series_id': series_id,
           'value': 150.0,
           'annotation': 'Manually corrected',
           'tags': ['reviewed', 'corrected'],
           'changed_by': 'user@example.com',
       }
   ]

   result = td.update_records(updates)

Or update through a SeriesCollection:

.. code-block:: python

   td.series("wind_forecast").update_records([
       {
           'batch_id': batch_id,
           'valid_time': datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc),
           'value': 150.0,
           'annotation': 'Corrected',
       }
   ])

For each update, use tri-state semantics:

- **Omit a field**: Leave it unchanged (use current value)
- **Set to None**: Explicitly clear the field (set to SQL NULL)
- **Set to a value**: Update to that value

The function returns:

.. code-block:: python

   {
       'updated': [...],      # List of successfully updated records
       'skipped_no_ops': [...] # List of skipped (no changes detected)
   }

**Note**: Updates only apply to overlapping series (flat values are immutable facts).

API Server
----------

The SDK provides functions to start and manage the TimeDB REST API server.

Starting the API Server (Blocking)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Start the API server in the current process:

.. code-block:: python

   td.start_api()

   # With custom host/port
   td.start_api(host="0.0.0.0", port=8080)

   # With auto-reload (development)
   td.start_api(reload=True)

This blocks until the server is stopped (Ctrl+C).

Starting the API Server (Non-blocking)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For use in notebooks or when you need the API to run in the background:

.. code-block:: python

   # Start in background thread
   started = td.start_api_background()

   if started:
       print("API server started")
   else:
       print("API server was already running")

Checking API Status
~~~~~~~~~~~~~~~~~~~

Check if the API server is running:

.. code-block:: python

   if td.check_api():
       # API is running
       pass
   else:
       # API is not running
       td.start_api_background()

Error Handling
--------------

The SDK raises specific exceptions:

- ``ValueError``: If tables don't exist (run ``td.create()`` first)
- ``IncompatibleUnitError``: If unit conversion fails due to dimensionality mismatch
- ``ValueError``: If DataFrame structure is invalid or units are inconsistent

Example error handling:

.. code-block:: python

   from timedb import IncompatibleUnitError

   try:
       result = td.insert_batch(df=df)
   except ValueError as e:
       if "TimeDB tables do not exist" in str(e):
           print("Please create the schema first: td.create()")
       else:
           raise
   except IncompatibleUnitError as e:
       print(f"Unit conversion error: {e}")

Best Practices
--------------

1. **Always use timezone-aware datetimes**: All time columns must be timezone-aware (UTC recommended)
2. **Use Pint for units**: Leverage Pint Quantity objects or pint-pandas Series for automatic unit handling
3. **Create schema first**: Run ``td.create()`` before inserting data
4. **Use known_time for backfills**: When inserting historical data, set ``known_time`` to when the data was actually known
5. **Use workflow_id**: Set meaningful workflow IDs to track data sources

Complete Example
----------------

.. code-block:: python

   import timedb as td
   import pandas as pd
   import pint
   from datetime import datetime, timezone, timedelta

   # 1. Create schema
   td.create()

   # 2. Prepare data
   ureg = pint.UnitRegistry()
   times = pd.date_range(
       start=datetime.now(timezone.utc),
       periods=24,
       freq='H',
       tz='UTC'
   )

   df = pd.DataFrame({
       "valid_time": times,
       "power": [100.0 + i for i in range(24)] * ureg.kW,
       "temperature": [20.0 + i*0.5 for i in range(24)] * ureg.degC,
   })

   # 3. Insert data
   result = td.insert_batch(
       df=df,
       workflow_id="forecast-v1",
       run_params={"model": "wind-forecast-v2", "version": "1.0"}
   )

   # 4. Read data
   df_read = td.read(
       series_ids=[result.series_ids['power']],
       start_valid=times[0],
       end_valid=times[-1]
   )

   print(df_read.head())

