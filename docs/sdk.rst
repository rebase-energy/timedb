SDK Usage
==========

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

Creating the Schema
-------------------

Before using the SDK, create the database schema:

.. code-block:: python

   td.create()

This creates all necessary tables. It's safe to run multiple times.

Deleting the Schema
-------------------

To delete all tables and data (use with caution):

.. code-block:: python

   td.delete()

**WARNING**: This will delete all data!

Inserting Data
--------------

The main function for inserting data is ``insert_run()``. It automatically:

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
       "power": [100.0, 105.0, 110.0] * 8 * ureg.kW,  # Series with kW unit
       "temperature": [20.0, 21.0, 22.0] * 8 * ureg.degC,  # Series with degC unit
   })

   # Insert the data
   result = td.insert_run(df=df)

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

   result = td.insert_run(df=df)

Custom Series Keys
~~~~~~~~~~~~~~~~~~

Override default series keys (column names):

.. code-block:: python

   result = td.insert_run(
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

   result = td.insert_run(
       df=df_intervals,
       valid_time_end_col='valid_time_end'
   )

Advanced Options
~~~~~~~~~~~~~~~~

Full function signature:

.. code-block:: python

   result = td.insert_run(
       df=pd.DataFrame(...),
       tenant_id=None,  # Optional, defaults to zeros UUID
       run_id=None,  # Optional, auto-generated if not provided
       workflow_id=None,  # Optional, defaults to "sdk-insert"
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

The SDK provides several functions for reading data:

Basic Read
~~~~~~~~~~

Read time series values:

.. code-block:: python

   df = td.read(
       series_id=None,  # Optional, filter by series ID
       tenant_id=None,  # Optional, defaults to zeros UUID
       start_valid=None,  # Optional, start of valid time range
       end_valid=None,  # Optional, end of valid time range
       return_mapping=False,  # If True, return (DataFrame, mapping_dict)
       all_versions=False,  # If True, include all versions
       return_value_id=False,  # If True, include value_id column
       tags_and_annotations=False,  # If True, include tags and annotation columns
   )

Returns a DataFrame with:

- Index: ``valid_time``
- Columns: ``series_id`` (one column per series)
- Each column has pint-pandas dtype based on ``series_unit``

Example:

.. code-block:: python

   # Read all series
   df = td.read()

   # Read specific series
   series_id = result.series_ids['power']
   df = td.read(series_id=series_id)

   # Read with time range
   df = td.read(
       start_valid=datetime(2024, 1, 1, tzinfo=timezone.utc),
       end_valid=datetime(2024, 1, 2, tzinfo=timezone.utc),
   )

   # Get mapping of series_id to series_key
   df, mapping = td.read(return_mapping=True)
   # mapping: {series_id: series_key, ...}

Flat Mode Read
~~~~~~~~~~~~~~

Read values in flat mode (latest known_time per valid_time):

.. code-block:: python

   df = td.read_values_flat(
       series_id=None,
       tenant_id=None,
       start_valid=None,
       end_valid=None,
       start_known=None,  # Start of known_time range
       end_known=None,  # End of known_time range
       all_versions=False,
       return_mapping=False,
       units=True,  # If True, return pint-pandas DataFrame
       return_value_id=False,
   )

Returns the latest version of each (valid_time, series_id) combination based on known_time.

Overlapping Mode Read
~~~~~~~~~~~~~~~~~~~~~

Read values in overlapping mode (all forecast revisions):

.. code-block:: python

   df = td.read_values_overlapping(
       series_id=None,
       tenant_id=None,
       start_valid=None,
       end_valid=None,
       start_known=None,
       end_known=None,
       all_versions=False,
       return_mapping=False,
       units=True,
   )

Returns all versions of forecasts, showing how predictions evolve over time. Useful for analyzing forecast revisions and backtesting.

Updating Records
----------------

Update existing records with new values, annotations, or tags:

.. code-block:: python

   updates = [
       {
           'run_id': run_id,
           'tenant_id': tenant_id,
           'valid_time': datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc),
           'series_id': series_id,
           'value': 150.0,  # New value
           'annotation': 'Manually corrected',  # Optional annotation
           'tags': ['reviewed', 'corrected'],  # Optional tags
           'changed_by': 'user@example.com',  # Optional
       }
   ]

   result = td.update_records(updates)

   # result contains:
   # {
   #     'updated': [...],  # List of updated records
   #     'skipped_no_ops': [...]  # List of skipped records (no changes)
   # }

For each update:

- Omit a field to leave it unchanged
- Set to ``None`` (or ``[]`` for tags) to explicitly clear it
- Set to a value to update it

Error Handling
-------------

The SDK raises specific exceptions:

- ``ValueError``: If tables don't exist (run ``td.create()`` first)
- ``IncompatibleUnitError``: If unit conversion fails due to dimensionality mismatch
- ``ValueError``: If DataFrame structure is invalid or units are inconsistent

Example error handling:

.. code-block:: python

   try:
       result = td.insert_run(df=df)
   except ValueError as e:
       if "TimeDB tables do not exist" in str(e):
           print("Please create the schema first: td.create()")
       else:
           raise
   except IncompatibleUnitError as e:
       print(f"Unit conversion error: {e}")

Best Practices
--------------

1. **Always use timezone-aware datetimes**: All time columns must be timezone-aware
2. **Use Pint for units**: Leverage Pint Quantity objects or pint-pandas Series for automatic unit handling
3. **Create schema first**: Run ``td.create()`` before inserting data
4. **Use known_time for backfills**: When inserting historical data, set ``known_time`` to when the data was actually known
5. **Use workflow_id**: Set meaningful workflow IDs to track data sources

Example Workflow
----------------

Complete example:

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
   result = td.insert_run(
       df=df,
       workflow_id="forecast-v1",
       run_params={"model": "wind-forecast-v2", "version": "1.0"}
   )

   # 4. Read data
   df_read = td.read(
       series_id=result.series_ids['power'],
       start_valid=times[0],
       end_valid=times[-1]
   )

   print(df_read.head())

