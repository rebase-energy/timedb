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

Creating Schema with Users Table
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For multi-tenant authentication support, create the schema with a users table:

.. code-block:: python

   from timedb import db
   import psycopg
   
   conninfo = "postgresql://user:password@localhost:5432/timedb"
   conn = psycopg.connect(conninfo)
   
   db.create_with_users.create_schema_users(conn)
   conn.close()

Alternatively, use the CLI:

.. code-block:: bash

   timedb create tables --with-users

This enables API key authentication and tenant isolation. See :ref:`Authentication <authentication>` for more details.

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

Reading with Tags and Annotations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Include tags and annotations as DataFrame columns:

.. code-block:: python

   df = td.read(
       series_id=series_id,
       tags_and_annotations=True
   )

The returned DataFrame includes additional columns:
- ``tags``: List of tags for each value
- ``annotation``: Annotation text for each value
- ``changed_by``: Email of the user who changed the value
- ``change_time``: When the value was last changed

Reading All Versions
~~~~~~~~~~~~~~~~~~~~~

By default, ``read()`` returns only the latest version of each value. To see all historical versions:

.. code-block:: python

   df = td.read(
       series_id=series_id,
       all_versions=True,
       return_value_id=True  # Include value_id to distinguish versions
   )

When using ``all_versions=True`` with ``return_value_id=True``, the DataFrame uses a MultiIndex ``(valid_time, value_id)`` to preserve multiple versions of the same ``valid_time``.

This is useful for:
- Auditing changes to time series values
- Viewing complete version history
- Analyzing how values evolved over time

Example: Reading version history with metadata:

.. code-block:: python

   df = td.read(
       series_id=series_id,
       all_versions=True,
       return_value_id=True,
       tags_and_annotations=True
   )
   
   # Each row represents a version with:
   # - valid_time: The time period this value applies to
   # - value_id: Unique identifier for this version
   # - value: The actual value
   # - tags: Tags associated with this version
   # - annotation: Annotation for this version
   # - changed_by: Who made this change
   # - change_time: When this change was made

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
       
       td.insert_run(
           df=df,
           run_start_time=known_time,  # known_time = when forecast was made
           workflow_id=f"forecast-run-{i}"
       )
   
   # Read all forecast revisions (overlapping mode)
   df_overlapping = td.read_values_overlapping(
       series_id=series_id,
       start_valid=base_time,
       end_valid=base_time + timedelta(days=5)
   )
   
   # Each forecast revision appears as a separate column or row
   # showing how predictions for the same future time evolve

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

Example: Batch updates with annotations and tags:

.. code-block:: python

   # Update multiple values
   updates = [
       {
           'run_id': run_id,
           'tenant_id': tenant_id,
           'valid_time': datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc),
           'series_id': series_id,
           'value': 150.0,
           'annotation': 'Manually corrected after sensor check',
           'tags': ['reviewed', 'corrected'],
           'changed_by': 'operator@example.com',
       },
       {
           'run_id': run_id,
           'tenant_id': tenant_id,
           'valid_time': datetime(2024, 1, 1, 13, 0, tzinfo=timezone.utc),
           'series_id': series_id,
           'value': 155.0,
           'tags': ['reviewed'],  # Add tag
           'changed_by': 'operator@example.com',
       },
       {
           'run_id': run_id,
           'tenant_id': tenant_id,
           'valid_time': datetime(2024, 1, 1, 14, 0, tzinfo=timezone.utc),
           'series_id': series_id,
           'annotation': None,  # Clear annotation
           'tags': [],  # Clear tags
       }
   ]
   
   result = td.update_records(updates)

Viewing Version History After Updates
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

After updating records, you can view the complete version history:

.. code-block:: python

   # Read all versions to see the history
   df_history = td.read(
       series_id=series_id,
       all_versions=True,
       return_value_id=True,
       tags_and_annotations=True
   )
   
   # Each update creates a new version while keeping old versions
   # You can see:
   # - Original value
   # - Updated value
   # - Who made the change (changed_by)
   # - When it was changed (change_time)
   # - Why it was changed (annotation)
   # - Quality flags (tags)

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

Starting the API Server
-----------------------

The SDK provides functions to start and check the TimeDB REST API server:

Starting the API Server (Blocking)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Start the API server in the current process (blocks until stopped):

.. code-block:: python

   td.start_api()

   # With custom host/port
   td.start_api(host="0.0.0.0", port=8080)
   
   # With auto-reload (development)
   td.start_api(reload=True)

The API will be available at ``http://<host>:<port>`` with interactive docs at ``/docs``.

Starting the API Server (Non-blocking)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For use in notebooks or when you need the API to run in the background:

.. code-block:: python

   # Start in background thread (non-blocking)
   started = td.start_api_background()
   
   if started:
       print("API server started")
   else:
       print("API server was already running")

The server runs in a separate thread and won't block your Python process.

Checking API Status
~~~~~~~~~~~~~~~~~~~~

Check if the API server is running and get information:

.. code-block:: python

   if td.check_api():
       # API is running - information was printed to console
       pass
   else:
       # API is not running - start it
       td.start_api_background()

The ``check_api()`` function prints API information including available endpoints.

Example: Complete API workflow in a notebook:

.. code-block:: python

   import timedb as td
   
   # Start API server in background
   if not td.check_api():
       td.start_api_background()
   
   # Now you can make API requests
   import requests
   response = requests.get("http://127.0.0.1:8000/values")
   data = response.json()

For detailed API usage, see :doc:`API Setup <api_setup>` and the API usage examples.

Authentication
--------------

TimeDB supports multi-tenant authentication using API keys. When the users table exists, authentication is required for all API endpoints.

Setting Up Authentication
~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Create schema with users table (see :ref:`Creating Schema with Users Table <creating-schema-with-users-table>`)

2. Create users with API keys:

.. code-block:: python

   from timedb import db
   import psycopg
   import uuid
   
   conn = psycopg.connect("postgresql://user:password@localhost:5432/timedb")
   
   tenant_id = uuid.uuid4()  # Or use an existing tenant ID
   
   # Create a user
   user_result = db.users.create_user(
       conn,
       tenant_id=tenant_id,
       email="user@example.com"
   )
   
   # user_result contains: api_key, user_id, email, tenant_id
   print(f"API Key: {user_result.api_key}")
   print(f"⚠️  Save this key - it's only shown once!")
   
   conn.commit()
   conn.close()

3. Use API keys in requests:

.. code-block:: python

   import requests
   
   api_key = "your-api-key-here"
   headers = {"X-API-Key": api_key}
   
   response = requests.get(
       "http://127.0.0.1:8000/values",
       headers=headers
   )

Tenant Isolation
~~~~~~~~~~~~~~~~

Each user's API key is tied to a ``tenant_id``. Users can only:
- Read data for their own tenant
- Write data for their own tenant
- Update records for their own tenant

Data from different tenants is completely isolated.

Managing Users
~~~~~~~~~~~~~~

List users:

.. code-block:: python

   users = db.users.list_users(conn, tenant_id=None)  # All users
   # or
   users = db.users.list_users(conn, tenant_id=tenant_id)  # Users for a tenant

Deactivate a user (revoke API access):

.. code-block:: python

   db.users.deactivate_user(conn, email="user@example.com")

Activate a user (restore API access):

.. code-block:: python

   db.users.activate_user(conn, email="user@example.com")

Regenerate API key:

.. code-block:: python

   new_key = db.users.regenerate_api_key(conn, email="user@example.com")
   print(f"New API Key: {new_key}")
   # ⚠️  Old key is immediately invalid

For more details, see the authentication example notebooks:
- ``examples/nb_08_authentication.ipynb`` - General authentication concepts
- ``examples/nb_08a_authentication_cli.ipynb`` - Using CLI for user management
- ``examples/nb_08b_authentication_sdk.ipynb`` - Using SDK for user management
