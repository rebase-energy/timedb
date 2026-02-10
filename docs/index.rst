.. timedb documentation master file

Welcome to timedb's documentation!
===================================

**timedb** is an opinionated schema and API built on top of PostgreSQL designed to handle overlapping time series revisions and auditable human-in-the-loop updates.

Most time series systems assume a single immutable value per timestamp. **timedb** is built for domains where data is revised, forecasted, reviewed, and corrected over time.

**timedb** lets you:

- ⏱️ Retain "time-of-knowledge" history through a three-dimensional time series data model
- ✍️ Make versioned ad-hoc updates to the time series data with annotations and tags
- 🔀 Represent both timestamp and time-interval time series simultaneously
- 🏷️ Organize data with labels for flexible filtering and discovery

Quick Start
-----------

.. code-block:: bash

   pip install timedb

.. code-block:: python

   from timedb import TimeDataClient
   import pandas as pd
   from datetime import datetime, timezone, timedelta

   # Create client and schema
   td = TimeDataClient()
   td.create()

   # Create a series with labels
   td.create_series(
       name='wind_power',
       unit='MW',
       labels={'site': 'offshore_1'},
       data_class='overlapping'  # Versioned forecasts
   )

   # Insert data using fluent API
   base_time = datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc)
   df = pd.DataFrame({
       'valid_time': [base_time + timedelta(hours=i) for i in range(24)],
       'wind_power': [100.0 + i * 2 for i in range(24)]
   })

   result = td.series('wind_power').where(site='offshore_1').insert(
       df=df,
       known_time=base_time
   )

   # Read latest values
   df_latest = td.series('wind_power').where(site='offshore_1').read()

   # Read all forecast revisions
   df_versions = td.series('wind_power').where(site='offshore_1').read(versions=True)

Documentation
-------------

.. toctree::
   :maxdepth: 1
   :caption: Contents:

   installation
   api_reference
   examples
   cli
   sdk
   api_setup

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`