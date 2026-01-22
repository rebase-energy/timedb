.. timedb documentation master file

Welcome to timedb's documentation!
===================================

**timedb** is an opinionated schema and API built on top of PostgreSQL designed to handle overlapping time series revisions and auditable human-in-the-loop updates.

Most time series systems assume a single immutable value per timestamp. **timedb** is built for domains where data is revised, forecasted, reviewed, and corrected over time.

**timedb** lets you:

- ⏱️ Retain "time-of-knowledge" history through a three-dimensional time series data model
- ✍️ Make versioned ad-hoc updates to the time series data with annotations and tags
- 🔀 Represent both timestamp and time-interval time series simultaneously

Quick Start
-----------

.. code-block:: bash

   pip install timedb

.. code-block:: python

   import timedb as td
   import pandas as pd
   from datetime import datetime, timezone, timedelta

   # Create database schema
   td.create()

   # Create time series data
   base_time = datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc)
   df = pd.DataFrame({
       'valid_time': [base_time + timedelta(hours=i) for i in range(24)],
       'value': [20.0 + i * 0.3 for i in range(24)]
   })

   # Insert and read back
   result = td.insert_run(df=df)
   df_read = td.read()

Documentation
-------------

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   installation
   cli
   sdk
   api_setup
   examples

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

