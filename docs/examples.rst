Examples
========

TimeDB includes interactive Jupyter notebooks that demonstrate key features and usage patterns.

Prerequisites
-------------

Before running the examples locally, ensure you have:

1. **PostgreSQL Database**: A PostgreSQL database (version 12+)

2. **Environment Variables**: Set your database connection string:

   .. code-block:: bash

      export TIMEDB_DSN="postgresql://user:password@host:port/database"

3. **Jupyter**: Install Jupyter to run the notebooks interactively:

   .. code-block:: bash

      pip install jupyter

Getting Started
---------------

.. toctree::
   :maxdepth: 1

   notebooks/nb_01_write_read_pandas
   notebooks/nb_02_units_validation

Core Features
-------------

.. toctree::
   :maxdepth: 1

   notebooks/nb_03_forecast_revisions
   notebooks/nb_04_timeseries_changes
   notebooks/nb_05_multiple_series

Advanced Topics
---------------

.. toctree::
   :maxdepth: 1

   notebooks/nb_06_advanced_querying
   notebooks/nb_07_api_usage
   notebooks/nb_08_authentication
   notebooks/nb_08a_authentication_cli
   notebooks/nb_08b_authentication_sdk

Workflow Examples
-----------------

The ``examples/workflows/`` directory contains real-world workflow examples that demonstrate how to use TimeDB with external data sources:

**Fingrid Wind Forecast** (``workflow_fingrid_wind_forecast.py``)

Fetches wind power forecast data from Fingrid's API and stores it in TimeDB:

- Scheduled data ingestion using Modal
- Working with external APIs
- Storing forecast data with proper ``known_time``

**Nord Pool Intraday** (``workflow_nordpool_id.py``)

Fetches intraday market data from Nord Pool and stores it with metadata:

- Working with interval data (``valid_time`` to ``valid_time_end``)
- Storing associated metadata
- Handling complex data structures

These workflows require additional setup (API keys, Modal configuration) but serve as templates for building your own data pipelines.
