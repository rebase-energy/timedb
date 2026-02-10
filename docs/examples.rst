Examples
========

TimeDB includes interactive Jupyter notebooks that demonstrate key features and usage patterns.

Before running the examples locally, ensure you have:

1. **PostgreSQL Database**: A PostgreSQL database (version 12+) with TimescaleDB

2. **Environment Variables**: Set your database connection string:

   .. code-block:: bash

      export TIMEDB_DSN="postgresql://user:password@host:port/database"

3. **Jupyter**: Install Jupyter to run the notebooks interactively:

   .. code-block:: bash

      pip install jupyter

Available Notebooks
-------------------

.. toctree::
   :maxdepth: 1

   notebooks/quickstart
   notebooks/nb_01_write_read_pandas
   notebooks/nb_02_units_validation
   notebooks/nb_03_forecast_revisions
   notebooks/nb_04_timeseries_changes
   notebooks/nb_05_multiple_series
   notebooks/nb_06_api_usage

Notebook Descriptions
---------------------

**Quickstart**: Get up and running in 5 minutes with basic insert, read, and versioning operations.

**nb_01_write_read_pandas**: Demonstrates the fluent API with label-based filtering and broad vs. targeted operations on multiple series.

**nb_02_units_validation**: Shows how TimeDB handles unit conversion and validation with Pint.

**nb_03_forecast_revisions**: Deep dive into overlapping series (versioned forecasts) with multiple revisions and historical tracking.

**nb_04_timeseries_changes**: Demonstrates updating records in-place for flat series and tracking changes over time.

**nb_05_multiple_series**: Work with multiple unrelated series in a single operation.

**nb_06_api_usage**: Learn how to use the REST API endpoints for all database operations.
