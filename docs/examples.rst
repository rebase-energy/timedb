Examples
========

TimeDB includes interactive Jupyter notebooks that demonstrate key features and usage patterns.

Before running the examples locally, ensure you have:

1. **PostgreSQL Database**: A PostgreSQL database (version 12+) with TimescaleDB

2. **Environment Variables**: Set your database connection string:

   .. code-block:: bash

      # Bash/Zsh
      export TIMEDB_DSN='postgresql://user:password@host:port/database'

   .. code-block:: fish

      # Fish
      set -x TIMEDB_DSN postgresql://user:password@host:port/database

3. **Jupyter**: Install Jupyter to run the notebooks interactively:

   .. code-block:: bash

      pip install jupyter

Available Notebooks
-------------------

.. toctree::
   :hidden:

   notebooks/quickstart
   notebooks/nb_01_write_read
   notebooks/nb_02_units_validation
   notebooks/nb_03_forecast_revisions
   notebooks/nb_04_relative_forecasts
   notebooks/nb_05_timeseries_changes
   notebooks/nb_06_api_usage

- :doc:`Quickstart <notebooks/quickstart>`
- :doc:`Writing and Reading <notebooks/nb_01_write_read>`
- :doc:`Units Validation <notebooks/nb_02_units_validation>`
- :doc:`Forecast Revisions <notebooks/nb_03_forecast_revisions>`
- :doc:`Relative Forecasts <notebooks/nb_04_relative_forecasts>`
- :doc:`Time Series Changes <notebooks/nb_05_timeseries_changes>`
- :doc:`REST API Usage <notebooks/nb_06_api_usage>`

Notebook Descriptions
---------------------

**Quickstart**: Get up and running in 5 minutes with basic insert, read, and versioning operations.

**nb_01_write_read**: Demonstrates the fluent API with label-based filtering, multiple insert types (DataFrame, TimeSeries, MultivariateTimeSeries), and broad vs. targeted operations.

**nb_02_units_validation**: Shows how TimeDB handles unit conversion and validation with Pint.

**nb_03_forecast_revisions**: Deep dive into overlapping series (versioned forecasts) with multiple revisions and historical tracking.

**nb_04_relative_forecasts**: Per-window knowledge_time cutoffs using ``read_relative()`` — day-ahead and shifted forecast retrieval.

**nb_05_timeseries_changes**: Demonstrates updating records and tracking changes over time for flat and overlapping series.

**nb_06_api_usage**: Examples of using the REST API for reading and writing time series data.
