Examples
========

TimeDB includes interactive Jupyter notebooks that demonstrate key features and usage patterns.

Before running the examples locally, ensure you have:

1. **PostgreSQL Database**: A PostgreSQL database (version 12+)

2. **Environment Variables**: Set your database connection string:

   .. code-block:: bash

      export TIMEDB_DSN="postgresql://user:password@host:port/database"

3. **Jupyter**: Install Jupyter to run the notebooks interactively:

   .. code-block:: bash

      pip install jupyter

.. toctree::
   :maxdepth: 1

   notebooks/quickstart
   notebooks/nb_01_write_read_pandas
   notebooks/nb_02_units_validation
   notebooks/nb_03_forecast_revisions
   notebooks/nb_04_timeseries_changes
   notebooks/nb_07_api_usage
