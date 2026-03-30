Examples
========

TimeDB includes interactive Jupyter notebooks that demonstrate key features and usage patterns.

Before running the examples locally, ensure you have:

1. **Databases**: PostgreSQL (series metadata) and ClickHouse (time-series values). See :doc:`installation` for setup instructions.

2. **Environment Variables**: Set your database connection strings:

   .. code-block:: bash

      # Bash/Zsh
      export TIMEDB_PG_DSN='postgresql://user:password@host:port/database'
      export TIMEDB_CH_URL='http://default:@localhost:8123/default'

   .. code-block:: fish

      # Fish
      set -x TIMEDB_PG_DSN postgresql://user:password@host:port/database
      set -x TIMEDB_CH_URL http://default:@localhost:8123/default

3. **Jupyter**: Install Jupyter to run the notebooks interactively:

   .. code-block:: bash

      pip install jupyter

Available Notebooks
-------------------

.. toctree::
   :hidden:

   notebooks/quickstart
   notebooks/nb_01_single_series
   notebooks/nb_02_multi_series
   notebooks/nb_03_forecast_revisions
   notebooks/nb_04_reads
   notebooks/nb_05_corrections
   notebooks/nb_06_api

- :doc:`Quickstart <notebooks/quickstart>`
- :doc:`Single-Series Insert and Read <notebooks/nb_01_single_series>`
- :doc:`Multi-Series Insert with td.write() <notebooks/nb_02_multi_series>`
- :doc:`Forecast Revisions <notebooks/nb_03_forecast_revisions>`
- :doc:`Querying Time Series Data <notebooks/nb_04_reads>`
- :doc:`Forecast Corrections <notebooks/nb_05_corrections>`
- :doc:`REST API Usage <notebooks/nb_06_api>`

Notebook Descriptions
---------------------

**Quickstart**: Core concepts in 5 minutes — insert a forecast, revise it, and query the latest vs. full history.

**nb_01_single_series**: Single-series insert and read using polars and pandas DataFrames, with label-based routing via ``.where()``.

**nb_02_multi_series**: Multi-series bulk ingestion and reading — covers ``create_series_many()``, long-format DataFrames with ``td.write()``, manifest-based multi-series reads with ``td.read()``, per-row knowledge_time, run grouping, and unit conversion.

**nb_03_forecast_revisions**: Overlapping series with multiple forecast runs — insert revisions and compare ``read()`` (latest) vs. ``read(overlapping=True)`` (full history).

**nb_04_reads**: All query patterns: latest values, valid-time and knowledge-time range filters, full revision history, and ``read_relative()`` for day-ahead forecasts.

**nb_05_corrections**: Correcting erroneous forecast values the ClickHouse-native way — append a new run with a later ``knowledge_time``, then audit via ``read(overlapping=True)``.

**nb_06_api**: REST API usage — insert and read via JSON and Arrow IPC, single-series (``POST /values``, ``GET /values``), multi-series write (``POST /write``) and read (``POST /read``), and bulk series creation.
