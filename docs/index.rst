.. timedb documentation master file

Welcome to TimeDB
=================

**TimeDB** is a minimal, stateless Python client for storing bitemporal
time-series data in `ClickHouse <https://clickhouse.com>`_.

It is designed for workflows where forecasts get revised, observations
get corrected, and historical backtesting requires querying *what was
known* at any point in time. Series identity (``series_id``) and any
catalog/metadata are the caller's responsibility — TimeDB just stores
and retrieves rows. Higher-level libraries like
`EnergyDB <https://github.com/rebase-energy/energydb>`_ build catalogs,
unit handling, and asset hierarchies on top.


The 3D temporal model
---------------------

Every row carries three orthogonal timestamps:

- ``valid_time`` — when the value applies (e.g. *forecast for Wed 12:00*)
- ``knowledge_time`` — when the value became known (*generated Mon 18:00*)
- ``change_time`` — when the row was written or corrected (*overridden Tue 09:00*)

Together they let TimeDB store overlapping forecast revisions, expose a
full audit trail of corrections, and answer "what did we know at time T?"
queries directly.


Quick Start
-----------

.. code-block:: bash

   pip install timedb

.. code-block:: python

   import polars as pl
   from datetime import datetime, timezone
   from timedb import TimeDBClient

   td = TimeDBClient()  # reads TIMEDB_CH_URL
   td.create()          # creates series_values + run_series tables

   # One forecast run for series_id=42, issued at 06:00.
   kt = datetime(2025, 1, 1, 6, tzinfo=timezone.utc)
   df = pl.DataFrame({
       "series_id":  [42] * 24,
       "valid_time": [datetime(2025, 1, 1, h, tzinfo=timezone.utc) for h in range(24)],
       "value":      [100.0 + i * 2 for i in range(24)],
   })
   td.write(df, retention="medium", knowledge_time=kt)

   # Latest value per valid_time (collapses overlapping runs).
   latest = td.read(series_ids=[42])

   # Full bitemporal history — every forecast run side-by-side.
   history = td.read(series_ids=[42], include_knowledge_time=True)


Documentation
-------------

.. toctree::
   :maxdepth: 1
   :caption: Contents:

   installation
   sdk
   reference
   examples
   development


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
