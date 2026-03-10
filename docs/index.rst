.. timedb documentation master file

Welcome to TimeDB
=================

**TimeDB** is an open-source, opinionated time-series database built on top of PostgreSQL and TimescaleDB. 

It is designed to natively handle **overlapping forecast revisions**, **auditable human-in-the-loop updates**, and **"time-of-knowledge" history**. Using a three-dimensional temporal data model, it provides a seamless workflow through its Python SDK and FastAPI backend.


Why TimeDB?
-----------

Most time-series systems assume one immutable value per timestamp. TimeDB supports real-world workflows where data changes and those changes must be preserved.

- 📊 **Forecast Revisions**: store multiple versions for the same valid time
- 🔄 **Auditable Updates**: in-place updates for flat data, versioned updates for overlapping data
- ⏪ **True Backtesting**: query what was known at a specific point in time
- 🏷️ **Label-Based Organization**: filter series by semantic labels

Quick Start
-----------

.. code-block:: bash

   pip install timedb

.. code-block:: python

   import timedb as td
   import pandas as pd
   from datetime import datetime, timezone

   # Create schema
   td.create()

   # Create an overlapping forecast series
   td.create_series(
       name="wind_power",
       unit="MW",
       labels={"site": "offshore_1"},
       overlapping=True,
   )

   # Insert one forecast run
   knowledge_time = datetime(2025, 1, 1, 18, 0, tzinfo=timezone.utc)
   df = pd.DataFrame({
       "valid_time": pd.date_range("2025-01-01", periods=24, freq="h", tz="UTC"),
       "value": [100 + i * 2 for i in range(24)],
   })

   td.get_series("wind_power").where(site="offshore_1").insert(
       data=df,
       knowledge_time=knowledge_time,
   )

   # Read latest values
   df_latest = td.get_series("wind_power").where(site="offshore_1").read()

   # Read full revision history
   df_versions = td.get_series("wind_power").where(site="offshore_1").read(overlapping=True)

Release Notes
-------------

For version-by-version changes, migration notes, and feature updates, see:

- `Changelog <https://github.com/rebase-energy/timedb/blob/main/CHANGELOG.md>`_

Documentation
-------------

.. toctree::
   :maxdepth: 1
   :caption: Contents:

   installation
   sdk
   api_reference
   cli
   api_setup
   examples


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`