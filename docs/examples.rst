Examples
========

TimeDB ships a single Jupyter notebook demonstrating the core workflow:
write a forecast run, revise it, query latest values, query full
history, and append a correction.

Before running the notebook locally, make sure ClickHouse is reachable
and ``TIMEDB_CH_URL`` points at it (see :doc:`installation`).

.. toctree::
   :hidden:

   notebooks/quickstart

- :doc:`Quickstart <notebooks/quickstart>`

Quickstart
----------

Walks through ``TimeDBClient.create()``, two forecast runs, latest vs.
bitemporal reads, and a forecast correction. The notebook at
``examples/quickstart.ipynb`` is the source of truth and is auto-copied
into ``docs/notebooks/`` at build time.
