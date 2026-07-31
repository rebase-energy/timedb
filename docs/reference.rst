Reference
=========

Public API
----------

.. autoclass:: timedb.TimeDBClient
   :members:
   :special-members: __init__
   :show-inheritance:

.. autodata:: timedb.RETENTION_TIERS

.. autoclass:: timedb.WriteResult
   :members:
   :show-inheritance:

.. autodata:: timedb.UnchangedScope

   The comparison key for ``write(skip_unchanged=True)``: ``"valid_time"``,
   ``"knowledge_time"``, or ``"auto"`` (per-series, driven by
   ``knowledge_time_scoped_series``).

.. autoclass:: timedb.PgEngineMeta
   :members:
   :show-inheritance:

Profiling helpers
-----------------

A lightweight phase-timer used by the read/write paths. Useful when
diagnosing slow queries or large bulk inserts.

.. automodule:: timedb.profiling
   :members:
