Reference
=========

Public API
----------

.. autoclass:: timedb.TimeDBClient
   :members:
   :special-members: __init__
   :show-inheritance:

.. autodata:: timedb.RETENTION_TIERS

Profiling helpers
-----------------

A lightweight phase-timer used by the read/write paths. Useful when
diagnosing slow queries or large bulk inserts.

.. automodule:: timedb.profiling
   :members:
