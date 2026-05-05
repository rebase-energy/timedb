SDK Usage
=========

The timedb SDK is a single class — :class:`~timedb.TimeDBClient` — that
talks to ClickHouse via HTTP. It owns no metadata, no catalog, no
fluent series API: callers identify series by integer ``series_id`` and
manage any naming / labeling / unit handling themselves.


Overview
--------

TimeDB stores rows in two tables:

- ``series_values`` — append-only time-series store. Sort key
  ``(series_id, valid_time, knowledge_time, change_time)`` covers both
  flat and bitemporal reads. Partitioned by ``(retention, month)`` so
  TTLs drop whole partitions.
- ``run_series`` — tiny ``(series_id → run_id)`` mapping. Lets
  "which runs touched this series" lookups skip the data table.

A single row carries:

- ``series_id`` — externally assigned integer identity
- ``valid_time`` — when the value applies
- ``knowledge_time`` — when the value became known (forecast issue time)
- ``change_time`` — when the row was written
- ``value`` — float64
- ``valid_time_end`` — interval end (default ``2200-01-01``, treat as +∞)
- ``run_id`` — a UUID7 truncated to 63 bits (one per write batch unless overridden)
- ``changed_by`` / ``annotation`` — optional audit text
- ``retention`` — TTL tier: ``short`` / ``medium`` / ``long`` / ``forever``


Connecting
----------

.. code-block:: python

   from timedb import TimeDBClient

   td = TimeDBClient()                                  # reads TIMEDB_CH_URL
   td = TimeDBClient(ch_url="http://localhost:8123/")   # explicit


Schema management
-----------------

Both calls are idempotent.

.. code-block:: python

   td.create()   # CREATE TABLE IF NOT EXISTS for series_values + run_series
   td.delete()   # DROP TABLE IF EXISTS for both — destroys all data


Writing data
------------

``write()`` accepts a Pandas or Polars DataFrame. Required columns are
``series_id``, ``valid_time``, ``value``. Everything else is optional;
missing columns get safe defaults stamped per batch.

.. code-block:: python

   import polars as pl
   from datetime import datetime, timezone

   kt = datetime(2025, 1, 1, 6, tzinfo=timezone.utc)
   df = pl.DataFrame({
       "series_id":  [42] * 24,
       "valid_time": [datetime(2025, 1, 1, h, tzinfo=timezone.utc) for h in range(24)],
       "value":      [100.0 + i * 2 for i in range(24)],
   })
   td.write(df, retention="medium", knowledge_time=kt)

Optional column / kwarg defaults:

- ``knowledge_time`` — kwarg, else ``datetime.now(UTC)`` for the batch.
  Cannot be passed as both kwarg *and* column simultaneously.
- ``change_time`` — column or ``datetime.now(UTC)`` for the batch.
- ``run_id`` — column or one client-generated UUID7 (top 63 bits) per batch.
  Time-sortable, and round-trips through ``Int64`` / ``UInt64`` cleanly.
- ``valid_time_end`` — column or sentinel ``2200-01-01``.
- ``changed_by`` / ``annotation`` — column or ``""``.
- ``retention`` — kwarg, column, or default ``"forever"`` (no TTL).
  Also cannot be passed as both kwarg and column.

All timestamp columns must be timezone-aware (``polars.Datetime`` with a
non-null ``time_zone``). Naive timestamps raise ``ValueError``.

Each ``write()`` call also writes one ``(series_id, run_id)`` row per
unique pair into ``run_series`` so that downstream metadata layers can
reverse-lookup runs without scanning ``series_values``.

Forecast revisions are just additional rows with a later
``knowledge_time``:

.. code-block:: python

   kt2 = datetime(2025, 1, 2, 6, tzinfo=timezone.utc)
   df2 = pl.DataFrame({
       "series_id":  [42] * 24,
       "valid_time": [datetime(2025, 1, 1, h, tzinfo=timezone.utc) for h in range(24)],
       "value":      [105.0 + i * 2 for i in range(24)],
   })
   td.write(df2, retention="medium", knowledge_time=kt2)

Corrections to a *specific* forecast run use the same pattern: pass an
older ``knowledge_time`` (the run being corrected) with a fresh
``change_time`` and the corrected ``value``. The reads documented below
collapse correction chains automatically.


Retention tiers
---------------

The full set of valid retention values is exported as
:data:`~timedb.RETENTION_TIERS`:

.. code-block:: python

   from timedb import RETENTION_TIERS
   # frozenset({"short", "medium", "long", "forever"})

Mapping to actual TTL (defined inline in the DDL):

- ``short``   → 180 days
- ``medium``  → 1095 days (~3 years)
- ``long``    → 1825 days (~5 years)
- ``forever`` → no TTL (the default)

TTL evaluation runs against ``valid_time`` and is partition-aligned, so
expirations drop whole partitions rather than walking rows.


Reading data
------------

``read()`` returns a Polars DataFrame. The shape depends on two boolean
flags:

.. list-table::
   :header-rows: 1
   :widths: 25 25 50

   * - ``include_knowledge_time``
     - ``include_updates``
     - Returned columns
   * - ``False`` (default)
     - ``False`` (default)
     - ``series_id, valid_time, value``
   * - ``False``
     - ``True``
     - ``series_id, valid_time, change_time, value, changed_by, annotation``
   * - ``True``
     - ``False``
     - ``series_id, knowledge_time, valid_time, value``
   * - ``True``
     - ``True``
     - ``series_id, valid_time, knowledge_time, change_time, value, changed_by, annotation``

Each combination answers a different question:

- *Default* — latest value per ``valid_time``, picking the row with the
  largest ``(knowledge_time, change_time)`` tuple.
- ``include_knowledge_time=True`` — every forecast run for each
  ``valid_time``, side-by-side. Within a single run, the latest correction
  wins.
- ``include_updates=True`` — full correction chain on the
  currently-winning forecast run, with ``changed_by`` / ``annotation``
  per state transition.
- Both — full bitemporal audit log: every state transition for every
  forecast run.

.. code-block:: python

   # Latest values
   latest = td.read(series_ids=[42])

   # Forecast history (one row per (knowledge_time, valid_time))
   history = td.read(series_ids=[42], include_knowledge_time=True)

   # Time range filters (UTC datetimes)
   from datetime import datetime, timezone
   window = td.read(
       series_ids=[42, 43],
       start_valid=datetime(2025, 1, 1, tzinfo=timezone.utc),
       end_valid=datetime(2025, 2, 1, tzinfo=timezone.utc),
   )

   # Filter by knowledge_time too
   recent_forecasts = td.read(
       series_ids=[42],
       start_known=datetime(2024, 12, 1, tzinfo=timezone.utc),
       end_known=datetime(2025, 1, 15, tzinfo=timezone.utc),
       include_knowledge_time=True,
   )

   # Restrict to one or more retention tiers
   tier = td.read(series_ids=[42], retention="medium")
   tiers = td.read(series_ids=[42], retention=["medium", "long"])

Filtering by ``retention`` is a partition prune at the storage layer —
reads restricted to one tier never touch the others.


Per-window cutoffs (``read_relative``)
--------------------------------------

For backtesting and day-ahead simulation, ``read_relative()`` returns —
for each window — the latest forecast issued at or before a per-window
cutoff. This simulates "what forecast was available at decision time".

**Low-level mode** — explicit window length and offset:

.. code-block:: python

   from datetime import datetime, timedelta, timezone

   df = td.read_relative(
       series_ids=[42],
       window_length=timedelta(hours=24),
       issue_offset=timedelta(hours=-12),  # 12h before each window start
       start_window=datetime(2026, 2, 1, tzinfo=timezone.utc),
       start_valid=datetime(2026, 2, 1, tzinfo=timezone.utc),
       end_valid=datetime(2026, 3, 1, tzinfo=timezone.utc),
   )

**Daily shorthand** — fixed 1-day windows with a human-friendly cutoff
(mirrors `Energy Quantified's instances.relative()
<https://energyquantified-python.readthedocs.io/en/latest/reference/reference.html#energyquantified.api.InstancesAPI.relative>`_):

.. code-block:: python

   from datetime import time

   df = td.read_relative(
       series_ids=[42],
       days_ahead=1,                # day-ahead
       time_of_day=time(6, 0),      # by 06:00 on the issue day
       start_valid=datetime(2026, 2, 1, tzinfo=timezone.utc),
       end_valid=datetime(2026, 2, 28, tzinfo=timezone.utc),
   )

The two parameter sets are mutually exclusive — mixing them raises
``ValueError``. Returns ``(series_id, valid_time, value)`` — one row
per cutoff-winning forecast.


Run lookups
-----------

Each ``write()`` call generates a single client-side ``run_id`` (a UUID7
truncated to 63 bits) unless ``run_id`` is supplied as a column on the
DataFrame. ``run_series`` indexes the (series, run) pairs:

.. code-block:: python

   # Newest run first.
   run_ids = td.read_run_series(series_id=42)


Best practices
--------------

1. **Always use timezone-aware UTC datetimes.** Naive timestamps raise.

   .. code-block:: python

      from datetime import datetime, timezone
      good = datetime(2025, 1, 1, 12, tzinfo=timezone.utc)

2. **Use** ``knowledge_time`` **for forecast revisions.** The same
   ``(series_id, valid_time)`` can have many ``knowledge_time`` values;
   each represents a distinct forecast run.

3. **Append corrections, don't UPDATE.** A correction is a new row with
   the same ``(series_id, valid_time, knowledge_time)`` tuple, a fresh
   ``change_time``, and the new ``value``. Reads pick the latest
   ``change_time`` automatically.

4. **Pick a retention tier per series, not per write.** The DDL
   partitions on ``retention``; mixing tiers within a single series
   defeats the partition pruning.

5. **Hold series metadata externally.** TimeDB doesn't track series
   names, units, or labels. Use ``energydb`` (or your own catalog table)
   to keep that mapping.


Error handling
--------------

.. code-block:: python

   try:
       td.write(df, retention="bogus")
   except ValueError as e:
       print(e)  # "Unknown retention 'bogus'. Valid values: [...]"

The most common ``ValueError``\ s come from:

- An unknown ``retention`` value (kwarg or column).
- Naive timestamps in any time column.
- Missing required columns (``series_id``, ``valid_time``, ``value``).
- Both kwarg and column supplied for ``retention`` or ``knowledge_time``.

Connection errors (ClickHouse unreachable, auth failure) propagate
directly from ``clickhouse-connect``.
