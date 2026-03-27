Reference
=========

timedb provides three complementary interfaces for time series data management:

- **SDK**: High-level Python API for programmatic access
- **REST API**: HTTP endpoints for remote access and integration
- **CLI**: Command-line tools for administrative operations

SDK Reference
-------------

The SDK provides a fluent, Pythonic interface for working with time series data.

Module-Level Functions
~~~~~~~~~~~~~~~~~~~~~~

These convenience functions use a lazy default client and are the recommended
entry point for most use cases (``import timedb as td``):

.. autofunction:: timedb.create
.. autofunction:: timedb.delete
.. autofunction:: timedb.create_series
.. autofunction:: timedb.create_series_many
.. autofunction:: timedb.get_series

Multi-Series Write
''''''''''''''''''

.. autofunction:: timedb.write

Main Client
~~~~~~~~~~~

For custom connection settings, use ``TimeDataClient`` directly:

.. autoclass:: timedb.TimeDataClient
   :members:
   :special-members: __init__
   :show-inheritance:

Series Operations
~~~~~~~~~~~~~~~~~

.. autoclass:: timedb.SeriesCollection
   :members:
   :show-inheritance:

Data Classes
~~~~~~~~~~~~

.. autoclass:: timedb.InsertResult
   :members:

Exceptions
~~~~~~~~~~

.. autoexception:: timedb.IncompatibleUnitError
   :members:
   :no-index:

REST API Reference
------------------

The REST API provides HTTP endpoints for reading and writing time series data.
It mirrors the SDK's workflow: create series first, then insert/read data
using name+labels or series_id to identify series.

All data endpoints support two serialization formats:

- **JSON** (default): Standard JSON request/response bodies
- **Arrow IPC stream**: High-performance binary format for bulk data transfer

Interactive Documentation
~~~~~~~~~~~~~~~~~~~~~~~~~~

When the API server is running, comprehensive interactive documentation is available:

- **Swagger UI**: ``http://<host>:<port>/docs`` - Full interactive API explorer
- **ReDoc**: ``http://<host>:<port>/redoc`` - Alternative documentation format

These provide live request/response examples, parameter descriptions, and test capabilities.

Key Endpoints
~~~~~~~~~~~~~

.. py:function:: POST /series

   Create a new time series (get-or-create — safe to call repeatedly).

   **Request body:**

   .. code-block:: json

      {
        "name": "wind_power",
        "unit": "MW",
        "labels": {"site": "Gotland", "turbine": "T01"},
        "overlapping": true,
        "retention": "medium"
      }

   **Fields:** ``name`` (str, required) · ``unit`` (str, default ``"dimensionless"``) ·
   ``labels`` (dict, default ``{}``) · ``overlapping`` (bool, default ``false``) ·
   ``retention`` (str, default ``"medium"``: ``"short"``, ``"medium"``, or ``"long"``) ·
   ``description`` (str, optional)

   **Response:**

   .. code-block:: json

      {"series_id": 1, "message": "Series created successfully"}

.. py:function:: POST /series/many

   Batch get-or-create multiple series in a single round-trip. Returns ``series_ids``
   in the same order as the input list.

   **Request body:**

   .. code-block:: json

      {
        "series": [
          {"name": "wind_power", "unit": "MW", "labels": {"turbine": "T01"}, "overlapping": true},
          {"name": "wind_power", "unit": "MW", "labels": {"turbine": "T02"}, "overlapping": true},
          {"name": "wind_power", "unit": "MW", "labels": {"turbine": "T03"}, "overlapping": true}
        ]
      }

   Each entry in ``series`` accepts the same fields as ``POST /series``.

   **Response:**

   .. code-block:: json

      {"series_ids": [1, 2, 3]}

.. py:function:: POST /values

   Insert time series data for a single series identified by ``name``+``labels`` or
   ``series_id``. The series must already exist. Flat series are inserted directly
   (``batch_id`` is null); overlapping series create a versioned batch.

   **JSON** (``Content-Type: application/json``):

   .. code-block:: json

      {
        "name": "wind_power",
        "labels": {"site": "Gotland"},
        "knowledge_time": "2025-01-01T06:00:00Z",
        "data": [
          {"valid_time": "2025-01-01T00:00:00Z", "value": 52.0},
          {"valid_time": "2025-01-01T01:00:00Z", "value": 52.4}
        ]
      }

   **Fields:** ``name`` · ``labels`` · ``series_id`` (alternative to name+labels) ·
   ``knowledge_time`` (defaults to now) · ``workflow_id`` (default ``"api-workflow"``) ·
   ``batch_params`` (dict, optional) · ``data`` (list of ``{valid_time, value, valid_time_end?}``)

   **Response:**

   .. code-block:: json

      {"batch_id": "019d29d5-0885-7068-ab2d-7758a9b41723", "series_id": 1, "rows_inserted": 24}

   ``batch_id`` is ``null`` for flat (non-overlapping) series.

   **Arrow IPC stream** (``Content-Type: application/vnd.apache.arrow.stream``):

   Send a Polars/PyArrow IPC stream as the request body. Required columns:
   ``valid_time`` (``timestamp[us, UTC]``), ``value`` (``float64``).
   Optional columns: ``valid_time_end``, ``knowledge_time``.
   Series identity and batch metadata are passed as query parameters (same names as JSON fields).

   .. code-block:: python

      buf = io.BytesIO()
      df.write_ipc_stream(buf)
      requests.post(
          "http://localhost:8000/values",
          params={"name": "wind_power", "labels": '{"site":"Gotland"}'},
          data=buf.getvalue(),
          headers={"Content-Type": "application/vnd.apache.arrow.stream"},
      )

.. py:function:: POST /write

   Insert multi-series data in long/tidy format. All target series must exist.
   Series routing is controlled by query parameters.

   **Query parameters:**

   - ``name_col`` (str, default ``"name"``): column whose values are series names
   - ``label_cols`` (str): comma-separated label column names, e.g. ``"site,turbine"``
   - ``knowledge_time`` (datetime, optional): broadcast knowledge_time for all rows
   - ``unit`` (str, optional): incoming unit — auto-converted to each series' canonical unit
   - ``batch_cols`` (str, optional): comma-separated columns that define separate batches
   - ``workflow_id``, ``batch_start_time``, ``batch_finish_time``, ``batch_params`` (optional)

   **JSON** (``Content-Type: application/json``):

   .. code-block:: json

      [
        {"name": "wind_power", "site": "Gotland", "valid_time": "2025-01-01T00:00:00Z", "value": 100.0},
        {"name": "wind_power", "site": "Aland",   "valid_time": "2025-01-01T00:00:00Z", "value": 95.0}
      ]

   **Arrow IPC stream** (``Content-Type: application/vnd.apache.arrow.stream``):

   Send a table with ``valid_time``, ``value``, and routing columns as a Polars/PyArrow IPC stream.

   **Response:** list of insert results, one per unique ``(series_id, batch_id)``:

   .. code-block:: json

      [
        {"batch_id": "019d29d5-...", "series_id": 1, "workflow_id": "api-workflow"},
        {"batch_id": "019d29d5-...", "series_id": 2, "workflow_id": "api-workflow"}
      ]

.. py:function:: GET /values

   Read time series values.

   **Query parameters:**

   - ``name`` · ``labels`` (JSON string, e.g. ``{"site":"Gotland"}``) · ``series_id`` — series filter
   - ``start_valid`` · ``end_valid`` — valid time range (ISO datetime, both inclusive)
   - ``start_known`` · ``end_known`` — knowledge_time range (overlapping series only)
   - ``overlapping`` (bool, default ``false``) — if ``true``, return one row per ``(knowledge_time, valid_time)``

   **JSON response** (default):

   .. code-block:: json

      {
        "count": 24,
        "data": [
          {"valid_time": "2025-01-01T00:00:00+0000", "value": 52.0},
          {"valid_time": "2025-01-01T01:00:00+0000", "value": 52.4}
        ]
      }

   With ``overlapping=true``, each record also has a ``"knowledge_time"`` field.

   **Arrow IPC stream** (``Accept: application/vnd.apache.arrow.stream``):

   .. code-block:: python

      response = requests.get(url, params=params,
                              headers={"Accept": "application/vnd.apache.arrow.stream"})
      df = pl.read_ipc_stream(response.content)

.. py:function:: GET /series

   List time series, optionally filtered.

   **Query parameters:** ``name`` · ``labels`` (JSON string) · ``unit`` · ``series_id``

   **Response:** list of series objects:

   .. code-block:: json

      [
        {
          "series_id": 1,
          "name": "wind_power",
          "unit": "MW",
          "labels": {"site": "Gotland"},
          "overlapping": true,
          "retention": "medium"
        }
      ]

.. py:function:: GET /series/labels

   List unique values for a label key.

   **Query parameters:** ``label_key`` (required) · ``name`` · ``labels``

   **Response:** ``{"label_key": "site", "values": ["Gotland", "Aland"]}``

.. py:function:: GET /series/count

   Count series matching filters.

   **Query parameters:** ``name`` · ``labels`` · ``unit``

   **Response:** ``{"count": 5}``

Pydantic Models
~~~~~~~~~~~~~~~

Request/Response models used by the REST API:

.. autoclass:: timedb.api.DataPoint
   :members:

.. autoclass:: timedb.api.InsertRequest
   :members:

.. autoclass:: timedb.api.InsertResponse
   :members:

.. autoclass:: timedb.api.CreateSeriesRequest
   :members:

.. autoclass:: timedb.api.CreateSeriesResponse
   :members:

.. autoclass:: timedb.api.CreateSeriesManyRequest
   :members:

.. autoclass:: timedb.api.CreateSeriesManyResponse
   :members:

.. autoclass:: timedb.api.SeriesInfo
   :members:

CLI Reference
-------------

The CLI provides command-line tools for administrative operations and server management.

API Server
~~~~~~~~~~

The ``timedb api`` command starts the REST API server:

.. code-block:: bash

   timedb api [OPTIONS]

**Options:**

- ``--host TEXT`` (default: 127.0.0.1): Host to bind API server to
- ``--port INTEGER`` (default: 8000): Port to bind API server to
- ``--reload``: Enable auto-reload for development (watch file changes)

**Examples:**

.. code-block:: bash

   # Start server on localhost
   timedb api

   # Start on all interfaces on port 9000
   timedb api --host 0.0.0.0 --port 9000

   # Start with auto-reload for development
   timedb api --reload

For additional CLI commands, see :doc:`cli`.

Units and Validation
--------------------

Unit conversion is integrated into both ingestion paths. Pass ``unit=`` to ``insert()``
or the ``write*`` methods to declare the incoming unit; TimeDB uses pint to compute a
scalar conversion factor to the series' canonical unit before writing.
:class:`~timedb.IncompatibleUnitError` is raised before any data reaches the database
when units are dimensionally incompatible (e.g., ``"kg"`` into an ``"MW"`` series).

Note: pint-pandas array dtype support was removed. Only scalar ``unit=`` kwarg conversion
is supported. ``TimeSeries`` objects carry their own unit and are converted automatically.

See the :doc:`SDK documentation <sdk>` for full details.
