API Reference
=============

timedb provides three complementary interfaces for time series data management:

- **SDK**: High-level Python API for programmatic access
- **REST API**: HTTP endpoints for remote access and integration
- **CLI**: Command-line tools for administrative operations

SDK Reference
-------------

The SDK provides a fluent, Pythonic interface for working with time series data.

Main Client
~~~~~~~~~~~

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

.. autoexception:: timedb.units.IncompatibleUnitError
   :members:
   :no-index:

REST API Reference
------------------

The REST API provides HTTP endpoints for reading and writing time series data.
It mirrors the SDK's workflow: create series first, then insert/read/update data
using name+labels or series_id to identify series.

Interactive Documentation
~~~~~~~~~~~~~~~~~~~~~~~~~~

When the API server is running, comprehensive interactive documentation is available:

- **Swagger UI**: ``http://<host>:<port>/docs`` - Full interactive API explorer
- **ReDoc**: ``http://<host>:<port>/redoc`` - Alternative documentation format

These provide live request/response examples, parameter descriptions, and test capabilities.

Key Endpoints
~~~~~~~~~~~~~

.. py:function:: POST /series

   Create a new time series.

   **Request Body:**

   - ``name`` (str): Human-readable identifier
   - ``description`` (str, optional): Series description
   - ``unit`` (str, default="dimensionless"): Canonical unit
   - ``labels`` (dict, default={}): Key-value labels for differentiation
   - ``overlapping`` (bool, default=false): true for versioned forecasts
   - ``retention`` (str, default="medium"): "short", "medium", or "long"

   **Returns:** CreateSeriesResponse with series_id

.. py:function:: POST /values

   Insert time series data. Specify the target series by name+labels or series_id.
   The series must already exist (use POST /series first).

   Routing is automatic: flat series are inserted directly (no batch),
   overlapping series create a batch with known_time tracking.

   **Request Body:**

   - ``name`` (str, optional): Series name (used with labels to resolve series)
   - ``labels`` (dict, optional): Labels for resolution
   - ``series_id`` (int, optional): Direct series_id (alternative to name+labels)
   - ``workflow_id`` (str, default="api-workflow"): Workflow identifier
   - ``known_time`` (datetime, optional): Time of knowledge (defaults to now)
   - ``batch_params`` (dict, optional): Custom batch parameters
   - ``data`` (list): Array of data points ``[{valid_time, value, valid_time_end?}]``

   **Returns:** InsertResponse with batch_id (null for flat), series_id, rows_inserted

.. py:function:: GET /values

   Read time series values with filtering.

   **Query Parameters:**

   - ``name`` (str, optional): Filter by series name
   - ``labels`` (str, optional): Filter by labels (JSON string, e.g. ``{"site":"Gotland"}``)
   - ``series_id`` (int, optional): Filter by series_id
   - ``start_valid`` (datetime, optional): Start of valid time range (ISO format)
   - ``end_valid`` (datetime, optional): End of valid time range (ISO format)
   - ``start_known`` (datetime, optional): Start of known_time range (ISO format)
   - ``end_known`` (datetime, optional): End of known_time range (ISO format)
   - ``versions`` (bool, default=false): Return all forecast revisions

   **Returns:** JSON object with count and data array

.. py:function:: PUT /values

   Update existing time series records. Creates new versions for overlapping series.

   Identify the series by ``series_id`` OR by ``name`` (+``labels``).

   For overlapping series, three lookup methods (all optional):

   - ``batch_id`` + ``valid_time``: target specific batch
   - ``known_time`` + ``valid_time``: target specific version
   - Just ``valid_time``: target latest version overall

   **Request Body** (``updates`` is a list of objects with these fields):

   - ``valid_time`` (datetime, required): The timestamp to update
   - ``series_id`` (int, optional): Series ID (alternative to name+labels)
   - ``name`` (str, optional): Series name (alternative to series_id)
   - ``labels`` (dict, default={}): Labels for series resolution
   - ``batch_id`` (int, optional): Target specific batch (overlapping only)
   - ``known_time`` (datetime, optional): Target specific version (overlapping only)
   - ``value`` (float, optional): New value (omit to leave unchanged, null to clear)
   - ``annotation`` (str, optional): Annotation text (omit/null/value tri-state)
   - ``tags`` (list[str], optional): Tags (omit/null/value tri-state)
   - ``changed_by`` (str, optional): Who made the change (audit trail)

   **Returns:** UpdateRecordsResponse with updated and skipped records

.. py:function:: GET /series

   List time series, optionally filtered by name, labels, unit, or series_id.

   **Query Parameters:**

   - ``name`` (str, optional): Filter by series name
   - ``labels`` (str, optional): Filter by labels (JSON string)
   - ``unit`` (str, optional): Filter by unit
   - ``series_id`` (int, optional): Filter by series_id

   **Returns:** List of SeriesInfo objects

.. py:function:: GET /series/labels

   List unique values for a specific label key.

   **Query Parameters:**

   - ``label_key`` (str, required): The label key to get unique values for
   - ``name`` (str, optional): Filter by series name
   - ``labels`` (str, optional): Filter by labels (JSON string)

   **Returns:** ``{"label_key": "...", "values": [...]}``

.. py:function:: GET /series/count

   Count time series matching filters.

   **Query Parameters:**

   - ``name`` (str, optional): Filter by series name
   - ``labels`` (str, optional): Filter by labels (JSON string)
   - ``unit`` (str, optional): Filter by unit

   **Returns:** ``{"count": N}``

Pydantic Models
~~~~~~~~~~~~~~~

Request/Response models used by the REST API:

.. autoclass:: timedb.api.DataPoint
   :members:

.. autoclass:: timedb.api.InsertRequest
   :members:

.. autoclass:: timedb.api.InsertResponse
   :members:

.. autoclass:: timedb.api.RecordUpdateRequest
   :members:

.. autoclass:: timedb.api.UpdateRecordsResponse
   :members:

.. autoclass:: timedb.api.CreateSeriesRequest
   :members:

.. autoclass:: timedb.api.CreateSeriesResponse
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

.. automodule:: timedb.units
   :members:
   :show-inheritance:
