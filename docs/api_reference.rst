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

REST API Reference
------------------

The REST API provides HTTP endpoints for reading and writing time series data.

Interactive Documentation
~~~~~~~~~~~~~~~~~~~~~~~~~~

When the API server is running, comprehensive interactive documentation is available:

- **Swagger UI**: ``http://<host>:<port>/docs`` - Full interactive API explorer
- **ReDoc**: ``http://<host>:<port>/redoc`` - Alternative documentation format

These provide live request/response examples, parameter descriptions, and test capabilities.

Key Endpoints
~~~~~~~~~~~~~

.. py:function:: GET /values

   Read time series values from the database.

   **Query Parameters:**

   - ``start_valid`` (datetime, optional): Start of valid time range (ISO format)
   - ``end_valid`` (datetime, optional): End of valid time range (ISO format)
   - ``start_known`` (datetime, optional): Start of known_time range (ISO format)
   - ``end_known`` (datetime, optional): End of known_time range (ISO format)
   - ``mode`` (str, default="flat"): Query mode - "flat" or "overlapping"

   **Modes:**

   - ``flat``: Returns latest value per (valid_time, series_id)
   - ``overlapping``: Returns all forecast revisions with their known_time

   **Returns:** JSON object with count and data array

.. py:function:: POST /upload

   Upload time series data (create a batch with associated values).

   **Request Body:**

   - ``workflow_id`` (str, optional): Workflow identifier (defaults to "api-workflow")
   - ``batch_start_time`` (datetime): Start time of the batch
   - ``batch_finish_time`` (datetime, optional): End time of the batch
   - ``known_time`` (datetime, optional): Time of knowledge (defaults to now)
   - ``batch_params`` (dict, optional): Custom batch parameters
   - ``value_rows`` (list): Array of value rows to insert
   - ``series_descriptions`` (dict, optional): Descriptions for new series

   **Returns:** CreateBatchResponse with batch_id and series_ids

.. py:function:: POST /series

   Create a new time series.

   **Request Body:**

   - ``name`` (str): Human-readable identifier
   - ``description`` (str, optional): Series description
   - ``unit`` (str, default="dimensionless"): Canonical unit
   - ``labels`` (dict, default={}): Key-value labels for differentiation
   - ``data_class`` (str, default="flat"): "flat" or "overlapping"
   - ``retention`` (str, default="medium"): "short", "medium", or "long"

   **Returns:** CreateSeriesResponse with series_id

.. py:function:: GET /list_timeseries

   List all available time series.

   **Returns:** JSON object mapping series_id to series information

.. py:function:: PUT /values

   Update existing time series records.

   **Request Body:**

   - ``updates`` (list): Array of RecordUpdateRequest objects

   **Returns:** UpdateRecordsResponse with updated records

Pydantic Models
~~~~~~~~~~~~~~~

Request/Response models used by the REST API:

.. autoclass:: timedb.api.ValueRow
   :members:

.. autoclass:: timedb.api.CreateBatchRequest
   :members:

.. autoclass:: timedb.api.CreateBatchResponse
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
