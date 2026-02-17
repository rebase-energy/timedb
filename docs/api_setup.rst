API Setup
=========

The timedb API server provides a REST API for accessing time series data. This documentation covers setting up and running the API server.

Prerequisites
-------------

Before setting up the API server, ensure:

1. **Database is set up**: The timedb schema must be created (see :doc:`CLI <cli>` or :doc:`SDK <sdk>`)
2. **Database connection configured**: Set ``TIMEDB_DSN`` or ``DATABASE_URL`` environment variable
3. **Dependencies installed**: FastAPI and uvicorn are included in timedb package

Starting the API Server
-----------------------

Using the CLI
~~~~~~~~~~~~~

The easiest way to start the API server is using the timedb CLI:

.. code-block:: bash

   timedb api

This starts the server on ``http://127.0.0.1:8000`` by default.

Options:

- ``--host``: Host to bind to (default: ``127.0.0.1``)
- ``--port``: Port to bind to (default: ``8000``)
- ``--reload``: Enable auto-reload for development

Examples:

.. code-block:: bash

   # Start on default host and port
   timedb api

   # Start on custom host and port (accessible from network)
   timedb api --host 0.0.0.0 --port 8080

   # Start with auto-reload for development
   timedb api --reload

Using Python SDK
~~~~~~~~~~~~~~~~

Start the server programmatically via the CLI is the recommended approach. See the :doc:`CLI <cli>` documentation for details.

Using uvicorn directly
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   uvicorn timedb.api:app --host 127.0.0.1 --port 8000

   # With auto-reload
   uvicorn timedb.api:app --host 127.0.0.1 --port 8000 --reload

Configuration
-------------

Database Connection
~~~~~~~~~~~~~~~~~~~

The API server requires a database connection. Set one of these environment variables:

- ``TIMEDB_DSN`` (preferred)
- ``DATABASE_URL`` (alternative)

You can use a ``.env`` file in your project root:

.. code-block:: text

   TIMEDB_DSN=postgresql://user:password@localhost:5432/timedb

The API will fail to start if the database connection is not configured.

Accessing the API
-----------------

Once the server is running, you can access:

- **API Root**: ``http://<host>:<port>/``
- **Interactive API Docs (Swagger UI)**: ``http://<host>:<port>/docs``
- **Alternative API Docs (ReDoc)**: ``http://<host>:<port>/redoc``

The interactive API docs at ``/docs`` provide a convenient way to explore and test the API endpoints.

API Endpoints Overview
----------------------

The API provides the following endpoints:

- ``GET /`` - API information and available endpoints
- ``POST /values`` - Insert time series data
- ``GET /values`` - Read time series values
- ``PUT /values`` - Update existing time series records
- ``POST /series`` - Create a new time series
- ``GET /series`` - List/filter time series
- ``GET /series/labels`` - List unique label values for a key
- ``GET /series/count`` - Count matching time series

Visit ``/docs`` when the server is running for detailed endpoint documentation with request/response schemas.

Updating Records via API
------------------------

Update overlapping records using the ``PUT /values`` endpoint:

.. code-block:: bash

   curl -X PUT http://localhost:8000/values \
     -H "Content-Type: application/json" \
     -d '{
       "updates": [
         {
           "valid_time": "2025-01-01T12:00:00Z",
           "series_id": 1,
           "batch_id": 42,
           "value": 150.0,
           "annotation": "Corrected reading",
           "tags": ["reviewed"],
           "changed_by": "user@example.com"
         }
       ]
     }'

Or using Python requests:

.. code-block:: python

   import requests

   response = requests.put(
       "http://localhost:8000/values",
       json={
           "updates": [
               {
                   "valid_time": valid_time.isoformat(),
                   "series_id": series_id,
                   "batch_id": batch_id,
                   "value": 150.0,
                   "annotation": "Corrected",
                   "tags": ["reviewed"],
                   "changed_by": "user@example.com"
               }
           ]
       }
   )
   result = response.json()
   print(f"Updated: {len(result['updated'])}")

Production Deployment
---------------------

For production deployment, consider:

1. **Use a production ASGI server**: Use gunicorn with uvicorn workers:

   .. code-block:: bash

      gunicorn timedb.api:app -w 4 -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:8000

2. **Set up reverse proxy**: Use nginx or another reverse proxy
3. **Enable HTTPS**: Use SSL/TLS certificates (e.g., via Let's Encrypt)
4. **Configure logging**: Set up proper logging for production
5. **Database connection pooling**: Consider using a connection pooler like PgBouncer
6. **Environment variables**: Use secure methods for managing secrets

Example: systemd Service
~~~~~~~~~~~~~~~~~~~~~~~~

Create ``/etc/systemd/system/timedb-api.service``:

.. code-block:: ini

   [Unit]
   Description=TimeDB API Server
   After=network.target

   [Service]
   Type=simple
   User=timedb
   WorkingDirectory=/opt/timedb
   Environment="TIMEDB_DSN=postgresql://user:password@localhost:5432/timedb"
   ExecStart=/usr/local/bin/uvicorn timedb.api:app --host 0.0.0.0 --port 8000
   Restart=always

   [Install]
   WantedBy=multi-user.target

Then:

.. code-block:: bash

   sudo systemctl daemon-reload
   sudo systemctl enable timedb-api
   sudo systemctl start timedb-api

Example: Docker
~~~~~~~~~~~~~~~

.. code-block:: dockerfile

   FROM python:3.11-slim

   WORKDIR /app

   RUN pip install timedb

   EXPOSE 8000

   CMD ["uvicorn", "timedb.api:app", "--host", "0.0.0.0", "--port", "8000"]

Build and run:

.. code-block:: bash

   docker build -t timedb-api .
   docker run -p 8000:8000 -e TIMEDB_DSN="postgresql://..." timedb-api

Troubleshooting
---------------

Server won't start
~~~~~~~~~~~~~~~~~~

- **Check database connection**: Ensure ``TIMEDB_DSN`` or ``DATABASE_URL`` is set correctly
- **Check database exists**: Verify the database is accessible
- **Check schema exists**: Run ``timedb create tables`` first
- **Check port availability**: Ensure the port is not already in use

Connection errors
~~~~~~~~~~~~~~~~~

- **Database not accessible**: Check network connectivity and firewall rules
- **Authentication failed**: Verify database credentials
- **Schema not found**: Ensure the timedb schema is created

Performance issues
~~~~~~~~~~~~~~~~~~

- **Connection pooling**: Consider using a connection pooler like PgBouncer
- **Database indexes**: TimeDB creates indexes automatically during schema creation
- **Query optimization**: Use appropriate filters (time ranges, series IDs) to limit data returned

