API Setup
=========

The timedb API server provides a REST API for accessing time series data. This documentation covers setting up and running the API server. For API user documentation (endpoints, request/response formats), see the separate API documentation page.

Prerequisites
-------------

Before setting up the API server, ensure:

1. **Database is set up**: The timedb schema must be created (see :doc:`CLI <cli>`)
2. **Database connection configured**: Set ``TIMEDB_DSN`` or ``DATABASE_URL`` environment variable
3. **FastAPI dependencies installed**: Included in timedb package, but ensure ``fastapi`` and ``uvicorn[standard]`` are available

Starting the API Server
------------------------

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

   # Start on custom host and port
   timedb api --host 0.0.0.0 --port 8080

   # Start with auto-reload for development
   timedb api --reload

Using Python Module
~~~~~~~~~~~~~~~~~~~

You can also start the server directly using Python:

.. code-block:: bash

   python -m timedb.api_server

Or using uvicorn directly:

.. code-block:: bash

   uvicorn timedb.api:app --host 127.0.0.1 --port 8000

Using uvicorn with reload:

.. code-block:: bash

   uvicorn timedb.api:app --host 127.0.0.1 --port 8000 --reload

Programmatic Usage
~~~~~~~~~~~~~~~~~~

You can also import and run the API programmatically:

.. code-block:: python

   import uvicorn
   from timedb import api

   if __name__ == "__main__":
       uvicorn.run(api.app, host="127.0.0.1", port=8000, reload=True)

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

Production Deployment
---------------------

For production deployment, consider:

1. **Use a production ASGI server**: While uvicorn works for development, consider using gunicorn with uvicorn workers for production:

   .. code-block:: bash

      gunicorn timedb.api:app -w 4 -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:8000

2. **Set up reverse proxy**: Use nginx or another reverse proxy in front of the API server
3. **Enable HTTPS**: Use SSL/TLS certificates (e.g., via Let's Encrypt)
4. **Configure logging**: Set up proper logging for production
5. **Database connection pooling**: Configure appropriate connection pool settings
6. **Environment variables**: Use secure methods for managing environment variables (e.g., secrets management)

Example Production Setup
~~~~~~~~~~~~~~~~~~~~~~~~~

Using systemd service (Linux):

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

Using Docker
~~~~~~~~~~~~

Example Dockerfile:

.. code-block:: dockerfile

   FROM python:3.11-slim

   WORKDIR /app

   COPY . .
   RUN pip install timedb

   EXPOSE 8000

   CMD ["uvicorn", "timedb.api:app", "--host", "0.0.0.0", "--port", "8000"]

Build and run:

.. code-block:: bash

   docker build -t timedb-api .
   docker run -p 8000:8000 -e TIMEDB_DSN="postgresql://..." timedb-api

API Endpoints Overview
----------------------

The API provides the following endpoints:

- ``GET /`` - API information
- ``GET /values`` - Read time series values
- ``POST /runs`` - Create a new run with values
- ``PUT /values`` - Update existing records
- ``POST /schema/create`` - Create database schema
- ``DELETE /schema/delete`` - Delete database schema

For detailed API documentation (request/response formats, examples), see the separate API documentation page or visit ``/docs`` when the server is running.

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
- **Database indexes**: Ensure appropriate indexes are created (timedb creates these automatically)
- **Query optimization**: Use appropriate filters (time ranges, series IDs) to limit data returned

For more help, check the API logs or visit the interactive API docs at ``/docs`` for endpoint-specific information.

