Installation
============

timedb can be installed using `pip <https://pypi.org/project/pip/>`_:

.. code-block:: bash

   pip install timedb

or if you are using `uv <https://docs.astral.sh/uv/>`_:

.. code-block:: bash

   uv add timedb

Requirements
------------

timedb requires:

- Python 3.8 or higher
- PostgreSQL database (with psycopg3 support)
- For API functionality: FastAPI and uvicorn (included in dependencies)

Dependencies
------------

timedb includes the following key dependencies:

- ``psycopg[binary]>=3.1`` - PostgreSQL adapter
- ``pandas>=2.0.0`` - Data manipulation
- ``pint>=0.23`` - Unit handling and conversion
- ``pint-pandas>=0.3`` - Pandas integration for Pint units
- ``fastapi>=0.104.0`` - API framework (for API server)
- ``uvicorn[standard]>=0.24.0`` - ASGI server (for API server)
- ``click>=8.0`` - CLI framework

Database Setup
--------------

Before using timedb, you need a PostgreSQL database. You can use a local PostgreSQL instance or a remote database.

Set up your database connection using one of these environment variables:

- ``TIMEDB_DSN`` - Preferred connection string
- ``DATABASE_URL`` - Alternative connection string (for compatibility)

Example connection strings:

.. code-block:: bash

   # Using TIMEDB_DSN
   export TIMEDB_DSN="postgresql://user:password@localhost:5432/timedb"

   # Or using DATABASE_URL
   export DATABASE_URL="postgresql://user:password@localhost:5432/timedb"

You can also use a ``.env`` file in your project root:

.. code-block:: text

   TIMEDB_DSN=postgresql://user:password@localhost:5432/timedb

The ``python-dotenv`` package (included in dependencies) will automatically load this file.

Verification
------------

After installation, verify that timedb is installed correctly:

.. code-block:: bash

   timedb --help

You should see the timedb CLI help message.

Next Steps
----------

Once installed, you can:

1. :doc:`Create the database schema <cli>` using the CLI
2. :doc:`Use the SDK <sdk>` to interact with your database
3. :doc:`Set up the API server <api_setup>` to serve data via REST API

