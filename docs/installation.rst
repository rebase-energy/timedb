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

- Python 3.9 or higher
- PostgreSQL database (version 12+)
- For API functionality: FastAPI and uvicorn (included in dependencies)

Dependencies
------------

timedb includes the following key dependencies:

- ``psycopg[binary]>=3.1`` - PostgreSQL adapter (psycopg3)
- ``pandas>=2.0.0`` - Data manipulation
- ``pint>=0.23`` - Unit handling and conversion
- ``pint-pandas>=0.3`` - Pandas integration for Pint units
- ``fastapi>=0.104.0`` - API framework (for REST API server)
- ``uvicorn[standard]>=0.24.0`` - ASGI server (for REST API server)
- ``typer>=0.9.0`` - CLI framework

Database Setup
--------------

Before using timedb, you need a PostgreSQL database. You can use:

- A local PostgreSQL instance
- A cloud-hosted database (e.g., Neon, Supabase, AWS RDS)
- A Docker container running PostgreSQL

Set up your database connection using one of these environment variables:

- ``TIMEDB_DSN`` - Preferred connection string
- ``DATABASE_URL`` - Alternative connection string (for compatibility with platforms like Heroku)

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

You should see the timedb CLI help message with available commands.

Create the database schema:

.. code-block:: bash

   timedb create tables

Or using Python:

.. code-block:: python

   import timedb as td
   td.create()

Next Steps
----------

Once installed, you can:

1. Explore the :doc:`Examples <examples>` to learn by doing
2. Use the :doc:`CLI <cli>` to manage your database schema
3. Use the :doc:`SDK <sdk>` to interact with your database from Python
4. :doc:`Set up the API server <api_setup>` to serve data via REST API

