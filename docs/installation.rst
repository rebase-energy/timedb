Installation
============

timedb can be installed using `pip <https://pypi.org/project/pip/>`_:

.. code-block:: bash

   pip install timedb

or if you are using `uv <https://docs.astral.sh/uv/>`_:

.. code-block:: bash

   uv add timedb

**Install from Cloned Repository:**

To install timedb from a cloned repository for development:

.. code-block:: bash

   git clone https://github.com/rebase-energy/timedb.git
   cd timedb
   pip install -e .

To also install test dependencies:

.. code-block:: bash

   pip install -e ".[test]"

The ``-e`` flag installs the package in editable mode, allowing you to make changes to the code and see them reflected immediately without reinstalling.

Requirements
------------

timedb requires:

- Python 3.9 or higher
- PostgreSQL database (version 12+) with TimescaleDB extension (version 2.0+)
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

Before using timedb, you need a PostgreSQL database with the TimescaleDB extension enabled. You can use:

- A local PostgreSQL instance with TimescaleDB installed
- A cloud-hosted TimescaleDB instance (e.g., Timescale Cloud)
- A Docker container running TimescaleDB

**TimescaleDB Installation with Docker (Recommended):**

The project includes a ``timescaledb-test`` directory with a pre-configured Docker Compose setup:

.. code-block:: bash

   cd timescaledb-test
   docker compose up -d

This starts a TimescaleDB container with:

- Host: ``localhost``
- Port: ``5432``
- User: ``postgres``
- Password: ``devpassword``
- Database: ``devdb``

.. note::

   The Docker Compose setup uses the **TimescaleDB Community Edition**, which is suitable for testing and development only. The Community Edition does not include advanced features such as:
   
   - Compression
   - Multi-node replication
   - Advanced analytics features
   
   For production use or to access all features, consider using `Timescale Cloud <https://www.timescale.com/cloud>`_ or the `Enterprise Edition <https://docs.timescale.com/enterprise-timescale/latest/>`_.

To stop the container:

.. code-block:: bash

   docker compose down

For local installation without Docker, see `TimescaleDB Installation Guide <https://docs.timescale.com/self-hosted/latest/install/>`_.

Set up your database connection using one of these environment variables:

- ``TIMEDB_DSN`` - Preferred connection string
- ``DATABASE_URL`` - Alternative connection string (for compatibility with platforms like Heroku)

Example connection strings:

.. code-block:: bash

   # Using TIMEDB_DSN (for local Docker setup)
   export TIMEDB_DSN="postgresql://postgres:devpassword@localhost:5432/devdb"

   # Or using DATABASE_URL
   export DATABASE_URL="postgresql://postgres:devpassword@localhost:5432/devdb"

You can also use a ``.env`` file in your project root:

.. code-block:: text

   TIMEDB_DSN=postgresql://postgres:devpassword@localhost:5432/devdb

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

