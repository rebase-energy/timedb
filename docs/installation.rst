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
- PostgreSQL database (version 14+) — stores series metadata
- ClickHouse database — stores all time-series values
- For API functionality: FastAPI and uvicorn (included in dependencies)

Dependencies
------------

timedb includes the following key dependencies:

- ``psycopg[binary]>=3.1`` - PostgreSQL adapter (psycopg3)
- ``clickhouse-connect>=0.7`` - ClickHouse client
- ``pandas>=2.0.0`` - Data manipulation
- ``fastapi>=0.104.0`` - API framework (for REST API server)
- ``uvicorn[standard]>=0.24.0`` - ASGI server (for REST API server)
- ``typer>=0.9.0`` - CLI framework

Optional dependencies can be installed with extras:

.. code-block:: bash

   pip install timedb[pint]       # pint for unit handling

Database Setup
--------------

timedb requires two databases:

- **PostgreSQL** (port 5433 by default) — stores series metadata (names, labels, units, routing)
- **ClickHouse** (port 8123 by default) — stores all time-series values (flat and overlapping tables)

**Local Setup with Docker (Recommended):**

The project includes a ``local-db`` directory with a pre-configured Docker Compose setup that starts both databases:

.. code-block:: bash

   cd local-db
   docker compose up -d

This starts:

- ``local_postgres`` on port ``5433`` (PostgreSQL 18)
- ``local_clickhouse`` on port ``8123`` (ClickHouse HTTP interface)

To stop the containers:

.. code-block:: bash

   docker compose down

Set up your database connections using environment variables:

.. code-block:: bash

   # Bash/Zsh
   export TIMEDB_PG_DSN='postgresql://postgres:devpassword@localhost:5433/devdb'
   export TIMEDB_CH_URL='http://default:@localhost:8123/default'

.. code-block:: fish

   # Fish
   set -x TIMEDB_PG_DSN postgresql://postgres:devpassword@localhost:5433/devdb
   set -x TIMEDB_CH_URL http://default:@localhost:8123/default

You can also use a ``.env`` file in your project root (copy from ``.env.example``):

.. code-block:: text

   TIMEDB_PG_DSN=postgresql://postgres:devpassword@localhost:5433/devdb
   TIMEDB_CH_URL=http://default:@localhost:8123/default

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

   from timedb import TimeDataClient
   td = TimeDataClient()
   td.create()

Next Steps
----------

Once installed, you can:

1. Explore the :doc:`Examples <examples>` to learn by doing
2. Use the :doc:`CLI <cli>` to manage your database schema
3. Use the :doc:`SDK <sdk>` to interact with your database from Python
4. :doc:`Set up the API server <api_setup>` to serve data via REST API
