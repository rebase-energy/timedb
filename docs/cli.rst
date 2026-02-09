Command Line Interface (CLI)
=============================

The timedb CLI provides commands for managing your database schema and running the API server.

Getting Help
------------

To see all available commands:

.. code-block:: bash

   timedb --help

To get help for a specific command:

.. code-block:: bash

   timedb create --help
   timedb create tables --help

Database Connection
-------------------

All CLI commands that interact with the database require a connection string. You can provide it in three ways:

1. **Command-line option**: Use ``--dsn`` or ``-d`` flag
2. **Environment variable**: Set ``TIMEDB_DSN`` or ``DATABASE_URL``
3. **``.env`` file**: Place a ``.env`` file in your project root

Example:

.. code-block:: bash

   timedb create tables --dsn postgresql://user:password@localhost:5432/timedb

Or:

.. code-block:: bash

   export TIMEDB_DSN="postgresql://user:password@localhost:5432/timedb"
   timedb create tables

Creating Tables
---------------

Create the timedb database schema:

.. code-block:: bash

   timedb create tables [OPTIONS]

Options:

- ``--dsn``, ``-d``: PostgreSQL connection string (or use environment variable)
- ``--schema``, ``-s``: Schema name to use for the tables (sets search_path for the DDL)
- ``--retention``, ``-r``: Default (medium) retention period (e.g., ``5 years``)
- ``--retention-short``: Retention for overlapping_short (default: ``6 months``)
- ``--retention-medium``: Retention for overlapping_medium (default: ``3 years``)
- ``--retention-long``: Retention for overlapping_long (default: ``5 years``)
- ``--yes``, ``-y``: Do not prompt for confirmation
- ``--dry-run``: Print the DDL and exit without executing

Examples:

.. code-block:: bash

   # Create basic tables
   timedb create tables

   # Create tables in a specific schema
   timedb create tables --schema timedb

   # Create tables with custom retention periods
   timedb create tables --retention "5 years"
   timedb create tables --retention-short "3 months" --retention-long "10 years"

   # Preview the DDL without executing
   timedb create tables --dry-run

   # Skip confirmation prompt
   timedb create tables --yes

The command is safe to run multiple times - it uses appropriate clauses to handle existing tables gracefully.

Deleting Tables
---------------

Delete all timedb tables and their data:

.. code-block:: bash

   timedb delete tables [OPTIONS]

Options:

- ``--dsn``, ``-d``: PostgreSQL connection string (or use environment variable)
- ``--schema``, ``-s``: Schema name to use for the tables (sets search_path for the DDL)
- ``--yes``, ``-y``: Do not prompt for confirmation

**WARNING**: This will delete ALL timedb tables and their data, including:

- ``batches_table``
- ``series_table``
- ``flat``
- ``overlapping_short``, ``overlapping_medium``, ``overlapping_long``
- All views

This action cannot be undone!

Example:

.. code-block:: bash

   timedb delete tables --yes

Starting the API Server
-----------------------

Start the FastAPI server for serving data via REST API:

.. code-block:: bash

   timedb api [OPTIONS]

Options:

- ``--host``: Host to bind to (default: ``127.0.0.1``)
- ``--port``: Port to bind to (default: ``8000``)
- ``--reload``: Enable auto-reload for development

Examples:

.. code-block:: bash

   # Start API server on default host and port
   timedb api

   # Start on custom host and port
   timedb api --host 0.0.0.0 --port 8080

   # Start with auto-reload for development
   timedb api --reload

Once started, the API will be available at:

- API: ``http://<host>:<port>``
- Interactive API docs: ``http://<host>:<port>/docs``
- Alternative API docs: ``http://<host>:<port>/redoc``

The API server requires the database connection to be configured via ``TIMEDB_DSN`` or ``DATABASE_URL`` environment variable.

For more information about the API, see :doc:`API Setup <api_setup>`.


