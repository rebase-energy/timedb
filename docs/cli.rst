Command Line Interface (CLI)
=============================

The timedb CLI provides commands for managing your database schema, users, and running the API server.

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
- ``--with-metadata``: Also create the optional metadata_table addon
- ``--with-users``: Also create the users_table for authentication
- ``--yes``, ``-y``: Do not prompt for confirmation
- ``--dry-run``: Print the DDL and exit without executing

Examples:

.. code-block:: bash

   # Create basic tables
   timedb create tables

   # Create tables in a specific schema
   timedb create tables --schema timedb

   # Create tables with metadata table
   timedb create tables --with-metadata

   # Create tables with users table for authentication
   timedb create tables --with-users

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

- ``runs_table``
- ``values_table``
- ``series_table``
- ``metadata_table`` (if created)
- ``users_table`` (if created)
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

User Management
---------------

TimeDB includes commands for managing users and API keys for multi-tenant authentication.

.. note::

   User management requires the users table to be created first:

   .. code-block:: bash

      timedb create tables --with-users

Creating Users
~~~~~~~~~~~~~~

Create a new user with an API key:

.. code-block:: bash

   timedb users create --tenant-id <uuid> --email <email>

Options:

- ``--tenant-id``, ``-t``: Tenant UUID (required)
- ``--email``, ``-e``: User email address (required)

Example:

.. code-block:: bash

   timedb users create --tenant-id 123e4567-e89b-12d3-a456-426614174000 --email user@example.com

The command will display the generated API key. **Save this key** - it will not be shown again.

Listing Users
~~~~~~~~~~~~~

List all users:

.. code-block:: bash

   timedb users list [OPTIONS]

Options:

- ``--tenant-id``, ``-t``: Filter by tenant UUID (optional)
- ``--include-inactive``: Include inactive users (default: only active users)

Example:

.. code-block:: bash

   # List all active users
   timedb users list

   # List users for a specific tenant
   timedb users list --tenant-id 123e4567-e89b-12d3-a456-426614174000

   # Include inactive users
   timedb users list --include-inactive

Regenerating API Keys
~~~~~~~~~~~~~~~~~~~~~

Regenerate an API key for a user:

.. code-block:: bash

   timedb users regenerate-key --email <email>

Options:

- ``--email``, ``-e``: User email address (required)
- ``--yes``, ``-y``: Skip confirmation prompt

Example:

.. code-block:: bash

   timedb users regenerate-key --email user@example.com

**Warning**: The old API key will be invalidated immediately.

Deactivating Users
~~~~~~~~~~~~~~~~~~

Deactivate a user (revoke API access):

.. code-block:: bash

   timedb users deactivate --email <email>

Options:

- ``--email``, ``-e``: User email address (required)
- ``--yes``, ``-y``: Skip confirmation prompt

Example:

.. code-block:: bash

   timedb users deactivate --email user@example.com

Activating Users
~~~~~~~~~~~~~~~~

Activate a previously deactivated user:

.. code-block:: bash

   timedb users activate --email <email>

Options:

- ``--email``, ``-e``: User email address (required)

Example:

.. code-block:: bash

   timedb users activate --email user@example.com

