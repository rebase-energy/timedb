# Development Setup

## Python Environment

This project uses [uv](https://docs.astral.sh/uv/) for dependency management. To set up your Python environment:

```bash
uv sync  # Creates .venv from uv.lock
```

To run Python scripts:

```bash
uv run file.py
```

## TimescaleDB Database

The easiest way to set up a development database is using Docker. Navigate to the `timescaledb-test/` directory and start the TimescaleDB container:

```bash
docker compose up -d
docker ps  # Verify the container is running
```

You'll have a TimescaleDB Community Edition instance running on port `5433`.

## Database Connection

Python scripts require the `TIMEDB_DSN` environment variable.


### Using .env File

Create a `.env` file in the project root:

```bash
TIMEDB_DSN=postgresql://postgres:devpassword@127.0.0.1:5433/devdb
```

The application will automatically load this file. Note: Do not include quotes around the connection string value.

### Using Environment Variables

**Bash/zsh:**
```bash
# Session-specific
export TIMEDB_DSN='postgresql://postgres:devpassword@127.0.0.1:5433/devdb'

# Global (add to ~/.bashrc or ~/.zshrc)
export TIMEDB_DSN='postgresql://postgres:devpassword@127.0.0.1:5433/devdb'
```

**Fish shell:**
```bash
# Session-specific
set -x TIMEDB_DSN 'postgresql://postgres:devpassword@127.0.0.1:5433/devdb'

# Global (permanent)
set -Ux TIMEDB_DSN 'postgresql://postgres:devpassword@127.0.0.1:5433/devdb'
```


### Inspect the Database

```bash
psql postgresql://postgres:devpassword@127.0.0.1:5433/devdb
```

## Database Management

### Helper Scripts (Fish Shell)

Convenient scripts are available in the `timescaledb-test/` directory for managing the TimescaleDB container:

#### Quick Restart

```bash
./timescaledb-test/restart-db.fish
```

Recreates the containers while preserving volumes and data. Use this when you want to restart the database without losing existing data.

#### Clean Restart

```bash
./timescaledb-test/clean-restart-db.fish
```

Completely removes containers, volumes, and local `pgdata` directory, then starts fresh. Use this to:
- Reset the database to a clean state
- Resolve permission or initialization issues
- Start development with a fresh schema

The script automatically waits for the database to be ready before returning.

## Visualizing Database Schema

To visualize tables and relationships interactively:

```bash
docker run -p 8080:80 ghcr.io/chartdb/chartdb:latest
```

Then open [http://localhost:8080](http://localhost:8080).

## Building Documentation

Generate HTML documentation from source:

```bash
sphinx-build -b html docs/ docs/_build/html
```

The built documentation will be in `docs/_build/html/`.