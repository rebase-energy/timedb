# TimeDB Quickstart

Get up and running with timedb in under 10 minutes.

## Prerequisites

- **Docker**: Required to run TimescaleDB
- **Python 3.9+**: Required to run timedb
- **uv** (recommended): For managing Python dependencies ([installation guide](https://docs.astral.sh/uv/getting-started/installation/))

## Step 1: Install timedb from Local Repository

Navigate to your cloned timedb repository and install it in development mode:

```bash
cd /path/to/timedb

# Using uv (recommended)
uv sync

# Or using pip
pip install -e .
```

## Step 2: Start TimescaleDB

Navigate to the `timescaledb-test/` directory and start the Docker container:

```bash
cd timescaledb-test
docker compose up -d
```

Verify the container is running:

```bash
docker ps
```

You should see a container named `local_timescaledb` running on port `5433` (mapped to the container's port `5432`).

### Database Details

- **Host**: `127.0.0.1`
- **Port**: `5433`
- **Username**: `postgres`
- **Password**: `devpassword`
- **Database**: `devdb`

## Step 3: Configure Environment Variables

Create a `.env` file in the project root (same directory as `README.md`):

```bash
# From the project root
cat > .env << EOF
TIMEDB_DSN=postgresql://postgres:devpassword@127.0.0.1:5433/devdb
EOF
```

The connection string format is:
```
postgresql://username:password@host:port/database
```

To verify the connection works:

```bash
psql postgresql://postgres:devpassword@127.0.0.1:5433/devdb
```

You should see a PostgreSQL prompt. Type `\q` to exit.

## Step 4: Run the Quickstart Notebook

Open the quickstart notebook in Jupyter:

```bash
# Using uv
uv run jupyter notebook examples/quickstart.ipynb

# Or using jupyter directly (if installed)
jupyter notebook examples/quickstart.ipynb
```

The notebook will:
1. Connect to your TimescaleDB instance
2. Create the timedb schema
3. Create a sample time series for wind power forecasts
4. Insert forecast revisions
5. Query and analyze the data

## What's in the Quickstart Notebook?

The quickstart notebook demonstrates:

- **Setup**: Initializing the TimeDataClient
- **Create a Series**: Defining a time series with units (e.g., "MW")
- **Insert Revisioned Data**: Adding multiple forecast revisions
- **Query Data**: Reading time series with time-of-knowledge filtering
- **Analyze Results**: Using pandas for data inspection

## Troubleshooting

### Container won't start
```bash
# Check docker daemon is running, then clean up any existing containers
docker compose down
docker compose up -d
```

### Connection refused on port 5433
- Verify Docker container is running: `docker ps`
- Wait a few seconds for database initialization to complete
- Check the logs: `docker compose logs db`

### Database already exists
If you want to start fresh:

```bash
# Stop the container and remove the data
docker compose down
rm -rf timescaledb-test/pgdata
docker compose up -d
```

### Import errors in notebook
Ensure your `.env` file is correct and reload the notebook kernel:
1. In Jupyter, click **Kernel** → **Restart & Clear Output**
2. Re-run the cells

## Next Steps

- Explore other example notebooks in the `examples/` directory
- Read the [API Reference](docs/api_reference.rst) for detailed documentation
- Review [DEVELOPMENT.md](DEVELOPMENT.md) for advanced setup topics
- Check out [TESTING.md](TESTING.md) to run the test suite

## Additional Resources

- [timedb README](README.md) — Overview and basic usage
- [API Documentation](docs/api_reference.rst) — Full API reference
- [Example Notebooks](examples/) — More complex examples with units, revisions, and backtesting
