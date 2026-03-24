# TimeDB Quickstart

Get up and running with timedb in under 10 minutes.

## Prerequisites

- **Docker**: Required to run PostgreSQL and ClickHouse
- **Python 3.9+**: Required to run timedb
- **uv** (recommended): For managing Python dependencies ([installation guide](https://docs.astral.sh/uv/getting-started/installation/))

## Step 1: Install timedb from Local Repository

Navigate to your cloned timedb repository and install it in development mode:

```bash
git clone https://github.com/rebase-energy/timedb.git
cd timedb
# Using uv (recommended)
uv sync # --all-extras to use notebooks, tests, docs

# Or using pip
pip install -e .
```

## Step 2: Start Databases

Navigate to the `local-db/` directory and start both containers:

```bash
cd local-db
docker compose up -d
```

Verify the containers are running:

```bash
docker ps
```

You should see:
- `local_postgres` running on port `5433` (PostgreSQL — series metadata)
- `local_clickhouse` running on port `8123` (ClickHouse — time-series values)

### Database Details

**PostgreSQL**
- **Host**: `127.0.0.1`
- **Port**: `5433`
- **Username**: `postgres`
- **Password**: `devpassword`
- **Database**: `devdb`

**ClickHouse**
- **Host**: `127.0.0.1`
- **HTTP Port**: `8123`

## Step 3: Configure Environment Variables

Create a `.env` file in the project root (same directory as `README.md`):

```bash
cp .env.example .env
```

This sets both required variables:

```text
TIMEDB_PG_DSN=postgresql://postgres:devpassword@127.0.0.1:5433/devdb
TIMEDB_CH_URL=http://default:@localhost:8123/default
```

To verify the PostgreSQL connection works:

```bash
psql postgresql://postgres:devpassword@127.0.0.1:5433/devdb
```

You should see a PostgreSQL prompt. Type `\q` to exit.

To verify the ClickHouse connection works:

```bash
curl http://localhost:8123/ping
```

You should see `Ok.` in the response.

## Step 4: Run the Quickstart Notebook

Open the quickstart notebook in Jupyter:

```bash
# Using uv
uv run jupyter notebook examples/quickstart.ipynb

# Or using jupyter directly (if installed)
jupyter notebook examples/quickstart.ipynb
```

The notebook will:
1. Connect to PostgreSQL and ClickHouse
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

### Connection refused on port 5433 or 8123
- Verify Docker containers are running: `docker ps`
- Wait a few seconds for database initialization to complete
- Check the logs: `docker compose logs`

### Database already exists
If you want to start fresh:

```bash
# Stop containers and remove all data
docker compose down -v
rm -rf local-db/pgdata local-db/chdata
docker compose up -d
```

### Import errors in notebook
Ensure your `.env` file is correct and reload the notebook kernel:
1. In Jupyter, click **Kernel** → **Restart & Clear Output**
2. Re-run the cells

## Next Steps

- Explore other example notebooks in the `examples/` directory
- Read the [Reference](docs/reference.rst) for detailed documentation
- Review [DEVELOPMENT.md](DEVELOPMENT.md) for advanced setup topics
- Check out [TESTING.md](TESTING.md) to run the test suite

## Additional Resources

- [timedb README](README.md) — Overview and basic usage
- [Reference](docs/reference.rst) — Full SDK, API, and CLI reference
- [Example Notebooks](examples/) — More complex examples with units, revisions, and backtesting
