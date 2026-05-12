# TimeDB Quickstart

Get up and running with timedb in under 10 minutes.

## Prerequisites

- **Docker**: Required to run ClickHouse
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

## Step 2: Start ClickHouse

Navigate to the `local-db/` directory and start the container:

```bash
cd local-db
docker compose up -d
```

Verify the container is running:

```bash
docker ps
```

You should see `timedb_clickhouse` running on port `8123`.

### Database Details

**ClickHouse**
- **Host**: `127.0.0.1`
- **HTTP Port**: `8123`
- **Username**: `default`
- **Password**: `devpassword`

## Step 3: Configure Environment Variables

Create a `.env` file in the project root (same directory as `README.md`):

```bash
cp .env.example .env
```

This sets the required variable:

```text
TIMEDB_CH_URL=http://default:devpassword@localhost:8123/default
```

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
1. Connect to ClickHouse
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

### Connection refused on port 8123
- Verify the Docker container is running: `docker ps`
- Wait a few seconds for ClickHouse initialization to complete
- Check the logs: `docker compose logs`

### Database already exists
If you want to start fresh:

```bash
# Stop the container and remove all data
docker compose down -v
rm -rf local-db/chdata
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
