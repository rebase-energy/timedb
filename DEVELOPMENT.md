# Development Setup

## 1) Clone the repository

If you have not cloned the project yet:

```bash
git clone https://github.com/rebase-energy/timedb.git
cd timedb
```

## 2) Installation

Set up your preferred Python virtual environment first, then install the package in editable mode with development dependencies.

Create a virtual environment (choose one):

```bash
uv venv
```

```bash
python -m venv .venv
```

Activate it:

```bash
# Bash/Zsh
source .venv/bin/activate

# Fish
source .venv/bin/activate.fish
```

Using standard `pip`:

```bash
pip install -e ".[notebooks,docs,test]"
```

Using `uv` (optional, faster dependency management):

```bash
uv pip install -e ".[notebooks,docs,test]"
```

If you skip virtual environment activation, `uv` will fail with `No virtual environment found`.

To run scripts inside your environment:

```bash
python file.py
# or
uv run file.py
```

## 3) Database Environment

TimeDB is a stateless library on top of **ClickHouse** (port `8123`) — it stores all time-series values there. There is no PostgreSQL dependency; series metadata lives in the calling application (e.g. [energydb](https://github.com/rebase-energy/energydb)).

Spin up ClickHouse locally using Docker:

```bash
cd local-db/
docker compose up -d
```

Verify the container is running:

```bash
docker ps
```

You should see `timedb_clickhouse` (port 8123).

## 4) Configuration

The application requires the `TIMEDB_CH_URL` environment variable.

Fastest option (recommended): from the repository root, copy the example environment file.

```bash
cp .env.example .env
```

The variable is already set correctly for the local Docker setup:

```text
TIMEDB_CH_URL=http://default:@localhost:8123/default
```

Alternatively, export the variable directly in your shell:

```bash
# Bash/Zsh
export TIMEDB_CH_URL='http://default:@localhost:8123/default'
```

```fish
# Fish
set -x TIMEDB_CH_URL http://default:@localhost:8123/default
```

## 5) Next Steps

Now you can try the examples in `examples/`, or build your own script using the SDK/API.

## 6) Database Management & Tools

### Helper scripts (Bash and Fish)

If you are using the local Docker setup, use scripts in `local-db/`:

- `./restart-db.sh` or `./restart-db.fish`: Restarts the container while preserving existing data.
- `./clean-restart-db.sh` or `./clean-restart-db.fish`: Removes the container, volume, and data, then starts fresh.

### Manual inspection

Connect to ClickHouse with the HTTP interface:

```bash
curl http://localhost:8123/ping
```

Or with the native client:

```bash
docker exec -it timedb_clickhouse clickhouse-client
```

## 7) Building Documentation

Generate HTML documentation with Sphinx:

```bash
sphinx-build -b html docs/ docs/_build/html
```

The built site will be available at `docs/_build/html/`.
