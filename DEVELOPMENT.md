# Development Setup

## 1) Clone the repository

If you have not cloned the project yet:

```bash
git clone https://github.com/rebase-energy/timedb.git
cd timedb
```

## 2) Installation

Set up your preferred Python virtual environment, then install the package in editable mode with development dependencies.

Using standard `pip`:

```bash
pip install -e ".[notebooks,docs,test]"
```

Using `uv` (optional, faster dependency management):

```bash
uv pip install -e ".[notebooks,docs,test]"
```

To run scripts inside your environment:

```bash
python file.py
# or
uv run file.py
```

## 3) Database Environment

If you do not already have PostgreSQL with the TimescaleDB extension set up, spin one up locally using Docker.

```bash
cd timescaledb-test/
docker compose up -d
```

This starts a local TimescaleDB Community Edition instance on port `5433`.

## 4) Configuration

The application requires a `TIMEDB_DSN` environment variable to connect to the database.

Fastest option (recommended): copy the example environment file in the project root.

```bash
cp .env.example .env
```

`TIMEDB_DSN` should already be set correctly for the local Docker setup.


```text
TIMEDB_DSN=postgresql://postgres:devpassword@127.0.0.1:5433/devdb
```

Alternatively, export the variable directly in your shell:

- Bash/Zsh: `export TIMEDB_DSN='...'`
- Fish: `set -x TIMEDB_DSN '...'`

## 5) Database Management & Tools

### Helper scripts (Bash and Fish)

If you are using the local Docker setup, use scripts in `timescaledb-test/`:

- `./restart-db.sh` or `./restart-db.fish`: Restarts containers while preserving existing data.
- `./clean-restart-db.sh` or `./clean-restart-db.fish`: Removes containers, volumes, and data, then starts fresh.

### Manual inspection

Connect directly with `psql`:

```bash
psql postgresql://postgres:devpassword@127.0.0.1:5433/devdb
```

## 6) Building Documentation

Generate HTML documentation with Sphinx:

```bash
sphinx-build -b html docs/ docs/_build/html
```

The built site will be available at `docs/_build/html/`.