# Setup

## Python

This project uses uv for python. Run 

```bash
uv sync # creates the .venv based on the uv.lock lockfile
```
To run python scripts then use
```bash
uv run file.py
```

## Postgresql database

It is recommended to use docker. Go to `postgresql/` and start the docker postgresql database with
```bash
docker compose up -d

docker ps # check if running
```
Now you should have a working test database on port `5432`.

## Connect to database

The python scripts require the DATABASE_URL environment variable to be set. On fish create it like 

```bash
set -x DATABASE_URL 'postgresql://devuser:devpassword@127.0.0.1:5432/devdb'
```

Inspect the database

```bash
psql postgresql://devuser:devpassword@127.0.0.1:5432/devdb
```

# Clean and restart database

```bash
docker compose down -v
rm -rf ./pgdata
docker compose up -d
```

# Fish helper scripts

If you use fish, helper scripts are available in the postgresql/ folder:

- Quick restart (recreate containers):
  ./postgresql/restart-db.fish

- Clean restart (down, remove pgdata, then up):
  ./postgresql/clean-restart-db.fish

# Documentation

```bash
sphinx-build -b html . _build/html
```