#!/usr/bin/env bash
# Clean restart: stop, remove volumes and local pgdata, then start

# Change to script directory
BASEDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$BASEDIR" || exit 1

echo "Stopping containers and removing volumes..."
docker compose down -v 2>/dev/null || true

echo "Removing local ./pgdata (if present)..."
if [ -d ./pgdata ]; then
    sudo rm -rf ./pgdata
fi

echo "Starting Postgres containers..."
docker compose up -d

echo "Waiting for database to be ready..."
sleep 10

echo ""
echo "Container status:"
docker ps | grep timescaledb

echo ""
echo "Database initialization status:"
docker logs local_timescaledb 2>&1 | grep -E '(ready|error|listening)' | tail -3
