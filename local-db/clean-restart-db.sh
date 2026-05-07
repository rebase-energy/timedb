#!/usr/bin/env bash
# Clean restart: stop, remove volume and local data dir, then start

# Change to script directory
BASEDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$BASEDIR" || exit 1

echo "Stopping container and removing volumes..."
docker compose down -v 2>/dev/null || true

echo "Removing local ./chdata (if present)..."
if [ -d ./chdata ]; then
    sudo rm -rf ./chdata
fi

echo "Starting container..."
docker compose up -d

echo "Waiting for ClickHouse to be ready..."
sleep 5

echo ""
echo "Container status:"
docker ps | grep -E "timedb_clickhouse"

echo ""
echo "ClickHouse health:"
curl -sS http://127.0.0.1:8123/ping || echo "ClickHouse not reachable yet"
