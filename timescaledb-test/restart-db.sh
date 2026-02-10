#!/usr/bin/env bash
# Quick restart: recreate containers (does not remove volumes)

# Change to script directory
BASEDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$BASEDIR" || exit 1

echo "Recreating Postgres containers..."
docker compose up -d --force-recreate --remove-orphans

echo ""
echo "Container status:"
docker ps | grep timescaledb
