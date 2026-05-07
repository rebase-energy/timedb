#!/usr/bin/env fish
# Clean restart: stop, remove volume and local data dir, then start

# Change to script directory
set -l BASEDIR (dirname (status -f))
cd $BASEDIR

printf 'Stopping container and removing volumes...\n'
docker compose down -v 2>/dev/null || true

printf 'Removing local ./chdata (if present)...\n'
if test -d ./chdata
    sudo rm -rf ./chdata
end

printf 'Starting container...\n'
docker compose up -d

printf 'Waiting for ClickHouse to be ready...\n'
sleep 5

printf '\nContainer status:\n'
docker ps | grep -E "timedb_clickhouse"

printf '\nClickHouse health:\n'
curl -sS http://127.0.0.1:8123/ping; or printf 'ClickHouse not reachable yet\n'
