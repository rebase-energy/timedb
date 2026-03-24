#!/usr/bin/env fish
# Clean restart: stop, remove volumes and local data dirs, then start

# Change to script directory
set -l BASEDIR (dirname (status -f))
cd $BASEDIR

printf 'Stopping containers and removing volumes...\n'
docker compose down -v 2>/dev/null || true

printf 'Removing local ./pgdata (if present)...\n'
if test -d ./pgdata
    sudo rm -rf ./pgdata
end

printf 'Removing local ./chdata (if present)...\n'
if test -d ./chdata
    sudo rm -rf ./chdata
end

printf 'Starting containers...\n'
docker compose up -d

printf 'Waiting for databases to be ready...\n'
sleep 10

printf '\nContainer status:\n'
docker ps | grep -E "local_postgres|local_clickhouse"

printf '\nPostgreSQL initialization status:\n'
docker logs local_postgres 2>&1 | grep -E '(ready|error|listening)' | tail -3
