#!/usr/bin/env fish
# Clean restart: stop, remove volumes and local pgdata, then start

# Change to script directory
set -l BASEDIR (dirname (status -f))
cd $BASEDIR

printf 'Stopping containers and removing volumes...\n'
docker compose down -v

printf 'Removing local ./pgdata (if present)...\n'
if test -d ./pgdata
    rm -rf ./pgdata
end

printf 'Starting Postgres containers...\n'
docker compose up -d

docker ps
