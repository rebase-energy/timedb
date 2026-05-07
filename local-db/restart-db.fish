#!/usr/bin/env fish
# Quick restart: recreate container (does not remove volumes)

# Change to script directory
set -l BASEDIR (dirname (status -f))
cd $BASEDIR

printf 'Recreating container...\n'
docker compose up -d --force-recreate --remove-orphans

printf '\nContainer status:\n'
docker ps | grep -E "timedb_clickhouse"
