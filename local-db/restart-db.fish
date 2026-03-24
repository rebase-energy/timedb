#!/usr/bin/env fish
# Quick restart: recreate containers (does not remove volumes)

# Change to script directory
set -l BASEDIR (dirname (status -f))
cd $BASEDIR

printf 'Recreating containers...\n'
docker compose up -d --force-recreate --remove-orphans

printf '\nContainer status:\n'
docker ps | grep -E "local_postgres|local_clickhouse"
