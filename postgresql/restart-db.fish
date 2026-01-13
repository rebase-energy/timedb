#!/usr/bin/env fish
# Quick restart: recreate containers (does not remove volumes)

# Change to script directory
set -l BASEDIR (dirname (status -f))
cd $BASEDIR

printf 'Recreating Postgres containers...\n'
docker compose up -d --force-recreate --remove-orphans

docker ps
