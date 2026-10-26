#!/usr/bin/env sh
set -eu

docker compose -f deploy/docker/compose.yml up --build quorum1 quorum2 quorum3
