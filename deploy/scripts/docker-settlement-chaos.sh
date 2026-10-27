#!/usr/bin/env sh
set -eu

seed="${1:-1}"
project="quorum-chaos-$seed"
compose_file="deploy/docker/compose.yml"
endpoints="quorum2:9090,quorum3:9090,quorum1:9090"

compose() {
  if docker compose version >/dev/null 2>&1; then
    docker compose -p "$project" -f "$compose_file" "$@"
  else
    docker-compose -p "$project" -f "$compose_file" "$@"
  fi
}

run_tool() {
  compose run --rm --no-deps tools "$@"
}

cleanup() {
  status=$?
  if [ "$status" -ne 0 ]; then
    compose logs --no-color quorum1 quorum2 quorum3 || true
  fi
  if [ "${QUORUM_KEEP_DOCKER:-0}" != "1" ]; then
    compose down -v --remove-orphans >/dev/null 2>&1 || true
  fi
  exit "$status"
}
trap cleanup EXIT INT TERM

compose up -d --build quorum1 quorum2 quorum3
compose build tools >/dev/null

ready_key="docker/ready-$seed"
ready=false
for _ in $(seq 1 90); do
  if run_tool /quorumctl -endpoints="$endpoints" -client-id="docker-ready-$seed" -seq=1 put "$ready_key" ok >/dev/null 2>&1; then
    if run_tool /quorumctl -endpoints="$endpoints" get "$ready_key" >/dev/null 2>&1; then
      ready=true
      break
    fi
  fi
  sleep 1
done

if [ "$ready" != true ]; then
  echo "cluster did not become API-ready" >&2
  exit 1
fi

echo "seed=$seed phase=settlement endpoints=$endpoints"
run_tool /scheduler-demo -endpoints="$endpoints" -timeout=5s

echo "seed=$seed phase=leader-kill target=quorum1"
compose kill quorum1 >/dev/null
sleep 15

echo "seed=$seed phase=post-kill-write"
run_tool /quorumctl -endpoints=quorum2:9090,quorum3:9090 -client-id="docker-chaos-$seed" -seq=1 put "docker/post-kill-$seed" ok
run_tool /quorumctl -endpoints=quorum2:9090,quorum3:9090 get "docker/post-kill-$seed"

echo "seed=$seed result=pass"
