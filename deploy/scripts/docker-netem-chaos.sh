#!/usr/bin/env sh
set -eu

seed="${1:-1}"
project="quorum-netem-$seed"
compose_file="deploy/docker/compose.yml"
netem_file="deploy/docker/compose.netem.yml"
endpoints="quorum2:9090,quorum3:9090,quorum1:9090"

compose() {
  if docker compose version >/dev/null 2>&1; then
    docker compose -p "$project" -f "$compose_file" -f "$netem_file" "$@"
  else
    docker-compose -p "$project" -f "$compose_file" -f "$netem_file" "$@"
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

apply_netem() {
  service="$1"
  delay_ms="$2"
  loss_pct="$3"
  compose exec -T "$service" sh -c "tc qdisc replace dev eth0 root netem delay ${delay_ms}ms loss ${loss_pct}%"
}

clear_netem() {
  service="$1"
  compose exec -T "$service" sh -c "tc qdisc del dev eth0 root 2>/dev/null || true"
}

compose up -d --build quorum1 quorum2 quorum3
compose build tools >/dev/null

ready_key="netem/ready-$seed"
ready=false
for _ in $(seq 1 90); do
  if run_tool /quorumctl -timeout=750ms -endpoints="$endpoints" -client-id="netem-ready-$seed" -seq=1 put "$ready_key" ok >/dev/null 2>&1; then
    if run_tool /quorumctl -timeout=750ms -endpoints="$endpoints" get "$ready_key" >/dev/null 2>&1; then
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

echo "seed=$seed phase=netem-apply target=quorum1 delay_ms=250 loss_pct=15"
apply_netem quorum1 250 15
sleep 5

echo "seed=$seed phase=netem-history endpoints=$endpoints"
run_tool /kv-history-check -timeout=3s -op-timeout=30s -endpoints="$endpoints" -key="netem/history-$seed" -seed="$seed" -rounds=4

echo "seed=$seed phase=netem-clear target=quorum1"
clear_netem quorum1
sleep 5

echo "seed=$seed phase=post-heal-write endpoints=$endpoints"
run_tool /quorumctl -timeout=2s -endpoints="$endpoints" -client-id="netem-heal-$seed" -seq=1 put "netem/heal-$seed" ok
run_tool /quorumctl -timeout=2s -endpoints="$endpoints" get "netem/heal-$seed"

echo "seed=$seed result=pass"
