#!/usr/bin/env bash
set -euo pipefail

seed="${1:-1}"
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
base="${TMPDIR:-/tmp}/quorum-process-chaos-${seed}"
log_dir="$base/logs"
bin_dir="$base/bin"
mkdir -p "$log_dir" "$bin_dir"
rm -rf "$base/node1" "$base/node2" "$base/node3"

(cd "$root" && go build -o "$bin_dir/quorumd" ./cmd/quorumd)
(cd "$root" && go build -o "$bin_dir/quorumctl" ./cmd/quorumctl)
(cd "$root" && go build -o "$bin_dir/scheduler-demo" ./cmd/scheduler-demo)

pids=()
cleanup() {
  for pid in "${pids[@]:-}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
    fi
  done
  wait || true
}
trap cleanup EXIT

start_node() {
  local id="$1" http="$2" client="$3" peer="$4" data="$5" election="$6"
  "$bin_dir/quorumd" \
    -node-id="$id" \
    -http-addr="127.0.0.1:$http" \
    -client-addr="127.0.0.1:$client" \
    -peer-addr="127.0.0.1:$peer" \
    -data-dir="$data" \
    -election-timeout="$election" \
    -heartbeat-timeout=1 \
    -peers="1=127.0.0.1:19201,2=127.0.0.1:19202,3=127.0.0.1:19203" \
    >"$log_dir/node${id}.log" 2>&1 &
  pids+=("$!")
}

wait_http() {
  local port="$1"
  for _ in $(seq 1 50); do
    if curl -fsS "http://127.0.0.1:$port/healthz" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.1
  done
  echo "node on http port $port did not become healthy" >&2
  return 1
}

start_node 1 18281 19291 19201 "$base/node1" 1
start_node 2 18282 19292 19202 "$base/node2" 50
start_node 3 18283 19293 19203 "$base/node3" 60
wait_http 18281
wait_http 18282
wait_http 18283

ready_key="chaos/ready-${seed}"
ready=false
for _ in $(seq 1 60); do
  if "$bin_dir/quorumctl" -endpoints=127.0.0.1:19292,127.0.0.1:19293,127.0.0.1:19291 -client-id "chaos-ready-${seed}" -seq 1 put "$ready_key" ok >/dev/null 2>&1 && \
     "$bin_dir/quorumctl" -endpoints=127.0.0.1:19292,127.0.0.1:19293,127.0.0.1:19291 get "$ready_key" >/dev/null 2>&1; then
    ready=true
    break
  fi
  sleep 0.2
done
if [ "$ready" != true ]; then
  echo "cluster did not become API-ready" >&2
  exit 1
fi

echo "seed=$seed phase=settlement endpoints=127.0.0.1:19292,127.0.0.1:19293,127.0.0.1:19291"
"$bin_dir/scheduler-demo" -endpoints=127.0.0.1:19292,127.0.0.1:19293,127.0.0.1:19291 -timeout=5s

echo "seed=$seed phase=leader-kill-smoke target=node1"
if kill -0 "${pids[0]}" 2>/dev/null; then
  kill "${pids[0]}"
  wait "${pids[0]}" 2>/dev/null || true
fi
sleep 13

echo "seed=$seed phase=post-kill-write"
"$bin_dir/quorumctl" -endpoints=127.0.0.1:19292,127.0.0.1:19293 -client-id chaos -seq 1 put chaos/post-kill ok
"$bin_dir/quorumctl" -endpoints=127.0.0.1:19292,127.0.0.1:19293 get chaos/post-kill

echo "seed=$seed result=pass logs=$log_dir"
