#!/usr/bin/env bash
set -euo pipefail

runs="${1:-5}"
start_seed="${2:-1}"
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
base="${TMPDIR:-/tmp}/quorum-process-failover-bench"
bin_dir="$base/bin"
mkdir -p "$bin_dir"

(cd "$root" && go build -o "$bin_dir/quorumd" ./cmd/quorumd)
(cd "$root" && go build -o "$bin_dir/quorumctl" ./cmd/quorumctl)

pids=()
cleanup() {
  for pid in "${pids[@]:-}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
    fi
  done
  wait 2>/dev/null || true
}
trap cleanup EXIT

start_node() {
  local id="$1" http="$2" client="$3" peer="$4" data="$5" election="$6" log="$7"
  "$bin_dir/quorumd" \
    -node-id="$id" \
    -http-addr="127.0.0.1:$http" \
    -client-addr="127.0.0.1:$client" \
    -peer-addr="127.0.0.1:$peer" \
    -data-dir="$data" \
    -election-timeout="$election" \
    -heartbeat-timeout=1 \
    -peers="1=127.0.0.1:19301,2=127.0.0.1:19302,3=127.0.0.1:19303" \
    >"$log" 2>&1 &
  pids+=("$!")
}

wait_http() {
  local port="$1"
  for _ in $(seq 1 80); do
    if curl -fsS "http://127.0.0.1:$port/healthz" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.1
  done
  echo "node on http port $port did not become healthy" >&2
  return 1
}

wait_api_ready() {
  local seed="$1"
  local endpoints="127.0.0.1:19392,127.0.0.1:19393,127.0.0.1:19391"
  local key="failover/ready-$seed"
  for _ in $(seq 1 120); do
    if "$bin_dir/quorumctl" -timeout=500ms -endpoints="$endpoints" -client-id="failover-ready-$seed" -seq=1 put "$key" ok >/dev/null 2>&1 && \
       "$bin_dir/quorumctl" -timeout=500ms -endpoints="$endpoints" get "$key" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.1
  done
  echo "cluster did not become API-ready for seed $seed" >&2
  return 1
}

run_once() {
  local seed="$1"
  local run_base="$base/run-$seed"
  local log_dir="$run_base/logs"
  rm -rf "$run_base"
  mkdir -p "$log_dir"
  pids=()

  start_node 1 18381 19391 19301 "$run_base/node1" 1 "$log_dir/node1.log"
  start_node 2 18382 19392 19302 "$run_base/node2" 50 "$log_dir/node2.log"
  start_node 3 18383 19393 19303 "$run_base/node3" 60 "$log_dir/node3.log"
  wait_http 18381
  wait_http 18382
  wait_http 18383
  wait_api_ready "$seed"

  local start_ns end_ns elapsed_ms
  start_ns="$(date +%s%N)"
  if kill -0 "${pids[0]}" 2>/dev/null; then
    kill "${pids[0]}"
    wait "${pids[0]}" 2>/dev/null || true
  fi

  local endpoints="127.0.0.1:19392,127.0.0.1:19393"
  local key="failover/post-kill-$seed"
  local seq=1
  for _ in $(seq 1 240); do
    if "$bin_dir/quorumctl" -timeout=250ms -endpoints="$endpoints" -client-id="failover-$seed" -seq="$seq" put "$key" ok >/dev/null 2>&1; then
      if "$bin_dir/quorumctl" -timeout=250ms -endpoints="$endpoints" get "$key" >/dev/null 2>&1; then
        end_ns="$(date +%s%N)"
        elapsed_ms="$(( (end_ns - start_ns) / 1000000 ))"
        echo "seed=$seed failover_write_ms=$elapsed_ms logs=$log_dir"
        cleanup
        pids=()
        return 0
      fi
    fi
    seq="$((seq + 1))"
    sleep 0.1
  done

  echo "seed=$seed failover_write_ms=timeout logs=$log_dir" >&2
  return 1
}

if ! [[ "$runs" =~ ^[0-9]+$ ]] || [ "$runs" -lt 1 ]; then
  echo "runs must be a positive integer" >&2
  exit 2
fi

measurements=()
for offset in $(seq 0 "$((runs - 1))"); do
  seed="$((start_seed + offset))"
  line="$(run_once "$seed")"
  echo "$line"
  measurements+=("$(printf '%s\n' "$line" | sed -n 's/.*failover_write_ms=\([0-9][0-9]*\).*/\1/p')")
done

printf '%s\n' "${measurements[@]}" | sort -n | awk '
  function ceil(x) { return x == int(x) ? x : int(x) + 1 }
  function clamp_index(i) { if (i < 1) return 1; if (i > NR) return NR; return i }
  { values[NR] = $1; sum += $1 }
  END {
    if (NR == 0) {
      exit 1
    }
    p50 = values[clamp_index(ceil(0.50 * NR))]
    p99 = values[clamp_index(ceil(0.99 * NR))]
    printf "summary runs=%d min_ms=%d p50_ms=%d p99_ms=%d max_ms=%d avg_ms=%.2f\n", NR, values[1], p50, p99, values[NR], sum / NR
  }
'
