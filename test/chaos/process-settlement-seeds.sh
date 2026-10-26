#!/usr/bin/env bash
set -euo pipefail

start_seed="${1:-1}"
count="${2:-3}"
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if ! [[ "$start_seed" =~ ^[0-9]+$ ]] || ! [[ "$count" =~ ^[0-9]+$ ]] || [ "$count" -lt 1 ]; then
  echo "usage: $0 [start-seed] [count]" >&2
  exit 2
fi

passed=0
for seed in $(seq "$start_seed" "$((start_seed + count - 1))"); do
  echo "seed=$seed phase=start"
  "$script_dir/process-settlement-smoke.sh" "$seed"
  passed=$((passed + 1))
  echo "seed=$seed phase=done"
done

echo "result=pass seeds=$passed start_seed=$start_seed end_seed=$((start_seed + count - 1))"
