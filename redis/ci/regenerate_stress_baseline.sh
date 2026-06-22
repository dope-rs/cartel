#!/usr/bin/env bash
set -euo pipefail

runner="${1:-}"
mode="${2:-review}"

if [[ -z "$runner" ]]; then
  echo "usage: $0 <runner-id> [review|write]" >&2
  exit 2
fi

if [[ "$mode" != "review" && "$mode" != "write" ]]; then
  echo "mode must be one of: review, write" >&2
  exit 2
fi

baseline_path="cartel/redis/ci/stress_baseline.json"
metrics_path="/tmp/dope-redis-stress-baseline-${runner}.jsonl"

echo "running deterministic stress profile for runner=${runner}"
DOPE_REDIS_STRESS_RUNNER="$runner" \
DOPE_REDIS_STRESS_OPS="${DOPE_REDIS_STRESS_OPS:-2000}" \
DOPE_REDIS_STRESS_WORKERS="${DOPE_REDIS_STRESS_WORKERS:-4}" \
DOPE_REDIS_STRESS_METRICS_OUT="$metrics_path" \
  cargo test -p dope-redis --test stress_profile -- --nocapture

python3 cartel/redis/scripts/validate_stress_metrics.py "$metrics_path"

if [[ "$mode" == "write" ]]; then
  python3 cartel/redis/scripts/regenerate_stress_baseline.py \
    "$metrics_path" \
    "$baseline_path" \
    "$runner" \
    --write
else
  python3 cartel/redis/scripts/regenerate_stress_baseline.py \
    "$metrics_path" \
    "$baseline_path" \
    "$runner"
fi
