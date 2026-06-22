#!/usr/bin/env bash
set -euo pipefail

if ! command -v basedpyright >/dev/null 2>&1; then
  echo "basedpyright is required. Install with: python3 -m pip install basedpyright" >&2
  exit 2
fi

basedpyright \
  --level warning \
  cartel/redis/scripts/validate_stress_metrics.py \
  cartel/redis/scripts/compare_stress_baseline.py \
  cartel/redis/scripts/regenerate_stress_baseline.py
