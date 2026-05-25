#!/usr/bin/env bash
# Runs every Concuerror model-checked property and reports a summary line each.
# Exits non-zero if any property reports errors.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

PROPS=(
  singleflight_computes_once
  concurrent_drift_single_heal
  stream_demand_exactly_once
  drift_edge_single_notify
  store_rollback_one_winner
  confidence_no_lost_update
)

fail=0
for p in "${PROPS[@]}"; do
  printf '%-32s ' "$p"
  out="$(bash "$ROOT/scripts/model_check.sh" "$p" 2>&1 | grep -iE 'Summary' | tail -1)"
  echo "$out"
  echo "$out" | grep -q '0 errors' || fail=1
done

if [ "$fail" -eq 0 ]; then
  echo "ALL MODEL CHECKS PASSED"
else
  echo "SOME MODEL CHECKS FAILED" >&2
fi
exit "$fail"
