#!/usr/bin/env bash
# Lance le model checking Concuerror sur les propriétés de concurrence de Fenrir.
# Prérequis : Concuerror buildé dans ../Concuerror (escript + ebin).
#
# Usage : scripts/model_check.sh [test_function]
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CC="${CONCUERROR_DIR:-$ROOT/../Concuerror}"
TEST="${1:-singleflight_computes_once}"

cd "$ROOT"
rebar3 as test compile >/dev/null
erlc -o _build/test/lib/fenrir/ebin +debug_info apps/fenrir/test/concuerror_tests.erl

erl -noshell \
  -pa "$CC/_build/default/lib/concuerror/ebin" \
  -pa "$CC/_build/default/lib/getopt/ebin" \
  -pa _build/test/lib/fenrir/ebin \
  -eval "R = concuerror:run([{module, concuerror_tests}, {test, $TEST}]), io:format(\"RESULT=~p~n\", [R]), case R of ok -> init:stop(0); _ -> init:stop(1) end."
