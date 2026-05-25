#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
rebar3 compile
bin/fenrir ingest examples/people.csv --to json -o examples/people.out.jsonl --sample 10
echo "--- output ---"
cat examples/people.out.jsonl
