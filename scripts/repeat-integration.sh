#!/usr/bin/env bash
# Repeats the integration suite to catch flaky behavior over multiple runs.
#
# Usage:
#   bash scripts/repeat-integration.sh [iterations]

set -euo pipefail

iterations="${1:-3}"

if ! [[ "$iterations" =~ ^[1-9][0-9]*$ ]]; then
  echo "Iteration count must be a positive integer." >&2
  exit 2
fi

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root_dir"

for ((i = 1; i <= iterations; i++)); do
  echo "[repeat-integration] iteration $i/$iterations"
  RUN_INTEGRATION=1 composer test:integration
done

echo "[repeat-integration] completed $iterations successful integration run(s)."