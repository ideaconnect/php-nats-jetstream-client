#!/usr/bin/env bash
# Verifies PHPUnit line coverage stays above a required threshold.
#
# Usage:
#   bash scripts/check-coverage.sh [threshold]

set -euo pipefail

threshold="${1:-90}"

if ! command -v grep >/dev/null 2>&1; then
  echo "Missing required command: grep" >&2
  exit 1
fi

if ! command -v sed >/dev/null 2>&1; then
  echo "Missing required command: sed" >&2
  exit 1
fi

if ! command -v awk >/dev/null 2>&1; then
  echo "Missing required command: awk" >&2
  exit 1
fi

if ! [[ "$threshold" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
  echo "Invalid threshold: $threshold"
  exit 2
fi

# Force a coverage driver on CLI runs.
export XDEBUG_MODE=coverage

output="$(vendor/bin/phpunit --coverage-text --only-summary-for-coverage-text --colors=never)"

echo "$output"

lines="$(printf '%s\n' "$output" | grep -E '^\s*Lines:\s*[0-9]+\.[0-9]+%')"
if [[ -z "$lines" ]]; then
  echo "Could not parse line coverage from PHPUnit output"
  exit 2
fi

percent="$(printf '%s\n' "$lines" | sed -E 's/^\s*Lines:\s*([0-9]+\.[0-9]+)%.*/\1/')"

if ! awk -v p="$percent" -v t="$threshold" 'BEGIN { exit (p+0 < t+0) ? 1 : 0 }'; then
  echo "Coverage check failed: line coverage ${percent}% is below ${threshold}%"
  exit 1
fi

echo "Coverage check passed: line coverage ${percent}% is >= ${threshold}%"
