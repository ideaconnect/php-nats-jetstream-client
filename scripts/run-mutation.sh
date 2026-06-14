#!/usr/bin/env bash
# Runs Infection mutation testing against the fast `unit` testsuite.
#
# Two modes:
#   * Full run (default): mutates all unit-covered code in src/ and enforces the strict MSI gate (default
#     90%; the suite currently scores ~93%). This is the CI quality gate. Slow (tens of minutes); the
#     ~90% gate leaves headroom for the small run-to-run variance from async mutants near the time budget.
#   * Diff run (INFECTION_DIFF_BASE set): mutates only the lines changed vs the given base ref
#     (--git-diff-lines). Fast — handy locally to check just your changes before pushing. Passes cleanly
#     when a change touches no src/ lines (--ignore-msi-with-no-mutations).
#
# Why unit-only: the `integration` suite needs a live dockerized NATS server, far too slow to re-run per
# mutant. Infection mutates only covered code by default, so integration-only paths (TLS upgrade, socket
# transport, ObjectStore download) are not mutated here and do not dilute the score. No Docker needed.
#
# Env (all overridable):
#   INFECTION_MIN_MSI          overall MSI floor (killed / total mutants). Build fails below it.
#   INFECTION_MIN_COVERED_MSI  covered-code MSI floor (killed / covered mutants). Build fails below it.
#   INFECTION_DIFF_BASE        git ref to diff against; when set, only changed lines are mutated.
#   INFECTION_THREADS          parallel workers (default: max = CPU count).
#
# Usage:
#   composer infection                                   # full baseline run
#   INFECTION_DIFF_BASE=origin/main composer infection   # strict gate on changed lines
#
# Requires a coverage driver (Xdebug or PCOV); this script forces XDEBUG_MODE=coverage.
set -uo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root_dir"

min_msi="${INFECTION_MIN_MSI:-90}"
min_covered_msi="${INFECTION_MIN_COVERED_MSI:-90}"
threads="${INFECTION_THREADS:-max}"
diff_base="${INFECTION_DIFF_BASE:-}"

args=(
  --threads="$threads"
  --test-framework-options="--testsuite=unit"
  --min-msi="$min_msi"
  --min-covered-msi="$min_covered_msi"
  --no-progress
)

if [ -n "$diff_base" ]; then
  echo "[mutation] diff mode: mutating only lines changed vs ${diff_base}"
  args+=(--git-diff-lines --git-diff-base="$diff_base" --ignore-msi-with-no-mutations)
else
  echo "[mutation] full mode: mutating all unit-covered code in src/"
fi

export XDEBUG_MODE=coverage

exec vendor/bin/infection "${args[@]}" "$@"
