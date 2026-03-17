#!/usr/bin/env bash
# Runs Behat feature suites against the local Docker Compose-backed NATS fixtures.
#
# Stages:
#   1. Validate JWT fixture artifacts used by auth-oriented scenarios
#   2. Start Docker Compose services when needed
#   3. Wait for all NATS fixture services to become ready
#   4. Run Behat for the selected suite or the full feature set
#
# Usage:
#   bash scripts/run-tests-bdd.sh
#
# Environment:
#   BEHAT_SUITE=core          Run a specific Behat suite
#   KEEP_NATS_SERVICES=1      Leave Docker Compose services running after completion
#   SKIP_JWT_FIXTURE_CHECK=1  Skip JWT fixture validation preflight

set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
keep_services="${KEEP_NATS_SERVICES:-0}"
skip_jwt_fixture_check="${SKIP_JWT_FIXTURE_CHECK:-0}"
behat_suite="${BEHAT_SUITE:-}"
had_running_services=0

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

cleanup() {
  if [[ "$keep_services" == "1" || "$had_running_services" == "1" ]]; then
    return
  fi

  echo "[test:bdd] stopping Docker Compose services"
  docker compose down
}

require_cmd docker
require_cmd bash

cd "$root_dir"

if [[ "$skip_jwt_fixture_check" != "1" ]]; then
  echo "[test:bdd] checking JWT fixtures"
  composer fixture:jwt:check
fi

if [[ -n "$(docker compose ps -q 2>/dev/null)" ]]; then
  had_running_services=1
fi

trap cleanup EXIT

echo "[test:bdd] starting Docker Compose services"
docker compose up -d

echo "[test:bdd] waiting for NATS services"
bash scripts/wait-for-nats-services.sh

echo "[test:bdd] running Behat"
if [[ -n "$behat_suite" ]]; then
  RUN_INTEGRATION=1 vendor/bin/behat --suite "$behat_suite"
else
  RUN_INTEGRATION=1 vendor/bin/behat
fi

echo "[test:bdd] completed successfully"
