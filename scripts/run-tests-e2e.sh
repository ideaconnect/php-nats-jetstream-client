#!/usr/bin/env bash
# Runs the repository test flow end to end against local Docker Compose fixtures.
#
# Stages:
#   1. Validate JWT fixture artifacts used by JWT integration tests
#   2. Start Docker Compose services when needed
#   3. Wait for all NATS fixture services to become ready
#   4. Run unit tests
#   5. Run integration tests with RUN_INTEGRATION=1 enabled
#   6. Run Behat feature tests against the same fixture stack
#
# Usage:
#   bash scripts/run-tests-e2e.sh
#
# Environment:
#   KEEP_NATS_SERVICES=1      Leave Docker Compose services running after completion
#   SKIP_JWT_FIXTURE_CHECK=1  Skip JWT fixture validation preflight
#   SKIP_JWT_FIXTURE_CHECK=true  Also accepted; truthy values are 1/true/yes/on
#   BEHAT_SUITE=core          Run a specific Behat suite during the e2e pass

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

is_truthy() {
  case "${1,,}" in
    1|true|yes|on)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

cleanup() {
  if [[ "$keep_services" == "1" || "$had_running_services" == "1" ]]; then
    return
  fi

  echo "[test:e2e] stopping Docker Compose services"
  docker compose down
}

require_cmd docker
require_cmd bash

cd "$root_dir"

if ! is_truthy "$skip_jwt_fixture_check"; then
  echo "[test:e2e] checking JWT fixtures"
  composer fixture:jwt:check
fi

if [[ -n "$(docker compose ps -q 2>/dev/null)" ]]; then
  had_running_services=1
fi

trap cleanup EXIT

echo "[test:e2e] starting Docker Compose services"
docker compose up -d

echo "[test:e2e] waiting for NATS services"
bash scripts/wait-for-nats-services.sh

echo "[test:e2e] running unit tests"
composer test:unit

echo "[test:e2e] running integration tests"
RUN_INTEGRATION=1 composer test:integration

echo "[test:e2e] running Behat"
if [[ -n "$behat_suite" ]]; then
  RUN_INTEGRATION=1 vendor/bin/behat --suite "$behat_suite"
else
  RUN_INTEGRATION=1 vendor/bin/behat
fi

echo "[test:e2e] completed successfully"