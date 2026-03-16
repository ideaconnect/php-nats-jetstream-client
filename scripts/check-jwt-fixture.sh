#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if ! command -v git >/dev/null 2>&1; then
  echo "Missing required command: git" >&2
  exit 1
fi

cd "$root_dir"

bash scripts/regenerate-jwt-fixture.sh

if ! git diff --quiet -- build/nats/jwt build/nats/jwt.conf; then
  echo "JWT fixture drift detected. Regenerate and commit the updated files:" >&2
  echo "  composer fixture:jwt" >&2
  git --no-pager diff -- build/nats/jwt build/nats/jwt.conf
  exit 1
fi

echo "JWT fixture check passed: committed artifacts match script output."