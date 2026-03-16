#!/usr/bin/env bash
set -euo pipefail

if ! command -v curl >/dev/null 2>&1; then
  echo "Missing required command: curl" >&2
  exit 1
fi

services=(18222 18223 18224 18225 18226 18227)
deadline_seconds="${NATS_WAIT_TIMEOUT_SECONDS:-30}"

if ! [[ "$deadline_seconds" =~ ^[1-9][0-9]*$ ]]; then
  echo "NATS_WAIT_TIMEOUT_SECONDS must be a positive integer." >&2
  exit 2
fi

wait_for_port() {
  local port="$1"
  local attempt

  for ((attempt = 1; attempt <= deadline_seconds; attempt++)); do
    if curl --silent --fail "http://127.0.0.1:${port}/varz" >/dev/null; then
      return 0
    fi

    sleep 1
  done

  echo "Timed out waiting for NATS monitoring endpoint on port ${port}." >&2
  return 1
}

for port in "${services[@]}"; do
  wait_for_port "$port"
done

echo "All NATS services are ready."