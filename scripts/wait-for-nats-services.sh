#!/usr/bin/env bash
# Waits until all Docker Compose NATS fixture services expose healthy varz endpoints.
#
# Usage:
#   bash scripts/wait-for-nats-services.sh

set -euo pipefail

if ! command -v curl >/dev/null 2>&1; then
  echo "Missing required command: curl" >&2
  exit 1
fi

# Monitoring ports from docker-compose.yml: nats, nats-token, nats-userpass, nats-tls, nats-nkey,
# nats-jwt, nats-tls-upgrade, nats-ws, nats-hub and nats-leaf. Keep this list in step with the compose
# file: a server left out is not waited for, and the first tests or examples that use it can start
# before it is up.
services=(18222 18223 18224 18225 18226 18227 18228 18229 18230 18231)
# The leaf node (nats-leaf) is ready once its connection to the hub (nats-hub) is up, not when its own
# monitoring port answers: until then a client on it has no route to the hub's JetStream (#273).
leaf_ports=(18231)
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

    if command -v docker >/dev/null 2>&1; then
      if [[ -n "$(docker compose ps --status exited --services 2>/dev/null)" ]]; then
        echo "Detected exited NATS fixture service while waiting for port ${port}." >&2
        docker compose ps >&2 || true
        docker compose logs --no-color >&2 || true
        return 1
      fi
    fi

    sleep 1
  done

  echo "Timed out waiting for NATS monitoring endpoint on port ${port}." >&2
  if command -v docker >/dev/null 2>&1; then
    docker compose ps >&2 || true
    docker compose logs --no-color >&2 || true
  fi
  return 1
}

for port in "${services[@]}"; do
  wait_for_port "$port"
done

wait_for_leaf_connection() {
  local port="$1"
  local attempt

  for ((attempt = 1; attempt <= deadline_seconds; attempt++)); do
    local leafz
    leafz="$(curl --silent --fail "http://127.0.0.1:${port}/leafz" || true)"
    if [[ "$leafz" =~ \"leafnodes\":\ *[1-9] ]]; then
      return 0
    fi

    sleep 1
  done

  echo "Timed out waiting for the leaf node on monitoring port ${port} to connect to its hub." >&2
  if command -v docker >/dev/null 2>&1; then
    docker compose ps >&2 || true
    docker compose logs --no-color nats-hub nats-leaf >&2 || true
  fi
  return 1
}

for port in "${leaf_ports[@]}"; do
  wait_for_leaf_connection "$port"
done

echo "All NATS services are ready."