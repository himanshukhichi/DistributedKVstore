#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
PROTO="$ROOT_DIR/src/main/proto/kv.proto"
CONTAINER_PROTO="/workspace/src/main/proto/kv.proto"
SERVICE="distkv.api.KVService"
TARGET="localhost:50051"
DOCKER_TARGET="distkv-node-1:50051"
COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-deploy}"
COMPOSE_NETWORK="${COMPOSE_PROJECT_NAME}_default"

run_grpcurl() {
  local target="$TARGET"
  local proto="$PROTO"
  if command -v grpcurl >/dev/null 2>&1; then
    grpcurl -plaintext -proto "$proto" "$@" "$target"
    return
  fi

  docker run --rm \
    --network "$COMPOSE_NETWORK" \
    -v "$ROOT_DIR:/workspace:ro" \
    fullstorydev/grpcurl:latest \
    -plaintext -proto "$CONTAINER_PROTO" "$@" "$DOCKER_TARGET"
}

put_key() {
  local key="$1"
  local payload
  payload="$(printf 'value-%s' "$key" | base64)"
  run_grpcurl \
    -d "{\"key\":\"$key\",\"value\":\"$payload\",\"consistency\":\"QUORUM\"}" \
    "$SERVICE/Put" >/dev/null
}

echo "Starting QUORUM write burst against $TARGET"
for i in $(seq 1 25); do
  put_key "chaos-before-$i"
done

echo "Stopping distkv-node-3"
docker stop distkv-node-3 >/dev/null

for i in $(seq 1 25); do
  put_key "chaos-after-$i"
done

echo "Reading a key written after the node failure"
run_grpcurl \
  -d '{"key":"chaos-after-25","consistency":"QUORUM"}' \
  "$SERVICE/Get"

echo "Restarting distkv-node-3"
docker start distkv-node-3 >/dev/null

echo "Chaos test completed"
