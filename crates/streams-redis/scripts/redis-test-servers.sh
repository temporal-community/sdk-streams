#!/usr/bin/env bash
# Starts the Redis servers of the Store tests, a standalone server with one replica and a cluster
# of three primaries with one replica each. With no local redis-server it uses docker, on a Linux
# host only, since the cluster nodes must reach each other at the addresses that they announce.
#
#   STREAMS_REDIS_URL=redis://localhost:6491/0
#   STREAMS_REDIS_CLUSTER_URL=redis://localhost:7291/0
set -euo pipefail

STANDALONE=${STREAMS_STANDALONE_PORT:-6491}
# A replica, so that the tests of `wait_replicas` get acknowledged writes.
REPLICA=${STREAMS_REPLICA_PORT:-6492}
# Six ports for the cluster nodes, so that test runs on one host can use different ones. A cluster
# with replicas lets a test tell a `WAIT` on the write's primary from one on another node.
read -r -a CLUSTER <<<"${STREAMS_CLUSTER_PORTS:-7291 7292 7293 7294 7295 7296}"
DATA=${STREAMS_REDIS_DATA:-${TMPDIR:-/tmp}/streams-redis}
# The docker image when there is no local redis-server, so CI can test an older Redis.
IMAGE=${STREAMS_REDIS_IMAGE:-redis:7}
mkdir -p "$DATA"

start() {
  local port=$1
  shift
  if redis-cli -p "$port" ping >/dev/null 2>&1; then
    return
  fi
  if command -v redis-server >/dev/null; then
    mkdir -p "$DATA/$port"
    redis-server --port "$port" --save '' --appendonly no --daemonize yes \
      --dir "$DATA/$port" --logfile "$DATA/$port.log" "$@"
  else
    docker run -d --rm --name "streams-redis-$port" --network host "$IMAGE" \
      redis-server --port "$port" --save '' --appendonly no "$@" >/dev/null
  fi
}

start "$STANDALONE"
start "$REPLICA" --replicaof 127.0.0.1 "$STANDALONE" --replica-announce-ip 127.0.0.1
for port in "${CLUSTER[@]}"; do
  start "$port" --cluster-enabled yes --cluster-config-file nodes.conf
done
for _ in $(seq 50); do
  redis-cli -p "${CLUSTER[${#CLUSTER[@]} - 1]}" ping >/dev/null 2>&1 && break
  sleep 0.1
done
if ! redis-cli -p "${CLUSTER[0]}" cluster info | grep -q cluster_state:ok; then
  nodes=()
  for port in "${CLUSTER[@]}"; do
    nodes+=("127.0.0.1:$port")
  done
  redis-cli --cluster create "${nodes[@]}" --cluster-replicas 1 --cluster-yes >/dev/null
  for _ in $(seq 50); do
    redis-cli -p "${CLUSTER[0]}" cluster info | grep -q cluster_state:ok && break
    sleep 0.1
  done
fi
for port in "$REPLICA" "${CLUSTER[@]}"; do
  for _ in $(seq 100); do
    redis-cli -p "$port" info replication |
      grep -q -e master_link_status:up -e connected_slaves:1 && break
    sleep 0.1
  done
done
echo "STREAMS_REDIS_URL=redis://localhost:$STANDALONE/0"
echo "STREAMS_REDIS_CLUSTER_URL=redis://localhost:${CLUSTER[0]}/0"
