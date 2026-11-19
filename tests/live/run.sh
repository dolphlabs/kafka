#!/bin/sh
# The client against real brokers in Docker: Apache Kafka 4.1 and 3.9
# (KRaft, one node, a PLAINTEXT and a SASL_PLAINTEXT SCRAM listener) and
# Redpanda (no SASL). For each: franz-go writes 500 records, tests/live
# runs, franz-go reads what it wrote; the output must match
# expected-<flavor>.txt. Needs docker and go.
set -eu

repo=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
slangc=${SLANGC:-slangc}
flavors=${LIVE_FLAVORS:-kafka-4.1 kafka-3.9 redpanda}
run_dir=$(mktemp -d "${TMPDIR:-/tmp}/kafka-live.XXXXXX")
name=slang-kafka-live
cleanup() { docker rm -f "$name" >/dev/null 2>&1 || true; rm -rf "$run_dir"; }
trap cleanup EXIT HUP INT TERM

(cd "$run_dir" && "$slangc" "$repo/tests/live/main.sl" -o "$run_dir/live" > "$run_dir/build.log" 2>&1) ||
    { cat "$run_dir/build.log"; exit 1; }
(cd "$repo/tests/live/peer" && go build -o "$run_dir/peer" .)

scram() {
    docker exec "$name" /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:19092 --alter \
        --add-config "$1=[password=live-secret]" --entity-type users --entity-name live >/dev/null 2>&1
}

start_kafka() {
    image=$1
    docker run -d --name "$name" -p 19092:19092 -p 19094:19094 \
        -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
        -e KAFKA_LISTENERS=PLAINTEXT://:19092,SASL://:19094,CONTROLLER://:9093 \
        -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:19092,SASL://localhost:19094 \
        -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,SASL:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT \
        -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
        -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
        -e KAFKA_SASL_ENABLED_MECHANISMS=SCRAM-SHA-512,SCRAM-SHA-256 \
        -e 'KAFKA_LISTENER_NAME_SASL_SCRAM___SHA___512_SASL_JAAS_CONFIG=org.apache.kafka.common.security.scram.ScramLoginModule required;' \
        -e 'KAFKA_LISTENER_NAME_SASL_SCRAM___SHA___256_SASL_JAAS_CONFIG=org.apache.kafka.common.security.scram.ScramLoginModule required;' \
        -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
        -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
        "$image" >/dev/null
    # the same port inside and out: the admin tool in the container
    # bootstraps on what the broker advertises
    # one mechanism per request: Kafka refuses two changes to a user at once
    for i in $(seq 1 60); do
        scram SCRAM-SHA-512 && scram SCRAM-SHA-256 && return 0
        sleep 1
    done
    echo "kafka did not start" >&2
    docker logs "$name" 2>&1 | tail -20 >&2
    return 1
}

start_redpanda() {
    docker run -d --name "$name" -p 19092:19092 redpandadata/redpanda:v25.2.1 redpanda start --mode dev-container \
        --smp 1 --kafka-addr internal://0.0.0.0:9092,external://0.0.0.0:19092 \
        --advertise-kafka-addr internal://localhost:9092,external://localhost:19092 >/dev/null
    for i in $(seq 1 90); do
        docker exec "$name" rpk cluster info >/dev/null 2>&1 && return 0
        sleep 1
    done
    echo "redpanda did not start" >&2
    return 1
}

status=0
for flavor in $flavors; do
    docker rm -f "$name" >/dev/null 2>&1 || true
    sasl=""
    case $flavor in
        kafka-4.1) start_kafka apache/kafka:4.1.0; sasl=localhost:19094 ;;
        kafka-3.9) start_kafka apache/kafka:3.9.1; sasl=localhost:19094 ;;
        redpanda) start_redpanda ;;
    esac
    case $flavor in
        redpanda) docker exec "$name" rpk topic create live-peer -p 3 >/dev/null ;;
        *) docker exec "$name" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --create \
               --topic live-peer --partitions 3 >/dev/null ;;
    esac
    "$run_dir/peer" produce localhost:19092 live-peer 500 > "$run_dir/actual" 2>&1 || true
    LIVE_BROKERS=localhost:19092 LIVE_SASL=$sasl LIVE_CONTAINER=$name "$run_dir/live" >> "$run_dir/actual" 2>&1 || true
    "$run_dir/peer" consume localhost:19092 live-out 500 >> "$run_dir/actual" 2>&1 || true
    expected="$repo/tests/live/expected-$flavor.txt"
    if [ -f "$expected" ] && cmp -s "$expected" "$run_dir/actual"; then
        printf '%s\n' "PASS live $flavor ($(wc -l < "$expected" | tr -d ' ') lines)"
    else
        printf '%s\n' "FAIL live $flavor" >&2
        diff "$expected" "$run_dir/actual" >&2 || cat "$run_dir/actual" >&2
        status=1
    fi
    docker rm -f "$name" >/dev/null 2>&1 || true
done
exit $status
