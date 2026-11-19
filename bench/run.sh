#!/bin/sh
# slang vs franz-go against one Apache Kafka 4.1 broker in Docker, ABBA
# (slang, go, go, slang) per workload so drift hits both equally. Each run
# is timed with /usr/bin/time for CPU (user + sys) and peak RSS. Results
# go to stdout; bench/README.md explains them. Needs docker and go.
set -eu

repo=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
slangc=${SLANGC:-slangc}
N=${BENCH_N:-1000000}
run_dir=$(mktemp -d "${TMPDIR:-/tmp}/kafka-bench.XXXXXX")
name=slang-kafka-bench
cleanup() { docker rm -f "$name" >/dev/null 2>&1 || true; rm -rf "$run_dir"; }
trap cleanup EXIT HUP INT TERM

(cd "$run_dir" && "$slangc" "$repo/bench/slang/main.sl" -o "$run_dir/slang" > "$run_dir/build.log" 2>&1) ||
    { cat "$run_dir/build.log"; exit 1; }
(cd "$repo/bench/go" && go build -o "$run_dir/go" .)

docker rm -f "$name" >/dev/null 2>&1 || true
docker run -d --name "$name" -p 19092:19092 \
    -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
    -e KAFKA_LISTENERS=PLAINTEXT://:19092,CONTROLLER://:9093 \
    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:19092 \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 apache/kafka:4.1.0 >/dev/null
for i in $(seq 1 60); do
    docker exec "$name" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --list >/dev/null 2>&1 && break
    sleep 1
done

topic() {
    docker exec "$name" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --create --topic "$1" \
        --partitions "$2" >/dev/null
}

# one run: client, mode, topic, extra env; prints its line plus cpu and rss
one() {
    client=$1 mode=$2 t=$3
    shift 3
    /usr/bin/time -l env BENCH_BROKERS=localhost:19092 BENCH_MODE="$mode" BENCH_TOPIC="$t" BENCH_N="$N" "$@" \
        "$run_dir/$client" > "$run_dir/out" 2> "$run_dir/time"
    cpu=$(awk '/ user / {print $3 + $5}' "$run_dir/time")
    rss=$(awk '/maximum resident set size/ {print int($1 / 1048576)}' "$run_dir/time")
    printf '%s cpu_s=%s rss_mb=%s\n' "$(cat "$run_dir/out")" "$cpu" "$rss"
}

k=0
for codec in 0 3 4; do
    for client in slang go go slang; do
        k=$((k + 1))
        topic "p$k" 6
        one "$client" produce "p$k" env BENCH_SIZE=100 BENCH_CODEC=$codec | sed "s/^/codec=$codec /"
        one "$client" consume "p$k" env BENCH_SIZE=100 | sed "s/^/codec=$codec /"
    done
done
for client in slang go go slang; do
    k=$((k + 1))
    topic "l$k" 1
    N=2000 one "$client" latency "l$k" env
done
