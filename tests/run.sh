#!/bin/sh
# The deterministic suite: the package's unit tests, then the vectors in
# tests/wire (written by franz-go through tests/wire/gen) and tests/codec
# (written by klauspost/compress, pierrec/lz4, golang/snappy and gzip
# through tests/codec/gen), and tests/protocol (every request and response
# against franz-go's kmsg, through tests/protocol/gen).
set -eu

repo=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
slangc=${SLANGC:-slangc}
run_dir=$(mktemp -d "${TMPDIR:-/tmp}/kafka-test.XXXXXX")
trap 'rm -rf "$run_dir"' EXIT HUP INT TERM

unit=$(cd "$repo" && "$slangc" test src/kafka 2>&1) || { printf '%s\n' "$unit" >&2; exit 1; }
printf '%s\n' "$unit" | grep -E '^ok: '

# Compiles tests/$1 to $run_dir/$1; the C compiler's warnings are not output.
build() {
    (cd "$run_dir" && "$slangc" "$repo/tests/$1/main.sl" -o "$run_dir/$1" > "$run_dir/$1.log" 2>&1) ||
        { cat "$run_dir/$1.log"; return 1; }
}
for t in wire codec protocol fake; do
    actual=$(build "$t" && cd "$run_dir" && KAFKA_TESTDATA="$repo/tests/$t" "./$t" 2>&1) || true
    expected=$(cat "$repo/tests/$t/expected.txt")
    if [ "$actual" != "$expected" ]; then
        printf '%s\n' "kafka tests ($t): output mismatch" >&2
        printf '%s\n' "$actual" > "$run_dir/actual.txt"
        diff "$repo/tests/$t/expected.txt" "$run_dir/actual.txt" | head -20 >&2 || true
        exit 1
    fi
    printf '%s\n' "PASS $t ($(printf '%s\n' "$expected" | wc -l | tr -d ' ') lines)"
done

# tests/cluster runs against in-process clusters (tests/kfake, franz-go's
# kfake); it needs go.
if ! command -v go >/dev/null 2>&1; then
    printf '%s\n' "SKIP cluster (no go on PATH)"
    exit 0
fi
(cd "$repo/tests/kfake" && go build -o "$run_dir/kfake" .)
mkfifo "$run_dir/hold"
"$run_dir/kfake" "$run_dir" < "$run_dir/hold" > "$run_dir/addrs" &
kfake_pid=$!
exec 3> "$run_dir/hold"
trap 'exec 3>&-; wait "$kfake_pid" 2>/dev/null; rm -rf "$run_dir"' EXIT HUP INT TERM
for i in $(seq 1 100); do
    [ -s "$run_dir/addrs" ] && break
    sleep 0.1
done
line=$(cat "$run_dir/addrs")
field() { printf '%s\n' "$line" | tr ' ' '\n' | sed -n "s/^$1=//p"; }
for t in cluster; do
    actual=$(build "$t" && cd "$run_dir" && KAFKA_PLAIN=$(field plain) KAFKA_SASL=$(field sasl) \
        KAFKA_TLS=$(field tls) KAFKA_CA=$(field ca) "./$t" 2>&1) || true
    expected=$(cat "$repo/tests/$t/expected.txt")
    if [ "$actual" != "$expected" ]; then
        printf '%s\n' "kafka tests ($t): output mismatch" >&2
        printf '%s\n' "$actual" > "$run_dir/actual.txt"
        diff "$repo/tests/$t/expected.txt" "$run_dir/actual.txt" | head -20 >&2 || true
        exit 1
    fi
    printf '%s\n' "PASS $t ($(printf '%s\n' "$expected" | wc -l | tr -d ' ') lines)"
done
