#!/bin/sh
# The deterministic suite: the package's unit tests, then the vectors in
# tests/wire (written by franz-go through tests/wire/gen).
set -eu

repo=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
slangc=${SLANGC:-slangc}
run_dir=$(mktemp -d "${TMPDIR:-/tmp}/kafka-test.XXXXXX")
trap 'rm -rf "$run_dir"' EXIT HUP INT TERM

(cd "$repo" && "$slangc" test src/kafka)
for t in wire; do
    actual=$(cd "$run_dir" && "$slangc" "$repo/tests/$t/main.sl" --run 2>&1)
    expected=$(cat "$repo/tests/$t/expected.txt")
    if [ "$actual" != "$expected" ]; then
        printf '%s\n' "kafka tests ($t): output mismatch" >&2
        printf '%s\n' "$actual" > "$run_dir/actual.txt"
        diff "$repo/tests/$t/expected.txt" "$run_dir/actual.txt" | head -20 >&2 || true
        exit 1
    fi
    printf '%s\n' "PASS $t ($(printf '%s\n' "$expected" | wc -l | tr -d ' ') lines)"
done
