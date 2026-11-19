# Benchmark: slang kafka vs franz-go

`bench/run.sh` starts one Apache Kafka 4.1 broker in Docker and runs the
same three workloads with this package (`bench/slang`) and franz-go
v1.22 (`bench/go`), ABBA (slang, go, go, slang) so drift hits both:

- **produce:** 1,000,000 unkeyed 100-byte records, acks=all, idempotent,
  5 ms linger, 1 MiB batches, to 6 partitions; no compression, lz4, zstd.
- **consume:** the same records from the start, 500 per poll.
- **latency:** 2000 records one at a time (linger 0), time from send to
  the consumer receiving it.

Each run is timed by `/usr/bin/time -l` for CPU (user + sys) and peak
RSS. Raw output: `results-2026-11-19.txt` (macOS, x86-64, the broker in
Docker Desktop on the same machine).

## Results (mean of the two runs each)

| workload | slang | franz-go | slang / go |
|---|---|---|---|
| produce, none | 270k rec/s, 5.4 s CPU, 233 MB | 850k rec/s, 1.2 s CPU, 45 MB | 0.32x |
| produce, lz4 | 300k rec/s, 5.4 s CPU, 234 MB | 2.26M rec/s, 0.8 s CPU, 70 MB | 0.13x |
| produce, zstd | 240k rec/s, 6.5 s CPU, 273 MB | 2.06M rec/s, 1.0 s CPU, 65 MB | 0.12x |
| consume, none | 610k rec/s, 1.5 s CPU, 196 MB | 2.04M rec/s, 0.4 s CPU, 39 MB | 0.30x |
| consume, lz4 | 390k rec/s, 2.7 s CPU, 251 MB | 2.31M rec/s, 0.6 s CPU, 297 MB | 0.17x |
| consume, zstd | 363k rec/s, 2.9 s CPU, 377 MB | 3.11M rec/s, 0.5 s CPU, 289 MB | 0.12x (one run; see below) |
| latency p50 / p99 | 1.25 / 2.18 ms, 4 MB | 1.36 / 2.26 ms, 13 MB | on par |

## What the gaps are

- **Latency is on par**, with a third of the memory.
- **Throughput is 3x behind uncompressed and 6-8x behind compressed.**
  Two causes are measured, not guessed:
  1. Every byte copied or decompressed in slang goes through a loop that
     pays a GC poll and two bounds checks per byte; the generated C shows
     it (see the codec PR). franz-go's codecs and copies are native
     (memmove, assembly lz4/zstd). A stdlib primitive that copies a range
     into an existing `bytes` (`byteutil.copy_into`) would remove most of
     this for the codecs, the record writer and the fetch path.
  2. Per-record allocation: a `Record` and its value per send, a
     `Message`, key and value copy per consumed record; franz-go reuses
     buffers and slices fetch responses without copying.
- **RSS is 4-5x franz-go's on produce**: the 64 MiB producer buffer fills
  (send outruns the sender), and the collector paces at twice the live
  heap. A smaller `buffer_bytes` trades it for back-pressure.

## Found by this benchmark

- **Fixed:** consuming uncompressed data ran at 14k rec/s. A fetch for
  caught-up partitions long-polled 500 ms while partitions with data
  waited behind it on the same connection. The consumer now fetches a
  broker's partitions together, one fetch in flight per broker, once
  the last response is drained (as franz-go does): 14k -> 610k rec/s.
- **Open:** the second zstd consume of 1M records dies with
  "a task overflowed its stack" (SIGBUS in the guard page), every time
  on a fresh process against the same topic. The fault is in system
  code called from the fetch task (deep decode path: fetch -> batch ->
  zstd frame -> block -> sequences -> buffer growth), and the slang
  runtime grows task stacks only at slang checkpoints with a 2 KB
  margin. Diagnosis stopped when the machine began refusing to launch
  any new binary (macOS amfid/syspolicyd stalled); next step is a
  backtrace and either a slang runtime fix (margin before native calls
  in deep tasks) or growing the fetch task's stack up front.
