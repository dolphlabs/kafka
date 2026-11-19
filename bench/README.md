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
| produce, none | 301k rec/s, 5.1 s CPU, 203 MB | 961k rec/s, 1.2 s CPU, 44 MB | 0.31x |
| produce, lz4 | 320k rec/s, 5.3 s CPU, 198 MB | 2.20M rec/s, 0.8 s CPU, 68 MB | 0.15x |
| produce, zstd | 249k rec/s, 6.7 s CPU, 257 MB | 2.28M rec/s, 0.9 s CPU, 60 MB | 0.11x |
| consume, none | 562k rec/s, 1.5 s CPU, 175 MB | 1.95M rec/s, 0.4 s CPU, 40 MB | 0.29x |
| consume, lz4 | 441k rec/s, 2.4 s CPU, 196 MB | 2.23M rec/s, 0.6 s CPU, 300 MB | 0.20x |
| consume, zstd | 363k rec/s, 2.9 s CPU, 355 MB | 3.15M rec/s, 0.5 s CPU, 294 MB | 0.12x |
| latency p50 / p99 | 1.37 / 2.21 ms, 5 MB | 1.38 / 2.79 ms, 13 MB | on par |

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

- **Fixed: slow uncompressed consuming (14k rec/s).** A fetch for
  caught-up partitions long-polled 500 ms while partitions with data
  waited behind it on the same connection. The consumer now fetches a
  broker's partitions together, one fetch in flight per broker, once the
  last response is drained (as franz-go does): 14k -> 560k rec/s.
- **Fixed in this package, open in slang: a stack overflow on large
  allocations in deep tasks.** Consuming 1M zstd records died with "a task
  overflowed its stack" (SIGBUS in the guard page) every time. The fault
  is `___chkstk_darwin` called from libsystem_malloc: macOS's malloc takes
  a path with a frame over 4 KB for large blocks, the slang runtime makes
  such allocations from a task's stack, and a task is only promised 2 KB
  below a slang call. Decompressing a fetch allocates megabytes deep in
  the fetch task. The fetch and produce tasks now grow their stacks
  first (`grow_stack` in consumer.sl): 0 crashes in 5 repeats and in
  this whole run. The real fix belongs in the slang runtime: reserve
  stack (as `sl_rt_need_stack` does for OpenSSL and SQLite) before a
  large allocation.
