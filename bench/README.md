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
RSS. Raw output: `results-2026-11-19-copy-into.txt` (this table; slang
with dolphlabs/slang#548) and `results-2026-11-19.txt` (before it), both
macOS x86-64 with the broker in Docker Desktop on the same machine.

## Results (mean of the two runs each)

| workload | slang | franz-go | slang / go |
|---|---|---|---|
| produce, none | 276k rec/s, 5.2 s CPU, 201 MB | 1.00M rec/s, 1.2 s CPU, 45 MB | 0.27x |
| produce, lz4 | 253k rec/s, 6.4 s CPU, 205 MB | 1.92M rec/s, 1.0 s CPU, 65 MB | 0.13x |
| produce, zstd | 252k rec/s, 6.8 s CPU, 224 MB | 1.48M rec/s, 1.3 s CPU, 65 MB | 0.17x |
| consume, none | 642k rec/s, 1.4 s CPU, 157 MB | 2.16M rec/s, 0.4 s CPU, 37 MB | 0.30x |
| consume, lz4 | 413k rec/s, 2.6 s CPU, 286 MB | 2.34M rec/s, 0.7 s CPU, 303 MB | 0.18x |
| consume, zstd | 345k rec/s, 3.0 s CPU, 292 MB | 2.14M rec/s, 0.7 s CPU, 290 MB | 0.16x |
| latency p50 / p99 | 1.53 / 3.03 ms, 5 MB | 1.62 / 2.59 ms, 13 MB | on par |

The two slang runs of a row differ by up to 40% (this machine also runs
the broker), so read the ratios to one significant figure.

## What the gaps are

- **Latency is on par**, with a third of the memory.
- **Throughput is 3x behind uncompressed and 5-8x behind compressed.**
  The cause is allocation and the collector, not the codecs:
  1. A profile of the zstd consume (`sample`) is mostly the collector
     and allocator: page allocation, marking, the minor collections'
     root set, zeroing and `madvise`. The zstd and record decoding
     functions are a small share.
  2. That work comes from allocating per record: a `Record` and its
     value per send; a `Message`, key and value copy per consumed record.
     franz-go reuses buffers and slices fetch responses without copying.
  3. Not the cause: byte copies. Codecs and the buffer writer now copy
     with `strings.copy_into` (one memmove, 14x a byte loop at 100 bytes,
     over 100x at 4 KB), and throughput did not move beyond run-to-run
     noise.
- **RSS is 4-5x franz-go's on produce**: the 64 MiB producer buffer fills
  (send outruns the sender), and the collector paces at twice the live
  heap. A smaller `buffer_bytes` trades it for back-pressure.

## Found by this benchmark

- **Fixed: slow uncompressed consuming (14k rec/s).** A fetch for
  caught-up partitions long-polled 500 ms while partitions with data
  waited behind it on the same connection. The consumer now fetches a
  broker's partitions together, one fetch in flight per broker, once the
  last response is drained (as franz-go does): 14k -> 560k rec/s.
- **Fixed in slang: a stack overflow in deep tasks.** Consuming 1M zstd
  records died with "a task overflowed its stack" (SIGBUS in the guard
  page) every time. The crash report shows the path: the decoder pushes to
  a list, the collector takes a fresh 16 KB page with `aligned_alloc`, and
  libmalloc trims the slack through `free_small`, which can put a 4 KB
  array on the stack (`small_free_scan_madvise_free`). A slang task is
  only promised 2 KB below a call. Collections started at a safepoint ran
  into the same `free()` path. slang now runs the collector, and its page
  and large-object allocations, on the worker's own stack
  (dolphlabs/slang#548), and this package's `grow_stack` workaround is
  gone.
