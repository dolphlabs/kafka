# kafka — Apache Kafka client for slang

**Status:** PRD. No code yet.
**Repository:** `github.com/dolphlabs/kafka`
**Import name:** `kafka`
**Package path:** `src/kafka/`

## Product goal

Give slang services a Kafka client for the jobs Kafka does in a backend:
an event log that services produce to (orders, payments, audit), consumer
groups that share a topic's partitions and resume from committed offsets,
and exactly-once pipelines (consume, transform, produce in one
transaction). Applications add one Git/tag pin in `slang.project`, import
`kafka`, and get deadlines, verified TLS, SASL, bounded memory and
explicit errors as part of the contract, the same as the `pg`, `nats`,
`mongo` and `amqp` clients.

The package speaks the Kafka wire protocol that Apache Kafka 3.x and 4.x
and Redpanda implement. Kafka Streams, Kafka Connect, the Schema Registry
and ksqlDB are applications over that protocol and are not part of this
package.

## Project understanding

slang compiles a package's `.sl` files into one namespace. Network
clients are written in slang over the stdlib `net` package, whose socket
waits park the calling task instead of blocking a worker thread; `net`
has deadline-bounded TCP and verified TLS. Packages are slang source (no
C), and package-level globals must be scalar literals, so lookup tables
(CRC-32C, codec tables) live in a value the client creates once.

The closest designs already in the tree:

- **`nats`** (stdlib): one reader task per connection, and a flusher task
  that turns bursts of publishes into a few large writes. The producer's
  sender has the same job.
- **`amqp`** (`dolphlabs/amqp`): a frame reader with strict bounds, a
  deadline timer task, waiters woken by channels, a fake-broker test
  suite and a live suite in Docker.

The Kafka protocol in the parts this package needs:

- **Framing.** Every request and response is a 4-byte big-endian size and
  then the message. A request header carries the API key, API version,
  correlation id and client id; a response header carries the
  correlation id. A broker answers a connection's requests in the order
  they were sent, so one connection pipelines many requests.
- **Versions.** Each API (Produce, Fetch, Metadata, ...) has numbered
  versions; `ApiVersions` tells the client which ones a broker supports.
  Versions from Kafka 2.4 onward are "flexible": compact (unsigned
  varint) lengths and tagged fields.
- **Records.** Produce and Fetch carry record batches (message format
  v2): a header with base offset, CRC-32C, attributes (codec,
  transactional, control), producer id, epoch and base sequence, then
  records whose fields are zigzag varints. A batch may be compressed
  (gzip, snappy, lz4 or zstd). Old topics may still hold message sets
  v0/v1 (CRC-32, one message each, nested when compressed).
- **Cluster.** A client bootstraps from any broker, then `Metadata`
  names every broker and each partition's leader. Produce and fetch go to
  the leader; an error such as `NOT_LEADER_OR_FOLLOWER` means the
  metadata is stale.
- **Groups.** A consumer group has a coordinator broker (found with
  `FindCoordinator`). Members `JoinGroup`, the elected leader computes
  the assignment, `SyncGroup` distributes it, members `Heartbeat` to stay
  in, and offsets are committed to and fetched from the coordinator.
- **Idempotence and transactions.** `InitProducerId` gives a producer an
  id and epoch; per-partition sequence numbers let the broker drop
  retried duplicates. Transactions add `AddPartitionsToTxn`,
  `AddOffsetsToTxn`, `TxnOffsetCommit` and `EndTxn`, and consumers with
  `read_committed` skip aborted records.
- **Security.** TLS, and SASL PLAIN, SCRAM-SHA-256/512 and OAUTHBEARER
  over `SaslHandshake` and `SaslAuthenticate`.

Nothing in slang speaks Kafka today. The stdlib has gzip
(`compress`), SHA-256/512 and HMAC (`crypto`), but no CRC-32C, snappy,
lz4 or zstd.

## Goals

- A standalone package that applications pin and lock, with no files
  copied into the compiler repository.
- Correct encoding of every request and decoding of every response the
  package uses, checked byte for byte against Go's franz-go (`kmsg`), with
  strict bounds on every size the broker controls.
- Record batches v2 with CRC-32C, headers, control records and all four
  codecs in both directions; message sets v0/v1 decoded.
- A cluster client: bootstrap, metadata cache and refresh, a connection
  per broker and purpose, TLS, SASL (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512,
  OAUTHBEARER), version negotiation and deadlines on every blocking call.
- A producer that batches per partition (linger and batch size),
  compresses, pipelines up to five requests per broker, and is idempotent
  by default: retries never duplicate or reorder a partition's records.
- A consumer for assigned partitions and for consumer groups (range,
  round-robin and sticky assignment, static membership), with automatic
  or manual offset commits, `earliest`/`latest` reset, seeking, pausing,
  and `read_committed`.
- A transactional producer for exactly-once consume-transform-produce.
- Admin calls: create and delete topics, add partitions, describe the
  cluster and topics, list and describe groups, delete groups, list
  offsets, delete records, describe and alter configs.
- Tests against scripted fake brokers for malformed and hostile input, and
  a live suite against Apache Kafka and Redpanda in Docker.
- Throughput, latency, CPU and memory measured against Go's franz-go on
  the same machine, with the gaps recorded.

## Deliberate limits

- **The classic group protocol (eager rebalancing) in v1.** Every member
  gives up its partitions at a rebalance. The cooperative-sticky
  incremental protocol and KIP-848's `ConsumerGroupHeartbeat` (Kafka 4.0)
  are follow-ups; the eager protocol works on every supported broker.
- **No Kafka Streams, Connect or Schema Registry.** Records are bytes;
  serialization is the application's (`json`, protobuf through `grpc`).
- **No GSSAPI (Kerberos) and no AWS MSK IAM SASL.** MSK IAM needs SigV4
  request signing, which can be added later from `s3`'s signer.
- **No share groups (KIP-932)** in v1.
- **Supported brokers:** Apache Kafka 3.0 and later (4.x included) and
  Redpanda 23.x and later. Brokers older than 2.8 lack some of the API
  versions the client uses; a missing one is refused at connect with an
  error naming the API (older brokers can still produce and consume when
  only an admin or transaction API is missing, so the check is per call).

## Proposed architecture

Files in `src/kafka/`:

- `wire.sl`: a growable `Buf` with in-place writes and back-patching
  (sizes and CRCs are written after the bytes they cover), and a `Reader`
  whose first failure sticks, so a decoder reads a whole structure and
  checks once. Ints, zigzag and unsigned varints, strings, bytes, arrays,
  UUIDs and tagged fields, in both classic and compact forms.
- `crc.sl`: CRC-32C (slicing by 8) for batches and CRC-32 for message
  sets v0/v1, over tables built once per client.
- `records.sl`: `Record`, `Header`, record batch v2 encode and decode,
  control records, and message set v0/v1 decode.
- `snappy.sl`, `lz4.sl`, `zstd.sl`, `codec.sl`: the codecs in slang. Snappy
  raw blocks and the xerial framing Java producers write; the LZ4 frame
  format; zstd frames (decoder complete: raw, RLE and compressed blocks,
  FSE and Huffman; encoder with greedy matching). gzip goes through the
  stdlib.
- `protocol.sl`, `api_*.sl`: request and response codecs, one version per
  API (below), with `ApiVersions` checked at connect.
- `errors.sl`: Kafka error codes, names and which are retriable.
- `conn.sl`: one broker connection: dial, TLS, SASL, `ApiVersions`, a
  writer that pipelines requests, and a reader task that matches
  responses to requests in order. A request times out with a deadline;
  a response larger than the configured maximum closes the connection
  before it is read.
- `client.sl`: bootstrap, the metadata cache, connections per broker and
  purpose (control, produce, fetch: a long-polling fetch would otherwise
  hold up a produce on the same connection), coordinator lookup.
- `producer.sl`, `partition.sl`: partitioners (murmur2, as Java's, for
  keyed records; sticky for unkeyed), per-partition accumulators, a sender
  task per broker, idempotent sequences, retries with backoff, delivery
  results.
- `consumer.sl`, `group.sl`, `assign.sl`: positions and fetch sessions per
  broker, a bounded buffer of fetched records, offset reset, group
  membership and heartbeats, assignors, offset commits.
- `txn.sl`: the transactional producer.
- `admin.sl`: the admin calls.

API versions (the highest each supported broker accepts that the
client implements; a broker that lacks one is refused at connect):

| API | Key | Version | Kafka since |
|---|---|---|---|
| Produce | 0 | 9 | 2.8 |
| Fetch | 1 | 12 | 2.7 |
| ListOffsets | 2 | 6 | 2.8 |
| Metadata | 3 | 9 | 2.4 |
| OffsetCommit | 8 | 8 | 2.4 |
| OffsetFetch | 9 | 7 | 2.5 |
| FindCoordinator | 10 | 3 | 2.4 |
| JoinGroup | 11 | 6 | 2.4 |
| Heartbeat | 12 | 4 | 2.4 |
| LeaveGroup | 13 | 4 | 2.4 |
| SyncGroup | 14 | 4 | 2.4 |
| DescribeGroups | 15 | 5 | 2.4 |
| ListGroups | 16 | 3 | 2.4 |
| SaslHandshake | 17 | 1 | 1.0 |
| ApiVersions | 18 | 3 | 2.4 |
| CreateTopics | 19 | 5 | 2.4 |
| DeleteTopics | 20 | 4 | 2.4 |
| DeleteRecords | 21 | 2 | 2.6 |
| InitProducerId | 22 | 4 | 2.7 |
| AddPartitionsToTxn | 24 | 3 | 2.8 |
| AddOffsetsToTxn | 25 | 3 | 2.8 |
| EndTxn | 26 | 3 | 2.8 |
| TxnOffsetCommit | 28 | 3 | 2.5 |
| DescribeConfigs | 32 | 4 | 2.8 |
| SaslAuthenticate | 36 | 2 | 2.5 |
| CreatePartitions | 37 | 2 | 2.5 |
| DeleteGroups | 42 | 2 | 2.4 |
| IncrementalAlterConfigs | 44 | 1 | 2.4 |

Kafka 2.8 is the first release with every one of them (checked against
franz-go's `kversion` tables); all are inside Kafka 4.0's supported
ranges, since KIP-896 removed only older versions.

Sketch of the API:

```slang
import "kafka";

let cfg = kafka.config(["kafka-1.internal:9093", "kafka-2.internal:9093"]);
cfg.tls = true;
cfg.sasl = kafka.scram_sha512("billing", secret);
let clr = kafka.connect(cfg, deadline);                 // result[Client, str]
guard let cl = clr else let e = err_of(clr) { panic(e); }

// Producing: send queues and returns at once; wait gives the offset.
let pr = kafka.producer(cl, kafka.producer_options()); // idempotent, acks=all
guard let p = pr else let e = err_of(pr) { panic(e); }
let rec = kafka.record("orders", to_bytes(order_id), body);
kafka.add_header(rec, "content-type", to_bytes("application/json"));
let sr = kafka.send(p, rec);                            // result[Pending, str]
guard let pend = sr else let e = err_of(sr) { panic(e); }
let dr = kafka.wait(p, pend, deadline);                 // result[Produced, str]: partition, offset
kafka.flush(p, deadline);

// Consuming in a group.
let co = kafka.consumer_options("billing");
co.reset = kafka.Reset.Earliest;
let cr = kafka.subscribe(cl, ["orders"], co, deadline); // result[Consumer, str]
guard let c = cr else let e = err_of(cr) { panic(e); }
while !proc.shutdown_requested() {
    let fr = kafka.poll(c, deadline);                   // result[[Message], str]; [] at the deadline
    guard let msgs = fr else let e = err_of(fr) { panic(e); }
    for m in msgs {
        handle(m.topic, m.partition, m.offset, m.key, m.value);
    }
    kafka.commit(c, deadline);                          // what poll returned
}
kafka.close_consumer(c, deadline);                       // commits and leaves the group
kafka.close(cl);
```

## Delivery milestones

Each milestone is a branch and a PR, merged in order.

1. **Wire.** `Buf` and `Reader`, all primitive types, CRC-32C and CRC-32,
   record batches v2 and message sets v0/v1, with vectors written by Go's
   franz-go and decoded and re-encoded byte for byte. No network.
2. **Codecs.** gzip, snappy (raw and xerial), lz4 frame and zstd, both
   directions, checked against Go's klauspost/compress, golang/snappy and
   pierrec/lz4 in both directions, plus truncated and hostile input.
3. **Protocol.** Every request and response codec in the table, checked
   against franz-go's `kmsg` encodings both ways.
4. **Connection and cluster.** Dial, TLS, SASL PLAIN/SCRAM/OAUTHBEARER,
   `ApiVersions`, pipelining, request deadlines, bootstrap, metadata and
   refresh, admin calls; tested against scripted fake brokers (refused
   logins, oversized and malformed responses, a broker that goes quiet).
5. **Producer.** Partitioners, batching, compression, acks 0/1/all,
   idempotence, retries on retriable errors and leader moves, delivery
   results, flush and close, bounded buffer memory.
6. **Consumer.** Assigned partitions, fetch with a bounded buffer, offset
   reset, seek, pause/resume, `read_committed`, then consumer groups:
   coordinator, join/sync/heartbeat/leave, range, round-robin and sticky
   assignment, static membership, automatic and manual commits.
7. **Transactions.** The transactional producer and
   `send_offsets_to_transaction`; aborted records skipped by
   `read_committed` consumers.
8. **Live suite.** Apache Kafka (KRaft) and Redpanda in Docker: produce and
   consume in order across partitions, every codec through the broker,
   groups rebalancing as members join and leave, a leader moving under a
   producer, transactions committed and aborted, TLS and SASL SCRAM, and
   records both ways with franz-go.
9. **Benchmark.** Produce throughput (acks=1, acks=all, compressed),
   consume throughput, end-to-end p50/p99 latency, CPU per record and RSS,
   against franz-go, ABBA on one machine.

## Security and compatibility requirements

- TLS verifies the chain and host name by default; turning verification
  off is not offered.
- SASL over a plaintext connection is allowed (some private networks run
  it) but PLAIN without TLS is refused unless the application opts in.
- Credentials never appear in errors or logs.
- Every size the broker controls is bounded: a response's size, a record
  batch's size, a decompressed batch (by the configured fetch maximum),
  array and string lengths (by the bytes that remain), record counts and
  header counts.
- A connection that stops answering within its request deadline is
  closed and its requests fail with a timeout, never left waiting.
- The producer's buffered memory is bounded (`buffer_bytes`); `send`
  fails fast when it is full instead of growing.

## Validation and release gates

- Unit tests for every codec, including truncated and malformed input.
- Byte-for-byte vectors against franz-go for records and every API.
- Fake-broker tests for connection errors, SASL and hostile responses.
- The live suite green against Apache Kafka 3.9 and 4.1 and Redpanda.
- Benchmark results recorded against franz-go, with any gap explained.
- A clean consumer install from a Git tag pin before the first release
  tag.

## Decisions taken

1. **Pull-based consuming** (`kafka.poll(c, deadline)` returning a batch),
   as `nats` and `amqp` do, so the buffering policy stays inside the
   package. A batch rather than one record per call, because Kafka
   delivers batches and per-call overhead matters at Kafka rates.
2. **Idempotent producer by default** (acks=all, as Java since 3.0):
   retries are safe by default; `idempotent = false` for acks 0/1.
3. **One fixed API version per API** rather than a range: fewer code
   paths, every one tested. The table above is the floor and ceiling.
4. **Codecs in slang**, not linked C libraries: no system dependencies,
   and no deep C code on an 8 KB task stack.
5. **Eager group rebalancing in v1**, with cooperative and KIP-848 as
   follow-ups (above).
