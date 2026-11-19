# kafka

An Apache Kafka client for slang: producers, consumers and consumer
groups over the stdlib `net` package, with deadlines, verified TLS, SASL
and bounded memory. The plan is [KAFKA_PRD.md](KAFKA_PRD.md).

**Status:** milestones 1-3 of 9: the wire layer (primitive types,
CRC-32C, record batches v2 and message sets v0/v1), the four codecs
(gzip through the stdlib; snappy, lz4 and zstd written in slang), and the
request and response codecs for the 28 APIs the client uses, and the
connection and cluster layers: TLS, SASL (PLAIN, SCRAM-SHA-256/512,
OAUTHBEARER), API version checks, pipelined requests with deadlines,
bootstrap and metadata, and the admin calls; and the producer (batching
per partition, murmur2 and sticky partitioning, every codec, acks 0/1/all,
idempotence with retries that never duplicate or reorder); and the
consumer (assigned partitions or consumer groups with range, round-robin
and sticky assignment, static membership, automatic and manual commits,
seek, pause and resume, every reset policy, `read_committed`); and
transactions (begin, commit, abort, fencing, and exactly-once
consume-transform-produce with `send_offsets_to_transaction`).

Needs slang built from `dev` at 7a5bfe1 or later (SCRAM-SHA-512 uses
`crypto.hmac_sha512`, which the 0.2.5 release lacks).

```slang
import "kafka";

let cfg = kafka.config(["kafka-1.internal:9093", "kafka-2.internal:9093"]);
cfg.tls = true;
cfg.sasl = kafka.scram_sha512("billing", secret);
let clr = kafka.connect(cfg, kafka.after_ms(10000));
guard let cl = clr else let e = err_of(clr) { panic(e); }
kafka.create_topic(cl, "orders", 12, 3, deadline);         // result[bool, str]: false if it existed
let end = kafka.list_offset(cl, "orders", 0, kafka.OFFSET_LATEST, deadline);

let pr = kafka.producer(cl, kafka.producer_options());     // idempotent, acks=all, 5 ms linger
guard let p = pr else let e = err_of(pr) { panic(e); }
let rec = kafka.record("orders", to_bytes(order_id), body);
kafka.add_header(rec, "content-type", to_bytes("application/json"));
let sr = kafka.send(p, rec);                                 // result[Pending, str], at once
guard let pend = sr else let e = err_of(sr) { panic(e); }    // e.g. the buffer is full
let dr = kafka.wait(p, pend, deadline);                      // result[Produced, str]: partition, offset
kafka.close_producer(p, deadline);                           // flushes first

let co = kafka.consumer_options("billing");
co.reset = kafka.Reset.Earliest;
let cr = kafka.subscribe(cl, ["orders"], co, deadline);     // result[Consumer, str]
guard let c = cr else let e = err_of(cr) { panic(e); }
while !proc.shutdown_requested() {
    let mr = kafka.poll(c, kafka.after_ms(1000));            // result[[Message], str]; [] at the deadline
    guard let msgs = mr else let e = err_of(mr) { panic(e); }
    for m in msgs { handle(m.key, m.value); }
}
kafka.close_consumer(c, deadline);                           // commits, leaves the group
kafka.close(cl);
```

## Tests

    make test SLANGC=/path/to/slangc

runs the unit tests (`slangc test src/kafka`) and `tests/wire`: record
batches and message sets written by Go's franz-go (`tests/wire/gen`),
decoded and printed, and where they come from a producer encoded again
byte for byte; and varints from Go's `encoding/binary` both ways. The
expected output is written by the generator from its own specs, not by
decoding, so the decoder is checked against an independent writer.

`tests/codec` decompresses 104 vectors that Go's klauspost/compress
(zstd at four levels, with and without checksums, small windows),
pierrec/lz4 (64 KB to 4 MB blocks, block and content checksums, content
size, HC), golang/snappy (raw and xerial-framed) and gzip wrote from eight
inputs; round trips each input through the slang encoders; truncates and
corrupts every vector (624 mutations, which must fail cleanly); and checks
that every decoder stops at its size limit.

`tests/protocol` encodes 34 requests and must match franz-go's `kmsg`
byte for byte, decodes 35 responses `kmsg` encoded and must print the
values `tests/protocol/gen` set, and refuses each response cut short.

`tests/fake` runs the client against scripted brokers that misbehave: a
broker older than Kafka 2.4, one too old for Produce v9, oversized and
negative frame sizes, a wrong correlation id, silence, a connection cut
mid-response, metadata never answered, and malformed metadata.
`tests/cluster` (needs go) runs against in-process clusters from
franz-go's `kfake` (`tests/kfake`): metadata, topic creation and deletion,
partitions, offsets, configs and groups; every SASL mechanism with right
and wrong credentials; TLS against a private CA, and refused without it;
and seed fallback. `tests/producer` (needs go) produces against kfake and
reads every record back: keyed placement and per-partition order across
1000 records, sticky spreading, tombstones and null headers, all four
codecs through the broker, acks 0/1/all, three injected
NOT_LEADER_OR_FOLLOWER answers, two dropped connections and a leader move
(no duplicates, order kept), MESSAGE_TOO_LARGE, the buffer limit, unknown
topics and partitions. tests/kfake's control port injects the faults.
`tests/consumer` (needs go): 300 records in order across assigned
partitions, seek, pause and resume, a position deleted from under the
consumer, every reset policy, a group that commits and resumes exactly
there, two members splitting six partitions and sharing 600 records with
no loss or duplicates, a member leaving, and a static member rejoining.
`tests/txn` (needs go): commits and aborts as `read_committed` and
`read_uncommitted` consumers see them, an open transaction holding
`read_committed` back until it commits, a second instance fencing the
first (PRODUCER_FENCED, its records never visible), 20 records consumed,
transformed and produced with the group's offsets committed in the same
transactions, and misuse.

    make live SLANGC=/path/to/slangc

runs `tests/live` against Apache Kafka 4.1 and 3.9 (KRaft) and Redpanda
25.2 in Docker (needs docker and go): 2000 keyed records in order and on
murmur2's partitions, every codec through the broker, a 900 KB record and
a 2 MB one refused, a group of two sharing 600 records, committed and
aborted transactions under `read_committed`, 500 records each way with
franz-go (`tests/live/peer`: same partitions, headers and values), SCRAM
with right and wrong passwords (Kafka), and the broker restarted while the
producer is mid-stream: 3000 records, each exactly once, in order.

Redpanda serves Produce, Fetch, ListOffsets, Metadata, InitProducerId and
AddOffsetsToTxn only below the versions this client prefers; each
connection negotiates the classic fallback for those (Produce v7, Fetch
v11, ListOffsets v4, Metadata v8, InitProducerId v3, AddOffsetsToTxn v1),
also checked byte for byte in `tests/protocol`.

    make interop SLANGC=/path/to/slangc

decompresses the slang encoders' output with the same Go libraries (needs
go).
